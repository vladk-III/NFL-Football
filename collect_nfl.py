#!/usr/bin/env python3
import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import requests
import pytz
import nfl_data_py as nfl

EASTERN = pytz.timezone("US/Eastern")
ODDS_BASE = "https://api.the-odds-api.com/v4"
ODDS_SPORT = "americanfootball_nfl"
DATA_DIR = Path("data")
MARKER_DIR = Path("data/.markers")

LOG = logging.getLogger("nfl_collector")

def odds_get(endpoint: str, api_key: str, params: dict | None = None) -> list | dict | None:
    url = f"{ODDS_BASE}{endpoint}"
    base_params = {"apiKey": api_key}
    base_params.update(params or {})
    for attempt in range(3):
        try:
            r = requests.get(url, params=base_params, timeout=30)
            if r.status_code == 429:
                time.sleep(2 ** (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            LOG.warning(f"Odds API attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)
    return None

def append_or_create_csv(df: pd.DataFrame, path: Path, dedup_cols: list[str] | None = None):
    if path.exists():
        existing = pd.read_csv(path)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df
    if dedup_cols:
        combined = combined.drop_duplicates(subset=dedup_cols, keep="last")
    combined.to_csv(path, index=False)
    return len(df)

def collect_games(year: int) -> pd.DataFrame:
    LOG.info(f"Fetching NFL schedule for year={year}")
    sched = nfl.import_schedules([year])
    rows = []
    for _, g in sched.iterrows():
        rows.append({
            "game_id": g.get("game_id"),
            "season": g.get("season"),
            "week": g.get("week"),
            "game_type": g.get("game_type"),
            "start_date": g.get("gameday"),
            "home_team": g.get("home_team"),
            "home_points": g.get("home_score"),
            "away_team": g.get("away_team"),
            "away_points": g.get("away_score"),
            "stadium": g.get("stadium"),
            "roof": g.get("roof"),
            "completed": not pd.isna(g.get("home_score"))
        })
    return pd.DataFrame(rows)

def collect_odds_api(api_key: str, hours: int = 48) -> pd.DataFrame:
    if not api_key:
        LOG.warning("No ODDS_API_KEY found, skipping live odds collection.")
        return pd.DataFrame()
    now = datetime.now(pytz.UTC)
    cutoff = now + timedelta(hours=hours)
    data = odds_get(f"/sports/{ODDS_SPORT}/odds", api_key, {
        "regions": "us",
        "markets": "h2h,spreads,totals",
        "oddsFormat": "american",
        "commenceTimeFrom": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "commenceTimeTo": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    
    if not data:
        return pd.DataFrame()
    rows = []
    ts = datetime.now(EASTERN).isoformat()
    for event in data:
        home = event.get("home_team", "")
        away = event.get("away_team", "")
        for book in event.get("bookmakers", []):
            book_key = book.get("key", "")
            row = {
                "snapshot_ts": ts,
                "game_id": event.get("id", ""),
                "home_team": home,
                "away_team": away,
                "provider": book_key,
                "spread": None, "spread_price": None,
                "over_under": None, "ou_price": None,
                "home_ml": None, "away_ml": None,
            }
            for market in book.get("markets", []):
                mkey = market.get("key")
                outcomes = market.get("outcomes", [])
                if mkey == "h2h":
                    for o in outcomes:
                        if o.get("name") == home: row["home_ml"] = o.get("price")
                        elif o.get("name") == away: row["away_ml"] = o.get("price")
                elif mkey == "spreads":
                    for o in outcomes:
                        if o.get("name") == home: row["spread"] = o.get("point"); row["spread_price"] = o.get("price")
                elif mkey == "totals":
                    for o in outcomes:
                        if o.get("name") == "Over": row["over_under"] = o.get("point"); row["ou_price"] = o.get("price")
            rows.append(row)
    return pd.DataFrame(rows)

def run_pregame(year: int, week: int, odds_key: str) -> dict:
    DATA_DIR.mkdir(exist_ok=True)
    stats = {"type": "pregame", "year": year, "week": week}
    games_df = collect_games(year)
    if not games_df.empty:
        stats["games"] = append_or_create_csv(games_df, DATA_DIR / "games.csv", ["game_id"])
    odds_api = collect_odds_api(odds_key, hours=48)
    if not odds_api.empty:
        stats["odds_rows"] = append_or_create_csv(odds_api, DATA_DIR / "odds_snapshots.csv")
    return stats

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["auto", "pregame", "postgame"], default="auto")
    parser.add_argument("--week", type=int)
    parser.add_argument("--year", type=int)
    args = parser.parseargs()
    
    odds_api_key = os.environ.get("ODDS_API_KEY", "")
    now_et = datetime.now(EASTERN)
    year = args.year or (now_et.year if now_et.month >= 8 else now_et.year - 1)
    
    # Bug Fix 3: Monday is 0, Tuesday is 1, Friday is 4
    mode = args.mode if args.mode != "auto" else ("postgame" if now_et.weekday() in (0, 1, 4) else "pregame")
    week = args.week or 1
    
    if mode == "pregame":
        stats = run_pregame(year, week, odds_api_key)
    else:
        stats = {"type": "postgame"} 
        
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            # Bug Fix 1: Removed double backslashes that were flattening the GitHub Actions output
            f.write(f"mode={mode}\nyear={year}\nweek={week}\nstats={json.dumps(stats)}\ncollected=true\n")

if __name__ == "__main__":
    main()
