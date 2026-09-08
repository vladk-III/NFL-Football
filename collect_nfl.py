#!/usr/bin/env python3
"""
NFL Football Betting Data Collector
===================================
State-aware, automated pipeline for collecting schedules, multi-book odds,
and post-game outcomes.

Data sources:
  1. nfl_data_py (nflverse) — schedules, game outcomes
  2. The Odds API — live multi-sportsbook odds
"""

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

LOG = logging.getLogger("nfl_collector")


def odds_get(endpoint: str, api_key: str, params: dict | None = None) -> list | dict | None:
    """GET request to The Odds API with backoff retries."""
    url = f"{ODDS_BASE}{endpoint}"
    base_params = {"apiKey": api_key}
    base_params.update(params or {})
    for attempt in range(3):
        try:
            r = requests.get(url, params=base_params, timeout=30)
            if r.status_code == 429:
                wait = 2 ** (attempt + 1)
                LOG.warning(f"Odds API rate limited, retrying in {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            LOG.warning(f"Odds API attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)
    LOG.error(f"Odds API endpoint {endpoint} failed after 3 attempts.")
    return None


def append_or_create_csv(df: pd.DataFrame, path: Path, dedup_cols: list[str] | None = None) -> int:
    """Append new data to existing CSV or write fresh, deduping on key columns if requested."""
    if path.exists():
        existing = pd.read_csv(path)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df
    if dedup_cols:
        combined = combined.drop_duplicates(subset=dedup_cols, keep="last")
    combined.to_csv(path, index=False)
    LOG.info(f"Saved {path.name}: {len(combined)} total rows ({len(df)} new)")
    return len(df)


def collect_games(year: int) -> pd.DataFrame:
    """Fetch NFL schedule and official game scores using nfl_data_py."""
    LOG.info(f"Fetching NFL schedule via nflverse for season {year}")
    try:
        sched = nfl.import_schedules([year])
    except Exception as e:
        LOG.error(f"Failed to fetch schedule for year {year}: {e}")
        return pd.DataFrame()

    rows = []
    for _, g in sched.iterrows():
        # Parse kickoff time
        gameday_str = str(g.get("gameday", ""))
        gametime_str = str(g.get("gametime", "")) if pd.notna(g.get("gametime")) else "13:00"
        
        try:
            # Construct ISO timestamp for kickoff calculation
            dt_str = f"{gameday_str}T{gametime_str}:00"
            start_dt = EASTERN.localize(datetime.fromisoformat(dt_str)).astimezone(pytz.UTC)
            start_iso = start_dt.isoformat()
        except Exception:
            start_iso = gameday_str

        rows.append({
            "game_id": g.get("game_id"),
            "season": g.get("season"),
            "week": g.get("week"),
            "game_type": g.get("game_type"),
            "start_date": start_iso,
            "home_team": g.get("home_team"),
            "home_points": g.get("home_score"),
            "away_team": g.get("away_team"),
            "away_points": g.get("away_score"),
            "stadium": g.get("stadium"),
            "roof": g.get("roof"),
            "completed": not pd.isna(g.get("home_score")),
        })
    return pd.DataFrame(rows)


def collect_odds_api(api_key: str, lookahead_hours: int = 72) -> pd.DataFrame:
    """Fetch live multi-bookmaker odds for upcoming NFL games."""
    if not api_key:
        LOG.warning("ODDS_API_KEY is missing. Skipping live odds collection.")
        return pd.DataFrame()

    now = datetime.now(pytz.UTC)
    cutoff = now + timedelta(hours=lookahead_hours)

    LOG.info(f"Querying live NFL odds between {now.isoformat()} and {cutoff.isoformat()}")
    data = odds_get(f"/sports/{ODDS_SPORT}/odds", api_key, {
        "regions": "us",
        "markets": "h2h,spreads,totals",
        "oddsFormat": "american",
        "commenceTimeFrom": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "commenceTimeTo": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
    })

    if not data:
        LOG.info("No odds data returned for the specified window.")
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


def compute_outcomes(games_df: pd.DataFrame, odds_path: Path) -> pd.DataFrame:
    """Compute closing line ATS/Total results for completed games."""
    completed = games_df[games_df["completed"] == True].copy()
    if completed.empty or not odds_path.exists():
        return pd.DataFrame()

    odds_df = pd.read_csv(odds_path)
    rows = []

    for _, g in completed.iterrows():
        gid = str(g["game_id"])
        h_pts, a_pts = int(g["home_points"]), int(g["away_points"])
        total = h_pts + a_pts
        margin = h_pts - a_pts

        game_odds = odds_df[odds_df["game_id"].astype(str) == gid]
        if game_odds.empty:
            continue

        last_odds = game_odds.sort_values("snapshot_ts").groupby("provider").last()
        for provider, lo in last_odds.iterrows():
            spread = lo.get("spread")
            ou = lo.get("over_under")

            ats_res = None
            if pd.notna(spread):
                spread_val = float(spread)
                if margin + spread_val > 0: ats_res = "home_cover"
                elif margin + spread_val < 0: ats_res = "away_cover"
                else: ats_res = "push"

            ou_res = None
            if pd.notna(ou):
                ou_val = float(ou)
                if total > ou_val: ou_res = "over"
                elif total < ou_val: ou_res = "under"
                else: ou_res = "push"

            rows.append({
                "game_id": gid,
                "home_team": g["home_team"],
                "away_team": g["away_team"],
                "home_points": h_pts,
                "away_points": a_pts,
                "total_points": total,
                "margin": margin,
                "provider": provider,
                "closing_spread": spread,
                "ats_result": ats_res,
                "closing_ou": ou,
                "ou_result": ou_res,
            })
    return pd.DataFrame(rows)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="NFL Data Collector")
    parser.add_argument("--mode", choices=["auto", "pregame", "postgame"], default="auto")
    parser.add_argument("--week", type=int, help="Override NFL week")
    parser.add_argument("--year", type=int, help="Override season year")
    
    # FIXED TYPO HERE: parse_args() instead of parseargs()
    args = parser.parse_args()

    odds_api_key = os.environ.get("ODDS_API_KEY", "")
    now_utc = datetime.now(pytz.UTC)
    now_et = now_utc.astimezone(EASTERN)

    year = args.year or (now_et.year if now_et.month >= 8 else now_et.year - 1)
    
    DATA_DIR.mkdir(exist_ok=True)
    stats = {"year": year, "mode": args.mode}

    # 1. Always update base schedule & game completion status
    games_df = collect_games(year)
    if not games_df.empty:
        stats["games_total"] = append_or_create_csv(games_df, DATA_DIR / "games.csv", ["game_id"])

    # Determine dynamic active week if not supplied
    if args.week:
        active_week = args.week
    elif not games_df.empty:
        # Default to the minimum incomplete week, or max week if all complete
        incomplete = games_df[games_df["completed"] == False]
        active_week = int(incomplete["week"].min()) if not incomplete.empty else int(games_df["week"].max())
    else:
        active_week = 1
    
    stats["week"] = active_week

    # 2. Dynamic Execution Model
    if args.mode in ("auto", "pregame"):
        # Fetch odds for upcoming games within lookahead window
        odds_df = collect_odds_api(odds_api_key, lookahead_hours=72)
        if not odds_df.empty:
            stats["odds_snapshots"] = append_or_create_csv(odds_df, DATA_DIR / "odds_snapshots.csv")

    if args.mode in ("auto", "postgame"):
        # Evaluate postgame outcomes for completed games
        outcomes_df = compute_outcomes(games_df, DATA_DIR / "odds_snapshots.csv")
        if not outcomes_df.empty:
            stats["outcomes_updated"] = append_or_create_csv(outcomes_df, DATA_DIR / "outcomes.csv", ["game_id", "provider"])

    # 3. Export outputs cleanly for GitHub Actions
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"mode={args.mode}\n")
            f.write(f"year={year}\n")
            f.write(f"week={active_week}\n")
            f.write(f"stats={json.dumps(stats)}\n")
            f.write("collected=true\n")

    LOG.info(f"Execution finished. Run stats: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
