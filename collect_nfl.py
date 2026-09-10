#!/usr/bin/env python3
"""
NFL Football Betting Data Collector
===================================
State-aware, automated pipeline for collecting schedules, multi-book odds,
weather, post-game outcomes, and play-by-play scoring/advanced metrics.
"""

import os
import sys
import json
import time
import logging
import argparse
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import pytz
import nfl_data_py as nfl

EASTERN = pytz.timezone("US/Eastern")
ODDS_BASE = "https://api.the-odds-api.com/v4"
ODDS_SPORT = "americanfootball_nfl"
DATA_DIR = Path("data")
CACHE_DIR = DATA_DIR / ".cache"
PBP_CACHE_MAX_AGE_HOURS = 6  # re-download at most ~4x/day, matching the collection schedule

LOG = logging.getLogger("nfl_collector")

# Static Mapping of 30 Current Primary NFL Venues
NFL_VENUES = {
    "State Farm Stadium": {"lat": 33.5276, "lon": -112.2626, "type": "retractable"},
    "Mercedes-Benz Stadium": {"lat": 33.7554, "lon": -84.4006, "type": "retractable"},
    "M&T Bank Stadium": {"lat": 39.2780, "lon": -76.6227, "type": "outdoors"},
    "Highmark Stadium": {"lat": 42.7738, "lon": -78.7870, "type": "outdoors"},
    "Bank of America Stadium": {"lat": 35.2258, "lon": -80.8528, "type": "outdoors"},
    "Soldier Field": {"lat": 41.8623, "lon": -87.6167, "type": "outdoors"},
    "Paycor Stadium": {"lat": 39.0955, "lon": -84.5161, "type": "outdoors"},
    "Cleveland Browns Stadium": {"lat": 41.5061, "lon": -81.6995, "type": "outdoors"},
    "AT&T Stadium": {"lat": 32.7473, "lon": -97.0945, "type": "retractable"},
    "Empower Field at Mile High": {"lat": 39.7439, "lon": -105.0201, "type": "outdoors"},
    "Ford Field": {"lat": 42.3400, "lon": -83.0456, "type": "dome"},
    "Lambeau Field": {"lat": 44.5013, "lon": -88.0622, "type": "outdoors"},
    "NRG Stadium": {"lat": 29.6847, "lon": -95.4107, "type": "retractable"},
    "Lucas Oil Stadium": {"lat": 39.7601, "lon": -86.1639, "type": "retractable"},
    "EverBank Stadium": {"lat": 30.3239, "lon": -81.6373, "type": "outdoors"},
    "GEHA Field at Arrowhead Stadium": {"lat": 39.0489, "lon": -94.4839, "type": "outdoors"},
    "Allegiant Stadium": {"lat": 36.0909, "lon": -115.1833, "type": "dome"},
    "SoFi Stadium": {"lat": 33.9535, "lon": -118.3390, "type": "dome"},
    "Hard Rock Stadium": {"lat": 25.9580, "lon": -80.2389, "type": "outdoors"},
    "U.S. Bank Stadium": {"lat": 44.9735, "lon": -93.2575, "type": "dome"},
    "Gillette Stadium": {"lat": 42.0909, "lon": -71.2643, "type": "outdoors"},
    "Caesars Superdome": {"lat": 29.9511, "lon": -90.0814, "type": "dome"},
    "MetLife Stadium": {"lat": 40.8135, "lon": -74.0745, "type": "outdoors"},
    "Lincoln Financial Field": {"lat": 39.9008, "lon": -75.1675, "type": "outdoors"},
    "Acrisure Stadium": {"lat": 40.4468, "lon": -80.0158, "type": "outdoors"},
    "Levi's Stadium": {"lat": 37.4032, "lon": -121.9697, "type": "outdoors"},
    "Lumen Field": {"lat": 47.5952, "lon": -122.3316, "type": "outdoors"},
    "Raymond James Stadium": {"lat": 27.9759, "lon": -82.5033, "type": "outdoors"},
    "Nissan Stadium": {"lat": 36.1665, "lon": -86.7713, "type": "outdoors"},
    "Northwest Stadium": {"lat": 38.9076, "lon": -76.8645, "type": "outdoors"}
}

def odds_get(endpoint: str, api_key: str, params: dict | None = None) -> list | dict | None:
    url = f"{ODDS_BASE}{endpoint}"
    base_params = {"apiKey": api_key}
    base_params.update(params or {})
    for attempt in range(3):
        try:
            r = requests.get(url, params=base_params, timeout=30)
            if r.status_code == 429:
                wait = 2 ** (attempt + 1)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            LOG.warning(f"Odds API attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)
    return None

def append_or_create_csv(df: pd.DataFrame, path: Path, dedup_cols: list[str] | None = None) -> int:
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
    LOG.info(f"Fetching NFL schedule via nflverse for season {year}")
    try:
        sched = nfl.import_schedules([year])
    except Exception as e:
        LOG.error(f"Failed to fetch schedule for year {year}: {e}")
        return pd.DataFrame()

    rows = []
    for _, g in sched.iterrows():
        gameday_str = str(g.get("gameday", ""))
        gametime_str = str(g.get("gametime", "")) if pd.notna(g.get("gametime")) else "13:00"
        try:
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
            "completed": not pd.isna(g.get("home_score")),
        })
    return pd.DataFrame(rows)

def _load_pbp_cached(year: int) -> pd.DataFrame | None:
    """Return a recent cached PBP pull if one exists and isn't stale, else None."""
    cache_path = CACHE_DIR / f"pbp_{year}.parquet"
    if not cache_path.exists():
        return None
    age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
    if age_hours > PBP_CACHE_MAX_AGE_HOURS:
        return None
    try:
        LOG.info(f"Using cached PBP data for {year} ({age_hours:.1f}h old)")
        return pd.read_parquet(cache_path)
    except Exception as e:
        LOG.warning(f"Failed to read PBP cache, will re-fetch: {e}")
        return None

def collect_pbp_features(year: int) -> pd.DataFrame:
    """
    Pulls play-by-play data via nfl_data_py to extract exact scoring events 
    (FGs, TDs, PATs, 2-pt conversions, safeties) and advanced covariates (EPA, Success Rate).

    The raw PBP pull is cached on disk for PBP_CACHE_MAX_AGE_HOURS, since a full
    season's play-by-play is a heavy download and this job runs multiple times a day.
    """
    pbp = _load_pbp_cached(year)
    if pbp is None:
        LOG.info(f"Fetching play-by-play data for {year} via nflverse")
        try:
            pbp = nfl.import_pbp([year])
        except Exception as e:
            LOG.error(f"Failed to fetch pbp for year {year}: {e}")
            return pd.DataFrame()

        if not pbp.empty:
            try:
                CACHE_DIR.mkdir(parents=True, exist_ok=True)
                pbp.to_parquet(CACHE_DIR / f"pbp_{year}.parquet", index=False)
            except Exception as e:
                LOG.warning(f"Failed to write PBP cache (non-fatal): {e}")

    if pbp.empty:
        return pd.DataFrame()

    # Isolate scoring and event flags
    pbp["fg_made"] = np.where(pbp["field_goal_result"] == "made", 1, 0)
    pbp["pat_made"] = np.where(pbp["extra_point_result"] == "good", 1, 0)
    pbp["two_pt_made"] = np.where(pbp["two_point_conv_result"] == "success", 1, 0)
    pbp["safety_made"] = np.where(pbp["safety"] == 1, 1, 0)
    pbp["td_made"] = np.where(pbp["touchdown"] == 1, 1, 0)

    # Offensive team scoring elements
    off_scoring = pbp.groupby(["game_id", "posteam"])[["fg_made", "pat_made", "two_pt_made"]].sum().reset_index()
    off_scoring.rename(columns={"posteam": "team"}, inplace=True)

    # Touchdowns mapped to td_team to capture pick-sixes and defensive/special teams scores accurately
    td_scoring = pbp[pbp["td_made"] == 1].groupby(["game_id", "td_team"])["td_made"].sum().reset_index()
    td_scoring.rename(columns={"td_team": "team", "td_made": "td_count"}, inplace=True)

    # Safeties awarded to defteam
    safety_scoring = pbp[pbp["safety_made"] == 1].groupby(["game_id", "defteam"])["safety_made"].sum().reset_index()
    safety_scoring.rename(columns={"defteam": "team", "safety_made": "safety_count"}, inplace=True)

    # Advanced Efficiency Metrics (EPA and Success Rate)
    valid_plays = pbp[(pbp["play_type"].isin(["pass", "run"])) & (pbp["epa"].notna())].copy()
    valid_plays["success"] = np.where(valid_plays["epa"] > 0, 1, 0)

    off_adv = valid_plays.groupby(["game_id", "posteam"])[["epa", "success"]].mean().reset_index()
    off_adv.rename(columns={"posteam": "team", "epa": "off_epa_per_play", "success": "off_success_rate"}, inplace=True)

    def_adv = valid_plays.groupby(["game_id", "defteam"])[["epa", "success"]].mean().reset_index()
    def_adv.rename(columns={"defteam": "team", "epa": "def_epa_per_play", "success": "def_success_rate"}, inplace=True)

    # Merge all metrics together per game per team
    features = off_scoring.merge(td_scoring, on=["game_id", "team"], how="outer")
    features = features.merge(safety_scoring, on=["game_id", "team"], how="outer")
    features = features.merge(off_adv, on=["game_id", "team"], how="outer")
    features = features.merge(def_adv, on=["game_id", "team"], how="outer")

    return features.fillna(0)

def collect_odds_api(api_key: str, lookahead_hours: int = 48) -> pd.DataFrame:
    if not api_key:
        return pd.DataFrame()
    now = datetime.now(pytz.UTC)
    cutoff = now + timedelta(hours=lookahead_hours)
    data = odds_get(f"/sports/{ODDS_SPORT}/odds", api_key, {
        "regions": "us", "markets": "h2h,spreads,totals",
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
                "snapshot_ts": ts, "game_id": event.get("id", ""),
                "home_team": home, "away_team": away, "provider": book_key,
                "spread": None, "spread_price": None, "over_under": None, "ou_price": None,
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

def collect_weather(games_df: pd.DataFrame) -> pd.DataFrame:
    if games_df.empty:
        return pd.DataFrame()

    rows = []
    outdoor_games = []
    now_utc = datetime.now(pytz.UTC)

    for _, g in games_df.iterrows():
        venue_name = str(g.get("stadium", ""))
        venue_info = NFL_VENUES.get(venue_name)
        start_raw = g.get("start_date")

        try:
            game_dt = datetime.fromisoformat(str(start_raw).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue

        if venue_info and venue_info.get("type") in ("dome", "retractable"):
            rows.append({
                "game_id": g.get("game_id"), "stadium": venue_name,
                "temperature": 72.0, "precipitation": 0.0, "wind_speed": 0.0,
                "roof_type": venue_info.get("type"), "is_indoor": True,
            })
            continue

        days_diff = (game_dt - now_utc).days
        if days_diff > 14 or days_diff < -80:
            continue

        if not venue_info:
            continue

        outdoor_games.append((g, game_dt, venue_info))

    if not outdoor_games:
        return pd.DataFrame(rows)

    games_by_date = defaultdict(list)
    for g, game_dt, venue_info in outdoor_games:
        date_str = game_dt.strftime("%Y-%m-%d")
        games_by_date[date_str].append((g, game_dt, venue_info))

    with requests.Session() as session:
        for date_str, daily_games in games_by_date.items():
            lats = ",".join(str(round(v["lat"], 4)) for _, _, v in daily_games)
            lons = ",".join(str(round(v["lon"], 4)) for _, _, v in daily_games)

            params = {
                "latitude": lats, "longitude": lons,
                "start_date": date_str, "end_date": date_str,
                "hourly": "temperature_2m,precipitation,windspeed_10m",
                "temperature_unit": "fahrenheit", "windspeed_unit": "mph", "precipitation_unit": "inch",
                "timezone": "UTC",
            }

            data = None
            for attempt in range(3):
                try:
                    r = session.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=20)
                    if r.status_code == 429:
                        time.sleep(2 ** (attempt + 2))
                        continue
                    data = r.json()
                    break
                except requests.RequestException:
                    time.sleep(2 ** attempt)

            if data is None:
                continue

            results = data if isinstance(data, list) else [data]
            if len(results) != len(daily_games):
                continue

            for (g, game_dt, v_info), loc_data in zip(daily_games, results):
                hourly = loc_data.get("hourly", {})
                times = hourly.get("time", [])
                if not times: continue

                target = game_dt.replace(minute=0, second=0, microsecond=0)
                target_str = target.strftime("%Y-%m-%dT%H:00")
                idx = times.index(target_str) if target_str in times else 0

                def _at(key):
                    vals = hourly.get(key, [])
                    return vals[idx] if idx < len(vals) else None

                rows.append({
                    "game_id": g.get("game_id"), "stadium": g.get("stadium"),
                    "temperature": _at("temperature_2m"),
                    "precipitation": _at("precipitation"),
                    "wind_speed": _at("windspeed_10m"),
                    "roof_type": v_info["type"],
                    "is_indoor": False,
                })
            time.sleep(0.5)

    return pd.DataFrame(rows)

def compute_outcomes(games_df: pd.DataFrame, odds_path: Path) -> pd.DataFrame:
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
                "game_id": gid, "provider": provider,
                "closing_spread": spread, "ats_result": ats_res,
                "closing_ou": ou, "ou_result": ou_res,
            })
    return pd.DataFrame(rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["auto", "pregame", "postgame"], default="auto")
    parser.add_argument("--year", type=int)
    parser.add_argument("--week", type=int)
    args = parser.parse_args()

    odds_api_key = os.environ.get("ODDS_API_KEY", "")
    now_utc = datetime.now(pytz.UTC)
    now_et = now_utc.astimezone(EASTERN)
    year = args.year or (now_et.year if now_et.month >= 8 else now_et.year - 1)

    DATA_DIR.mkdir(exist_ok=True)
    stats = {"year": year, "mode": args.mode}
    if args.week is not None:
        stats["week"] = args.week

    games_df = collect_games(year)
    if not games_df.empty:
        stats["games_total"] = append_or_create_csv(games_df, DATA_DIR / "games.csv", ["game_id"])

    scoped_games_df = games_df
    if args.week is not None and not games_df.empty and "week" in games_df.columns:
        scoped_games_df = games_df[games_df["week"] == args.week]
        if scoped_games_df.empty:
            LOG.warning(f"No games found for week {args.week} in season {year}; falling back to full schedule.")
            scoped_games_df = games_df

    upcoming_games = scoped_games_df[
        scoped_games_df["start_date"] > (now_utc - timedelta(hours=6)).isoformat()
    ]
    if not upcoming_games.empty:
        weather_df = collect_weather(upcoming_games)
        if not weather_df.empty:
            stats["weather"] = append_or_create_csv(weather_df, DATA_DIR / "weather.csv", ["game_id"])

    if args.mode in ("auto", "pregame"):
        odds_df = collect_odds_api(odds_api_key, lookahead_hours=48)
        if not odds_df.empty:
            stats["odds_snapshots"] = append_or_create_csv(odds_df, DATA_DIR / "odds_snapshots.csv")

    if args.mode in ("auto", "postgame"):
        outcomes_df = compute_outcomes(scoped_games_df, DATA_DIR / "odds_snapshots.csv")
        if not outcomes_df.empty:
            stats["outcomes_updated"] = append_or_create_csv(outcomes_df, DATA_DIR / "outcomes.csv", ["game_id", "provider"])

        # Fetch play-by-play metrics for completed games and append to team_game_stats.csv
        pbp_df = collect_pbp_features(year)
        if not pbp_df.empty:
            completed_game_ids = scoped_games_df[scoped_games_df["completed"] == True]["game_id"].unique()
            pbp_df = pbp_df[pbp_df["game_id"].isin(completed_game_ids)]
            if not pbp_df.empty:
                stats["pbp_features"] = append_or_create_csv(pbp_df, DATA_DIR / "team_game_stats.csv", ["game_id", "team"])

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"mode={args.mode}\nstats={json.dumps(stats)}\ncollected=true\n")

if __name__ == "__main__":
    main()
