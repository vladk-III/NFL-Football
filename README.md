# NFL Football Betting Data Collector

Automated pipeline that collects game schedules, multi-sportsbook betting odds, weather, and post-game outcomes for the NFL — committed daily as CSVs to this repo via GitHub Actions.

## Architecture

Mirrors the CFB data collector pattern: idempotent, season-aware, with email alerts, but optimized for the NFL schedule (Thursday, Sunday, Monday).

```
Pre-game (Thu, Sat, Sun, Mon)         Post-game (Fri, Mon, Tue)
┌─────────────────────┐               ┌─────────────────────┐
│ • NFL Schedule       │               │ • Re-fetch scores    │
│ • Odds API (multi-   │               │ • Compute ATS / O-U  │
│   book spreads/ML)   │               │ • Update weather     │
│ • Weather forecast   │               │   (actual vs forecast)│
│ • (Team Stats Stub)  │               │ • Write outcomes.csv │
└─────────────────────┘               └─────────────────────┘
```

### Schedule

| Cron (UTC)         | ET Equivalent | Purpose |
|--------------------|---------------|---------|
| `0 14 * * 0,1,4,6` | 10 AM ET      | Morning odds snapshot (Thu, Sat, Sun, Mon) |
| `0 20 * * 0,1,4,6` | 4 PM ET       | Afternoon snapshot |
| `0 8 * * 1,5`      | 4 AM ET       | TNF & SNF results (Fri, Mon) |
| `0 14 * * 2`       | 10 AM ET      | Final cleanup for MNF scores (Tue) |

## Setup

### 1. Get API Keys
- **The Odds API:** Free tier (500 requests/month) - https://the-odds-api.com
- **nfl_data_py:** No key required (pulls from nflverse)

### 2. Set Repository Secrets
Go to **Settings → Secrets and variables → Actions** and add:
- `ODDS_API_KEY`: The Odds API key
- `EMAIL_USER` / `EMAIL_APP_PASSWORD` / `EMAIL_TO`: Gmail notification config

## Data Files (in `data/`)

### `games.csv`
Contains NFL schedules, venues, dome status, and final scores.

### `odds_snapshots.csv`
Timestamped odds — multiple rows per game. Tracks line movement across the week.

### `weather.csv`
Temperature, wind, precipitation for open-air stadiums. Domes are automatically tagged and handled.

### `outcomes.csv`
**The training labels.** One row per (game, provider). Computed postgame.

## Pending Architecture Decisions
- **Team Stats:** Unlike CFBD, NFL advanced metrics (EPA, DVOA) require processing play-by-play data. This is currently stubbed pending user architectural decisions.
- **Injury Reports:** Not currently integrated, but vital for NFL line movement.
