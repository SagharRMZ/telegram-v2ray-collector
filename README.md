# Telegram Channel Collector (template)

This repo is a **general-purpose** template that:
- Runs on a schedule (every 5 hours) using GitHub Actions
- Reads messages from channels you can access (via Telethon session)
- Extracts items via regex patterns you define in `patterns.json`
- Keeps a rolling window (default 50 hours) and prunes older items
- Commits updated outputs back to the repo

## Setup

1) Put your channels in `channels.csv` (channel,limit).
2) Put your patterns in `patterns.json`.
3) Create Telegram API credentials (api_id/api_hash) and a Telethon session string.

### GitHub Secrets
Create these repository secrets:
- `TG_API_ID`
- `TG_API_HASH`
- `TG_SESSION`

## Outputs
For each pattern item `{ "name": "X", ... }`, the workflow writes `X.txt`.

## Notes
- `store.json` keeps extracted items + timestamps.
- `state.json` keeps last message id per channel, so each run fetches only newer messages.
