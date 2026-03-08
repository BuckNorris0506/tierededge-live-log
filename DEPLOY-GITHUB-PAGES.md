# Deploy to GitHub Pages (Live Bet Log)

## 1) Create repo
Create a GitHub repo (example: `tierededge-live-log`).

## 2) Push this project

```bash
cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log
git init
git add .
git commit -m "Initial live bet log"
git branch -M main
git remote add origin <YOUR_REPO_URL>
git push -u origin main
```

## 3) Enable Pages
In GitHub repo settings:
- Pages -> Build and deployment
- Source: `Deploy from branch`
- Branch: `main`
- Folder: `/public`

If `/public` is not available in Pages UI, copy contents of `public` to repo root or `docs`, then point Pages there.

## 4) Update manually

```bash
cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log
./scripts/update-live-log.sh
```

## 5) Auto update and auto push
Set environment variable to local clone path of your GitHub Pages repo:

```bash
export LIVE_LOG_DEPLOY_REPO=/absolute/path/to/your/repo
```

Optional (recommended if you do not want open bets public in real time):

```bash
export REDACT_PENDING=true
```

Then run:

```bash
cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log
./scripts/update-live-log.sh
```

This will:
- rebuild `public/data.json`
- sync `public/` into that repo
- commit and push changes

## 6) Schedule it (every 5 minutes)

```bash
crontab -e
```

Add:

```cron
*/5 * * * * export LIVE_LOG_DEPLOY_REPO=/absolute/path/to/your/repo; cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log && /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/update-live-log.sh >> /tmp/tierededge-live-log-cron.log 2>&1
```

## Security notes
- This publishes exactly what is in `betting-state.md`.
- If you do not want pending plays public, remove that section from source or add redaction before publish.
