# Deploy to GitHub Pages (Live Bet Log)

## Current production model

TieredEdge's live GitHub Pages site currently serves **repo-root artifacts from `main`**.

That means the live site is reading:
- `https://bucknorris0506.github.io/tierededge-live-log/data.json`
- `https://bucknorris0506.github.io/tierededge-live-log/standalone.html`

and those files must exist at the **repo root** in the published branch.

`public/` remains the internal build directory, but it is **not** the trusted live publish target for the current production setup. The rebuild path writes `public/*` first and then syncs those files back to repo root before any optional commit/push.

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
- Folder: `/ (root)`

Do not treat `/public` as the live Pages source unless you intentionally change the GitHub Pages repo settings and the deploy script to match. The current production site is root-on-`main`.

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
- rebuild `public/standalone.html`
- sync `public/` back into repo root
- commit and push the root deploy artifacts used by GitHub Pages
- commit and push changes

## 6) Schedule it (every 10 minutes)

```bash
crontab -e
```

Add:

```cron
*/10 * * * * export PATH=/usr/local/bin:/usr/bin:/bin; export LIVE_LOG_DEPLOY_REPO=/absolute/path/to/your/repo; cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log && /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/update-live-log.sh >> /tmp/tierededge-live-log-cron.log 2>&1
```

Do not run `node scripts/build-live-log.mjs` from a second cron entry. `update-live-log.sh` is the only supported scheduled rebuild entrypoint.

## Security notes
- The live site publishes the repo-root deploy artifacts after sync from `public/`.
- If you do not want pending plays public, add redaction before publish.

## Build and sync model

`update-live-log.sh` currently does this:

1. Build canonical state and public artifacts in `public/`
2. Build `public/data.json`
3. Build `public/standalone.html`
4. Sync `public/` back to repo root with `rsync`
5. Optionally commit and push repo-root deploy artifacts

Important repo-root files kept current for GitHub Pages:
- `data.json`
- `index.html`
- `app.js`
- `styles.css`
- `standalone.html`
- text outputs such as `decision-terminal.txt`

## Live verification

After deploy, verify the actual live JSON instead of trusting the local build:

```bash
curl -L -s https://bucknorris0506.github.io/tierededge-live-log/data.json | jq '{pending_count, pending_bets_len:(.pending_bets|length), open_risk_summary}'
```

Expected checks:
- `pending_count` is present and correct
- `pending_bets_len` matches the real pending tickets
- `open_risk_summary` is populated
