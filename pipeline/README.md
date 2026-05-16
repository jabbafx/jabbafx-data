# pipeline/

The Python scripts and shell orchestrator that produce the derived JSONs in
`../13f/` from public SEC EDGAR 13F-HR filings.

## What's here

| File | Purpose |
|---|---|
| `fetch_edgar.py` | Downloads 13F-HR informationTable XMLs for the 24 tracked funds from SEC EDGAR. Rate-limited 10 req/s, mandatory UA header, 13F-HR original filings only. |
| `parse_13f.py` | XML → structured positions JSON. Namespace-agnostic. CUSIP→ticker via OpenFIGI (free no-key tier, cached). Per-fund value-unit detection (handles both post-2023 dollars and legacy thousands conventions). |
| `compute_confluence.py` | Aggregates per-CUSIP across funds. Produces the `high_confluence` list with signal-strength buckets (high ≥6 funds, medium 4-5, low 3). |
| `cron.sh` | The orchestrator. Auto-derives target quarter, runs the three parsers in sequence, copies outputs into this repo's `13f/`, commits, and pushes. Uses `flock` for single-instance safety. |

## Where they run

The runtime copies live at `/root/jabbafx-data-pipeline/parsers/` on Brazil
VPS 2 (`76.13.229.7`). The files in this directory are the **source of
truth**; the VPS copies are the **runtime**. They drift only across a
push → pull → manual copy step (see Update workflow below).

Hardcoded VPS paths in each parser (`BASE_DIR = "/root/jabbafx-data-pipeline"`)
are intentional. These scripts are not portable; they're canonical-path
tools for one host. Don't refactor for portability.

## Schedule

Cron entry:

```
# JabbaFX 13F quarterly parse — day after SEC 45-day deadline, 03:00 UTC
0 3 16 2,5,8,11 * /root/jabbafx-data-pipeline/staging/jabbafx-data/pipeline/cron.sh >> /root/jabbafx-data-pipeline/logs/cron.wrapper.log 2>&1
```

Fires Feb 16 / May 16 / Aug 16 / Nov 16 at 03:00 UTC — one day after each
45-day SEC reporting deadline (Feb 14 / May 15 / Aug 14 / Nov 14).

## Output

Each successful run commits two files to this repo's `main` branch:

- `13f/positions/<YYYY-QN>.json` — per-fund holdings
- `13f/confluence/<YYYY-QN>-analysis.json` — multi-fund signals

Commit author: `JabbaFX VPS Cron <vps-cron@jabbafx.local>`.
Commit subject: `<YYYY-QN>: automated parse <ISO-UTC-timestamp>`.

`funds.json` is NEVER touched by cron — only by operator-initiated commits.

## Logs

On the VPS:

- `/root/jabbafx-data-pipeline/logs/cron.log` — orchestrator messages,
  grep-able `JABBAFX-13F-{START,OK,FAIL}` markers per fire
- `/root/jabbafx-data-pipeline/logs/fetch_edgar.log`, `parse_13f.log`,
  `compute_confluence.log` — each parser's full verbose output
- `/root/jabbafx-data-pipeline/logs/cron.wrapper.log` — anything from the
  cron wrapper itself (rare)

## Update workflow

To roll a fix to the parsers:

1. Edit the file here on the Mac.
2. `git push` to the repo.
3. SSH to VPS 2, `cd /root/jabbafx-data-pipeline/staging/jabbafx-data && git pull`.
4. `cp pipeline/<changed>.py /root/jabbafx-data-pipeline/parsers/`.

The `cp` is manual on purpose — the cron itself uses the staged clone's
`pipeline/cron.sh`, which IS auto-updated by the cron's own `git pull` step,
but the **parsers** stay decoupled because they're invoked by absolute path
from the parsers/ directory. Manual `cp` is the audit point.

To run on demand:

```
/root/jabbafx-data-pipeline/staging/jabbafx-data/pipeline/cron.sh \
  --quarter 2026-Q2 --force
```

Flags:
- `--quarter YYYY-QN` — override auto-derived target
- `--force` — re-fetch raw XML and re-parse even if outputs exist
- `--no-push` — commit locally, skip `git push` (test mode)

## Why publishing is HTTPS-served static JSON

JabbaFX is a static dashboard with no backend of its own. Putting derived
data in a public GitHub repo lets the frontend fetch it via
`raw.githubusercontent.com` with zero auth, zero CORS, and free CDN. The
fetcher + parser run on the VPS where rate-limited SEC API access is easier
to manage; the result is published here.
