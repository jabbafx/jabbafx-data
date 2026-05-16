# jabbafx-data

Structured public-data feeds powering the JabbaFX dashboard's Institutional
Intelligence layer.

## What lives here

Parsed and processed outputs from free public sources (SEC EDGAR, CFTC, FRED,
etc.), committed as static JSON. The JabbaFX frontend at
`https://jabbafx.netlify.app` fetches these files directly via GitHub's raw
URLs — no API server, no auth.

## Modules

- `13f/` — Quarterly 13F-HR institutional positioning across 24 tracked funds
  - `funds.json` — master fund list
  - `positions/YYYY-QX.json` — quarterly position snapshots
  - `confluence/YYYY-QX-analysis.json` — multi-fund overlap analytics

Future modules (Form 4 insider, COT, etc.) will be added as separate top-level
directories.

## Data sources

All data is sourced from public filings and free public APIs. SEC EDGAR is the
primary source for the 13F module. No paid subscriptions, no scraped data, no
PII.

## License

MIT — see [LICENSE](LICENSE).

## No PII

This repository contains no personal information. Manager names attached to
funds are public-record fund principals as disclosed in filings.
