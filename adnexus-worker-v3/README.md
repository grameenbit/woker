# AdNexus Worker v3 — Google Ads Alternative

## Architecture

```
Publisher's Website
  └─ <script src="https://adnexus-worker.fuyadheart.workers.dev/ad.js">
       ├─ Sets __adnx_uid cookie (2 year, cross-site)
       ├─ POST /track  → page URL, title, referrer collected
       └─ GET  /serve  → returns best matching ad based on user interests
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/ad.js` | Publisher embed script |
| POST | `/track` | Page view tracking (called by embed) |
| GET | `/serve` | Ad serving — returns JSON ad |
| GET | `/click` | Click tracking + redirect |
| GET | `/pixel.gif` | 1x1 tracking pixel |
| GET | `/analytics/publisher?publisher_id=&days=7` | Publisher stats |
| GET | `/analytics/advertiser?advertiser_id=&days=30` | Advertiser stats |
| GET | `/analytics/admin?days=30` | Admin stats |
| GET | `/analytics/user?uid=` | User profile (admin) |
| GET | `/health` | Health check |

## Deploy

```bash
cd adnexus-worker-v3

# 1. Install wrangler
npm install

# 2. Login to Cloudflare
npx wrangler login

# 3. Set secrets
npx wrangler secret bulk secrets.json

# 4. Deploy
npx wrangler deploy
```

## Publisher Integration

### Basic (Auto ad slots)
```html
<!-- Set your site ID -->
<script>window.__adnx_site = 'YOUR_SITE_ID';</script>

<!-- Load the ad script -->
<script src="https://adnexus-worker.fuyadheart.workers.dev/ad.js" async></script>

<!-- Place ad slots anywhere -->
<div data-adnx-slot data-adnx-format="banner_300x250" style="width:300px;height:250px;"></div>
<div data-adnx-slot data-adnx-format="banner_728x90" style="width:728px;height:90px;"></div>
```

### Manual (JavaScript API)
```html
<div id="my-ad"></div>
<script>
  // Serve ad into a specific div
  adnxServe('my-ad', 'YOUR_SITE_ID', 'banner_300x250');
</script>
```

## Ad Formats

| Format | Size |
|--------|------|
| `banner_300x250` | Medium Rectangle |
| `banner_728x90` | Leaderboard |
| `banner_320x50` | Mobile Banner |
| `banner_160x600` | Wide Skyscraper |
| `native` | Native Ad |
| `interstitial` | Full Page |

## Tracking Flow (Like Google Ads)

1. **First visit**: New `__adnx_uid` cookie set (2 years)
2. **Every page**: URL, title, referrer sent to `/track`
3. **Every 5 pages**: AI (Claude) analyzes history → extracts interests + keywords
4. **Ad request**: User profile matched against campaigns → best ad served
5. **Cross-site**: Same UID works across ALL sites using this script

## Interest Targeting

Claude AI analyzes browsing history and extracts:
- **Interests**: `["technology", "gaming", "finance"]`
- **Keywords**: `["buy iphone 15", "cheap flights", "python tutorial"]`
- **Intent**: `browsing | research | buying | comparison`

Campaigns target by:
- `target_categories` — broad interest match
- `target_keywords` — specific keyword match
- `geo_targets` — country codes
- `device_types` — mobile/desktop/tablet
- `os_types` — Android/iOS/Windows

## Fraud Detection

Every request analyzed for:
- Known VPN providers (NordVPN, ExpressVPN, etc.)
- Datacenter/hosting ASNs
- Risk score 0-100
- High risk (>80) traffic blocked from ad serving

## Analytics in Frontend

Stats pages call:
```js
// Publisher
fetch(`${WORKER}/analytics/publisher?publisher_id=${uid}&days=7`)

// Advertiser  
fetch(`${WORKER}/analytics/advertiser?advertiser_id=${uid}&days=30`)
```

Returns: impressions, clicks, revenue, CTR, eCPM, daily breakdown, country/device stats, keyword performance
