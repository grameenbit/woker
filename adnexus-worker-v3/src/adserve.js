// ─── adserve.js ───────────────────────────────────────────────────────────────
// GET /serve  — Ad selection engine + response
// GET /click  — Click tracking + redirect
//
// Ad formats:   banner | interstitial | webview | iframe
// Bid types:    CPM (per 1000 impressions) | CPC (per click)
// Revenue:      Configurable publisher/admin split (from platform_config table)

import { fingerprint, getGeoTier }                       from './fingerprint.js';
import { getUIDFromRequest, newUID, buildSetCookie, getProfileFromKV } from './cookie.js';
import { createDB }                                       from './db.js';
import { json }                                           from './cors.js';

// ── Revenue share (cached in KV 5 min) ────────────────────────────────────────
export async function getRevenueShare(env) {
  const cacheKey = 'cfg:rev_share';
  try {
    const cached = await env.AD_CACHE.get(cacheKey);
    if (cached) return JSON.parse(cached);
  } catch {}
  try {
    const D    = createDB(env);
    const rows = await D.select('platform_config', 'key=in.(revenue_share_publisher,revenue_share_admin)');
    if (Array.isArray(rows) && rows.length) {
      const cfg = Object.fromEntries(rows.map(r => [r.key, parseFloat(r.value)]));
      const share = {
        publisher: cfg['revenue_share_publisher'] ?? 0.80,
        admin:     cfg['revenue_share_admin']     ?? 0.20,
      };
      await env.AD_CACHE.put(cacheKey, JSON.stringify(share), { expirationTtl: 300 });
      return share;
    }
  } catch (e) { console.error('Revenue share fetch failed:', e); }
  return { publisher: 0.80, admin: 0.20 };
}

// ── Campaign scoring ───────────────────────────────────────────────────────────
function scoreAd(campaign, profile, f) {
  if (campaign.status !== 'active') return -1;

  const totalBudget = parseFloat(campaign.budget_total || campaign.total_budget || 0);
  const spent       = parseFloat(campaign.spent_total  || 0);
  if (totalBudget > 0 && spent >= totalBudget) return -1;

  // Geo targeting (hard filter)
  const geoTargets = campaign.geo_targets || campaign.target_countries || [];
  if (geoTargets.length > 0 && !geoTargets.includes(f.country)) return -1;

  // Device targeting (hard filter)
  const devTargets = campaign.device_types || campaign.device_targets || [];
  if (devTargets.length > 0 && !devTargets.includes(f.deviceType)) return -1;

  let score = 0;
  const interests   = profile?.interests    || [];
  const keywords    = profile?.keywords     || [];
  const topCategory = profile?.top_category || '';
  const cats = campaign.target_categories || (campaign.category ? [campaign.category] : []);
  const kws  = campaign.target_keywords   || [];

  // Interest matching
  if (cats.includes(topCategory))                             score += 60;
  score += cats.filter(c => interests.includes(c)).length * 25;

  // Keyword matching
  for (const kw of kws)
    if (keywords.some(uk => uk.toLowerCase().includes(kw.toLowerCase()))) score += 35;

  // Geo tier bonus (Tier 1 = highest value)
  score += (4 - getGeoTier(f.country).tier) * 15;

  // Prefer CPC for high buy-intent users
  if (campaign.bid_type === 'cpc' && profile?.intent === 'buying') score += 20;

  // Small random factor to avoid always showing same ad
  score += Math.random() * 5;

  return score;
}

// ── Format-specific ad payload ─────────────────────────────────────────────────
function buildAdPayload(campaign, clickUrl, format) {
  const base = {
    campaign_id: campaign.id,
    bid_type:    campaign.bid_type || 'cpm',
    format,
    click_url:   clickUrl,
    target_url:  campaign.target_url || '#',
  };

  switch (format) {
    // ── Interstitial: full-screen overlay ──────────────────────────────────────
    case 'interstitial':
      return {
        ...base,
        title:       campaign.ad_title || campaign.name || '',
        content:     campaign.ad_content || campaign.ad_description || '',
        description: campaign.ad_description || '',
        cta_text:    campaign.cta_text || 'Learn More',
        skip_after:  campaign.skip_duration || 5, // seconds until skip button appears
        image_url:   campaign.creative_url || null,
      };

    // ── WebView: loads webview_url first, then CTA bar ─────────────────────────
    case 'webview':
      return {
        ...base,
        webview_url: campaign.webview_url || campaign.target_url || '#',
        cta_text:    campaign.cta_text || 'Visit Site',
        skip_after:  campaign.skip_duration || 5,
        title:       campaign.ad_title || campaign.name || '',
      };

    // ── iFrame: loads iframe_url first, then CTA bar ───────────────────────────
    case 'iframe':
      return {
        ...base,
        iframe_url: campaign.iframe_url || campaign.webview_url || campaign.target_url || '#',
        cta_text:   campaign.cta_text || 'Open',
        skip_after: campaign.skip_duration || 5,
        title:      campaign.ad_title || campaign.name || '',
      };

    // ── Banner (default): image or text ad ────────────────────────────────────
    default:
      return {
        ...base,
        content:     campaign.ad_content || campaign.ad_title || campaign.name || '',
        image_url:   campaign.creative_url || null,
        headline:    campaign.ad_title || campaign.name || '',
        description: campaign.ad_description || '',
        cta_text:    campaign.cta_text || 'Learn More',
      };
  }
}

// ── GET /serve ─────────────────────────────────────────────────────────────────
export async function handleServe(req, env) {
  const url    = new URL(req.url);
  const siteId = url.searchParams.get('site_id') || req.headers.get('X-Site-ID') || '';
  const format = url.searchParams.get('format')  || 'banner_300x250';
  const pageUrl= url.searchParams.get('page')    || '';
  const f      = fingerprint(req);
  const uid    = (url.searchParams.get('uid')?.length === 32 ? url.searchParams.get('uid') : null)
              || getUIDFromRequest(req)
              || newUID();

  // Block very high-risk VPN traffic from earning revenue
  if (f.riskScore > 85 && f.isVPN) {
    return json({ error: 'traffic_blocked', code: 'vpn_high_risk' }, 403);
  }

  const profile = await getProfileFromKV(uid, env);

  // Fetch active campaigns (KV-cached 60s)
  const cacheKey = 'camps:active';
  let campaigns = null;
  try { const c = await env.AD_CACHE.get(cacheKey); if (c) campaigns = JSON.parse(c); } catch {}
  if (!campaigns) {
    const D = createDB(env);
    campaigns = await D.select('campaigns',
      'status=eq.active' +
      '&select=id,name,status,bid_type,ad_format,ad_type,budget_total,spent_total,impressions,clicks,' +
      'cpm_rate,cpc_rate,geo_targets,target_countries,device_types,target_categories,target_keywords,' +
      'advertiser_id,category,target_url,ad_title,ad_content,ad_description,cta_text,' +
      'skip_duration,webview_url,iframe_url,creative_url' +
      '&order=cpm_rate.desc&limit=200'
    ).catch(() => []);
    if (campaigns?.length) await env.AD_CACHE.put(cacheKey, JSON.stringify(campaigns), { expirationTtl: 60 });
  }
  if (!campaigns?.length) return json({ error: 'no_campaigns' }, 204);

  // Filter by format family, then score
  const formatFamily = format.startsWith('banner') ? 'banner' : format;
  const eligible = campaigns.filter(c => {
    const cf = (c.ad_format || c.ad_type || 'banner').toLowerCase();
    return formatFamily === 'banner' ? (cf.includes('banner') || cf === 'display') : cf === formatFamily;
  });

  const scored = (eligible.length ? eligible : campaigns)
    .map(c => ({ c, score: scoreAd(c, profile, f) }))
    .filter(x => x.score >= 0)
    .sort((a, b) => b.score - a.score);

  if (!scored.length) return json({ error: 'no_matching_ad' }, 204);

  const { c: campaign } = scored[0];
  const gt = getGeoTier(f.country);

  // Revenue calculation
  const isCPC     = (campaign.bid_type || 'cpm') === 'cpc';
  const cpmRate   = isCPC ? 0 : Math.max(parseFloat(campaign.cpm_rate) || 0, gt.min_cpm);
  const cpcRate   = Math.max(parseFloat(campaign.cpc_rate) || 0, gt.min_cpc);
  const impRevenue = isCPC ? 0 : cpmRate / 1000;

  const share     = await getRevenueShare(env);
  const pubRevenue = impRevenue * share.publisher;

  const clickUrl = `${url.origin}/click?cid=${campaign.id}&uid=${uid}&sid=${siteId}&dest=${encodeURIComponent(campaign.target_url || '#')}&bid=${campaign.bid_type || 'cpm'}`;
  const adPayload = buildAdPayload(campaign, clickUrl, format);

  const D   = createDB(env);
  const now = new Date().toISOString();

  env.ctx?.waitUntil?.(Promise.all([
    D.insert('ad_events', {
      event_type:       'impression',
      uid,
      campaign_id:      campaign.id,
      site_id:          siteId,
      advertiser_id:    campaign.advertiser_id,
      ad_format:        format,
      page_url:         pageUrl,
      matched_category: profile?.top_category || null,
      country:          f.country,
      city:             f.city,
      device_type:      f.deviceType,
      os_type:          f.os,
      ip_address:       f.ip,
      is_vpn:           f.isVPN,
      cpm_rate:         cpmRate,
      cpc_rate:         cpcRate,
      revenue:          impRevenue,
      created_at:       now,
    }),
    D.update('campaigns', { id: campaign.id }, {
      impressions: (parseInt(campaign.impressions) || 0) + 1,
      spent_total: ((parseFloat(campaign.spent_total) || 0) + impRevenue).toFixed(6),
    }),
  ]));

  return json({
    uid,
    ad: adPayload,
    _meta: {
      bid_type:  campaign.bid_type || 'cpm',
      cpm_rate:  cpmRate,
      cpc_rate:  cpcRate,
      geo_tier:  gt.tier,
      pub_share: `${Math.round(share.publisher * 100)}%`,
      score:     Math.round(scored[0].score),
    },
  }, 200, {
    'Set-Cookie':    buildSetCookie(uid, env.COOKIE_DOMAIN || ''),
    'Cache-Control': 'no-store, no-cache',
  });
}

// ── GET /click ─────────────────────────────────────────────────────────────────
export async function handleClick(req, env) {
  const url     = new URL(req.url);
  const cid     = url.searchParams.get('cid');
  const uid     = url.searchParams.get('uid');
  const sid     = url.searchParams.get('sid');
  const dest    = url.searchParams.get('dest') || '#';
  const bidType = url.searchParams.get('bid')  || 'cpm';

  if (cid && uid) {
    // Rate-limit: 1 click per uid per campaign per hour (prevents click fraud)
    const rateKey = `clk:${uid}:${cid}`;
    const recent  = await env.RATE_LIMIT.get(rateKey).catch(() => null);

    if (!recent) {
      await env.RATE_LIMIT.put(rateKey, '1', { expirationTtl: 3600 });
      const f   = fingerprint(req);
      const gt  = getGeoTier(f.country);
      const D   = createDB(env);
      const now = new Date().toISOString();

      // CPC: earn on click; CPM: click is free (impression already earned)
      const cpcRevenue = bidType === 'cpc' ? gt.min_cpc : 0;

      env.ctx?.waitUntil?.(Promise.all([
        D.insert('ad_events', {
          event_type:  'click',
          uid,
          campaign_id: cid,
          site_id:     sid,
          country:     f.country,
          city:        f.city,
          device_type: f.deviceType,
          os_type:     f.os,
          ip_address:  f.ip,
          is_vpn:      f.isVPN,
          cpc_rate:    gt.min_cpc,
          revenue:     cpcRevenue,
          created_at:  now,
        }),
        D.update('campaigns', { id: cid }, {
          clicks: 'clicks+1',
          ...(cpcRevenue > 0 ? { spent_total: `spent_total+${cpcRevenue}` } : {}),
        }),
      ]));
    }
  }

  return Response.redirect(decodeURIComponent(dest), 302);
}
