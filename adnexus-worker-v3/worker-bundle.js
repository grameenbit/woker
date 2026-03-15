// ═══════════════════════════════════════════════════════════════════════════════
// AdNexus Worker v3.1 — Advanced Ad Serving Backend
// Cloudflare Worker — Single File Bundle
//
// Routes:
//   GET  /ad.js                    Publisher embed script
//   POST /track                    Page view + interest tracking
//   GET  /serve                    Ad serving (JSON response)
//   GET  /click                    Click tracking + redirect
//   GET  /pixel.gif                1x1 tracking pixel
//   GET  /analytics/publisher      Publisher stats API
//   GET  /analytics/advertiser     Advertiser stats API
//   GET  /analytics/admin          Admin stats API
//   GET  /health                   Health check
//
// Ad Formats: banner | interstitial | webview | iframe
// Bid Types:  CPM (per 1000 impressions) | CPC (per click)
// Revenue:    Publisher 80% | Admin 20% (configurable from admin panel)
// ═══════════════════════════════════════════════════════════════════════════════

'use strict';

// ──────────────────────────────────────────────────────────────────────────────
// CORS
// ──────────────────────────────────────────────────────────────────────────────
const CH = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Site-ID',
  'Access-Control-Max-Age': '86400',
};
function j(body, status = 200, extra = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', ...CH, ...extra },
  });
}
function optR() { return new Response(null, { status: 204, headers: CH }); }

// ──────────────────────────────────────────────────────────────────────────────
// FINGERPRINT — Geo / Device / VPN from Cloudflare headers
// ──────────────────────────────────────────────────────────────────────────────
const HOSTING_ASNS = new Set([
  'AS14061','AS16276','AS24940','AS63949','AS45102','AS8100',
  'AS20473','AS62240','AS7922','AS15169','AS8075','AS16509',
  'AS13335','AS54113','AS36351','AS394711','AS46844',
]);
const VPN_KW  = ['nordvpn','expressvpn','surfshark','cyberghost','purevpn','protonvpn','ipvanish','tunnelbear','windscribe','mullvad','hidemyass','pia','privateinternetaccess'];
const DC_KW   = ['amazon','aws','google cloud','microsoft azure','digitalocean','linode','vultr','ovh','hetzner','contabo','hosting','server','datacenter','cloud','vps','colocation'];

function parseUA(ua = '') {
  let dt = 'desktop';
  if (/ipad|tablet|kindle/i.test(ua)) dt = 'tablet';
  else if (/mobile|android|iphone|ipod|blackberry|windows phone|opera mini/i.test(ua)) dt = 'mobile';
  let os = 'Unknown';
  if      (/windows nt/i.test(ua))  os = 'Windows';
  else if (/android/i.test(ua))     { const v = ua.match(/android\s([\d.]+)/i); os = `Android${v ? ' '+v[1] : ''}`; }
  else if (/iphone|ipad/i.test(ua)) { const v = ua.match(/os\s([\d_]+)/i); os = `iOS${v ? ' '+v[1].replace(/_/g,'.') : ''}`; }
  else if (/mac os x/i.test(ua))    os = 'macOS';
  else if (/linux/i.test(ua))       os = 'Linux';
  let browser = 'Unknown';
  if      (/edg\//i.test(ua))    browser = 'Edge';
  else if (/opr\//i.test(ua))    browser = 'Opera';
  else if (/chrome/i.test(ua))   browser = 'Chrome';
  else if (/firefox/i.test(ua))  browser = 'Firefox';
  else if (/safari/i.test(ua))   browser = 'Safari';
  return { dt, os, browser };
}

function detectProxy(asn = '', isp = '') {
  const org = isp.toLowerCase();
  let isVPN = false, isProxy = false, risk = 0, signals = [];
  const asnKey = `AS${String(asn).replace('AS','').trim()}`;
  if (HOSTING_ASNS.has(asnKey))               { isProxy = true; risk += 40; signals.push({ type:'datacenter', label:'Datacenter IP', desc: isp }); }
  if (VPN_KW.some(k => org.includes(k)))      { isVPN = true; risk += 60; signals.push({ type:'vpn', label:'Known VPN', desc: isp }); }
  else if (DC_KW.some(k => org.includes(k)))  { isProxy = true; risk = Math.max(risk, 35); if (!signals.length) signals.push({ type:'proxy', label:'Hosting IP', desc: isp }); }
  return { isVPN, isProxy, riskScore: Math.min(100, risk), signals };
}

function fp(req) {
  const cf = req.cf || {}, h = req.headers;
  const ua = h.get('User-Agent') || '';
  const ip = h.get('CF-Connecting-IP') || h.get('X-Forwarded-For')?.split(',')[0]?.trim() || '0.0.0.0';
  const asn = String(cf.asn || ''), isp = cf.asOrganization || '';
  const { dt, os, browser } = parseUA(ua);
  const proxy = detectProxy(asn, isp);
  return {
    country: cf.country || 'XX', region: cf.region || '', city: cf.city || '',
    timezone: cf.timezone || '', ip, asn, isp,
    deviceType: dt, os, browser, ua, ...proxy,
  };
}

// Geo tier pricing
const GEO_TIERS = {
  1: { min_cpm: 5.00, min_cpc: 0.50, countries: ['US','GB','CA','AU','DE','FR','NL','SE','JP','SG','CH','NO','DK','FI','NZ','IE','AT','BE','IL','AE'] },
  2: { min_cpm: 2.00, min_cpc: 0.20, countries: ['IN','BR','MX','ID','PH','TH','MY','VN','TR','ZA','KR','SA','QA','KW','CL','CO','PE','AR','PL','CZ','HU','RO'] },
  3: { min_cpm: 0.50, min_cpc: 0.05, countries: ['BD','PK','NG','EG','KE','GH','TZ','ET','UG','ZW','SD','SN','CM','CI','RW','MZ','MW','ZM','AF','MM','KH'] },
};
function geoTier(country) {
  for (const [t, d] of Object.entries(GEO_TIERS))
    if (d.countries.includes(country)) return { tier: parseInt(t), ...d };
  return { tier: 3, ...GEO_TIERS[3] };
}

// ──────────────────────────────────────────────────────────────────────────────
// COOKIE / UID
// ──────────────────────────────────────────────────────────────────────────────
const CNAME = '__adnx_uid';
const CAGE  = 60 * 60 * 24 * 365 * 2; // 2 years

function getUID(req) {
  const c = req.headers.get('Cookie') || '';
  const m = c.match(new RegExp(`(?:^|;\\s*)${CNAME}=([^;]+)`));
  if (m?.[1]?.length === 32) return m[1];
  const u = new URL(req.url);
  const p = u.searchParams.get('uid');
  if (p?.length === 32) return p;
  return null;
}
function mkUID() { return crypto.randomUUID().replace(/-/g, ''); }
function setCookie(uid, domain) {
  const parts = [`${CNAME}=${uid}`, `Max-Age=${CAGE}`, `Path=/`, `SameSite=None`, `Secure`, `HttpOnly`];
  if (domain && !domain.includes('localhost')) parts.push(`Domain=${domain}`);
  return parts.join('; ');
}

async function getProfile(uid, env) {
  try { const r = await env.USER_PROFILES.get(uid); return r ? JSON.parse(r) : null; } catch { return null; }
}
async function saveProfile(uid, p, env) {
  try { await env.USER_PROFILES.put(uid, JSON.stringify(p), { expirationTtl: CAGE }); } catch {}
}
function mergeVisit(existing, v) {
  const p = existing || { uid: v.uid, first_seen: v.ts, page_history: [], interests: [], keywords: [], top_category: null, page_count: 0 };
  p.last_seen = v.ts; p.country = v.country || p.country; p.city = v.city || p.city;
  p.device_type = v.deviceType || p.device_type; p.os_type = v.os || p.os_type; p.browser = v.browser || p.browser;
  p.ip_address = v.ip || p.ip_address; p.isVPN = v.isVPN || p.isVPN; p.isProxy = v.isProxy || p.isProxy;
  p.riskScore = Math.max(p.riskScore || 0, v.riskScore || 0);
  p.page_history = [{ url: v.url, title: v.title, referrer: v.referrer, ts: v.ts }, ...(p.page_history || [])].slice(0, 200);
  p.page_count = (p.page_count || 0) + 1;
  return p;
}

// ──────────────────────────────────────────────────────────────────────────────
// DB — Supabase REST client
// ──────────────────────────────────────────────────────────────────────────────
function db(env) {
  const BASE = env.SUPABASE_URL;
  const KEY  = env.SUPABASE_SERVICE_KEY;
  const H    = { 'apikey': KEY, 'Authorization': `Bearer ${KEY}`, 'Content-Type': 'application/json', 'Prefer': 'return=minimal' };
  const HR   = { ...H, 'Prefer': 'return=representation' };

  return {
    async sel(t, q = '') {
      const r = await fetch(`${BASE}/rest/v1/${t}?${q}`, { headers: HR });
      if (!r.ok) { console.error(`SEL ${t}:`, await r.text()); return []; }
      return r.json();
    },
    async upsert(t, body) {
      const r = await fetch(`${BASE}/rest/v1/${t}`, { method: 'POST', headers: { ...H, 'Prefer': 'resolution=merge-duplicates,return=minimal' }, body: JSON.stringify(body) });
      if (!r.ok && r.status !== 201) console.error(`UPSERT ${t} ${r.status}:`, await r.text());
    },
    async ins(t, body) {
      const r = await fetch(`${BASE}/rest/v1/${t}`, { method: 'POST', headers: H, body: JSON.stringify(body) });
      if (!r.ok && r.status !== 201) console.error(`INS ${t} ${r.status}:`, await r.text());
    },
    async upd(t, match, body) {
      const q = Object.entries(match).map(([k,v]) => `${k}=eq.${encodeURIComponent(v)}`).join('&');
      const r = await fetch(`${BASE}/rest/v1/${t}?${q}`, { method: 'PATCH', headers: H, body: JSON.stringify(body) });
      if (!r.ok) console.error(`UPD ${t}:`, await r.text());
    },
  };
}

// ──────────────────────────────────────────────────────────────────────────────
// REVENUE SHARE — read from Supabase platform_config (cached in KV 5min)
// ──────────────────────────────────────────────────────────────────────────────
async function getRevenueShare(env) {
  const cacheKey = 'config:revenue_share';
  try {
    const cached = await env.AD_CACHE.get(cacheKey);
    if (cached) return JSON.parse(cached);
  } catch {}
  // Fetch from DB
  try {
    const D = db(env);
    const rows = await D.sel('platform_config', 'key=in.(revenue_share_publisher,revenue_share_admin)');
    if (Array.isArray(rows) && rows.length) {
      const cfg = {};
      rows.forEach(r => { cfg[r.key] = parseFloat(r.value); });
      const share = {
        publisher: cfg['revenue_share_publisher'] ?? 0.80,
        admin:     cfg['revenue_share_admin']     ?? 0.20,
      };
      await env.AD_CACHE.put(cacheKey, JSON.stringify(share), { expirationTtl: 300 });
      return share;
    }
  } catch (e) { console.error('Revenue share fetch failed:', e); }
  return { publisher: 0.80, admin: 0.20 }; // fallback
}

// ──────────────────────────────────────────────────────────────────────────────
// AI INTEREST ANALYSIS — buildpicoapps free WebSocket AI
// ──────────────────────────────────────────────────────────────────────────────
function shouldAnalyze(n) { return n % 5 === 0 && n > 0; }

async function analyzeInterests(pageHistory, env) {
  const pages = pageHistory.slice(0, 40)
    .map(p => `URL: ${p.url || ''}${p.title ? ' | ' + p.title : ''}`)
    .join('\n');

  const systemPrompt = `You are an ad targeting AI. When given browsing history, respond ONLY with a valid JSON object (no markdown, no explanation) using exactly this structure: {"interests":["cat1","cat2"],"keywords":["kw1","kw2"],"top_category":"cat1","intent":"browsing"}. Use only these categories: technology, gaming, finance, health, travel, fashion, sports, news, entertainment, ecommerce, food, automotive, education, real_estate, crypto, software. Keywords must be specific ad-targetable phrases.`;

  return new Promise((resolve) => {
    const timeout = setTimeout(() => resolve(null), 12000);
    try {
      const ws = new WebSocket('wss://backend.buildpicoapps.com/api/chatbot/chat');
      let text = '';
      ws.addEventListener('open', () => {
        ws.send(JSON.stringify({
          chatId: crypto.randomUUID(),
          appId: env.AI_APP_ID || 'language-industry',
          systemPrompt,
          message: `Analyze for ad targeting:\n${pages}`,
        }));
      });
      ws.addEventListener('message', e => { text += e.data; });
      ws.addEventListener('close', () => {
        clearTimeout(timeout);
        try {
          const m = text.match(/\{[\s\S]*\}/);
          if (!m) { resolve(null); return; }
          const p = JSON.parse(m[0]);
          resolve({
            interests:    Array.isArray(p.interests) ? p.interests.slice(0, 10) : [],
            keywords:     Array.isArray(p.keywords)  ? p.keywords.slice(0, 20)  : [],
            top_category: p.top_category || null,
            intent:       p.intent || 'browsing',
            analyzed_at:  new Date().toISOString(),
            source:       'buildpicoapps',
          });
        } catch { resolve(null); }
      });
      ws.addEventListener('error', () => { clearTimeout(timeout); resolve(null); });
    } catch (e) { clearTimeout(timeout); resolve(null); }
  });
}

// Heuristic fallback (no API needed)
function heuristic(pageHistory) {
  const CATS = {
    technology:    /tech|software|code|app|github|android|ios|api|developer/i,
    gaming:        /game|gaming|steam|xbox|playstation|esport|twitch|minecraft/i,
    finance:       /bank|invest|stock|crypto|bitcoin|forex|trading|loan|finance/i,
    health:        /health|fitness|diet|gym|exercise|medicine|doctor|wellness/i,
    travel:        /travel|flight|hotel|booking|airbnb|visa|holiday|vacation/i,
    fashion:       /fashion|clothing|shoes|dress|style|wear|beauty|makeup/i,
    sports:        /sport|football|cricket|soccer|basketball|tennis|nba|fifa|ipl/i,
    entertainment: /movie|music|celebrity|netflix|youtube|series|film/i,
    ecommerce:     /shop|buy|price|deal|sale|discount|amazon|daraz|product/i,
    education:     /learn|course|tutorial|study|exam|skill|university/i,
    food:          /food|recipe|restaurant|cook|eat|meal|delivery/i,
    automotive:    /car|bike|auto|vehicle|motor|electric vehicle|ev/i,
  };
  const allText = pageHistory.map(p => `${p.url} ${p.title}`).join(' ');
  const scores = {};
  for (const [cat, pat] of Object.entries(CATS)) {
    const n = (allText.match(new RegExp(pat.source, 'gi')) || []).length;
    if (n > 0) scores[cat] = n;
  }
  const interests = Object.entries(scores).sort((a,b)=>b[1]-a[1]).slice(0,6).map(([k])=>k);
  const keywords = [];
  for (const p of pageHistory.slice(0,20)) {
    try {
      const u = new URL(p.url || 'https://x');
      const q = u.searchParams.get('q') || u.searchParams.get('search');
      if (q) keywords.push(q.toLowerCase().trim());
    } catch {}
  }
  return { interests, keywords: [...new Set(keywords)].slice(0,15), top_category: interests[0] || 'general', source: 'heuristic' };
}

// ──────────────────────────────────────────────────────────────────────────────
// TRACKER — POST /track
// ──────────────────────────────────────────────────────────────────────────────
async function handleTrack(req, env) {
  let body = {}; try { body = await req.json(); } catch {}
  const f = fp(req); const now = new Date().toISOString();
  let uid = (body.uid?.length === 32 ? body.uid : null) || getUID(req) || mkUID();
  const visit = { uid, url: body.url || '', title: body.title || '', referrer: body.referrer || '', ts: now, siteId: body.site_id || '', ...f };
  let profile = await getProfile(uid, env);
  profile = mergeVisit(profile, visit);
  await saveProfile(uid, profile, env);
  const D = db(env);
  const ops = Promise.all([
    D.upsert('user_profiles', {
      uid, first_seen: profile.first_seen || now, last_seen: now, page_count: profile.page_count,
      country: f.country, region: f.region, city: f.city, timezone: f.timezone,
      ip_address: f.ip, isp: f.isp, is_vpn: f.isVPN, is_proxy: f.isProxy,
      device_type: f.deviceType, os_type: f.os, browser: f.browser,
      interests: JSON.stringify(profile.interests || []), keywords: JSON.stringify(profile.keywords || []),
      top_category: profile.top_category || null, page_history: JSON.stringify(profile.page_history || []),
      risk_score: f.riskScore,
    }),
    D.ins('page_views', {
      uid, site_id: visit.siteId, page_url: visit.url, page_title: visit.title, referrer: visit.referrer,
      country: f.country, city: f.city, device_type: f.deviceType, os_type: f.os, browser: f.browser,
      is_vpn: f.isVPN, is_proxy: f.isProxy, ip_address: f.ip, risk_score: f.riskScore, created_at: now,
    }),
  ]);
  const aiOp = shouldAnalyze(profile.page_count) ? (async () => {
    const analysis = (await analyzeInterests(profile.page_history, env)) || heuristic(profile.page_history);
    if (analysis) {
      profile.interests = analysis.interests; profile.keywords = analysis.keywords; profile.top_category = analysis.top_category;
      await Promise.all([
        saveProfile(uid, profile, env),
        D.upd('user_profiles', { uid }, { interests: JSON.stringify(analysis.interests), keywords: JSON.stringify(analysis.keywords), top_category: analysis.top_category }),
      ]);
    }
  })() : Promise.resolve();
  env.ctx?.waitUntil?.(Promise.all([ops, aiOp]));
  return j({ uid, ok: true }, 200, { 'Set-Cookie': setCookie(uid, env.COOKIE_DOMAIN || '') });
}

// ──────────────────────────────────────────────────────────────────────────────
// AD SCORING
// ──────────────────────────────────────────────────────────────────────────────
function scoreAd(campaign, profile, f) {
  if (campaign.status !== 'active') return -1;
  const totalBudget = parseFloat(campaign.budget_total || campaign.total_budget || 0);
  const spent = parseFloat(campaign.spent_total || 0);
  if (totalBudget > 0 && spent >= totalBudget) return -1;

  // Geo targeting
  const geo = campaign.geo_targets || campaign.target_countries || [];
  if (geo.length > 0 && !geo.includes(f.country)) return -1;

  // Device targeting
  const devs = campaign.device_types || campaign.device_targets || [];
  if (devs.length > 0 && !devs.includes(f.deviceType)) return -1;

  let score = 0;
  const interests = profile?.interests || [], keywords = profile?.keywords || [], topCat = profile?.top_category || '';
  const cats = campaign.target_categories || (campaign.category ? [campaign.category] : []);
  const kws  = campaign.target_keywords || [];

  if (cats.includes(topCat)) score += 60;
  score += cats.filter(c => interests.includes(c)).length * 25;
  for (const kw of kws) if (keywords.some(uk => uk.toLowerCase().includes(kw.toLowerCase()))) score += 35;

  const gt = geoTier(f.country);
  score += (4 - gt.tier) * 15;

  // Prefer CPC for click-intent users, CPM for browsers
  if (campaign.bid_type === 'cpc' && profile?.intent === 'buying') score += 20;

  score += Math.random() * 5;
  return score;
}

// ──────────────────────────────────────────────────────────────────────────────
// AD FORMATS — build rich ad payload per format type
// ──────────────────────────────────────────────────────────────────────────────
function buildAdPayload(campaign, clickUrl, format) {
  const base = {
    campaign_id:  campaign.id,
    bid_type:     campaign.bid_type || 'cpm',
    format,
    click_url:    clickUrl,
    target_url:   campaign.target_url || '#',
  };

  switch (format) {
    case 'interstitial':
      return {
        ...base,
        // Interstitial: title + content + description + skip + CTA
        title:        campaign.ad_title || campaign.name || '',
        content:      campaign.ad_content || campaign.ad_description || '',
        description:  campaign.ad_description || '',
        cta_text:     campaign.cta_text || 'Learn More',
        skip_after:   campaign.skip_duration || 5, // seconds before skip button appears
        image_url:    campaign.creative_url || null,
      };

    case 'webview':
      return {
        ...base,
        // WebView: loads webview_url first, then shows skip + CTA
        webview_url:  campaign.webview_url || campaign.target_url || '#',
        cta_text:     campaign.cta_text || 'Visit Site',
        skip_after:   campaign.skip_duration || 5,
        title:        campaign.ad_title || campaign.name || '',
      };

    case 'iframe':
      return {
        ...base,
        // iFrame: loads iframe_url first, then shows skip + CTA
        iframe_url:   campaign.iframe_url || campaign.webview_url || campaign.target_url || '#',
        cta_text:     campaign.cta_text || 'Open',
        skip_after:   campaign.skip_duration || 5,
        title:        campaign.ad_title || campaign.name || '',
      };

    case 'banner_728x90':
    case 'banner_300x250':
    case 'banner_320x50':
    case 'banner_160x600':
    default:
      return {
        ...base,
        // Banner: content + target_url only
        content:    campaign.ad_content || campaign.ad_title || campaign.name || '',
        image_url:  campaign.creative_url || null,
        text_only:  !campaign.creative_url,
        // Text ad fallback fields
        headline:    campaign.ad_title || campaign.name || '',
        description: campaign.ad_description || '',
        cta_text:    campaign.cta_text || 'Learn More',
      };
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// AD SERVE — GET /serve
// ──────────────────────────────────────────────────────────────────────────────
async function handleServe(req, env) {
  const url    = new URL(req.url);
  const siteId = url.searchParams.get('site_id') || req.headers.get('X-Site-ID') || '';
  const format = url.searchParams.get('format') || 'banner_300x250';
  const pageUrl= url.searchParams.get('page')   || '';
  const f      = fp(req);
  const uid    = (url.searchParams.get('uid')?.length === 32 ? url.searchParams.get('uid') : null) || getUID(req) || mkUID();

  // Block very high-risk VPN traffic
  if (f.riskScore > 85 && f.isVPN) return j({ error: 'traffic_blocked', code: 'vpn_high_risk' }, 403);

  const profile = await getProfile(uid, env);

  // Fetch campaigns (KV cached 60s)
  const cacheKey = `camps:active`;
  let campaigns = null;
  try { const c = await env.AD_CACHE.get(cacheKey); if (c) campaigns = JSON.parse(c); } catch {}
  if (!campaigns) {
    const D = db(env);
    campaigns = await D.sel('campaigns',
      'status=eq.active&select=id,name,status,bid_type,ad_format,ad_type,budget_total,spent_total,impressions,clicks,cpm_rate,cpc_rate,geo_targets,target_countries,device_types,target_categories,target_keywords,advertiser_id,publisher_id,category,target_url,ad_title,ad_content,ad_description,cta_text,skip_duration,webview_url,iframe_url,creative_url&order=cpm_rate.desc&limit=200'
    ).catch(() => []);
    if (campaigns?.length) await env.AD_CACHE.put(cacheKey, JSON.stringify(campaigns), { expirationTtl: 60 });
  }

  if (!campaigns?.length) return j({ error: 'no_campaigns' }, 204);

  // Filter by format + score
  const formatFamily = format.startsWith('banner') ? 'banner' : format;
  const eligible = campaigns.filter(c => {
    const cf = (c.ad_format || c.ad_type || 'banner').toLowerCase();
    if (formatFamily === 'banner') return cf.includes('banner') || cf === 'display';
    return cf === formatFamily;
  });

  const scored = (eligible.length ? eligible : campaigns)
    .map(c => ({ c, score: scoreAd(c, profile, f) }))
    .filter(x => x.score >= 0)
    .sort((a, b) => b.score - a.score);

  if (!scored.length) return j({ error: 'no_matching_ad' }, 204);

  const { c: campaign } = scored[0];
  const gt = geoTier(f.country);

  // Revenue calculation — CPM or CPC
  const isCPC = (campaign.bid_type || 'cpm') === 'cpc';
  const cpmRate = isCPC ? 0 : Math.max(parseFloat(campaign.cpm_rate) || 0, gt.min_cpm);
  const cpcRate = isCPC ? Math.max(parseFloat(campaign.cpc_rate) || 0, gt.min_cpc) : parseFloat(campaign.cpc_rate) || 0;
  const impRevenue = isCPC ? 0 : cpmRate / 1000;

  // Revenue share
  const share = await getRevenueShare(env);
  const pubRevenue = impRevenue * share.publisher;

  // Build click tracking URL
  const clickUrl = `${url.origin}/click?cid=${campaign.id}&uid=${uid}&sid=${siteId}&dest=${encodeURIComponent(campaign.target_url || '#')}&bid=${campaign.bid_type || 'cpm'}`;

  // Build format-specific payload
  const adPayload = buildAdPayload(campaign, clickUrl, format);

  const D = db(env);
  const now = new Date().toISOString();
  env.ctx?.waitUntil?.(Promise.all([
    D.ins('ad_events', {
      event_type: 'impression', uid, campaign_id: campaign.id, site_id: siteId,
      advertiser_id: campaign.advertiser_id, ad_format: format, page_url: pageUrl,
      matched_category: profile?.top_category || null, country: f.country, city: f.city,
      device_type: f.deviceType, os_type: f.os, ip_address: f.ip, is_vpn: f.isVPN,
      cpm_rate: cpmRate, cpc_rate: cpcRate, revenue: impRevenue, created_at: now,
    }),
    D.upd('campaigns', { id: campaign.id }, {
      impressions: (parseInt(campaign.impressions) || 0) + 1,
      spent_total: ((parseFloat(campaign.spent_total) || 0) + impRevenue).toFixed(6),
    }),
  ]));

  return j({
    uid,
    ad: adPayload,
    _meta: {
      bid_type:   campaign.bid_type || 'cpm',
      cpm_rate:   cpmRate,
      cpc_rate:   cpcRate,
      geo_tier:   gt.tier,
      pub_share:  `${Math.round(share.publisher * 100)}%`,
      score:      Math.round(scored[0].score),
    },
  }, 200, {
    'Set-Cookie':    setCookie(uid, env.COOKIE_DOMAIN || ''),
    'Cache-Control': 'no-store, no-cache',
  });
}

// ──────────────────────────────────────────────────────────────────────────────
// CLICK — GET /click
// ──────────────────────────────────────────────────────────────────────────────
async function handleClick(req, env) {
  const url  = new URL(req.url);
  const cid  = url.searchParams.get('cid');
  const uid  = url.searchParams.get('uid');
  const sid  = url.searchParams.get('sid');
  const dest = url.searchParams.get('dest') || '#';
  const bidType = url.searchParams.get('bid') || 'cpm';

  if (cid && uid) {
    const rk = `clk:${uid}:${cid}`;
    const rl = await env.RATE_LIMIT.get(rk).catch(() => null);
    if (!rl) {
      await env.RATE_LIMIT.put(rk, '1', { expirationTtl: 3600 });
      const f = fp(req); const gt = geoTier(f.country);
      const D = db(env); const now = new Date().toISOString();
      // CPC ads earn on click; CPM earn on impression only
      const cpcRevenue = bidType === 'cpc' ? gt.min_cpc : 0;
      const share = await getRevenueShare(env);
      env.ctx?.waitUntil?.(Promise.all([
        D.ins('ad_events', {
          event_type: 'click', uid, campaign_id: cid, site_id: sid,
          country: f.country, city: f.city, device_type: f.deviceType, os_type: f.os,
          ip_address: f.ip, is_vpn: f.isVPN, cpc_rate: gt.min_cpc, revenue: cpcRevenue, created_at: now,
        }),
        // Update campaign click count + CPC spend
        cpcRevenue > 0 ? D.upd('campaigns', { id: cid }, { clicks: 'clicks+1' }) : D.upd('campaigns', { id: cid }, { clicks: 'clicks+1' }),
      ]));
    }
  }
  return Response.redirect(decodeURIComponent(dest), 302);
}

// ──────────────────────────────────────────────────────────────────────────────
// ANALYTICS
// ──────────────────────────────────────────────────────────────────────────────
async function handlePubAnalytics(req, env) {
  const u = new URL(req.url);
  const pid  = u.searchParams.get('publisher_id');
  const days = parseInt(u.searchParams.get('days') || '7');
  if (!pid) return j({ error: 'publisher_id required' }, 400);
  const since = new Date(Date.now() - days * 86400000).toISOString();
  const D = db(env);
  const [events, views, share] = await Promise.all([
    D.sel('ad_events', `publisher_id=eq.${pid}&created_at=gte.${since}&select=event_type,revenue,country,device_type,ad_format,created_at&limit=5000`),
    D.sel('page_views', `created_at=gte.${since}&select=country,device_type,browser,is_vpn&limit=2000`),
    getRevenueShare(env),
  ]);
  const imps = events.filter(e => e.event_type === 'impression');
  const clks = events.filter(e => e.event_type === 'click');
  const grossRev = imps.reduce((a, e) => a + (parseFloat(e.revenue) || 0), 0);
  const netRev   = grossRev * share.publisher;
  const daily = {};
  for (const e of events) {
    const d = e.created_at.split('T')[0];
    if (!daily[d]) daily[d] = { date: d, impressions: 0, clicks: 0, revenue: 0 };
    if (e.event_type === 'impression') { daily[d].impressions++; daily[d].revenue += (parseFloat(e.revenue) || 0) * share.publisher; }
    else daily[d].clicks++;
  }
  const cMap = {}, dMap = {}, fMap = {};
  for (const e of imps) {
    cMap[e.country || '?'] = (cMap[e.country || '?'] || 0) + 1;
    dMap[e.device_type || '?'] = (dMap[e.device_type || '?'] || 0) + 1;
    fMap[e.ad_format || '?'] = (fMap[e.ad_format || '?'] || 0) + 1;
  }
  const vpnPct = views.length ? ((views.filter(v => v.is_vpn).length / views.length) * 100).toFixed(1) : 0;
  return j({
    revenue_share: `${Math.round(share.publisher * 100)}%`,
    summary: {
      impressions: imps.length, clicks: clks.length,
      gross_revenue: +grossRev.toFixed(4), net_revenue: +netRev.toFixed(4),
      ctr: imps.length ? ((clks.length / imps.length) * 100).toFixed(2) : 0,
      eCPM: imps.length ? ((grossRev / imps.length) * 1000).toFixed(4) : 0,
      page_views: views.length, vpn_pct: vpnPct,
    },
    daily: Object.values(daily).sort((a, b) => a.date.localeCompare(b.date)),
    countries: Object.entries(cMap).sort((a,b)=>b[1]-a[1]).slice(0,20).map(([country,count])=>({country,count})),
    devices: Object.entries(dMap).map(([device,count])=>({device,count})),
    formats: Object.entries(fMap).map(([format,count])=>({format,count})),
  });
}

async function handleAdvAnalytics(req, env) {
  const u = new URL(req.url);
  const aid  = u.searchParams.get('advertiser_id');
  const days = parseInt(u.searchParams.get('days') || '30');
  if (!aid) return j({ error: 'advertiser_id required' }, 400);
  const since = new URL(req.url).searchParams.get('since') || new Date(Date.now() - days * 86400000).toISOString();
  const D = db(env);
  const [events, campaigns] = await Promise.all([
    D.sel('ad_events', `advertiser_id=eq.${aid}&created_at=gte.${since}&select=event_type,revenue,country,device_type,ad_format,matched_keyword,matched_category,created_at&limit=10000`),
    D.sel('campaigns', `advertiser_id=eq.${aid}&select=id,name,status,impressions,clicks,spent_total,budget_total,cpm_rate,cpc_rate,ad_format,bid_type`),
  ]);
  const imps = events.filter(e => e.event_type === 'impression');
  const clks = events.filter(e => e.event_type === 'click');
  const totalSpent = events.reduce((a, e) => a + (parseFloat(e.revenue) || 0), 0);
  const daily = {};
  for (const e of events) {
    const d = e.created_at.split('T')[0];
    if (!daily[d]) daily[d] = { date: d, impressions: 0, clicks: 0, spent: 0 };
    if (e.event_type === 'impression') { daily[d].impressions++; daily[d].spent += parseFloat(e.revenue) || 0; }
    else daily[d].clicks++;
  }
  const kwMap = {};
  for (const e of events.filter(e => e.matched_keyword)) {
    const k = e.matched_keyword;
    if (!kwMap[k]) kwMap[k] = { keyword: k, impressions: 0, clicks: 0 };
    if (e.event_type === 'impression') kwMap[k].impressions++;
    else kwMap[k].clicks++;
  }
  const cMap = {};
  for (const e of imps) { const c = e.country || '?'; if (!cMap[c]) cMap[c] = { country: c, impressions: 0, clicks: 0 }; cMap[c].impressions++; }
  for (const e of clks) { const c = e.country || '?'; if (!cMap[c]) cMap[c] = { country: c, impressions: 0, clicks: 0 }; cMap[c].clicks++; }
  return j({
    summary: {
      impressions: imps.length, clicks: clks.length, total_spent: +totalSpent.toFixed(4),
      ctr: imps.length ? ((clks.length / imps.length) * 100).toFixed(2) : 0,
      avg_cpm: imps.length ? ((totalSpent / imps.length) * 1000).toFixed(4) : 0,
    },
    daily:    Object.values(daily).sort((a,b)=>a.date.localeCompare(b.date)),
    keywords: Object.values(kwMap).sort((a,b)=>b.impressions-a.impressions).slice(0,20),
    countries:Object.values(cMap).sort((a,b)=>b.impressions-a.impressions).slice(0,20),
    campaigns: Array.isArray(campaigns) ? campaigns : [],
  });
}

async function handleAdminAnalytics(req, env) {
  const u = new URL(req.url);
  const days = parseInt(u.searchParams.get('days') || '30');
  const since = new Date(Date.now() - days * 86400000).toISOString();
  const D = db(env);
  const [events, profiles, share] = await Promise.all([
    D.sel('ad_events', `created_at=gte.${since}&select=event_type,revenue&limit=50000`),
    D.sel('user_profiles', `select=uid,country,device_type,top_category,is_vpn,page_count&limit=5000`),
    getRevenueShare(env),
  ]);
  const imps = events.filter(e => e.event_type === 'impression');
  const gross = events.reduce((a, e) => a + (parseFloat(e.revenue) || 0), 0);
  const catMap = {};
  for (const p of (profiles || [])) if (p.top_category) catMap[p.top_category] = (catMap[p.top_category] || 0) + 1;
  return j({
    revenue_config: { publisher_share: `${Math.round(share.publisher * 100)}%`, admin_share: `${Math.round(share.admin * 100)}%` },
    summary: {
      total_impressions: imps.length,
      total_clicks:      events.filter(e => e.event_type === 'click').length,
      gross_revenue:     +gross.toFixed(4),
      admin_revenue:     +(gross * share.admin).toFixed(4),
      publisher_revenue: +(gross * share.publisher).toFixed(4),
      tracked_users:     (profiles || []).length,
      vpn_users:         (profiles || []).filter(p => p.is_vpn).length,
    },
    top_categories: Object.entries(catMap).sort((a,b)=>b[1]-a[1]).slice(0,10).map(([c,n])=>({category:c,users:n})),
  });
}

// ──────────────────────────────────────────────────────────────────────────────
// AD.JS EMBED SCRIPT — GET /ad.js
// ──────────────────────────────────────────────────────────────────────────────
function getAdScript(origin) {
  return `/* AdNexus Ad Script v3.1 — ${origin} */
(function(w,d){
'use strict';
var W='${origin}',C='__adnx_uid';
function gCk(n){var m=document.cookie.match(new RegExp('(?:^|;\\\\s*)'+n+'=([^;]*)'));return m?m[1]:null;}
function sCk(n,v){document.cookie=n+'='+v+';max-age=63072000;path=/;SameSite=None;Secure';}
function uid(){var u=gCk(C);if(!u||u.length!==32){u='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'.replace(/x/g,function(){return(Math.random()*16|0).toString(16)});sCk(C,u);}return u;}
function track(u){var d={uid:u,url:location.href,title:document.title,referrer:document.referrer,site_id:w.__adnx_site||''};if(navigator.sendBeacon)navigator.sendBeacon(W+'/track',new Blob([JSON.stringify(d)],{type:'application/json'}));else fetch(W+'/track',{method:'POST',body:JSON.stringify(d),headers:{'Content-Type':'application/json'},credentials:'include',keepalive:true}).catch(function(){});}
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}

/* ── Render ad by format ── */
function render(el, ad) {
  var f = ad.format || 'banner_300x250';
  var html = '';

  if (f === 'interstitial') {
    /* Full-screen overlay with skip timer */
    html = '<div id="adnx-overlay" style="position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.85);z-index:999999;display:flex;align-items:center;justify-content:center;font-family:sans-serif">'
      + '<div style="background:#fff;max-width:520px;width:90%;border-radius:12px;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,.4)">'
      + (ad.image_url ? '<img src="'+esc(ad.image_url)+'" style="width:100%;max-height:260px;object-fit:cover">' : '')
      + '<div style="padding:24px">'
      + '<div style="font-size:20px;font-weight:700;color:#111;margin-bottom:8px">'+esc(ad.title)+'</div>'
      + '<div style="font-size:15px;color:#444;margin-bottom:6px">'+esc(ad.content)+'</div>'
      + '<div style="font-size:13px;color:#666;margin-bottom:20px">'+esc(ad.description)+'</div>'
      + '<div style="display:flex;gap:10px;align-items:center">'
      + '<a href="'+ad.click_url+'" target="_blank" rel="noopener" style="flex:1;background:#2563eb;color:#fff;padding:12px;border-radius:8px;text-align:center;text-decoration:none;font-weight:600">'+esc(ad.cta_text)+'</a>'
      + '<button id="adnx-skip" onclick="adnxCloseInterstitial()" style="background:#f3f4f6;border:none;padding:12px 16px;border-radius:8px;cursor:pointer;color:#888;font-size:13px" disabled>Skip in <span id="adnx-st">'+esc(String(ad.skip_after||5))+'</span>s</button>'
      + '</div></div></div></div>';
    document.body.insertAdjacentHTML('beforeend', html);
    /* Countdown timer */
    var t = ad.skip_after || 5;
    var iv = setInterval(function(){
      t--;
      var st = document.getElementById('adnx-st');
      var sb = document.getElementById('adnx-skip');
      if (st) st.textContent = t;
      if (t <= 0) {
        clearInterval(iv);
        if (sb) { sb.textContent = 'Skip'; sb.disabled = false; sb.style.color = '#333'; }
      }
    }, 1000);
    w.adnxCloseInterstitial = function(){ var o=document.getElementById('adnx-overlay'); if(o) o.remove(); };
    return;
  }

  if (f === 'webview') {
    /* WebView: iframe loads webview_url, then shows CTA */
    html = '<div id="adnx-overlay" style="position:fixed;top:0;left:0;width:100%;height:100%;background:#000;z-index:999999;display:flex;flex-direction:column;font-family:sans-serif">'
      + '<iframe src="'+esc(ad.webview_url)+'" style="flex:1;border:0;width:100%"></iframe>'
      + '<div style="background:#111;padding:12px 16px;display:flex;gap:10px;align-items:center">'
      + '<span style="color:#fff;flex:1;font-size:13px">'+esc(ad.title)+'</span>'
      + '<a href="'+ad.click_url+'" target="_blank" rel="noopener" style="background:#2563eb;color:#fff;padding:10px 18px;border-radius:8px;text-decoration:none;font-weight:600;font-size:13px">'+esc(ad.cta_text)+'</a>'
      + '<button id="adnx-skip" onclick="adnxCloseInterstitial()" style="background:#333;border:none;color:#aaa;padding:10px 14px;border-radius:8px;cursor:pointer;font-size:13px" disabled>'+esc(String(ad.skip_after||5))+'s</button>'
      + '</div></div>';
    document.body.insertAdjacentHTML('beforeend', html);
    var t2 = ad.skip_after || 5;
    var iv2 = setInterval(function(){
      t2--;
      var sb2 = document.getElementById('adnx-skip');
      if (sb2) sb2.textContent = t2 > 0 ? t2+'s' : 'Skip';
      if (t2 <= 0) { clearInterval(iv2); if(sb2){sb2.disabled=false;sb2.style.color='#fff';} }
    }, 1000);
    w.adnxCloseInterstitial = function(){ var o=document.getElementById('adnx-overlay'); if(o) o.remove(); };
    return;
  }

  if (f === 'iframe') {
    /* iFrame: similar to webview */
    html = '<div id="adnx-overlay" style="position:fixed;top:0;left:0;width:100%;height:100%;background:#000;z-index:999999;display:flex;flex-direction:column;font-family:sans-serif">'
      + '<div style="background:#111;padding:8px 16px;display:flex;align-items:center;gap:10px">'
      + '<span style="color:#fff;font-size:13px;flex:1">'+esc(ad.title)+'</span>'
      + '<span style="color:#888;font-size:11px">Advertisement</span></div>'
      + '<iframe src="'+esc(ad.iframe_url)+'" style="flex:1;border:0;width:100%;background:#fff"></iframe>'
      + '<div style="background:#111;padding:12px 16px;display:flex;gap:10px;align-items:center">'
      + '<a href="'+ad.click_url+'" target="_blank" rel="noopener" style="flex:1;background:#059669;color:#fff;padding:10px;border-radius:8px;text-align:center;text-decoration:none;font-weight:600;font-size:13px">'+esc(ad.cta_text)+'</a>'
      + '<button id="adnx-skip" onclick="adnxCloseInterstitial()" style="background:#333;border:none;color:#aaa;padding:10px 14px;border-radius:8px;cursor:pointer;font-size:13px" disabled>'+esc(String(ad.skip_after||5))+'s</button>'
      + '</div></div>';
    document.body.insertAdjacentHTML('beforeend', html);
    var t3 = ad.skip_after || 5;
    var iv3 = setInterval(function(){
      t3--;
      var sb3 = document.getElementById('adnx-skip');
      if (sb3) sb3.textContent = t3 > 0 ? t3+'s' : 'Skip';
      if (t3 <= 0) { clearInterval(iv3); if(sb3){sb3.disabled=false;sb3.style.color='#fff';} }
    }, 1000);
    w.adnxCloseInterstitial = function(){ var o=document.getElementById('adnx-overlay'); if(o) o.remove(); };
    return;
  }

  /* ── Banner (default) ── */
  if (ad.image_url) {
    html = '<a href="'+ad.click_url+'" target="_blank" rel="noopener" style="display:block">'
      + '<img src="'+esc(ad.image_url)+'" style="max-width:100%;height:auto;border:0;border-radius:4px" alt="'+esc(ad.content)+'">'
      + '</a>';
  } else {
    html = '<a href="'+ad.click_url+'" target="_blank" rel="noopener" style="text-decoration:none;display:block">'
      + '<div style="background:#f0f7ff;border:1px solid #bfdbfe;padding:12px 16px;border-radius:8px">'
      + '<div style="color:#1d4ed8;font-weight:700;font-size:14px">'+esc(ad.headline||ad.content)+'</div>'
      + (ad.description?'<div style="color:#374151;font-size:12px;margin-top:4px">'+esc(ad.description)+'</div>':'')
      + '<div style="color:#065f46;font-size:11px;margin-top:6px;font-weight:600">'+esc(ad.cta_text||'Learn More')+'</div>'
      + '</div></a>';
  }
  html += '<div style="font-size:10px;color:#9ca3af;text-align:right;margin-top:2px">Ad</div>';
  el.innerHTML = html;
}

/* ── Auto-serve all [data-adnx-slot] elements ── */
function init() {
  var u = uid();
  track(u);
  var slots = d.querySelectorAll('[data-adnx-slot]');
  for (var i = 0; i < slots.length; i++) {
    (function(s) {
      var fmt = s.getAttribute('data-adnx-format') || 'banner_300x250';
      var sid = s.id || ('adnx-'+Math.random().toString(36).slice(2));
      if (!s.id) s.id = sid;
      fetch(W+'/serve?site_id='+encodeURIComponent(w.__adnx_site||'')+'&format='+encodeURIComponent(fmt)+'&page='+encodeURIComponent(location.href)+'&uid='+u,
        {credentials:'include'})
        .then(function(r){return r.status===200?r.json():null;})
        .then(function(data){if(data&&data.ad)render(s,data.ad);})
        .catch(function(){});
    })(slots[i]);
  }
  /* SPA navigation tracking */
  var lastUrl = location.href;
  new MutationObserver(function(){
    if (location.href !== lastUrl) { lastUrl = location.href; track(uid()); }
  }).observe(d.body||d.documentElement,{subtree:true,childList:true});
}

/* ── Public API ── */
w.adnxServe = function(elId, siteId, format) {
  var u = uid();
  fetch(W+'/serve?site_id='+encodeURIComponent(siteId||w.__adnx_site||'')+'&format='+encodeURIComponent(format||'banner_300x250')+'&page='+encodeURIComponent(location.href)+'&uid='+u,
    {credentials:'include'})
    .then(function(r){return r.json();})
    .then(function(data){var el=d.getElementById(elId);if(el&&data&&data.ad)render(el,data.ad);})
    .catch(function(){});
};

if (d.readyState==='loading') d.addEventListener('DOMContentLoaded',init); else init();
})(window,document);`;
}

// ──────────────────────────────────────────────────────────────────────────────
// 1×1 GIF
// ──────────────────────────────────────────────────────────────────────────────
const PIXEL = new Uint8Array([0x47,0x49,0x46,0x38,0x39,0x61,0x01,0x00,0x01,0x00,0x80,0x00,0x00,0xFF,0xFF,0xFF,0x00,0x00,0x00,0x21,0xF9,0x04,0x01,0x00,0x00,0x00,0x00,0x2C,0x00,0x00,0x00,0x00,0x01,0x00,0x01,0x00,0x00,0x02,0x02,0x44,0x01,0x00,0x3B]);

// ──────────────────────────────────────────────────────────────────────────────
// MAIN EXPORT
// ──────────────────────────────────────────────────────────────────────────────
export default {
  async fetch(req, env, ctx) {
    env.ctx = ctx;
    const url = new URL(req.url), path = url.pathname, method = req.method;

    if (method === 'OPTIONS') return optR();

    if (path === '/ad.js')
      return new Response(getAdScript(url.origin), { headers: { 'Content-Type': 'application/javascript;charset=utf-8', 'Cache-Control': 'public,max-age=3600', ...CH } });

    if (path === '/track'  && method === 'POST') return handleTrack(req, env);
    if (path === '/serve')                        return handleServe(req, env);
    if (path === '/click')                        return handleClick(req, env);

    if (path === '/pixel.gif')
      return new Response(PIXEL, { headers: { 'Content-Type': 'image/gif', 'Cache-Control': 'no-store' } });

    if (path === '/analytics/publisher')  return handlePubAnalytics(req, env);
    if (path === '/analytics/advertiser') return handleAdvAnalytics(req, env);
    if (path === '/analytics/admin')      return handleAdminAnalytics(req, env);

    if (path === '/health')
      return j({ status: 'ok', version: '3.1.0', ts: new Date().toISOString() });

    return j({ error: 'not found', path }, 404);
  },
};
