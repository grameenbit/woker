// ─── analytics.js ─────────────────────────────────────────────────────────────
// Stats endpoints for publisher / advertiser / admin dashboards.
//   GET /analytics/publisher?publisher_id=&days=7
//   GET /analytics/advertiser?advertiser_id=&days=30
//   GET /analytics/admin?days=30

import { createDB }       from './db.js';
import { getRevenueShare } from './adserve.js';
import { json }           from './cors.js';

// ── Publisher stats ────────────────────────────────────────────────────────────
export async function handlePublisherAnalytics(req, env) {
  const u = new URL(req.url);
  const pid  = u.searchParams.get('publisher_id');
  const days = parseInt(u.searchParams.get('days') || '7');
  if (!pid) return json({ error: 'publisher_id required' }, 400);

  const since = new Date(Date.now() - days * 86400000).toISOString();
  const D     = createDB(env);

  const [events, views, sites, share] = await Promise.all([
    D.select('ad_events',
      `publisher_id=eq.${pid}&created_at=gte.${since}` +
      `&select=event_type,revenue,country,device_type,ad_format,created_at&limit=5000`),
    D.select('page_views',
      `created_at=gte.${since}&select=country,device_type,browser,is_vpn&limit=2000`),
    D.select('sites', `publisher_id=eq.${pid}&select=id,domain,status`),
    getRevenueShare(env),
  ]);

  const imps     = events.filter(e => e.event_type === 'impression');
  const clks     = events.filter(e => e.event_type === 'click');
  const grossRev = imps.reduce((a, e) => a + (parseFloat(e.revenue) || 0), 0);
  const netRev   = grossRev * share.publisher;

  // Daily breakdown
  const daily = {};
  for (const e of events) {
    const d = e.created_at.split('T')[0];
    if (!daily[d]) daily[d] = { date: d, impressions: 0, clicks: 0, revenue: 0 };
    if (e.event_type === 'impression') { daily[d].impressions++; daily[d].revenue += (parseFloat(e.revenue) || 0) * share.publisher; }
    else daily[d].clicks++;
  }

  // Breakdowns
  const countryMap = {}, deviceMap = {}, formatMap = {};
  for (const e of imps) {
    countryMap[e.country    || '?'] = (countryMap[e.country    || '?'] || 0) + 1;
    deviceMap [e.device_type|| '?'] = (deviceMap [e.device_type|| '?'] || 0) + 1;
    formatMap [e.ad_format  || '?'] = (formatMap [e.ad_format  || '?'] || 0) + 1;
  }

  const vpnCount = (views || []).filter(v => v.is_vpn).length;

  return json({
    revenue_share: `${Math.round(share.publisher * 100)}%`,
    summary: {
      impressions:   imps.length,
      clicks:        clks.length,
      gross_revenue: +grossRev.toFixed(4),
      net_revenue:   +netRev.toFixed(4),
      ctr:           imps.length ? ((clks.length / imps.length) * 100).toFixed(2) : '0',
      eCPM:          imps.length ? ((grossRev / imps.length) * 1000).toFixed(4) : '0',
      page_views:    (views || []).length,
      vpn_pct:       views.length ? ((vpnCount / views.length) * 100).toFixed(1) : '0',
    },
    daily:    Object.values(daily).sort((a, b) => a.date.localeCompare(b.date)),
    countries: Object.entries(countryMap).sort((a,b)=>b[1]-a[1]).slice(0,20).map(([country,count])=>({country,count})),
    devices:   Object.entries(deviceMap).map(([device,count])=>({device,count})),
    formats:   Object.entries(formatMap).map(([format,count])=>({format,count})),
    sites:     Array.isArray(sites) ? sites : [],
  });
}

// ── Advertiser stats ───────────────────────────────────────────────────────────
export async function handleAdvertiserAnalytics(req, env) {
  const u = new URL(req.url);
  const aid  = u.searchParams.get('advertiser_id');
  const days = parseInt(u.searchParams.get('days') || '30');
  if (!aid) return json({ error: 'advertiser_id required' }, 400);

  const since = new Date(Date.now() - days * 86400000).toISOString();
  const D     = createDB(env);

  const [events, campaigns] = await Promise.all([
    D.select('ad_events',
      `advertiser_id=eq.${aid}&created_at=gte.${since}` +
      `&select=event_type,revenue,country,device_type,ad_format,matched_keyword,matched_category,created_at&limit=10000`),
    D.select('campaigns',
      `advertiser_id=eq.${aid}&select=id,name,status,impressions,clicks,spent_total,budget_total,cpm_rate,cpc_rate,ad_format,bid_type`),
  ]);

  const imps       = events.filter(e => e.event_type === 'impression');
  const clks       = events.filter(e => e.event_type === 'click');
  const totalSpent = events.reduce((a, e) => a + (parseFloat(e.revenue) || 0), 0);

  const daily = {};
  for (const e of events) {
    const d = e.created_at.split('T')[0];
    if (!daily[d]) daily[d] = { date: d, impressions: 0, clicks: 0, spent: 0 };
    if (e.event_type === 'impression') { daily[d].impressions++; daily[d].spent += parseFloat(e.revenue) || 0; }
    else daily[d].clicks++;
  }

  // Keyword performance
  const kwMap = {};
  for (const e of events.filter(e => e.matched_keyword)) {
    const k = e.matched_keyword;
    if (!kwMap[k]) kwMap[k] = { keyword: k, impressions: 0, clicks: 0 };
    if (e.event_type === 'impression') kwMap[k].impressions++;
    else kwMap[k].clicks++;
  }

  // Country performance
  const cMap = {};
  for (const e of imps) { const c = e.country || '?'; if (!cMap[c]) cMap[c] = {country:c,impressions:0,clicks:0}; cMap[c].impressions++; }
  for (const e of clks) { const c = e.country || '?'; if (!cMap[c]) cMap[c] = {country:c,impressions:0,clicks:0}; cMap[c].clicks++; }

  return json({
    summary: {
      impressions:  imps.length,
      clicks:       clks.length,
      total_spent:  +totalSpent.toFixed(4),
      ctr:          imps.length ? ((clks.length / imps.length) * 100).toFixed(2) : '0',
      avg_cpm:      imps.length ? ((totalSpent / imps.length) * 1000).toFixed(4) : '0',
    },
    daily:    Object.values(daily).sort((a,b) => a.date.localeCompare(b.date)),
    keywords: Object.values(kwMap).sort((a,b) => b.impressions - a.impressions).slice(0, 20),
    countries:Object.values(cMap).sort((a,b) => b.impressions - a.impressions).slice(0, 20),
    campaigns: Array.isArray(campaigns) ? campaigns : [],
  });
}

// ── Admin stats ────────────────────────────────────────────────────────────────
export async function handleAdminAnalytics(req, env) {
  const u    = new URL(req.url);
  const days = parseInt(u.searchParams.get('days') || '30');
  const since= new Date(Date.now() - days * 86400000).toISOString();
  const D    = createDB(env);

  const [events, profiles, share] = await Promise.all([
    D.select('ad_events', `created_at=gte.${since}&select=event_type,revenue&limit=50000`),
    D.select('user_profiles', `select=uid,country,device_type,top_category,is_vpn,page_count&limit=5000`),
    getRevenueShare(env),
  ]);

  const imps  = events.filter(e => e.event_type === 'impression');
  const gross = events.reduce((a, e) => a + (parseFloat(e.revenue) || 0), 0);

  const catMap = {};
  for (const p of (profiles || [])) if (p.top_category) catMap[p.top_category] = (catMap[p.top_category] || 0) + 1;

  return json({
    revenue_config: {
      publisher_share: `${Math.round(share.publisher * 100)}%`,
      admin_share:     `${Math.round(share.admin     * 100)}%`,
    },
    summary: {
      total_impressions: imps.length,
      total_clicks:      events.filter(e => e.event_type === 'click').length,
      gross_revenue:     +gross.toFixed(4),
      admin_revenue:     +(gross * share.admin).toFixed(4),
      publisher_revenue: +(gross * share.publisher).toFixed(4),
      tracked_users:     (profiles || []).length,
      vpn_users:         (profiles || []).filter(p => p.is_vpn).length,
    },
    top_categories: Object.entries(catMap)
      .sort((a,b) => b[1]-a[1]).slice(0, 10)
      .map(([category, users]) => ({ category, users })),
  });
}
