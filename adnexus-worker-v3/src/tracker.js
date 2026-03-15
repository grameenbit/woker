// ─── tracker.js ───────────────────────────────────────────────────────────────
// POST /track — receives page views from the ad.js embed script.
// 1. Sets/reads __adnx_uid cookie
// 2. Updates KV profile (fast, synchronous)
// 3. Writes to Supabase user_profiles + page_views (background)
// 4. Triggers AI interest analysis every 5 page views (background)

import { fingerprint }                          from './fingerprint.js';
import { getUIDFromRequest, newUID, buildSetCookie, getProfileFromKV, saveProfileToKV, mergePageVisit } from './cookie.js';
import { analyzeWithAI, heuristicAnalyze, shouldAnalyze } from './ai-analyze.js';
import { createDB }                             from './db.js';
import { json }                                 from './cors.js';

export async function handleTrack(req, env) {
  let body = {};
  try { body = await req.json(); } catch {}

  const f   = fingerprint(req);
  const now = new Date().toISOString();

  // Resolve UID: body > cookie > new
  const uid = (body.uid?.length === 32 ? body.uid : null)
           || getUIDFromRequest(req)
           || newUID();

  // Build visit record
  const visit = {
    uid,
    url:      body.url      || '',
    title:    body.title    || '',
    referrer: body.referrer || '',
    ts:       now,
    siteId:   body.site_id  || '',
    ...f,
  };

  // Update KV profile (synchronous — must finish before response)
  let profile = await getProfileFromKV(uid, env);
  profile = mergePageVisit(profile, visit);
  await saveProfileToKV(uid, profile, env);

  const D = createDB(env);

  // Background: DB writes + optional AI analysis
  const bgWork = Promise.all([
    // Upsert user profile in Supabase
    D.upsert('user_profiles', {
      uid,
      first_seen:   profile.first_seen || now,
      last_seen:    now,
      page_count:   profile.page_count,
      country:      f.country,
      region:       f.region,
      city:         f.city,
      timezone:     f.timezone,
      ip_address:   f.ip,
      isp:          f.isp,
      is_vpn:       f.isVPN,
      is_proxy:     f.isProxy,
      device_type:  f.deviceType,
      os_type:      f.os,
      browser:      f.browser,
      interests:    JSON.stringify(profile.interests    || []),
      keywords:     JSON.stringify(profile.keywords     || []),
      top_category: profile.top_category || null,
      page_history: JSON.stringify(profile.page_history || []),
      risk_score:   f.riskScore,
    }),

    // Insert page view event
    D.insert('page_views', {
      uid,
      site_id:     visit.siteId,
      page_url:    visit.url,
      page_title:  visit.title,
      referrer:    visit.referrer,
      country:     f.country,
      city:        f.city,
      device_type: f.deviceType,
      os_type:     f.os,
      browser:     f.browser,
      is_vpn:      f.isVPN,
      is_proxy:    f.isProxy,
      ip_address:  f.ip,
      risk_score:  f.riskScore,
      created_at:  now,
    }),

    // AI interest analysis (every 5 page views)
    shouldAnalyze(profile.page_count) ? (async () => {
      const analysis = (await analyzeWithAI(profile.page_history, env))
                    || heuristicAnalyze(profile.page_history);
      if (analysis) {
        profile.interests    = analysis.interests;
        profile.keywords     = analysis.keywords;
        profile.top_category = analysis.top_category;
        await Promise.all([
          saveProfileToKV(uid, profile, env),
          D.update('user_profiles', { uid }, {
            interests:    JSON.stringify(analysis.interests),
            keywords:     JSON.stringify(analysis.keywords),
            top_category: analysis.top_category,
          }),
        ]);
      }
    })() : Promise.resolve(),
  ]);

  env.ctx?.waitUntil?.(bgWork);

  return json(
    { uid, ok: true, page_count: profile.page_count },
    200,
    { 'Set-Cookie': buildSetCookie(uid, env.COOKIE_DOMAIN || '') }
  );
}
