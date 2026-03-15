// ─── index.js ─────────────────────────────────────────────────────────────────
// AdNexus Worker v3.1 — Main Router
//
// Routes:
//   GET  /ad.js                    Publisher embed script (auto-tracking + ad slots)
//   POST /track                    Page view + interest tracking
//   GET  /serve                    Ad serving (JSON response)
//   GET  /click                    Click tracking + redirect to target URL
//   GET  /pixel.gif                1×1 tracking pixel (noscript / email)
//   GET  /analytics/publisher      Publisher stats API
//   GET  /analytics/advertiser     Advertiser stats API
//   GET  /analytics/admin          Admin stats API
//   GET  /health                   Health check

import { json, preflight, CORS_HEADERS } from './cors.js';
import { handleTrack }                   from './tracker.js';
import { handleServe, handleClick }      from './adserve.js';
import {
  handlePublisherAnalytics,
  handleAdvertiserAnalytics,
  handleAdminAnalytics,
} from './analytics.js';

// ── 1×1 transparent GIF ───────────────────────────────────────────────────────
const PIXEL_GIF = new Uint8Array([
  0x47,0x49,0x46,0x38,0x39,0x61,0x01,0x00,0x01,0x00,
  0x80,0x00,0x00,0xFF,0xFF,0xFF,0x00,0x00,0x00,0x21,
  0xF9,0x04,0x01,0x00,0x00,0x00,0x00,0x2C,0x00,0x00,
  0x00,0x00,0x01,0x00,0x01,0x00,0x00,0x02,0x02,0x44,
  0x01,0x00,0x3B,
]);

// ── Publisher embed script (/ad.js) ───────────────────────────────────────────
// This is the script publishers paste on their website.
// It handles:
//   1. UID cookie (cross-site, 2-year persistence)
//   2. Page view tracking (POST /track)
//   3. Auto-serve ads into [data-adnx-slot] elements
//   4. Renders 4 ad formats: banner | interstitial | webview | iframe
//   5. SPA navigation tracking via MutationObserver
function buildAdScript(origin) {
  return `/* AdNexus Ad Script v3.1 — ${origin} */
(function(w,d){
'use strict';
var W='${origin}',C='__adnx_uid';
/* ── UID ── */
function gCk(n){var m=document.cookie.match(new RegExp('(?:^|;\\\\s*)'+n+'=([^;]*)'));return m?m[1]:null;}
function sCk(n,v){document.cookie=n+'='+v+';max-age=63072000;path=/;SameSite=None;Secure';}
function uid(){var u=gCk(C);if(!u||u.length!==32){u='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'.replace(/x/g,function(){return(Math.random()*16|0).toString(16)});sCk(C,u);}return u;}
/* ── Track page view ── */
function track(u){var d={uid:u,url:location.href,title:document.title,referrer:document.referrer,site_id:w.__adnx_site||''};if(navigator.sendBeacon)navigator.sendBeacon(W+'/track',new Blob([JSON.stringify(d)],{type:'application/json'}));else fetch(W+'/track',{method:'POST',body:JSON.stringify(d),headers:{'Content-Type':'application/json'},keepalive:true}).catch(function(){});}
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
/* ── Render ad by format ── */
function render(el,ad){
  var f=ad.format||'banner_300x250';
  if(f==='interstitial'){
    document.body.insertAdjacentHTML('beforeend',
      '<div id="adnx-ol" style="position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.85);z-index:999999;display:flex;align-items:center;justify-content:center;font-family:sans-serif">'
      +'<div style="background:#fff;max-width:520px;width:90%;border-radius:12px;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,.4)">'
      +(ad.image_url?'<img src="'+esc(ad.image_url)+'" style="width:100%;max-height:260px;object-fit:cover">':'')
      +'<div style="padding:24px">'
      +'<div style="font-size:20px;font-weight:700;color:#111;margin-bottom:8px">'+esc(ad.title)+'</div>'
      +'<div style="font-size:15px;color:#444;margin-bottom:6px">'+esc(ad.content)+'</div>'
      +'<div style="font-size:13px;color:#666;margin-bottom:20px">'+esc(ad.description)+'</div>'
      +'<div style="display:flex;gap:10px;align-items:center">'
      +'<a href="'+ad.click_url+'" target="_blank" rel="noopener" style="flex:1;background:#2563eb;color:#fff;padding:12px;border-radius:8px;text-align:center;text-decoration:none;font-weight:600">'+esc(ad.cta_text)+'</a>'
      +'<button id="adnx-sk" onclick="adnxClose()" style="background:#f3f4f6;border:none;padding:12px 16px;border-radius:8px;cursor:pointer;color:#888;font-size:13px" disabled>Skip in <span id="adnx-st">'+esc(String(ad.skip_after||5))+'</span>s</button>'
      +'</div></div></div></div>');
    var t=ad.skip_after||5,iv=setInterval(function(){t--;var st=document.getElementById('adnx-st'),sb=document.getElementById('adnx-sk');if(st)st.textContent=t;if(t<=0){clearInterval(iv);if(sb){sb.textContent='Skip';sb.disabled=false;sb.style.color='#333';}}},1000);
    w.adnxClose=function(){var o=document.getElementById('adnx-ol');if(o)o.remove();};
    return;
  }
  if(f==='webview'||f==='iframe'){
    var iUrl=f==='webview'?ad.webview_url:ad.iframe_url;
    document.body.insertAdjacentHTML('beforeend',
      '<div id="adnx-ol" style="position:fixed;bottom:-400px;right:20px;width:320px;height:400px;background:#fff;z-index:999999;display:flex;flex-direction:column;font-family:sans-serif;box-shadow:0 -4px 20px rgba(0,0,0,0.2);border-radius:12px 12px 0 0;transition:bottom 0.5s ease-in-out;overflow:hidden;">'
      +'<div style="background:#111;padding:8px 16px;display:flex;align-items:center;gap:10px"><span style="color:#fff;font-size:13px;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">'+esc(ad.title)+'</span><span style="color:#888;font-size:11px">Ad</span></div>'
      +'<iframe src="'+esc(iUrl)+'" style="flex:1;border:0;width:100%;background:#fff"></iframe>'
      +'<div style="background:#111;padding:12px 16px;display:flex;gap:10px;align-items:center">'
      +'<a href="'+ad.click_url+'" target="_blank" rel="noopener" style="flex:1;background:#2563eb;color:#fff;padding:10px;border-radius:8px;text-align:center;text-decoration:none;font-weight:600;font-size:13px">'+esc(ad.cta_text)+'</a>'
      +'<button id="adnx-sk" onclick="adnxClose()" style="background:#333;border:none;color:#aaa;padding:10px 14px;border-radius:8px;cursor:pointer;font-size:13px" disabled>'+esc(String(ad.skip_after||5))+'s</button>'
      +'</div></div>');
    setTimeout(function(){var o=document.getElementById('adnx-ol');if(o)o.style.bottom='0px';},100);
    var t2=ad.skip_after||5,iv2=setInterval(function(){t2--;var sb2=document.getElementById('adnx-sk');if(sb2)sb2.textContent=t2>0?t2+'s':'Skip';if(t2<=0){clearInterval(iv2);if(sb2){sb2.disabled=false;sb2.style.color='#fff';}}},1000);
    w.adnxClose=function(){var o=document.getElementById('adnx-ol');if(o){o.style.bottom='-400px';setTimeout(function(){o.remove();},500);}};
    return;
  }
  /* Banner */
  var h='';
  if(ad.image_url){h='<a href="'+ad.click_url+'" target="_blank" rel="noopener" style="display:block"><img src="'+esc(ad.image_url)+'" style="max-width:100%;height:auto;border:0;border-radius:4px" alt="'+esc(ad.content)+'"></a>';}
  else{h='<a href="'+ad.click_url+'" target="_blank" rel="noopener" style="text-decoration:none;display:block"><div style="background:#f0f7ff;border:1px solid #bfdbfe;padding:12px 16px;border-radius:8px"><div style="color:#1d4ed8;font-weight:700;font-size:14px">'+esc(ad.headline||ad.content)+'</div>'+(ad.description?'<div style="color:#374151;font-size:12px;margin-top:4px">'+esc(ad.description)+'</div>':'')+'<div style="color:#065f46;font-size:11px;margin-top:6px;font-weight:600">'+esc(ad.cta_text||'Learn More')+'</div></div></a>';}
  h+='<div style="font-size:10px;color:#9ca3af;text-align:right;margin-top:2px">Ad</div>';
  el.innerHTML=h;
}
/* ── Serve ad into element ── */
function serve(el,fmt,u){
  fetch(W+'/serve?site_id='+encodeURIComponent(w.__adnx_site||'')+'&format='+encodeURIComponent(fmt)+'&page='+encodeURIComponent(location.href)+'&uid='+u)
    .then(function(r){return r.status===200?r.json():null;})
    .then(function(data){if(data&&data.ad)render(el,data.ad);})
    .catch(function(){});
}
/* ── Public API ── */
w.adnxServe=function(id,site,fmt){var u=uid();serve(d.getElementById(id),fmt||'banner_300x250',u);};
/* ── Auto-init ── */
function init(){
  var u=uid();track(u);
  d.querySelectorAll('[data-adnx-slot]').forEach(function(s){
    var fmt=s.getAttribute('data-adnx-format')||'banner_300x250';
    if(!s.id)s.id='adnx-'+Math.random().toString(36).slice(2);
    serve(s,fmt,u);
  });
  var last=location.href;
  new MutationObserver(function(){if(location.href!==last){last=location.href;track(uid());}}).observe(d.body||d.documentElement,{subtree:true,childList:true});
}
if(d.readyState==='loading')d.addEventListener('DOMContentLoaded',init);else init();
})(window,document);`;
}

// ── Main Worker export ─────────────────────────────────────────────────────────
export default {
  async fetch(req, env, ctx) {
    // Attach ctx so background ops can use waitUntil()
    env.ctx = ctx;

    const url    = new URL(req.url);
    const path   = url.pathname;
    const method = req.method;

    // CORS preflight
    if (method === 'OPTIONS') return preflight();

    // ── Routes ────────────────────────────────────────────────────────────────
    if (path === '/ad.js')
      return new Response(buildAdScript(url.origin), {
        headers: {
          'Content-Type':  'application/javascript; charset=utf-8',
          'Cache-Control': 'public, max-age=3600',
          ...CORS_HEADERS,
        },
      });

    if (path === '/track'  && method === 'POST') return handleTrack(req, env);
    if (path === '/serve')                        return handleServe(req, env);
    if (path === '/click')                        return handleClick(req, env);

    if (path === '/pixel.gif')
      return new Response(PIXEL_GIF, {
        headers: { 'Content-Type': 'image/gif', 'Cache-Control': 'no-store' },
      });

    if (path === '/analytics/publisher')  return handlePublisherAnalytics(req, env);
    if (path === '/analytics/advertiser') return handleAdvertiserAnalytics(req, env);
    if (path === '/analytics/admin')      return handleAdminAnalytics(req, env);

    if (path === '/health')
      return json({ status: 'ok', version: '3.1.0', ts: new Date().toISOString() });

    return json({ error: 'not found', path }, 404);
  },
};  d.querySelectorAll('[data-adnx-slot]').forEach(function(s){
    var fmt=s.getAttribute('data-adnx-format')||'banner_300x250';
    if(!s.id)s.id='adnx-'+Math.random().toString(36).slice(2);
    serve(s,fmt,u);
  });
  var last=location.href;
  new MutationObserver(function(){if(location.href!==last){last=location.href;track(uid());}}).observe(d.body||d.documentElement,{subtree:true,childList:true});
}
if(d.readyState==='loading')d.addEventListener('DOMContentLoaded',init);else init();
})(window,document);`;
}

// ── Main Worker export ─────────────────────────────────────────────────────────
export default {
  async fetch(req, env, ctx) {
    // Attach ctx so background ops can use waitUntil()
    env.ctx = ctx;

    const url    = new URL(req.url);
    const path   = url.pathname;
    const method = req.method;

    // CORS preflight
    if (method === 'OPTIONS') return preflight();

    // ── Routes ────────────────────────────────────────────────────────────────
    if (path === '/ad.js')
      return new Response(buildAdScript(url.origin), {
        headers: {
          'Content-Type':  'application/javascript; charset=utf-8',
          'Cache-Control': 'public, max-age=3600',
          ...CORS_HEADERS,
        },
      });

    if (path === '/track'  && method === 'POST') return handleTrack(req, env);
    if (path === '/serve')                        return handleServe(req, env);
    if (path === '/click')                        return handleClick(req, env);

    if (path === '/pixel.gif')
      return new Response(PIXEL_GIF, {
        headers: { 'Content-Type': 'image/gif', 'Cache-Control': 'no-store' },
      });

    if (path === '/analytics/publisher')  return handlePublisherAnalytics(req, env);
    if (path === '/analytics/advertiser') return handleAdvertiserAnalytics(req, env);
    if (path === '/analytics/admin')      return handleAdminAnalytics(req, env);

    if (path === '/health')
      return json({ status: 'ok', version: '3.1.0', ts: new Date().toISOString() });

    return json({ error: 'not found', path }, 404);
  },
};
