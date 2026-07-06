from flask import Flask, jsonify, request, g
import os
import time
import json
import threading
import multiprocessing
import requests
import redis
from collections import deque
from prometheus_flask_exporter import PrometheusMetrics
from prometheus_client import Gauge, Histogram

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

app = Flask(__name__)
metrics = PrometheusMetrics(app)

# Servis A'nın veritabanı bağlantı sağlığını Prometheus'a yayınlar.
# Sadece Servis A bu metriği kaydeder (B/C yanlış 0 yaymasın diye).
# 1 = bağlı, 0 = bağlanamıyor. n8n bu metriği görüp DB çöktüğünde müdahale eder.
DB_UP = None


# --- GLOBAL DEĞİŞKENLER ---
# Uygulama çalıştığı sürece bu liste şişmeye devam edecek ve RAM kullanımını artıracak.
memory_leak_list = []

# CPU stres durumu (arayüzün canlı gösterebilmesi için)
CPU_STRESS_SECONDS = 30
CPU_STRESS_ALERT_THRESHOLD = 100  # bu saniyeyi geçince n8n Telegram bildirimi gönderir
cpu_stress_until = 0.0
cpu_stress_lock = threading.Lock()

# Aktif CPU stresinin kalan saniyesini Prometheus'a yayınlar (TÜM servisler).
# n8n bu metriği okuyup 100sn üstünde Telegram uyarısı verir.
# set_function sayesinde scrape anında güncel kalan süre hesaplanır.
CPU_STRESS_GAUGE = Gauge(
    "servis_cpu_stress_seconds", "Aktif CPU stres kalan saniye (tum servisler)"
)
CPU_STRESS_GAUGE.set_function(lambda: max(0.0, cpu_stress_until - time.time()))

# --- GECİKME (LATENCY) ÖLÇÜMÜ ---
# Son isteklerin süresini ms cinsinden tutan kayan pencere (rolling window).
latency_samples = deque(maxlen=50)
# Yapay gecikme enjeksiyonu (Servis C "/latency-stres" ile demo amaçlı yük).
LATENCY_STRESS_SECONDS = 30       # her tiklamada sure +30 sn (CPU stresi gibi birikir)
LATENCY_STEP = 100                # her tiklamada gecikme +100 ms (100, 200, 300...)
LATENCY_WARN_THRESHOLD = 300      # bu ms -> Telegram UYARI (henuz aksiyon yok)
LATENCY_ACTION_THRESHOLD = 600    # bu ms -> Telegram + otonom restart
LATENCY_MAX_MS = 1200             # ust sinir
latency_inject_until = 0.0
latency_inject_ms = 0
latency_stress_lock = threading.Lock()
latency_state = {"warn_sent": False, "action_sent": False}
# Ölçüm/enjeksiyon dışında tutulacak yollar (sık ve hızlı çağrılır, ortalamayı bozmasın).
LATENCY_EXCLUDE = {"/metrics", "/api/status", "/favicon.ico"}

REQUEST_LATENCY = Histogram(
    "servis_request_latency_seconds",
    "HTTP istek suresi (saniye)",
    ["servis"],
)


def _avg_latency_ms():
    """Son isteklerin ortalamasi. Yapay gecikme aktifken en az inject_ms doner;
    boylece tek tiklamada Prometheus/n8n yuksek gecikmeyi hemen gorur."""
    sample_avg = (
        round(sum(latency_samples) / len(latency_samples), 1) if latency_samples else 0.0
    )
    if time.time() < latency_inject_until and latency_inject_ms > 0:
        return max(sample_avg, float(latency_inject_ms))
    return sample_avg


# Son isteklerin ortalama gecikmesini (ms) Prometheus'a yayınlar; n8n/Gemini bunu okur.
LATENCY_GAUGE = Gauge("servis_avg_latency_ms", "Son isteklerin ortalama gecikmesi (ms)")
LATENCY_GAUGE.set_function(_avg_latency_ms)

# --- RAM TAHMİN / TREND KATMANI (basit doğrusal regresyon) ---
# Son ~3 dakikanın RAM örnekleri (zaman, MB). Eğimden 2 dk sonrasını öngörür.
ram_trend_samples = deque(maxlen=18)
_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096


def _rss_mb():
    """Bu sürecin RSS bellek kullanımını MB cinsinden döndürür (Linux /proc)."""
    try:
        with open("/proc/self/statm") as f:
            pages = int(f.read().split()[1])
        return pages * _PAGE_SIZE / 1048576.0
    except Exception:
        return 0.0


# RAM proaktif tahmin eşiği (MB). ETA bu eşiğe ne kadar kaldığını hesaplar.
RAM_PREDICT_THRESHOLD_MB = 250
# Eğim güven (R²) ve hız eşikleri: anomali (kontrolsüz artış) tespiti için.
RAM_TREND_R2_MIN = 0.70        # regresyon uyum iyiliği bu degerin ustundeyse guvenilir
RAM_TREND_HIZ_MIN = 5.0        # MB/dk; bunun ustundeki artis "hizli buyume"


def _ram_trend_analiz():
    """Son RAM örneklerine en küçük kareler (OLS) doğrusal regresyonu uygular.
    Döndürür: anlık değer, eğim (MB/dk), 2 dk tahmini, R² güven skoru,
    eşiğe kalan süre (ETA, sn) ve anomali bayrağı.
    Bu, basit 'son değere bak' yaklaşımından farklı olarak TREND + GÜVEN üretir;
    böylece LLM 'RAM şu an normal ama X MB/dk hızla artıyor, ~Y dk sonra patlar'
    şeklinde proaktif yorum yapabilir."""
    pts = list(ram_trend_samples)
    son = round(pts[-1][1], 2) if pts else 0.0
    bos = {
        "son_mb": son, "egim_mb_dk": 0.0, "tahmin_mb": son,
        "r2": 0.0, "eta_sn": -1, "anomali": False,
    }
    if len(pts) < 4:
        return bos
    n = len(pts)
    t0 = pts[0][0]
    xs = [p[0] - t0 for p in pts]
    ys = [p[1] for p in pts]
    mx = sum(xs) / n
    my = sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return bos
    slope = sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / den  # MB/sn
    intercept = my - slope * mx
    ss_tot = sum((y - my) ** 2 for y in ys)
    ss_res = sum((ys[i] - (intercept + slope * xs[i])) ** 2 for i in range(n))
    r2 = (1 - ss_res / ss_tot) if ss_tot > 0 else 0.0
    egim_dk = slope * 60.0
    tahmin = max(0.0, son + slope * 120.0)
    if slope > 0.001 and son < RAM_PREDICT_THRESHOLD_MB:
        eta = (RAM_PREDICT_THRESHOLD_MB - son) / slope
    else:
        eta = -1
    anomali = (egim_dk >= RAM_TREND_HIZ_MIN and r2 >= RAM_TREND_R2_MIN)
    return {
        "son_mb": son,
        "egim_mb_dk": round(egim_dk, 2),
        "tahmin_mb": round(tahmin, 2),
        "r2": round(max(0.0, r2), 3),
        "eta_sn": int(round(eta)),
        "anomali": anomali,
    }


def _predict_ram_mb():
    return _ram_trend_analiz()["tahmin_mb"]


# Prometheus metrikleri: tahmin, eğim (hız) ve güven (R²). n8n/Gemini proaktif karar için okur.
RAM_PREDICT_GAUGE = Gauge("servis_ram_tahmin_mb", "2 dk sonrasi RAM ongorusu (MB, OLS trend)")
RAM_PREDICT_GAUGE.set_function(lambda: _ram_trend_analiz()["tahmin_mb"])
RAM_SLOPE_GAUGE = Gauge("servis_ram_egim_mb_dk", "RAM artis hizi (MB/dakika, OLS egimi)")
RAM_SLOPE_GAUGE.set_function(lambda: _ram_trend_analiz()["egim_mb_dk"])
RAM_TREND_R2_GAUGE = Gauge("servis_ram_trend_guven", "RAM trend regresyon guveni (R2, 0-1)")
RAM_TREND_R2_GAUGE.set_function(lambda: _ram_trend_analiz()["r2"])


def _ram_trend_monitor():
    """Her 10 sn'de bir RAM örneği toplar (tahmin penceresi için)."""
    while True:
        ram_trend_samples.append((time.time(), _rss_mb()))
        time.sleep(10)


@app.before_request
def _latency_start():
    g._t0 = time.time()
    # Yapay gecikme aktifse (ve hariç tutulan yol değilse) isteği bilerek yavaşlat.
    if request.path not in LATENCY_EXCLUDE and time.time() < latency_inject_until:
        time.sleep(latency_inject_ms / 1000.0)


@app.after_request
def _latency_end(response):
    t0 = getattr(g, "_t0", None)
    if t0 is not None and request.path not in LATENCY_EXCLUDE:
        dt_ms = (time.time() - t0) * 1000.0
        latency_samples.append(dt_ms)
        try:
            REQUEST_LATENCY.labels(servis=service_name).observe(dt_ms / 1000.0)
        except Exception:
            pass
    return response

# Veritabanı kayıt eşikleri (Servis A)
DB_WARN_THRESHOLD = 10   # bu sayıyı geçince UYARI (n8n -> Telegram)
DB_MAX_THRESHOLD = 20    # bu sayıyı geçince retention (en eski kayıtlar silinir, n8n -> Telegram)

# n8n webhook (Servis A -> n8n -> Telegram anlık bildirim)
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "http://n8n:5678/webhook/db-uyari")
# Baglanti kop/bagla olaylari icin ayri webhook
N8N_BAGLANTI_WEBHOOK_URL = os.getenv(
    "N8N_BAGLANTI_WEBHOOK_URL", "http://n8n:5678/webhook/db-baglanti"
)
# Servis B onbellek sisme/temizlenme olaylari icin webhook
N8N_CACHE_WEBHOOK_URL = os.getenv(
    "N8N_CACHE_WEBHOOK_URL", "http://n8n:5678/webhook/cache-uyari"
)
# Servis C gecikme olaylari icin webhook (uyari / aksiyon)
N8N_LATENCY_WEBHOOK_URL = os.getenv(
    "N8N_LATENCY_WEBHOOK_URL", "http://n8n:5678/webhook/latency-uyari"
)
# Redis onbellek anahtar esigi: bu sayiyi gecince "sisti" sayilir
CACHE_FLOOD_THRESHOLD = 500
# Onbellek durumu gecisini (normal<->sismis) izlemek icin
cache_conn_state = {"last_over": None}

# Terminal/SQL degisikliklerini izlemek icin (web + psql ayni mantik)
db_monitor_lock = threading.Lock()
db_monitor_state = {"last_count": 0, "warn_sent": False, "skip_delete_notify_until": 0.0}

# Manuel baglanti kesme (buton/terminal). True iken DB baglantisi reddedilir.
db_blocked = False
# Baglanti durumu gecisini (up<->down) izlemek icin
db_conn_state = {"last_up": None}

service_name = os.getenv("SERVICE_NAME", "Bilinmeyen Servis")
is_service_a = service_name == "Servis_A"
is_service_b = service_name == "Servis_B"
is_service_c = service_name == "Servis_C"

redis_host = os.getenv("REDIS_HOST", "redis")
cache_key_a = "servis_a:items"

# --- PostgreSQL ayarları (Servis A) ---
pg_host = os.getenv("POSTGRES_HOST", "postgres")
pg_port = int(os.getenv("POSTGRES_PORT", "5432"))
pg_db = os.getenv("POSTGRES_DB", "servis_a")
pg_user = os.getenv("POSTGRES_USER", "servis_a")
pg_pass = os.getenv("POSTGRES_PASSWORD", "servis_a_pass")


PANEL_TEMPLATE = """<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>__TITLE__ · Kontrol Paneli</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
  body{background:#0f172a;color:#e2e8f0;font-family:system-ui,-apple-system,sans-serif;margin:0}
  .wrap{max-width:880px;margin:0 auto;padding:28px 20px 60px}
  .card{background:#1e293b;border:1px solid #334155;border-radius:18px;padding:22px;margin-bottom:18px}
  h3{font-size:13px;text-transform:uppercase;letter-spacing:.08em;color:#94a3b8;margin:0 0 12px}
  code{background:#0f172a;padding:2px 7px;border-radius:6px;font-size:.85em;color:#7dd3fc}
  .grp{margin-bottom:18px}
  .grp:last-child{margin-bottom:0}
  .btn{border:none;border-radius:10px;padding:10px 16px;margin:0 8px 8px 0;font-size:14px;font-weight:600;cursor:pointer;color:#fff;transition:transform .05s,filter .15s}
  .btn:hover{filter:brightness(1.15)}
  .btn:active{transform:scale(.97)}
  .sky{background:#0284c7}.violet{background:#7c3aed}.emerald{background:#059669}
  .amber{background:#d97706}.red{background:#dc2626}
  .danger-zone{border-top:1px dashed #475569;padding-top:16px}
  .note{font-size:12.5px;color:#94a3b8;margin:10px 0 0;line-height:1.5}
  .statline{display:flex;gap:18px;flex-wrap:wrap;align-items:center;margin-top:6px}
  .pill{font-size:12px;padding:3px 10px;border-radius:999px;background:#0f172a;border:1px solid #334155}
  .bar{height:8px;background:#0f172a;border-radius:999px;overflow:hidden;margin-top:10px}
  .bar > div{height:100%;background:#22c55e;transition:width .3s,background .3s}
  pre{background:#0b1120;border:1px solid #1e293b;border-radius:12px;padding:14px;font-size:12.5px;
      max-height:280px;overflow:auto;white-space:pre-wrap;word-break:break-word;color:#cbd5e1;margin:0}
  a{color:#7dd3fc;text-decoration:none}
  .dot{width:9px;height:9px;border-radius:50%;display:inline-block;margin-right:6px}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
  @keyframes slidein{from{transform:translateX(40px);opacity:0}to{transform:translateX(0);opacity:1}}
  .cpu-load{margin-top:14px;background:#0f172a;border:1px solid #7f1d1d;border-radius:12px;padding:12px 14px}
  .cpu-load .lab{display:flex;justify-content:space-between;font-size:13px;color:#fca5a5;font-weight:700}
  .cpu-load .pbar{height:9px;background:#1e293b;border-radius:999px;overflow:hidden;margin-top:9px}
  .cpu-load .pbar>div{height:100%;background:#ef4444;animation:pulse 1s infinite}
  .db-gauge{margin-top:14px;font-size:13px;color:#cbd5e1}
  .db-gauge .lab{display:flex;justify-content:space-between;font-weight:600}
  .db-gauge .g{height:9px;background:#0f172a;border-radius:999px;overflow:hidden;margin-top:8px}
  .db-gauge .g>div{height:100%;transition:width .3s,background .3s}
  .toast{background:#1e293b;border-left:4px solid #38bdf8;border-radius:10px;padding:12px 14px;font-size:13px;
         color:#e2e8f0;box-shadow:0 10px 28px rgba(0,0,0,.45);animation:slidein .25s ease;line-height:1.45}
  .toast.warn{border-color:#f59e0b}.toast.crit{border-color:#ef4444}.toast.info{border-color:#38bdf8}
  .inp{background:#0f172a;border:1px solid #334155;border-radius:10px;padding:10px 12px;color:#e2e8f0;
       font-size:14px;min-width:220px;flex:1;max-width:420px}
  .row{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:10px}
</style>
</head>
<body>
<div class="wrap">

  <div class="card">
    <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px">
      <div>
        <div style="font-size:34px">__ICON__</div>
        <h1 style="font-size:24px;font-weight:800;color:#fff;margin:6px 0 2px">__TITLE__</h1>
        <div style="color:#94a3b8;font-size:14px">__SUBTITLE__</div>
      </div>
      <span class="pill"><span id="dot" class="dot" style="background:#22c55e"></span><span id="state">çalışıyor</span></span>
    </div>
    <div class="statline">
      <span class="pill">Simüle RAM yükü: <b id="ram">0</b> MB</span>
      <span class="pill" id="extra">—</span>
    </div>
    <div class="bar"><div id="rambar" style="width:0%"></div></div>
    <div id="loadbox"></div>
  </div>

  <div class="card">
    <h3>Bu servis ne yapıyor?</h3>
    <p style="line-height:1.65;margin:0;color:#cbd5e1">__DESC__</p>
  </div>

  <div class="card">
    __ACTIONS__
  </div>

  <div class="card">
    <h3>İşlem Sonuçları</h3>
    <pre id="log">Bir butona tıklayın, sonuç burada görünecek...</pre>
  </div>

  <div style="text-align:center;font-size:12.5px;color:#64748b">
    <a href="http://localhost:3000" target="_blank">← Ana Dashboard</a> ·
    <a href="/metrics" target="_blank">/metrics</a> ·
    <a href="/health" target="_blank">/health</a>
  </div>
</div>

<div id="toasts" style="position:fixed;top:16px;right:16px;display:flex;flex-direction:column;gap:10px;z-index:50;max-width:340px"></div>

<script>
const SERVICE = "__SERVICE__";
function log(msg){
  const el=document.getElementById('log');
  const t=new Date().toLocaleTimeString('tr-TR');
  el.textContent = '['+t+'] '+msg+'\\n\\n'+el.textContent;
}
async function call(path,label){
  log('→ ' + (label||path) + ' çağrılıyor...');
  try{
    const res = await fetch(path, {signal:AbortSignal.timeout(15000)});
    const txt = await res.text();
    let out; try{ out = JSON.stringify(JSON.parse(txt), null, 2); }catch{ out = txt; }
    log('✓ ['+res.status+'] '+label+'\\n'+out);
    try{
      const j=JSON.parse(txt);
      if(j.silinen_kayitlar && j.silinen_kayitlar.length)
        notify('🗑️ <b>Retention:</b> '+j.silinen_kayitlar.length+' eski kayit silindi: '+j.silinen_kayitlar.map(k=>k.note||('#'+k.id)).join(', '),'crit');
      if(j.toplam_kalan_sn!==undefined && j.eklenen_sn)
        notify('🔥 <b>+'+j.eklenen_sn+' sn CPU stresi</b>. Toplam kalan: '+j.toplam_kalan_sn+' sn','crit');
    }catch(e){}
  }catch(e){ log('✗ Hata: '+e.message+'  (servis çökmüş ya da meşgul olabilir)'); }
}
async function addDbRecord(){
  const inp=document.getElementById('dbnote');
  const note=(inp&&inp.value||'').trim();
  if(!note){ notify('⚠️ Lutfen kayit metni girin (ornek: Siparis #42)','warn'); return; }
  await call('/db/add?note='+encodeURIComponent(note),'DB Kayit: '+note);
  if(inp) inp.value='';
}
async function dbDisconnect(){
  if(!confirm('PostgreSQL bağlantısı koparılacak. n8n algılayıp Gemini ile analiz edecek ve yeniden bağlayacak. Telegram\\'a "koptu" ve "bağlandı" mesajı gelecek. Devam?')) return;
  notify('🔌 <b>Bağlantı koparılıyor...</b> n8n algılayıp yeniden kuracak.','crit');
  await call('/db/disconnect','Bağlantıyı Kopar');
}
async function cacheFlood(){
  if(!confirm('Redis önbelleği binlerce kayıtla şişirilecek. n8n algılayıp Gemini ile analiz edecek ve önbelleği temizleyecek. Telegram\\'a "şişti" ve "temizlendi" mesajı gelecek. Devam?')) return;
  notify('💣 <b>Önbellek şişiriliyor...</b> n8n algılayıp temizleyecek.','crit');
  await call('/cache/flood','Önbelleği Şişir');
}
function crash(){
  if(!confirm(SERVICE+' tamamen çökecek (process exit). Gemini ~1dk içinde yeniden başlatacak. Devam?')) return;
  log('💥 /boz çağrıldı — servis kapanıyor. Bağlantı kopacak, bu normal.');
  fetch('/boz').catch(()=>log('✓ Servis çöktü. Dashboard\\'da kısa süre "Çökmüş" görünecek, sonra Gemini ayağa kaldıracak.'));
}
async function islemGonder(){
  try{
    const r = await fetch('http://localhost:8080/islem',{signal:AbortSignal.timeout(6000)});
    const d = await r.json();
    const yedek = d.rol==='YEDEK';
    log('🔀 /islem → yanıtlayan: '+d.yanitlayan_servis+' ('+d.rol+')');
    notify((yedek?'🟡 <b>YEDEK yanıtladı</b> ('+d.yanitlayan_servis+'). Primary çökük ama kesinti YOK — trafik yedeğe yönlendirildi.':'🟢 <b>PRIMARY yanıtladı</b> ('+d.yanitlayan_servis+'). Normal akış.'), yedek?'warn':'info');
  }catch(e){
    log('❌ /islem isteği başarısız: '+e);
    notify('❌ <b>İşlem isteği başarısız.</b> Gateway veya servisler kapalı olabilir.','crit');
  }
}
const RANK={normal:0,uyari:1,kritik:2};
let prev={cpu:false, cpu_kalan:0, cpu_alarm:false, ram:'normal', db:'normal', lat:0};
// NOT: Tarayıcı/OS bildirimi KULLANILMAZ. Bildirimler n8n -> LLM analizi -> Telegram
// üzerinden gider. Buradaki notify yalnızca sayfa içi görsel ipucu (toast) gösterir.
function notify(msg,type){
  const box=document.getElementById('toasts');
  const t=document.createElement('div');
  t.className='toast '+(type||'info');
  t.innerHTML=msg + '<div style="font-size:11px;color:#94a3b8;margin-top:4px">↗ n8n analiz edip Telegram\\'a gönderiyor</div>';
  box.appendChild(t);
  setTimeout(()=>{ t.style.transition='opacity .4s'; t.style.opacity='0'; setTimeout(()=>t.remove(),400); }, 6500);
  log('🔔 ' + msg.replace(/<[^>]+>/g,''));
}
function ramLevel(mb){ return mb>300?'kritik':mb>150?'uyari':'normal'; }

async function poll(){
  try{
    const r = await fetch('/api/status',{signal:AbortSignal.timeout(4000)});
    const d = await r.json();
    const mb = d.ram_simulasyon_yuku_mb||0;
    document.getElementById('ram').textContent = mb;
    const bar = document.getElementById('rambar');
    bar.style.width = Math.min(100,(mb/500)*100)+'%';
    bar.style.background = mb>300?'#ef4444':mb>150?'#f59e0b':'#22c55e';
    document.getElementById('dot').style.background = '#22c55e';
    document.getElementById('state').textContent = 'çalışıyor';

    let extra='—';
    if(d.db_durum!==undefined) extra = 'PostgreSQL: '+(d.db_durum==='bagli'?'✅ bağlı':'❌ çökmüş');
    else if(d.redis_durum!==undefined) extra = 'Redis: '+(d.redis_durum==='bagli'?'✅ bağlı ('+d.cache_anahtar+' anahtar)':'❌ çökmüş');
    if(d.avg_latency_ms!==undefined){
      const lat=d.latency_seviye_ms||d.avg_latency_ms||0;
      const uy=d.latency_uyari_esigi||300, ak=d.latency_aksiyon_esigi||600;
      const licon = lat>=ak?'🔴':lat>=uy?'🟡':'🟢';
      extra += '  ·  ⏱ Gecikme: '+licon+' '+lat+' ms';
    }
    document.getElementById('extra').textContent = extra;

    // --- RAM altı canlı yük göstergeleri ---
    let html='';
    if(d.cpu_stres_aktif){
      const kalan=d.cpu_kalan_sn||0;
      const esik=d.cpu_alarm_esigi||100;
      const w=Math.min(100, Math.max(8, (kalan/120)*100));
      const asti=d.cpu_telegram_uyari;
      const altmsg = asti
        ? '🚨 '+esik+' sn AŞILDI — n8n Telegram bildirimi gönderiyor!'
        : 'Her tıklamada +30 sn eklenir. '+esik+' sn aşılınca Telegram uyarısı gelir ('+(esik-kalan)+' sn kaldı).';
      html += '<div class="cpu-load"><div class="lab"><span>🔥 CPU STRESİ AKTİF — anormal işlemci yükü</span><span>'+kalan+' sn kaldı</span></div><div class="pbar"><div style="width:'+w+'%"></div></div><div style="font-size:11px;color:'+(asti?'#fca5a5':'#94a3b8')+';margin-top:6px">'+altmsg+'</div></div>';
    }
    if(d.db_manual_count!==undefined){
      const n=d.db_manual_count, lim=d.db_limit||20;
      const col = d.db_uyari==='kritik'?'#ef4444':d.db_uyari==='uyari'?'#f59e0b':'#22c55e';
      const gp = Math.min(100,(n/lim)*100);
      const etiket = d.db_uyari==='kritik'?'🚨 KRİTİK (retention sınırı)':d.db_uyari==='uyari'?'⚠️ UYARI (sınıra yaklaşıldı)':'✅ normal';
      html += '<div class="db-gauge"><div class="lab"><span>📦 Manuel kayıt: '+n+' / '+lim+'</span><span style="color:'+col+'">'+etiket+'</span></div><div class="g"><div style="width:'+gp+'%;background:'+col+'"></div></div></div>';
    }
    if(d.latency_seviye_ms!==undefined && d.latency_seviye_ms>0){
      const lat=d.latency_seviye_ms, uy=d.latency_uyari_esigi||300, ak=d.latency_aksiyon_esigi||600;
      const gp=Math.min(100,(lat/ak)*100);
      const col=lat>=ak?'#ef4444':lat>=uy?'#f59e0b':'#22c55e';
      const et=lat>=ak?'🚨 AKSİYON eşiği':lat>=uy?'⚠️ UYARI eşiği':'✅ normal';
      html += '<div class="db-gauge"><div class="lab"><span>⏱ Gecikme: '+lat+' ms (uyari '+uy+' / aksiyon '+ak+')</span><span style="color:'+col+'">'+et+'</span></div><div class="g"><div style="width:'+gp+'%;background:'+col+'"></div></div></div>';
    }
    if(d.ram_tahmin && d.ram_tahmin.egim_mb_dk > 2){
      const t=d.ram_tahmin;
      const col = t.anomali?'#ef4444':'#f59e0b';
      const etaTxt = t.eta_sn>0 ? ('~'+Math.round(t.eta_sn/60)+' dk sonra 250 MB eşiği') : 'eşik uzak';
      const guven = Math.round((t.r2||0)*100);
      const baslik = t.anomali?'🔮 ANOMALİ ÖNGÖRÜSÜ — hızlı RAM artışı':'🔮 RAM trend tahmini';
      html += '<div class="cpu-load"><div class="lab"><span>'+baslik+'</span><span>'+t.egim_mb_dk+' MB/dk</span></div><div style="font-size:11px;color:'+col+';margin-top:6px">Tahmin (2 dk): '+t.tahmin_mb+' MB · Güven (R²): %'+guven+' · '+etaTxt+'</div></div>';
    }
    document.getElementById('loadbox').innerHTML = html;

    // --- Bildirimler (seviye yükseldikçe) ---
    if(d.cpu_stres_aktif){
      if(!prev.cpu) notify('🔥 <b>CPU stresi başladı</b> ('+d.cpu_kalan_sn+' sn). Anormal işlemci yükü!','crit');
      else if(d.cpu_kalan_sn > (prev.cpu_kalan||0) + 5)
        notify('🔥 <b>+30 sn CPU stresi eklendi</b>. Toplam kalan: '+d.cpu_kalan_sn+' sn','crit');
      if(d.cpu_telegram_uyari && !prev.cpu_alarm)
        notify('🚨 <b>CPU stresi '+(d.cpu_alarm_esigi||100)+' sn aştı!</b> n8n → Telegram bildirimi gönderiyor.','crit');
      prev.cpu_alarm = d.cpu_telegram_uyari;
      prev.cpu_kalan = d.cpu_kalan_sn;
    } else { prev.cpu_kalan = 0; prev.cpu_alarm = false; }
    prev.cpu = d.cpu_stres_aktif;

    const rl = ramLevel(mb);
    if(RANK[rl] > RANK[prev.ram]){
      if(rl==='kritik') notify('🚨 <b>RAM kritik seviyede</b> ('+mb+' MB)! Gemini müdahale edebilir.','crit');
      else notify('⚠️ <b>RAM yükü yükseldi</b> ('+mb+' MB). İzleniyor.','warn');
    }
    prev.ram = rl;

    if(d.db_uyari!==undefined){
      if(RANK[d.db_uyari] > RANK[prev.db]){
        if(d.db_uyari==='kritik') notify('🚨 <b>DB kapasite sınırına ulaşıldı</b> ('+d.db_manual_count+'/'+d.db_limit+'). Sonraki eklemede eski kayıtlar temizlenecek (retention).','crit');
        else notify('⚠️ <b>DB kayıt sayısı eşiği aştı</b> ('+d.db_manual_count+'). Dikkatli ekleyin.','warn');
      }
      prev.db = d.db_uyari;
    }

    const latLv = d.latency_seviye_ms||0;
    if(latLv > (prev.lat||0)){
      const uy=d.latency_uyari_esigi||300, ak=d.latency_aksiyon_esigi||600;
      if(latLv>=ak && (prev.lat||0)<ak) notify('🚨 <b>Gecikme aksiyon eşiğine ulaştı</b> ('+latLv+' ms). n8n servisi yeniden başlatacak.','crit');
      else if(latLv>=uy && (prev.lat||0)<uy) notify('⚠️ <b>Gecikme uyarı eşiğine ulaştı</b> ('+latLv+' ms). Telegram bildirimi gidiyor.','warn');
      else if(latLv>0) notify('⏱ <b>Gecikme +100 ms</b> (toplam '+latLv+' ms).','warn');
    }
    prev.lat = latLv;
  }catch(e){
    document.getElementById('dot').style.background = '#ef4444';
    document.getElementById('state').textContent = 'ulaşılamıyor';
  }
}
poll(); setInterval(poll, 2000);
</script>
</body>
</html>"""


def get_redis_client():
    return redis.Redis(host=redis_host, port=6379, decode_responses=True, socket_timeout=3)


def get_pg_connection():
    """Her istekte yeni bağlantı açar. Böylece PostgreSQL çökse bile,
    yeniden ayağa kalktığında otomatik olarak tekrar bağlanır (self-healing).
    db_blocked True iken bağlantı manuel olarak koparılmış sayılır (demo)."""
    if db_blocked:
        raise psycopg2.OperationalError(
            "Baglanti manuel olarak koparildi (db_blocked=True)"
        )
    return psycopg2.connect(
        host=pg_host,
        port=pg_port,
        dbname=pg_db,
        user=pg_user,
        password=pg_pass,
        connect_timeout=3,
    )


def init_service_a_db(retries: int = 30) -> None:
    """Servis_A için PostgreSQL tablolarını hazırlar.
    PostgreSQL ilk açılışta geç kalkabilir; o yüzden retry ile bekler."""
    last_err = None
    for attempt in range(retries):
        try:
            conn = get_pg_connection()
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS service_events (
                    id SERIAL PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    note TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute("ALTER TABLE service_events ADD COLUMN IF NOT EXISTS note TEXT")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS items (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL
                )
                """
            )
            cur.execute("SELECT COUNT(*) FROM items")
            if cur.fetchone()[0] == 0:
                cur.executemany(
                    "INSERT INTO items(name) VALUES (%s)",
                    [("kalem",), ("defter",), ("silgi",)],
                )
            cur.execute("INSERT INTO service_events(event_type) VALUES (%s)", ("service_start",))
            cur.close()
            conn.close()
            print("[DB] PostgreSQL bağlantısı kuruldu ve tablolar hazır.")
            return
        except Exception as exc:
            last_err = exc
            print(f"[DB] PostgreSQL bekleniyor ({attempt + 1}/{retries})... {exc}")
            time.sleep(2)
    print(f"[DB] PostgreSQL'e bağlanılamadı: {last_err}")


def _notify_baglanti(event, detay=""):
    """Baglanti kop/bagla olayini n8n webhook'una gonderir (n8n -> Gemini -> Telegram)."""
    try:
        requests.post(
            N8N_BAGLANTI_WEBHOOK_URL,
            json={"event": event, "servis": service_name, "detay": detay},
            timeout=3,
        )
    except Exception as exc:
        print(f"[n8n] Baglanti webhook atlandi: {exc}")


def _notify_cache(event, detay=""):
    """Onbellek sisme/temizlenme olayini n8n webhook'una gonderir (n8n -> Gemini -> Telegram)."""
    try:
        requests.post(
            N8N_CACHE_WEBHOOK_URL,
            json={"event": event, "servis": service_name, "detay": detay},
            timeout=3,
        )
    except Exception as exc:
        print(f"[n8n] Cache webhook atlandi: {exc}")


def _notify_latency(event, gecikme_ms, detay=""):
    """Gecikme uyari/aksiyon olayini n8n webhook'una gonderir (n8n -> Gemini -> Telegram)."""
    try:
        requests.post(
            N8N_LATENCY_WEBHOOK_URL,
            json={
                "event": event,
                "servis": service_name,
                "gecikme_ms": gecikme_ms,
                "detay": detay,
            },
            timeout=3,
        )
    except Exception as exc:
        print(f"[n8n] Latency webhook atlandi: {exc}")


def _check_latency_thresholds(current_ms: int) -> None:
    """Gecikme esiklerini kontrol eder: once uyari Telegram, ust sinirda aksiyon."""
    if current_ms >= LATENCY_WARN_THRESHOLD and not latency_state["warn_sent"]:
        latency_state["warn_sent"] = True
        _notify_latency(
            "uyari",
            current_ms,
            f"Gecikme {current_ms} ms'e ulasti (uyari esigi {LATENCY_WARN_THRESHOLD} ms). "
            "Servis yavasliyor; henuz otomatik mudahale yok.",
        )
    if current_ms >= LATENCY_ACTION_THRESHOLD and not latency_state["action_sent"]:
        latency_state["action_sent"] = True
        _notify_latency(
            "aksiyon",
            current_ms,
            f"Gecikme {current_ms} ms'e ulasti (aksiyon esigi {LATENCY_ACTION_THRESHOLD} ms). "
            "Otonom servis yeniden baslatma baslatildi.",
        )


def _cache_monitor() -> None:
    """Servis_B: Redis önbellek anahtar sayısını her 3 sn izler. Eşik aşılınca
    (şişme) n8n'e 'sisti', tekrar normale dönünce 'temizlendi' bildirimi gönderir."""
    time.sleep(4)
    while True:
        over = None
        try:
            rc = get_redis_client()
            size = rc.dbsize()
            over = size > CACHE_FLOOD_THRESHOLD
        except Exception:
            over = None
        if over is not None:
            last = cache_conn_state["last_over"]
            if last is not None and over != last:
                if over:
                    _notify_cache(
                        "sisti",
                        f"Redis onbellek anahtar sayisi {size} (esik {CACHE_FLOOD_THRESHOLD}). Asiri sismis.",
                    )
                else:
                    _notify_cache(
                        "temizlendi",
                        "Redis onbellek temizlendi, normal seviyeye dondu.",
                    )
            cache_conn_state["last_over"] = over
        time.sleep(3)


def _db_health_monitor() -> None:
    """Her 3 saniyede bir DB bağlantısını test eder, Prometheus metriğini günceller
    ve bağlantı durumu DEĞİŞİNCE (koptu/bağlandı) n8n'e bildirim gönderir."""
    while True:
        try:
            conn = get_pg_connection()
            conn.cursor().execute("SELECT 1")
            conn.close()
            up = 1
        except Exception:
            up = 0
        DB_UP.set(up)

        last = db_conn_state["last_up"]
        if last is not None and up != last:
            if up == 0:
                _notify_baglanti(
                    "koptu",
                    "PostgreSQL baglantisi koptu (manuel kesme veya konteyner durması)."
                    if not db_blocked else "PostgreSQL baglantisi manuel olarak koparildi.",
                )
            else:
                _notify_baglanti("baglandi", "PostgreSQL baglantisi yeniden kuruldu.")
        db_conn_state["last_up"] = up
        time.sleep(3)


def _fetch_manual_records(cur):
    cur.execute(
        """
        SELECT id, note, created_at FROM service_events
        WHERE event_type='manual_add'
        ORDER BY id ASC
        """
    )
    return [
        {"id": row[0], "note": row[1] or f"#{row[0]}", "created_at": row[2].isoformat()}
        for row in cur.fetchall()
    ]


def _apply_manual_retention(cur):
    """20'yi gecen manuel kayitlarda en eskileri siler. Silinen kayit listesini dondurur."""
    cur.execute("SELECT COUNT(*) FROM service_events WHERE event_type='manual_add'")
    manual_count = cur.fetchone()[0]
    if manual_count <= DB_MAX_THRESHOLD:
        return [], manual_count

    cur.execute(
        """
        SELECT id, note, created_at FROM service_events
        WHERE event_type='manual_add' AND id NOT IN (
            SELECT id FROM service_events
            WHERE event_type='manual_add'
            ORDER BY id DESC LIMIT %s
        )
        ORDER BY id ASC
        """,
        (DB_MAX_THRESHOLD,),
    )
    silinen = [
        {"id": row[0], "note": row[1] or f"#{row[0]}", "created_at": row[2].isoformat()}
        for row in cur.fetchall()
    ]
    cur.execute(
        """
        DELETE FROM service_events
        WHERE event_type='manual_add' AND id NOT IN (
            SELECT id FROM service_events
            WHERE event_type='manual_add'
            ORDER BY id DESC LIMIT %s
        )
        """,
        (DB_MAX_THRESHOLD,),
    )
    return silinen, DB_MAX_THRESHOLD


def _sync_manual_records(trigger_note=None, source="web"):
    """Manuel kayit sayisini kontrol eder: retention, uyari, silme bildirimleri.
    Web arayuzu ve terminal/psql degisiklikleri icin ortak mantik."""
    with db_monitor_lock:
        conn = get_pg_connection()
        conn.autocommit = True
        cur = conn.cursor()
        prev_count = db_monitor_state["last_count"]

        cur.execute("SELECT COUNT(*) FROM service_events WHERE event_type='manual_add'")
        manual_count = cur.fetchone()[0]

        silinen_kayitlar, manual_count = _apply_manual_retention(cur)
        retention_applied = len(silinen_kayitlar) > 0

        kaynak_etiket = {
            "web": "Web paneli",
            "postgresql": "PostgreSQL (terminal/psql)",
            "panel_reset": "Web panel sifirlama",
        }.get(source, source)

        if retention_applied:
            silinen_isimler = ", ".join(k["note"] for k in silinen_kayitlar)
            yeni = f' "{trigger_note}"' if trigger_note else ""
            _notify_n8n(
                "🗑️ SERVİS A — VERİTABANI RETENTION (KAPASİTE AŞILDI)\n\n"
                f"Kaynak: {kaynak_etiket}\n"
                f"Yeni kayit{yeni} eklendi; retention devreye girdi.\n"
                f"Silinen eski kayit(lar) ({len(silinen_kayitlar)}): {silinen_isimler}\n"
                f"Manuel kayit {DB_MAX_THRESHOLD} limitinde tutuluyor."
            )
            db_monitor_state["warn_sent"] = True
        elif manual_count > DB_WARN_THRESHOLD and not db_monitor_state["warn_sent"]:
            _notify_n8n(
                "⚠️ SERVİS A — VERİTABANI UYARISI\n\n"
                f"Kaynak: {kaynak_etiket}\n"
                f"Manuel kayit sayisi {manual_count} oldu ({DB_WARN_THRESHOLD} esigi asildi).\n"
                f"{DB_MAX_THRESHOLD} sinirina ulasilirsa en eski kayitlar otomatik silinecek."
            )
            db_monitor_state["warn_sent"] = True
        elif manual_count <= DB_WARN_THRESHOLD:
            db_monitor_state["warn_sent"] = False

        if (
            not retention_applied
            and manual_count < prev_count
            and time.time() > db_monitor_state["skip_delete_notify_until"]
        ):
            _notify_n8n(
                "🗑️ SERVİS A — ELLE SİLME (PostgreSQL)\n\n"
                f"Kaynak: {kaynak_etiket}\n"
                f"Manuel kayit sayisi {prev_count} → {manual_count} dustu.\n"
                "Kayitlar terminal/psql uzerinden silinmis olabilir."
            )

        kalan_kayitlar = _fetch_manual_records(cur)
        db_monitor_state["last_count"] = manual_count
        conn.close()

        uyari = (
            "kritik" if manual_count >= DB_MAX_THRESHOLD
            else "uyari" if manual_count > DB_WARN_THRESHOLD
            else "normal"
        )
        return {
            "manual_count": manual_count,
            "uyari": uyari,
            "silinen_kayitlar": silinen_kayitlar,
            "kalan_kayitlar": kalan_kayitlar,
        }


def _db_manual_monitor() -> None:
    """PostgreSQL'deki manuel kayit degisikliklerini izler (terminal INSERT/DELETE).
    Web /db/add ile ayni esik + n8n bildirim mantigini uygular."""
    time.sleep(3)
    try:
        with db_monitor_lock:
            conn = get_pg_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM service_events WHERE event_type='manual_add'")
            db_monitor_state["last_count"] = cur.fetchone()[0]
            conn.close()
    except Exception:
        pass

    while True:
        try:
            with db_monitor_lock:
                conn = get_pg_connection()
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM service_events WHERE event_type='manual_add'")
                current = cur.fetchone()[0]
                conn.close()
            if current != db_monitor_state["last_count"]:
                _sync_manual_records(source="postgresql")
        except Exception as exc:
            print(f"[DB] Manuel izleyici atlandi: {exc}")
        time.sleep(5)


# RAM trend/tahmin izleyici tüm servislerde çalışır (proaktif erken uyarı için).
threading.Thread(target=_ram_trend_monitor, daemon=True).start()

if is_service_a:
    DB_UP = Gauge("servis_a_db_up", "Servis A PostgreSQL baglanti durumu (1=ok, 0=down)")
    DB_UP.set(0)
    init_service_a_db()
    threading.Thread(target=_db_health_monitor, daemon=True).start()
    threading.Thread(target=_db_manual_monitor, daemon=True).start()

if is_service_b:
    threading.Thread(target=_cache_monitor, daemon=True).start()


def _service_ui_config():
    """Her servise özel arayüz içeriği (başlık, açıklama, butonlar)."""
    if is_service_a:
        return {
            "icon": "🗄️",
            "title": "Servis A",
            "subtitle": "PostgreSQL Veritabanı Servisi",
            "accent": "sky",
            "desc": (
                "Servis A, sistemin <b>veritabanı servisidir</b>. Kalıcı verileri "
                "<b>PostgreSQL</b>'de saklar. Bu servisin uzmanlığı <b>veritabanı yönetimi</b>: "
                "kayıt ekleme/silme, retention (eski kayıt temizleme) ve bağlantı sağlığı. "
                "Veritabanı bağlantısı her 3 saniyede bir izlenir (<code>servis_a_db_up</code> metriği); "
                "bağlantı koparsa veya PostgreSQL çökerse n8n + Gemini bunu <b>otomatik</b> algılayıp onarır."
            ),
            "actions": """
              <div class="grp">
                <h3>📦 Veritabanı İşlemleri</h3>
                <div class="row">
                  <input id="dbnote" class="inp" type="text" placeholder="Kayit metni (ornek: Siparis #42, Musteri Ali)" maxlength="120">
                  <button class="btn sky" onclick="addDbRecord()">DB'ye Kayıt Ekle</button>
                </div>
                <button class="btn sky" onclick="call('/db/list','Manuel Kayitlar')">Kayitlari Listele</button>
                <button class="btn sky" onclick="call('/db/health','DB Durumu')">DB Durumu</button>
                <button class="btn sky" onclick="call('/data/items','Verileri Getir')">Verileri Getir (Redis cache'li)</button>
                <p class="note">Her sayfa yenilemede (F5) manuel kayitlar 0'a sifirlanir. Kayit ekledikce yukselir. Limit: 15+ uyari, 21. kayitta en eski kayit otomatik silinir (retention).</p>
              </div>
              <div class="grp danger-zone">
                <h3>🔌 Veritabanı Bağlantısı (otonom kurtarma)</h3>
                <button class="btn red" onclick="dbDisconnect()">Bağlantıyı Kopar</button>
                <p class="note">Bağlantıyı koparınca n8n otomatik algılar, Gemini analiz eder, bağlantıyı yeniden kurar. Telegram'a önce "koptu" sonra "bağlandı" mesajı gelir.<br>Terminalden koparmak için: <code>docker stop tasarim_proje-postgres-1</code></p>
              </div>
            """,
        }
    if is_service_b:
        return {
            "icon": "⚡",
            "title": "Servis B",
            "subtitle": "Redis Önbellek (Cache) Servisi",
            "accent": "violet",
            "desc": (
                "Servis B, sistemin <b>önbellek (cache) servisidir</b>. Verileri <b>Redis</b>'te "
                "RAM üzerinde geçici olarak (TTL'li) saklayarak sistemi hızlandırır ve veritabanı "
                "yükünü azaltır. Bu servisin uzmanlığı <b>önbellek yönetimi</b>: yazma, okuma, "
                "istatistik ve temizleme. Önbellek aşırı şişerse n8n + Gemini bunu <b>otomatik</b> "
                "algılayıp önbelleği temizler (servisi hiç kapatmadan kendini onarır)."
            ),
            "actions": """
              <div class="grp">
                <h3>⚡ Önbellek İşlemleri</h3>
                <button class="btn violet" onclick="call('/cache/stats','Cache İstatistik')">Cache İstatistik</button>
                <button class="btn violet" onclick="call('/cache/set/demo?value=merhaba&ttl=120','Cache Yaz')">Cache'e Yaz (demo=merhaba)</button>
                <button class="btn violet" onclick="call('/cache/get/demo','Cache Oku')">Cache Oku (demo)</button>
                <button class="btn violet" onclick="call('/cache/evict','Cache Temizle')">Cache Temizle (manuel)</button>
              </div>
              <div class="grp danger-zone">
                <h3>💣 Önbellek Şişirme (otonom kurtarma)</h3>
                <button class="btn red" onclick="cacheFlood()">Önbelleği Şişir</button>
                <p class="note">Önbelleğe binlerce kayıt yazar (eşik: 500 anahtar). Eşik aşılınca n8n otomatik algılar, Gemini analiz eder ve önbelleği temizler. Telegram'a önce "şişti" sonra "temizlendi" mesajı gelir.<br>Terminalden şişirmek için: <code>curl http://localhost:5002/cache/flood</code></p>
              </div>
            """,
        }
    return {
        "icon": "🧩",
        "title": "Servis C",
        "subtitle": "Dayanıklılık / Çökert-Düzelt Servisi",
        "accent": "emerald",
        "desc": (
            "Servis C, sistemin <b>dayanıklılık (resilience) servisidir</b>. Bu servisin uzmanlığı "
            "<b>kaynak/çökme senaryolarını</b> test etmek: RAM sızıntısı, CPU stresi, gecikme ve ani çökme. "
            "Ayrıca <b>/islem</b> trafiğinin <b>PRIMARY</b> sunucusudur; çökerse gateway trafiği "
            "otomatik <b>YEDEK</b>'e (Servis A) yönlendirir (failover) ve kullanıcı kesinti yaşamaz. "
            "Prometheus metrikleri bozulunca n8n + Gemini <b>otomatik</b> müdahale eder."
        ),
        "actions": """
          <div class="grp">
            <h3>ℹ️ Durum</h3>
            <button class="btn emerald" onclick="call('/durum','Durum')">Detaylı Durum</button>
            <button class="btn emerald" onclick="call('/health','Health')">Health Kontrol</button>
          </div>
          <div class="grp">
            <h3>🔀 Trafik Yönlendirme (Failover)</h3>
            <button class="btn emerald" onclick="islemGonder()">İşlem İsteği Gönder (gateway üzerinden)</button>
            <p class="note">İstek önce <b>PRIMARY</b> (Servis C) tarafından yanıtlanır. Servis C'yi çökertip tekrar bu butona basarsan, isteği <b>YEDEK</b> (Servis A) yanıtlar — kesinti olmaz. Yaniti hangi servisin verdigi gosterilir.<br>Terminalden: <code>curl http://localhost:8080/islem</code></p>
          </div>
          <div class="grp danger-zone">
            <h3>💥 Çökertme / Stres (Gemini düzeltsin diye)</h3>
            <button class="btn amber" onclick="call('/leak?mb=100','RAM +100MB')">RAM Yükselt (+100MB)</button>
            <button class="btn amber" onclick="call('/cpu-stres','CPU Stres +30sn')">CPU Stres (+30sn ekle)</button>
            <button class="btn amber" onclick="call('/latency-stres','Gecikme +100ms')">Gecikme Yükselt (+100ms)</button>
            <button class="btn red" onclick="crash()">Servisi Çökert (/boz)</button>
            <p class="note">💡 Her tiklamada gecikme +100 ms artar (100→200→300…). <b>300 ms</b> olunca Telegram UYARI gelir; <b>600 ms</b> olunca Gemini servisi yeniden baslatir.</p>
          </div>
        """,
    }


def _notify_n8n(mesaj):
    """n8n webhook'una bildirim gonderir (n8n daha sonra Telegram'a iletir).
    Fire-and-forget: hata olursa ana akisi bozmaz."""
    try:
        requests.post(
            N8N_WEBHOOK_URL,
            json={"mesaj": mesaj, "servis": service_name},
            timeout=3,
        )
    except Exception as exc:
        print(f"[n8n] Webhook bildirimi atlandi: {exc}")


def _reset_manual_records():
    """Servis_A manuel kayıtlarını temizler (her sayfa yenilemede 0'dan başlasın)."""
    try:
        conn = get_pg_connection()
        conn.autocommit = True
        conn.cursor().execute("DELETE FROM service_events WHERE event_type='manual_add'")
        conn.close()
        with db_monitor_lock:
            db_monitor_state["last_count"] = 0
            db_monitor_state["warn_sent"] = False
            db_monitor_state["skip_delete_notify_until"] = time.time() + 8
    except Exception as exc:
        print(f"[DB] Manuel kayıt sıfırlama atlandı: {exc}")


@app.route("/")
def panel():
    """Servise özel HTML kontrol paneli."""
    # Her sayfa yenilemesinde (F5) Servis A manuel kayıtları sıfırlanır.
    if is_service_a:
        _reset_manual_records()
    cfg = _service_ui_config()
    html = PANEL_TEMPLATE
    for token, value in {
        "__ICON__": cfg["icon"],
        "__TITLE__": cfg["title"],
        "__SUBTITLE__": cfg["subtitle"],
        "__DESC__": cfg["desc"],
        "__ACTIONS__": cfg["actions"],
        "__ACCENT__": cfg["accent"],
        "__SERVICE__": service_name,
    }.items():
        html = html.replace(token, value)
    return html


@app.route("/api/status")
def api_status():
    """Panelin canlı durum bilgisi için JSON döndürür."""
    cpu_kalan = max(0, int(round(cpu_stress_until - time.time())))
    resp = {
        "servis": service_name,
        "durum": "calisiyor",
        "leak_eleman": len(memory_leak_list),
        "ram_simulasyon_yuku_mb": len(memory_leak_list) * 10,
        "cpu_stres_aktif": cpu_kalan > 0,
        "cpu_kalan_sn": cpu_kalan,
        "cpu_alarm_esigi": CPU_STRESS_ALERT_THRESHOLD,
        "cpu_telegram_uyari": cpu_kalan > CPU_STRESS_ALERT_THRESHOLD,
        "avg_latency_ms": _avg_latency_ms(),
        "latency_aktif": time.time() < latency_inject_until,
        "latency_seviye_ms": latency_inject_ms if time.time() < latency_inject_until else 0,
        "latency_uyari_esigi": LATENCY_WARN_THRESHOLD,
        "latency_aksiyon_esigi": LATENCY_ACTION_THRESHOLD,
        "ram_tahmin": _ram_trend_analiz(),
    }
    if is_service_a:
        try:
            conn = get_pg_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM service_events WHERE event_type='manual_add'")
            manual_count = cur.fetchone()[0]
            conn.close()
            resp["db_durum"] = "bagli"
            resp["db_manual_count"] = manual_count
            resp["db_limit"] = DB_MAX_THRESHOLD
            resp["db_uyari"] = (
                "kritik" if manual_count >= DB_MAX_THRESHOLD
                else "uyari" if manual_count > DB_WARN_THRESHOLD
                else "normal"
            )
        except Exception:
            resp["db_durum"] = "cokmus"
    elif is_service_b:
        try:
            rc = get_redis_client()
            rc.ping()
            resp["redis_durum"] = "bagli"
            resp["cache_anahtar"] = rc.dbsize()
        except Exception:
            resp["redis_durum"] = "cokmus"
    return jsonify(resp)


@app.route("/islem")
def islem():
    """TRAFİK YÖNLENDİRME (failover) demosu icin is yuku endpoint'i.
    Gateway bu istegi once PRIMARY'ye (Servis C) gonderir; Servis C cokmusse
    otomatik olarak YEDEK'e (Servis A) yonlendirir. Yaniti hangi servisin
    verdigi acikca gorunur, boylece kesintisizlik (downtime=0) gosterilir."""
    rol = "PRIMARY" if is_service_c else "YEDEK"
    sonuc = sum(i * i for i in range(2000))  # kucuk, deterministik is yuku
    return jsonify(
        {
            "yanitlayan_servis": service_name,
            "rol": rol,
            "sonuc": sonuc,
            "zaman": time.strftime("%H:%M:%S"),
            "mesaj": f"Bu istek {service_name} ({rol}) tarafindan yanitlandi.",
        }
    )


@app.route("/leak")
def leak():
    """Belleğe istenen kadar MB ekleyerek RAM sızıntısı simüle eder."""
    raw = request.args.get("mb", "100")
    try:
        mb = max(10, min(500, int(raw)))
    except ValueError:
        mb = 100
    chunks = max(1, mb // 10)
    for _ in range(chunks):
        memory_leak_list.append("X" * (10 * 1024 * 1024))
    return jsonify(
        {
            "servis": service_name,
            "eklenen_mb": chunks * 10,
            "toplam_simule_yuk_mb": len(memory_leak_list) * 10,
            "mesaj": "RAM yükü artırıldı. Gemini fark edip müdahale edecek.",
        }
    )


@app.route("/latency-stres")
def latency_stres():
    """Her tiklamada gecikmeyi +100 ms artirir (100, 200, 300...).
    300 ms -> Telegram uyari; 600 ms -> Telegram + otonom restart."""
    global latency_inject_until, latency_inject_ms
    if not is_service_c:
        return jsonify({"servis": service_name, "message": "bu endpoint sadece Servis_C icin"}), 400
    with latency_stress_lock:
        now = time.time()
        if now >= latency_inject_until:
            latency_inject_ms = 0
            latency_state["warn_sent"] = False
            latency_state["action_sent"] = False
        latency_inject_ms = min(LATENCY_MAX_MS, latency_inject_ms + LATENCY_STEP)
        latency_inject_until = max(latency_inject_until, now) + LATENCY_STRESS_SECONDS
        kalan = int(round(latency_inject_until - now))
        current = latency_inject_ms
        _check_latency_thresholds(current)
    return jsonify(
        {
            "servis": service_name,
            "status": "ok",
            "gecikme_ms": current,
            "eklenen_ms": LATENCY_STEP,
            "toplam_kalan_sn": kalan,
            "uyari_esigi": LATENCY_WARN_THRESHOLD,
            "aksiyon_esigi": LATENCY_ACTION_THRESHOLD,
            "mesaj": (
                f"Gecikme {current} ms. "
                f"Uyari: {LATENCY_WARN_THRESHOLD} ms, aksiyon: {LATENCY_ACTION_THRESHOLD} ms."
            ),
        }
    )


@app.route("/durum")
def durum():
    return jsonify(
        {
            "servis": service_name,
            "durum": "calisiyor",
            "memory_leak_eleman_sayisi": len(memory_leak_list),
            "memory_leak_yuku_mb": len(memory_leak_list) * 10,
            "ortalama_gecikme_ms": _avg_latency_ms(),
        }
    )


@app.route("/health")
def health():
    """Tüm servisler için ortak health endpoint."""
    response = {
        "servis": service_name,
        "status": "ok",
        "ram_simulasyon_yuku_mb": len(memory_leak_list) * 10,
    }
    if is_service_a:
        try:
            conn = get_pg_connection()
            conn.cursor().execute("SELECT 1")
            conn.close()
            response["db"] = "ok"
        except Exception as exc:
            response["status"] = "degraded"
            response["db"] = f"hata: {exc}"
    elif is_service_b:
        try:
            rc = get_redis_client()
            rc.ping()
            response["redis"] = "ok"
            response["cache_size"] = rc.dbsize()
        except Exception as exc:
            response["status"] = "degraded"
            response["redis"] = f"hata: {exc}"
    return jsonify(response)


@app.route("/db/health")
def db_health():
    """Servis_A PostgreSQL bağlantısını test eder."""
    if not is_service_a:
        return jsonify({"servis": service_name, "db": "kullanilmiyor"})
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM service_events")
        event_count = cur.fetchone()[0]
        conn.close()
        return jsonify(
            {
                "servis": service_name,
                "db": "bagli",
                "engine": "postgresql",
                "host": pg_host,
                "event_count": event_count,
            }
        )
    except Exception as exc:
        return jsonify({"servis": service_name, "db": "hata", "error": str(exc)}), 500


@app.route("/db/disconnect")
def db_disconnect():
    """Servis_A: PostgreSQL bağlantısını MANUEL koparır (demo).
    Sağlık izleyici bunu görüp n8n'e 'koptu' bildirimi gönderir; n8n workflow'u
    Gemini ile analiz edip bağlantıyı yeniden kurar."""
    global db_blocked
    if not is_service_a:
        return jsonify({"servis": service_name, "db": "kullanilmiyor"})
    db_blocked = True
    return jsonify(
        {
            "servis": service_name,
            "db": "koparildi",
            "message": "PostgreSQL baglantisi manuel koparildi. n8n algilayip yeniden kuracak.",
        }
    )


@app.route("/db/reconnect")
def db_reconnect():
    """Servis_A: manuel koparılan bağlantıyı geri açar (n8n recovery bunu çağırır)."""
    global db_blocked
    if not is_service_a:
        return jsonify({"servis": service_name, "db": "kullanilmiyor"})
    db_blocked = False
    return jsonify(
        {
            "servis": service_name,
            "db": "yeniden_acildi",
            "message": "PostgreSQL baglantisi yeniden acildi.",
        }
    )


@app.route("/db/add")
def db_add():
    """Servis_A: manuel metinli kayit ekler. Esikler _sync_manual_records ile islenir."""
    if not is_service_a:
        return jsonify({"servis": service_name, "db": "kullanilmiyor"})
    note = (request.args.get("note") or request.form.get("note") or "").strip()
    if not note:
        return jsonify({"servis": service_name, "error": "note parametresi gerekli (ornek: ?note=Siparis%2042)"}), 400
    if len(note) > 120:
        note = note[:120]
    try:
        conn = get_pg_connection()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO service_events(event_type, note) VALUES (%s, %s)",
            ("manual_add", note),
        )
        conn.close()

        result = _sync_manual_records(trigger_note=note, source="web")
        manual_count = result["manual_count"]
        uyari = result["uyari"]
        silinen_kayitlar = result["silinen_kayitlar"]
        kalan_kayitlar = result["kalan_kayitlar"]
        temizlenen = len(silinen_kayitlar)

        if temizlenen > 0:
            silinen_isimler = ", ".join(k["note"] for k in silinen_kayitlar)
            mesaj = (
                f"KAPASITE ASILDI! \"{note}\" eklendi ama retention devreye girdi. "
                f"Silinen eski kayitlar ({temizlenen}): {silinen_isimler}. "
                f"Limit {DB_MAX_THRESHOLD} kayitta tutuldu."
            )
        elif uyari == "uyari":
            mesaj = (
                f"UYARI: \"{note}\" eklendi. Manuel kayit sayisi {manual_count}. "
                f"{DB_MAX_THRESHOLD} sinirina yaklasildi, asilirsa en eski kayitlar silinecek."
            )
        else:
            mesaj = f"Kayit eklendi: \"{note}\""

        return jsonify(
            {
                "servis": service_name,
                "db": "bagli",
                "message": mesaj,
                "eklenen": note,
                "manual_kayit": manual_count,
                "uyari_seviyesi": uyari,
                "temizlenen_kayit": temizlenen,
                "silinen_kayitlar": silinen_kayitlar,
                "kalan_kayitlar": kalan_kayitlar,
                "limit": DB_MAX_THRESHOLD,
            }
        )
    except Exception as exc:
        return jsonify({"servis": service_name, "db": "hata", "error": str(exc)}), 500


@app.route("/db/list")
def db_list():
    """Servis_A manuel kayitlari listeler (retention sonrasi ne kaldigini gormek icin)."""
    if not is_service_a:
        return jsonify({"servis": service_name, "db": "kullanilmiyor"})
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, note, created_at FROM service_events
            WHERE event_type='manual_add'
            ORDER BY id ASC
            """
        )
        kayitlar = [
            {"id": row[0], "note": row[1] or f"#{row[0]}", "created_at": row[2].isoformat()}
            for row in cur.fetchall()
        ]
        conn.close()
        return jsonify(
            {
                "servis": service_name,
                "manual_kayit": len(kayitlar),
                "limit": DB_MAX_THRESHOLD,
                "uyari_seviyesi": (
                    "kritik" if len(kayitlar) >= DB_MAX_THRESHOLD
                    else "uyari" if len(kayitlar) > DB_WARN_THRESHOLD
                    else "normal"
                ),
                "kayitlar": kayitlar,
            }
        )
    except Exception as exc:
        return jsonify({"servis": service_name, "db": "hata", "error": str(exc)}), 500


@app.route("/data/items")
def data_items():
    """Servis_A veri listesi: önce Redis cache, yoksa PostgreSQL'den oku ve cache'le."""
    if not is_service_a:
        return jsonify({"servis": service_name, "message": "bu endpoint sadece Servis_A icin"}), 400

    try:
        rc = get_redis_client()
        cached = rc.get(cache_key_a)
        if cached:
            return jsonify({"servis": service_name, "source": "redis_cache", "items": json.loads(cached)})
    except Exception:
        pass

    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM items ORDER BY id")
        items = [{"id": row[0], "name": row[1]} for row in cur.fetchall()]
        conn.close()
    except Exception as exc:
        return jsonify({"servis": service_name, "source": "postgresql", "error": str(exc)}), 500

    try:
        rc = get_redis_client()
        rc.setex(cache_key_a, 60, json.dumps(items))
    except Exception:
        pass

    return jsonify({"servis": service_name, "source": "postgresql", "items": items})


@app.route("/cache/set/<key>")
def cache_set(key):
    if not is_service_b:
        return jsonify({"servis": service_name, "message": "bu endpoint sadece Servis_B icin"}), 400
    value = request.args.get("value", "")
    ttl = int(request.args.get("ttl", "120"))
    rc = get_redis_client()
    rc.setex(f"servis_b:{key}", ttl, value)
    return jsonify({"servis": service_name, "status": "ok", "key": key, "ttl": ttl})


@app.route("/cache/get/<key>")
def cache_get(key):
    if not is_service_b:
        return jsonify({"servis": service_name, "message": "bu endpoint sadece Servis_B icin"}), 400
    rc = get_redis_client()
    value = rc.get(f"servis_b:{key}")
    return jsonify({"servis": service_name, "key": key, "value": value})


@app.route("/cache/stats")
def cache_stats():
    if not is_service_b:
        return jsonify({"servis": service_name, "message": "bu endpoint sadece Servis_B icin"}), 400
    rc = get_redis_client()
    info = rc.info(section="memory")
    return jsonify(
        {
            "servis": service_name,
            "used_memory_human": info.get("used_memory_human"),
            "maxmemory_human": info.get("maxmemory_human"),
            "dbsize": rc.dbsize(),
        }
    )


@app.route("/cache/evict")
def cache_evict():
    if not is_service_b:
        return jsonify({"servis": service_name, "message": "bu endpoint sadece Servis_B icin"}), 400
    rc = get_redis_client()
    rc.flushdb()
    return jsonify({"servis": service_name, "status": "ok", "message": "cache temizlendi"})


@app.route("/cache/flood")
def cache_flood():
    """Servis_B: Redis önbelleğini MANUEL şişirir (demo). dbsize eşiği aşınca
    _cache_monitor n8n'e 'sisti' bildirimi gönderir; Gemini analiz edip cache temizler."""
    if not is_service_b:
        return jsonify({"servis": service_name, "message": "bu endpoint sadece Servis_B icin"}), 400
    count = int(request.args.get("count", "2000"))
    rc = get_redis_client()
    pipe = rc.pipeline()
    for i in range(count):
        pipe.set(f"flood:{i}", "x" * 200)
    pipe.execute()
    return jsonify(
        {
            "servis": service_name,
            "status": "ok",
            "yazilan": count,
            "dbsize": rc.dbsize(),
            "message": "Onbellek sisirildi. n8n algilayip Gemini ile temizleyecek.",
        }
    )


@app.route("/boz")
def boz():
    """Servisi anında tamamen kapatır (Process Exit). 'up=0' (çökme) testi için."""
    os._exit(1)


def _cpu_burn(end_time):
    while time.time() < end_time:
        pass


@app.route("/cpu-stres")
def cpu_stres():
    """Her tiklamada +30 sn CPU stresi ekler (sure birikir). Arayuz canli gosterir.
    Kalan sure 100sn'yi gecince n8n Telegram bildirimi gonderir."""
    global cpu_stress_until
    with cpu_stress_lock:
        now = time.time()
        cpu_stress_until = max(cpu_stress_until, now) + CPU_STRESS_SECONDS
        toplam_kalan = int(round(cpu_stress_until - now))
        end = cpu_stress_until
    try:
        multiprocessing.Process(target=_cpu_burn, args=(end,), daemon=True).start()
    except Exception:
        threading.Thread(target=_cpu_burn, args=(end,), daemon=True).start()
    alarm = toplam_kalan > CPU_STRESS_ALERT_THRESHOLD
    return jsonify(
        {
            "servis": service_name,
            "mesaj": f"+{CPU_STRESS_SECONDS} sn eklendi. Toplam kalan: {toplam_kalan} sn."
            + (" 100sn ASILDI — Telegram uyarisi gelecek!" if alarm else ""),
            "eklenen_sn": CPU_STRESS_SECONDS,
            "toplam_kalan_sn": toplam_kalan,
            "alarm_esigi": CPU_STRESS_ALERT_THRESHOLD,
            "telegram_uyari": alarm,
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
