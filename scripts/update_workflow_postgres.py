#!/usr/bin/env python3
"""PostgreSQL geçişi sonrası n8n workflow'larını günceller.
- DB çökme algılama (servis_a_db_up metriği)
- Gemini araçlarını sadeleştirir (rabbitmq/ban kaldırılır)
- Güvenlik + unban workflow'larını devre dışı bırakır
ÖNEMLİ: n8n DURDURULMUŞ olmalı (docker compose down/stop).
"""
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB = Path(__file__).resolve().parents[1] / "n8n_data" / "database.sqlite"
ORCH_ID = "OtomonMikroservisOrkestratoru"
ACTIVE_VERSION = "8f678e91-919c-4742-9055-5d61e2f576a8"

# Temel metrikler + uygulama içinde hesaplanan RAM TAHMİNİ (servis_ram_tahmin_mb).
# Tahmin app.py'de doğrusal regresyonla üretilir; burada normal metrik gibi okunur.
PROM_QUERY = (
    '{__name__=~"process_resident_memory_bytes|up|servis_a_db_up|'
    'servis_cpu_stress_seconds|servis_avg_latency_ms|servis_ram_tahmin_mb|'
    'servis_ram_egim_mb_dk|servis_ram_trend_guven",'
    'job="mikroservisler"}'
)

CPU_ALERT_THRESHOLD = 100   # CPU stresi bu saniyeyi gecince Telegram uyarisi
LATENCY_THRESHOLD = 800     # orkestrator yedek esigi (Servis C webhook 300/600 kullanir)
RAM_PREDICT_THRESHOLD = 250  # 2 dk sonra RAM (MB) bunu asacaksa proaktif uyari
RAM_HIZ_MIN = 5.0           # MB/dk; bu hizin ustu + yuksek guven = anomali (hizli buyume)
RAM_GUVEN_MIN = 0.70        # R2 guven esigi (tahmine guvenmek icin)

PARSE_JS = r"""// Prometheus'tan gelen metrikleri servis bazında ayrıştır
const results = $input.first().json.data.result;
const services = {};
let dbUp = 1;

for (const r of results) {
  const metricName = r.metric.__name__;
  const instance = r.metric.instance;
  const servisAdi = instance.split(':')[0];

  if (metricName === 'servis_a_db_up') {
    if (servisAdi === 'servis_a') dbUp = r.value[1] === '0' ? 0 : 1;
    continue;
  }

  if (!services[servisAdi]) services[servisAdi] = { servis: servisAdi, instance };

  if (metricName === 'process_resident_memory_bytes') {
    const bytes = parseFloat(r.value[1]);
    services[servisAdi].ram_bytes = bytes;
    services[servisAdi].ram_mb = parseFloat((bytes / 1048576).toFixed(2));
  } else if (metricName === 'up') {
    services[servisAdi].up = r.value[1] === '0' ? 0 : 1;
  } else if (metricName === 'servis_cpu_stress_seconds') {
    services[servisAdi].cpu_stress_sn = Math.round(parseFloat(r.value[1]));
  } else if (metricName === 'servis_avg_latency_ms') {
    services[servisAdi].avg_latency_ms = Math.round(parseFloat(r.value[1]));
  } else if (metricName === 'servis_ram_tahmin_mb') {
    services[servisAdi].ram_tahmin_mb = parseFloat(parseFloat(r.value[1]).toFixed(2));
  } else if (metricName === 'servis_ram_egim_mb_dk') {
    services[servisAdi].ram_egim_mb_dk = parseFloat(parseFloat(r.value[1]).toFixed(2));
  } else if (metricName === 'servis_ram_trend_guven') {
    services[servisAdi].ram_trend_guven = parseFloat(parseFloat(r.value[1]).toFixed(3));
  }
}

const CPU_ESIK = __CPU_ESIK__;
const LAT_ESIK = __LAT_ESIK__;
const TAHMIN_ESIK = __TAHMIN_ESIK__;
const HIZ_MIN = __HIZ_MIN__;
const GUVEN_MIN = __GUVEN_MIN__;
const beklenenServisler = ['servis_a', 'servis_b', 'servis_c'];
for (const s of beklenenServisler) {
  if (!services[s]) services[s] = { servis: s, instance: s + ':5000', ram_mb: 0, ram_bytes: 0, up: 0 };
  if (services[s].up === undefined) services[s].up = 1;
  if (services[s].ram_mb === undefined) services[s].ram_mb = 0;
  if (services[s].cpu_stress_sn === undefined) services[s].cpu_stress_sn = 0;
  if (services[s].avg_latency_ms === undefined) services[s].avg_latency_ms = 0;
  if (services[s].ram_tahmin_mb === undefined) services[s].ram_tahmin_mb = 0;
  if (services[s].ram_egim_mb_dk === undefined) services[s].ram_egim_mb_dk = 0;
  if (services[s].ram_trend_guven === undefined) services[s].ram_trend_guven = 0;
}

const izlenecekServisler = Object.values(services)
  .map((s) => {
    // Eşiğe kalan süre (ETA): mevcut RAM'den TAHMIN_ESIK'e, eğim hızıyla kaç dakika
    let eta_dk = -1;
    if (s.ram_egim_mb_dk > 0.1 && s.ram_mb < TAHMIN_ESIK) {
      eta_dk = parseFloat(((TAHMIN_ESIK - s.ram_mb) / s.ram_egim_mb_dk).toFixed(1));
    }
    // Anomali (proaktif): hızlı artış + yüksek güven + henüz kritik değil
    const ram_anomali = (s.ram_egim_mb_dk >= HIZ_MIN && s.ram_trend_guven >= GUVEN_MIN && s.ram_mb <= 200 && s.up === 1);
    const tahmin_riski = ram_anomali || (s.ram_tahmin_mb > TAHMIN_ESIK && s.ram_mb <= 150 && s.up === 1);
    const high_latency = s.avg_latency_ms > LAT_ESIK;
    const cpu_stress_high = s.cpu_stress_sn > CPU_ESIK;
    // servis_c primary trafik sunucusu: cokerse failover devreye girer
    const primary_down = (s.servis === 'servis_c' && s.up === 0);
    return { ...s, cpu_stress_high, high_latency, tahmin_riski, ram_anomali, eta_dk, primary_down,
      predictive_risk: (s.up === 0) ? 'CRITICAL'
        : (cpu_stress_high || s.ram_mb > 200) ? 'HIGH'
        : (high_latency) ? 'HIGH'
        : (tahmin_riski) ? 'EARLY_WARNING'
        : 'OK' };
  })
  .filter((s) => s.ram_mb > 150 || s.up === 0 || s.cpu_stress_high || s.high_latency || s.tahmin_riski);

// PostgreSQL çökmüşse (servis_a_db_up=0) postgres'i müdahale listesine ekle
if (dbUp === 0) {
  izlenecekServisler.push({
    servis: 'postgres',
    instance: 'postgres:5432',
    containerName: 'tasarim_proje-postgres-1',
    up: 0,
    ram_mb: 0,
    db_down: true,
    predictive_risk: 'CRITICAL'
  });
}

return [{ json: { servisler: izlenecekServisler } }];"""

AGENT_SYSTEM = """Sen tam otonom DevOps süper ajanısın. Verilen metrikleri analiz et, gerekirse get_logs ile log oku, KARAR VER ve uygun aracı ÇALIŞTIR.

Kullanabileceğin araçlar:
- get_logs: Docker loglarını oku (parametre: konteyner adı, örn tasarim_proje-servis_a-1)
- restart_service: Konteyneri yeniden başlat (parametre: konteyner adı)
- cache_evict_servis_b: Servis B Redis önbelleğini temizle (parametre yok, boş string gönder)

Karar rehberi (nihai kararı sen verirsin):
- up=0 (servis çökmüş): önce get_logs ile incele, sonra restart_service ile ilgili konteyneri yeniden başlat
- RAM yüksek (>150MB): bellek sızıntısı olabilir; restart_service uygula
- servis_b + yüksek RAM: önce cache_evict_servis_b dene, yetmezse restart_service
- db_down=true VEYA servis=postgres: VERİTABANI ÇÖKMÜŞ demektir. restart_service ile tasarim_proje-postgres-1 konteynerini yeniden başlat.
- cpu_stress_high=true (CPU stresi 100sn üstü): Servis anormal işlemci yükü altında. Bu KESİNLİKLE bir sorundur, SISTEM-OK YAZMA. Durumu Türkçe raporla (Telegram'a bildirim gidecek). Yükü temizlemek istersen restart_service kullanabilirsin; kullanmasan da mutlaka bildirim üret.
- high_latency=true (ortalama gecikme yüksek): Servis yavaş yanıt veriyor, kullanıcı deneyimi bozuluyor. get_logs ile incele; kalıcı görünüyorsa restart_service uygula. Mutlaka Türkçe rapor üret.
- tahmin_riski=true (PROAKTİF/ÖNGÖRÜ): RAM şu an kritik değil AMA trend analizine göre kritik eşiğe gidiyor. Bu bir ERKEN UYARIDIR. Sana şu kanıtlar verilir: ram_egim_mb_dk (RAM kaç MB/dakika hızla artıyor), ram_trend_guven (R² regresyon güven skoru, 1'e yakınsa tahmin güvenilir), eta_dk (mevcut hızla kritik eşiğe kaç dakika kaldığı). ram_anomali=true ise artış hem hızlı hem yüksek güvenilirliktedir. Raporunda bu sayıları kullan (örn: 'RAM 18 MB/dk hızla artıyor, R²=0.96 güvenle ~7 dk sonra eşiği aşacak'). Sorun büyümeden get_logs ile bak ve gerekiyorsa erkenden restart_service ile RAM'i sıfırla. Bunun bir TAHMİNE DAYALI ÖNLEME (predictive maintenance) aksiyonu olduğunu belirt.
- primary_down=true (servis_c çökmüş): Servis C aynı zamanda /islem trafiğinin PRIMARY sunucusudur. Çöktüğünde Nginx gateway trafiği otomatik olarak YEDEK'e (Servis A) yönlendirdi; yani kullanıcılar kesinti yaşamadı (downtime=0). Sen primary'i (tasarim_proje-servis_c-1) restart_service ile geri getir. Raporda 'trafik yedeğe yönlendirildi, kesinti olmadı, primary geri getiriliyor' bilgisini ver.

ZORUNLU rapor formatı (Türkçe) — yanıtının İLK satırı tam olarak şu olmalı:
SERVIS: <servis_adı> | KONTEYNER: <konteyner_adı>

🔍 Kök Neden: ...
⚙️ Yapılan Aksiyonlar: (hangi araçlar çalıştı)
✅ Sonuç: ...

Gerçek bir sorun yoksa ve araç çalıştırmadıysan yanıtının sonuna SISTEM-OK ekle."""

AGENT_PROMPT = ("=OTONOM MÜDAHALE GÖREVİ\n"
                "Servis: {{ $json.servis }}\n"
                "Konteyner: {{ $json.containerName }}\n"
                "RAM: {{ $json.ram_mb }} MB\n"
                "Durum: {{ $json.up === 0 ? 'DOWN' : 'RUNNING' }}\n"
                "DB çökmüş mü: {{ $json.db_down ? 'EVET' : 'hayır' }}\n"
                "CPU stres: {{ $json.cpu_stress_sn }} sn (100 üstü = anormal işlemci yükü)\n"
                "CPU stres alarmı: {{ $json.cpu_stress_high ? 'EVET (100sn aşıldı)' : 'hayır' }}\n"
                "Ortalama gecikme: {{ $json.avg_latency_ms }} ms (800 üstü = yüksek latency, yedek)\n"
                "Yüksek gecikme: {{ $json.high_latency ? 'EVET' : 'hayır' }}\n"
                "RAM tahmini (2 dk sonra): {{ $json.ram_tahmin_mb }} MB\n"
                "RAM artış hızı: {{ $json.ram_egim_mb_dk }} MB/dk\n"
                "Trend güveni (R²): {{ $json.ram_trend_guven }}\n"
                "Eşiğe kalan süre (ETA): {{ $json.eta_dk }} dk\n"
                "Tahmin riski (proaktif): {{ $json.tahmin_riski ? 'EVET (RAM kritiğe gidiyor)' : 'hayır' }}\n"
                "Primary (Servis C) çökmesi / failover: {{ $json.primary_down ? 'EVET - trafik yedeğe yönlendirildi' : 'hayır' }}\n"
                "Risk: {{ $json.predictive_risk }}\n\n"
                "Analiz et, gerekirse log oku, karar ver ve araçları çalıştır. "
                "İlk satır: SERVIS: {{ $json.servis }} | KONTEYNER: {{ $json.containerName }}")

REMOVE_TOOLS = {"check_queue Aracı", "ban_ip Aracı"}


def update_orchestrator(nodes, conns):
    new_nodes = []
    for n in nodes:
        if n["name"] in REMOVE_TOOLS:
            continue
        if n["name"] == "Prometheus Metrikleri Çek":
            n["parameters"]["queryParameters"]["parameters"][0]["value"] = PROM_QUERY
        elif n["name"] == "Metrikleri Ayrıştır":
            n["parameters"]["jsCode"] = (
                PARSE_JS
                .replace("__CPU_ESIK__", str(CPU_ALERT_THRESHOLD))
                .replace("__LAT_ESIK__", str(LATENCY_THRESHOLD))
                .replace("__TAHMIN_ESIK__", str(RAM_PREDICT_THRESHOLD))
                .replace("__HIZ_MIN__", str(RAM_HIZ_MIN))
                .replace("__GUVEN_MIN__", str(RAM_GUVEN_MIN))
            )
        elif n["name"] == "Bağışıklık Ajanı (Gemini)":
            n["parameters"]["options"]["systemMessage"] = AGENT_SYSTEM
            n["parameters"]["text"] = AGENT_PROMPT
        new_nodes.append(n)

    for t in REMOVE_TOOLS:
        conns.pop(t, None)

    return new_nodes, conns


def main():
    c = sqlite3.connect(DB)
    c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    # workflow_history (n8n bunu çalıştırıyor)
    row = c.execute(
        "SELECT nodes, connections FROM workflow_history WHERE versionId=?", (ACTIVE_VERSION,)
    ).fetchone()
    nodes, conns = json.loads(row[0]), json.loads(row[1])
    nodes, conns = update_orchestrator(nodes, conns)
    c.execute(
        "UPDATE workflow_history SET nodes=?, connections=?, updatedAt=? WHERE versionId=?",
        (json.dumps(nodes), json.dumps(conns), now, ACTIVE_VERSION),
    )
    c.execute(
        "UPDATE workflow_entity SET nodes=?, connections=?, updatedAt=? WHERE id=?",
        (json.dumps(nodes), json.dumps(conns), now, ORCH_ID),
    )
    print("Orkestratör güncellendi (DB algılama + sade araçlar)")

    # Güvenlik + unban workflow'larını devre dışı bırak
    for wid in ("GuvenlikIpKorumaWorkflow", "OtomatikUnbanWorkflow"):
        c.execute("UPDATE workflow_entity SET active=0 WHERE id=?", (wid,))
        c.execute("DELETE FROM workflow_published_version WHERE workflowId=?", (wid,))
        print(f"Devre dışı: {wid}")

    c.commit()
    c.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    # Doğrulama
    v = json.loads(c.execute(
        "SELECT nodes FROM workflow_history WHERE versionId=?", (ACTIVE_VERSION,)
    ).fetchone()[0])
    tool_names = [n["name"] for n in v if "Aracı" in n["name"]]
    print("Kalan araçlar:", tool_names)
    print("DB OK")


if __name__ == "__main__":
    main()
