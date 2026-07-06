#!/usr/bin/env python3
"""Otonom Gemini workflow'larını oluşturur ve n8n SQLite veritabanına yazar."""
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "n8n_data" / "database.sqlite"
GEMINI_CRED = {"googlePalmApi": {"id": "BAcNUipRMfkaFtgV", "name": "Google Gemini(PaLM) Api account"}}

AUTONOMOUS_SYSTEM = """Sen tam otonom DevOps süper ajanısın. Verilen metrikleri analiz et, gerekirse get_logs ile log oku, KARAR VER ve uygun aracı ÇALIŞTIR.

Kullanabileceğin araçlar:
- get_logs: Docker loglarını oku (parametre: konteyner adı, örn tasarim_proje-servis_a-1)
- restart_service: Konteyneri yeniden başlat (parametre: konteyner adı)
- cache_evict_servis_b: Servis B Redis önbelleğini temizle (parametre yok, boş string gönder)
- check_rabbitmq_queue: Servis C kuyruk derinliğini oku (parametre yok)
- ban_gateway_ip: Şüpheli dış IP'yi nginx gateway'de banla (parametre: IP adresi, örn 203.0.113.5)

Karar rehberi (sen nihai kararı verirsin):
- up=0 veya RAM çok yüksek: önce log oku, sonra restart_service
- servis_b + yüksek RAM: cache_evict_servis_b dene, yetmezse restart_service
- servis_c + kuyruk dolu: check_rabbitmq_queue, gerekirse restart_service veya worker uyarısı yaz
- Açık saldırı/DDoS şüphesi: ban_gateway_ip (sadece net şüpheli dış IP için)

Rapor formatı (Türkçe, kısa):
🔍 Kök Neden: ...
⚙️ Yapılan Aksiyonlar: (hangi araçlar çalıştı)
✅ Sonuç: ...

Gerçek sorun yoksa ve araç çalıştırmadıysan çıktının sonuna SISTEM-OK ekle.
Sorun varsa mutlaka en az bir araç çalıştır veya neden çalıştıramadığını yaz."""

REPORT_JS = r"""// Agent node sadece output alanını geçirir; servis bilgilerini önceki node'dan al
const prevData = $('Konteyner Adı Türet').item.json;

const servis = String($json.servis || prevData.servis || 'bilinmeyen');
const servisLabel = servis.replace(/_/g, ' ').toUpperCase();
const ram = Number($json.ram_mb ?? prevData.ram_mb ?? 0);
const up = Number($json.up ?? prevData.up ?? 1);
const risk = String($json.predictive_risk || prevData.predictive_risk || '');
const containerName = String($json.containerName || prevData.containerName || '');
const aiOutput = ($json.output || '').toString().trim();

const hasAction = /restart|ban|evict|banland|yeniden|temizlendi|mudahale|uygulandi|calistirildi/i.test(aiOutput);
const isOk = aiOutput.includes('SISTEM-OK') && !hasAction;

if (isOk) {
  return [{ json: { ...$json, output: 'SISTEM-OK' } }];
}

const msg = '🤖 OTONOM MÜDAHALE (Gemini)\n'
  + '📌 Servis: ' + servisLabel + '\n'
  + '📦 Konteyner: ' + containerName + '\n'
  + '📊 RAM: ' + ram.toFixed(2) + ' MB | up=' + up + ' | Risk: ' + risk + '\n\n'
  + aiOutput;

return [{ json: { ...$json, output: msg } }];"""

CACHE_EVICT_JS = r"""const http = require('http');
return new Promise((resolve) => {
  const req = http.get('http://servis_b:5000/cache/evict', (res) => {
    let d = '';
    res.on('data', (c) => { d += c; });
    res.on('end', () => resolve('cache_evict_servis_b: ' + (res.statusCode === 200 ? 'basarili' : 'hata ' + res.statusCode) + ' ' + d.slice(0, 120));
  });
  req.on('error', (e) => resolve('cache_evict_servis_b hata: ' + e.message));
  req.setTimeout(8000, () => { req.destroy(); resolve('cache_evict_servis_b: timeout'); });
});"""

QUEUE_JS = r"""const http = require('http');
return new Promise((resolve) => {
  const req = http.get('http://servis_c:5000/queue/stats', (res) => {
    let d = '';
    res.on('data', (c) => { d += c; });
    res.on('end', () => {
      try {
        const p = JSON.parse(d);
        const q = p.queue || {};
        resolve('kuyruk: messages=' + (q.messages||0) + ' consumers=' + (q.consumers||0) + ' ready=' + (q.messages_ready||0));
      } catch (e) { resolve('queue parse hata: ' + d.slice(0, 100)); }
    });
  });
  req.on('error', (e) => resolve('queue hata: ' + e.message));
  req.setTimeout(8000, () => { req.destroy(); resolve('queue: timeout'); });
});"""

BAN_IP_JS = r"""const execSync = require('child_process').execSync;
const ip = (query || '').trim();
if (!/^\d+\.\d+\.\d+\.\d+$/.test(ip)) return 'Gecersiz IP: ' + ip;
const gatewayContainer = 'tasarim_proje-servis_d_gateway-1';
const banFile = '/etc/nginx/blocked_ips.conf';
const now = Date.now();
try {
  const already = execSync(
    `/usr/bin/docker exec ${gatewayContainer} grep -c 'deny ${ip};' ${banFile} || true`,
    { timeout: 6000 }
  ).toString().trim();
  if (parseInt(already, 10) > 0) return `IP ${ip} zaten banli`;
  const entry = `deny ${ip}; # banned_at=${now}`;
  execSync(
    `/usr/bin/docker exec ${gatewayContainer} sh -c "echo '${entry}' >> ${banFile}"`,
    { timeout: 10000 }
  );
  execSync(`/usr/bin/docker exec ${gatewayContainer} nginx -s reload`, { timeout: 10000 });
  return `IP ${ip} banlandi ve nginx reload edildi`;
} catch (e) {
  return 'ban_gateway_ip hata: ' + e.message.slice(0, 150);
}"""

SECURITY_PREPARE_JS = r"""const execSync = require('child_process').execSync;
const gatewayContainer = 'tasarim_proje-servis_d_gateway-1';
const threshold = 120;
let logs = '';
try {
  logs = execSync(`/usr/bin/docker logs --since 65s ${gatewayContainer} 2>&1`, { timeout: 15000 }).toString();
} catch (e) {
  return [{ json: { output: 'SISTEM-OK', detail: 'Log okunamadi', logSummary: '' } }];
}
const counts = {};
function parseIp(line) {
  const m = line.match(/^(\d+\.\d+\.\d+\.\d+)/);
  return m ? m[1] : null;
}
for (const line of logs.split('\n')) {
  const ip = parseIp(line.trim());
  if (!ip) continue;
  if (ip.startsWith('127.') || ip.startsWith('172.') || ip.startsWith('10.') || ip.startsWith('192.168.')) continue;
  counts[ip] = (counts[ip] || 0) + 1;
}
const suspects = Object.entries(counts)
  .filter(([, n]) => n >= threshold)
  .map(([ip, n]) => ({ ip, requests: n }));
if (suspects.length === 0) {
  return [{ json: { output: 'SISTEM-OK', detail: 'Supheli IP yok', suspects: [], logSummary: logs.slice(0, 1500) } }];
}
return [{
  json: {
    output: 'SUPHELI_IP',
    detail: 'Gemini karar verecek',
    suspects,
    threshold,
    logSummary: logs.slice(0, 2000)
  }
}];"""

SECURITY_SYSTEM = """Sen otonom güvenlik ajanısın. Gateway nginx log özetine ve şüpheli IP listesine bak.

Araçlar:
- ban_gateway_ip: Verilen IP'yi nginx blocked_ips.conf'a ekle (parametre: IP)
- get_gateway_logs: Son 65 sn gateway loglarını oku (parametre yok)

Karar: Gerçek saldırı/DDoS ise ban_gateway_ip çalıştır. Yanlış pozitif (iç ağ, tek seferlik spike) ise banlama.

Rapor (Türkçe):
🚫 Güvenlik Olayı veya SISTEM-OK
Banlanan IP'ler, gerekçe.

Şüphe yoksa çıktıda SISTEM-OK yaz ve araç kullanma."""

SECURITY_REPORT_JS = r"""const aiOutput = ($json.output || '').toString().trim();
const detail = String($json.detail || '');
const suspects = $json.suspects || [];
if (aiOutput.includes('SISTEM-OK') && !/ban|banlandi/i.test(aiOutput)) {
  return [{ json: { output: 'SISTEM-OK', detail: 'Supheli IP yok' } }];
}
const suspectStr = suspects.map((s) => s.ip + ' (' + s.requests + ' istek)').join(', ');
return [{
  json: {
    output: aiOutput.includes('Güvenlik') ? aiOutput : ('🚫 Güvenlik Olayı\n' + aiOutput),
    detail: detail + (suspectStr ? '\nAday IP: ' + suspectStr : '')
  }
}];"""

GET_LOGS_JS = r"""const execSync = require('child_process').execSync;
const containerName = (query || '').trim();
try {
  const logs = execSync(`/usr/bin/docker logs --tail 80 ${containerName} 2>&1`, { timeout: 15000 }).toString();
  return logs || '(Log bos)';
} catch (e) {
  return 'Log alinamadi: ' + e.message.slice(0, 150);
}"""

RESTART_JS = r"""const execSync = require('child_process').execSync;
const containerName = (query || '').trim();
try {
  execSync(`/usr/bin/docker restart ${containerName} 2>&1`, { timeout: 45000 });
  const durum = execSync(`/usr/bin/docker inspect --format '{{.State.Status}}' ${containerName} 2>&1`, { timeout: 10000 }).toString().trim();
  return `${containerName} yeniden baslatildi | Durum: ${durum}`;
} catch (e) {
  return 'Restart basarisiz: ' + e.message.slice(0, 150);
}"""


def tool_node(node_id, name, description, js_code, position):
    return {
        "parameters": {
            "name": name,
            "description": description,
            "language": "javaScript",
            "jsCode": js_code,
        },
        "type": "@n8n/n8n-nodes-langchain.toolCode",
        "typeVersion": 1.3,
        "position": position,
        "id": node_id,
        "name": name.replace("_", " ").title() + " Aracı" if "Aracı" not in name else name,
    }


def fix_tool_name(node, tool_name, display_name):
    node["parameters"]["name"] = tool_name
    node["name"] = display_name
    return node


def build_orchestrator():
    with open(ROOT / "workflow_yeni.json", encoding="utf-8") as f:
        wf = json.load(f)[0]

    for node in wf["nodes"]:
        if node["name"] == "Bağışıklık Ajanı (Gemini)":
            node["parameters"]["options"]["systemMessage"] = AUTONOMOUS_SYSTEM
            node["parameters"]["text"] = (
                "=OTONOM MÜDAHALE GÖREVİ\n"
                "Servis: {{ $json.servis }}\n"
                "Konteyner: {{ $json.containerName }}\n"
                "RAM: {{ $json.ram_mb }} MB\n"
                "Durum: {{ $json.up === 0 ? 'DOWN' : 'RUNNING' }}\n"
                "Risk: {{ $json.predictive_risk }}\n\n"
                "Metrikleri analiz et, gerekirse log oku, karar ver ve araçları çalıştır."
            )
        elif node["name"] == "Fallback Uygula ve Raporla":
            node["name"] = "Otonom Rapor Oluştur"
            node["parameters"]["jsCode"] = REPORT_JS
        elif node["name"] == "Not: Super-Agent":
            node["parameters"]["content"] = (
                "🧠 **Otonom Süper Ajan (Gemini)**\n"
                "Karar + restart + cache + kuyruk + IP ban araçları.\n"
                "Sabit kurallar kaldırıldı; aksiyonları Gemini seçer."
            )
        elif node["id"] == "a1b2c3d4-0008-0008-0008-000000000008":
            node["parameters"]["jsCode"] = GET_LOGS_JS
            node["parameters"]["name"] = "get_logs"
        elif node["id"] == "a1b2c3d4-0009-0009-0009-000000000009":
            node["parameters"]["jsCode"] = RESTART_JS
            node["parameters"]["name"] = "restart_service"
        elif node["name"] == "Gemini 2.5 Flash":
            node["credentials"] = GEMINI_CRED

    new_tools = [
        fix_tool_name(
            {
                "parameters": {
                    "name": "cache_evict_servis_b",
                    "description": "Servis B Redis onbellegini temizler. Parametre gerekmez.",
                    "language": "javaScript",
                    "jsCode": CACHE_EVICT_JS,
                },
                "type": "@n8n/n8n-nodes-langchain.toolCode",
                "typeVersion": 1.3,
                "position": [360, 240],
                "id": "a1b2c3d4-0018-0018-0018-000000000018",
                "name": "cache_evict Aracı",
            },
            "cache_evict_servis_b",
            "cache_evict Aracı",
        ),
        fix_tool_name(
            {
                "parameters": {
                    "name": "check_rabbitmq_queue",
                    "description": "Servis C RabbitMQ kuyruk istatistiklerini okur.",
                    "language": "javaScript",
                    "jsCode": QUEUE_JS,
                },
                "type": "@n8n/n8n-nodes-langchain.toolCode",
                "typeVersion": 1.3,
                "position": [600, 240],
                "id": "a1b2c3d4-0019-0019-0019-000000000019",
                "name": "check_queue Aracı",
            },
            "check_rabbitmq_queue",
            "check_queue Aracı",
        ),
        fix_tool_name(
            {
                "parameters": {
                    "name": "ban_gateway_ip",
                    "description": "Supheli dis IP'yi nginx gateway uzerinde banlar. Parametre: IP (orn 1.2.3.4)",
                    "language": "javaScript",
                    "jsCode": BAN_IP_JS,
                },
                "type": "@n8n/n8n-nodes-langchain.toolCode",
                "typeVersion": 1.3,
                "position": [840, 240],
                "id": "a1b2c3d4-0020-0020-0020-000000000020",
                "name": "ban_ip Aracı",
            },
            "ban_gateway_ip",
            "ban_ip Aracı",
        ),
    ]
    existing_ids = {n["id"] for n in wf["nodes"]}
    for t in new_tools:
        if t["id"] not in existing_ids:
            wf["nodes"].append(t)
            existing_ids.add(t["id"])

    agent_name = "Bağışıklık Ajanı (Gemini)"
    tool_nodes = [
        "get_logs Aracı",
        "restart_service Aracı",
        "cache_evict Aracı",
        "check_queue Aracı",
        "ban_ip Aracı",
    ]
    for tname in tool_nodes:
        wf["connections"][tname] = {
            "ai_tool": [[{"node": agent_name, "type": "ai_tool", "index": 0}]]
        }

    wf["connections"]["Bağışıklık Ajanı (Gemini)"]["main"] = [
        [{"node": "Otonom Rapor Oluştur", "type": "main", "index": 0}]
    ]
    if "Fallback Uygula ve Raporla" in wf["connections"]:
        del wf["connections"]["Fallback Uygula ve Raporla"]
    wf["connections"]["Otonom Rapor Oluştur"] = wf["connections"].get(
        "Otonom Rapor Oluştur",
        {"main": [[{"node": "SISTEM-OK Filtresi", "type": "main", "index": 0}]]},
    )

    return wf


def build_security():
    with open(ROOT / "workflow_security.json", encoding="utf-8") as f:
        wf = json.load(f)[0]

    wf["nodes"] = [
        n
        for n in wf["nodes"]
        if n["type"] not in ("@n8n/n8n-nodes-langchain.agent", "@n8n/n8n-nodes-langchain.lmChatGoogleGemini", "@n8n/n8n-nodes-langchain.toolCode")
        and n["name"] != "Log Analiz + Ban"
    ]

    wf["nodes"].extend(
        [
            {
                "parameters": {"jsCode": SECURITY_PREPARE_JS},
                "type": "n8n-nodes-base.code",
                "typeVersion": 2,
                "position": [-80, 0],
                "id": "sec-0002",
                "name": "Gateway Log Özeti",
            },
            {
                "parameters": {
                    "promptType": "define",
                    "text": "=Güvenlik analizi:\nŞüpheli IP adayları: {{ JSON.stringify($json.suspects) }}\nEşik: {{ $json.threshold }} istek/dk\n\nLog özeti (son 2000 karakter):\n{{ $json.logSummary }}",
                    "options": {"systemMessage": SECURITY_SYSTEM},
                },
                "type": "@n8n/n8n-nodes-langchain.agent",
                "typeVersion": 3.1,
                "continueOnFail": True,
                "position": [160, 0],
                "id": "sec-agent",
                "name": "Güvenlik Ajanı (Gemini)",
            },
            {
                "parameters": {
                    "modelName": "models/gemini-2.5-flash",
                    "options": {"temperature": 0.2, "maxOutputTokens": 1024},
                },
                "type": "@n8n/n8n-nodes-langchain.lmChatGoogleGemini",
                "typeVersion": 1,
                "position": [160, 200],
                "id": "sec-gemini",
                "name": "Gemini 2.5 Flash (Güvenlik)",
                "credentials": GEMINI_CRED,
            },
            fix_tool_name(
                {
                    "parameters": {
                        "name": "ban_gateway_ip",
                        "description": "IP banla",
                        "language": "javaScript",
                        "jsCode": BAN_IP_JS,
                    },
                    "type": "@n8n/n8n-nodes-langchain.toolCode",
                    "typeVersion": 1.3,
                    "position": [360, 200],
                    "id": "sec-ban-tool",
                    "name": "ban_ip Aracı",
                },
                "ban_gateway_ip",
                "ban_ip Aracı",
            ),
            {
                "parameters": {"jsCode": SECURITY_REPORT_JS},
                "type": "n8n-nodes-base.code",
                "typeVersion": 2,
                "position": [400, 0],
                "id": "sec-report",
                "name": "Güvenlik Raporu",
            },
        ]
    )

    # Keep filter + telegram from original
    for n in wf["nodes"]:
        if n["name"] == "SISTEM-OK Filtresi":
            filt = n
        if n["name"] == "Telegram Bildirimi":
            tg = n

    wf["connections"] = {
        "Schedule Trigger": {"main": [[{"node": "Gateway Log Özeti", "type": "main", "index": 0}]]},
        "Gateway Log Özeti": {"main": [[{"node": "Güvenlik Ajanı (Gemini)", "type": "main", "index": 0}]]},
        "Güvenlik Ajanı (Gemini)": {"main": [[{"node": "Güvenlik Raporu", "type": "main", "index": 0}]]},
        "Güvenlik Raporu": {"main": [[{"node": "SISTEM-OK Filtresi", "type": "main", "index": 0}]]},
        "SISTEM-OK Filtresi": {"main": [[{"node": "Telegram Bildirimi", "type": "main", "index": 0}]]},
        "Gemini 2.5 Flash (Güvenlik)": {
            "ai_languageModel": [[{"node": "Güvenlik Ajanı (Gemini)", "type": "ai_languageModel", "index": 0}]]
        },
        "ban_ip Aracı": {"ai_tool": [[{"node": "Güvenlik Ajanı (Gemini)", "type": "ai_tool", "index": 0}]]},
    }
    return wf


def sync_to_db(workflow_id: str, wf: dict):
    conn = sqlite3.connect(DB)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    conn.execute(
        """
        UPDATE workflow_entity
        SET nodes = ?, connections = ?, updatedAt = ?
        WHERE id = ?
        """,
        (json.dumps(wf["nodes"]), json.dumps(wf["connections"]), now, workflow_id),
    )
    conn.commit()
    conn.close()


def main():
    orch = build_orchestrator()
    sec = build_security()

    out_orch = ROOT / "workflow_yeni.json"
    out_sec = ROOT / "workflow_security.json"
    with open(out_orch, "w", encoding="utf-8") as f:
        json.dump([orch], f, ensure_ascii=False, indent=2)
    with open(out_sec, "w", encoding="utf-8") as f:
        json.dump([sec], f, ensure_ascii=False, indent=2)

    sync_to_db("OtomonMikroservisOrkestratoru", orch)
    sync_to_db("GuvenlikIpKorumaWorkflow", sec)

    print("OK: OtomonMikroservisOrkestratoru + GuvenlikIpKorumaWorkflow guncellendi")
    print(f"   Dosyalar: {out_orch}, {out_sec}")
    print(f"   DB: {DB}")


if __name__ == "__main__":
    main()
