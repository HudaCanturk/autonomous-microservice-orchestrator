#!/usr/bin/env python3
"""4 webhook workflow'unu Gemini hatasina dayanikli hale getirir.

Sorun: Akis 'Webhook -> Gemini -> Telegram (-> Recovery)' seklindeydi.
Gemini rate limit/ag hatasi alinca akis Gemini'de duruyor; Telegram VE
onarim (recovery) hic calismiyordu.

Cozum (her workflow icin):
  - Gemini (agent) dugumu: onError = continueRegularOutput (hata olsa da devam)
  - Telegram dugumu: onError = continueRegularOutput (onarim her zaman calissin)
  - Araya 'Mesaj Hazirla' kod dugumu eklenir: Gemini ciktisi varsa onu,
    yoksa webhook'tan gelen ham mesajdan Turkce bir bildirim uretir.
  - Baglanti: Gemini.main -> Mesaj Hazirla -> Telegram  (Telegram->Recovery korunur)

Idempotent: 'Mesaj Hazirla' zaten varsa o workflow atlanir.
n8n DURDURULMUS halde calistirilmalidir (SQLite guvenligi icin).
"""
import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path

DB = Path(__file__).resolve().parents[1] / "n8n_data" / "database.sqlite"

# Her workflow icin Gemini basarisiz olursa kullanilacak ham-mesaj uretimi.
# (webhook_name, fallback_js_govde) — fallback, 'text' degiskenini doldurur.
WORKFLOWS = {
    "DbUyariWebhookWorkflow": {
        "webhook": "DB Uyari Webhook",
        "fallback": (
            "text = '\\U0001F916 DB OLAY (otomatik bildirim)\\n'"
            " + '\\U0001F4CC Servis: ' + (body.servis || 'Servis_A') + '\\n'"
            " + (body.mesaj || 'Veritabani olayi olustu.');"
        ),
    },
    "DbBaglantiWebhookWorkflow": {
        "webhook": "DB Baglanti Webhook",
        "fallback": (
            "const ev = body.event || '';"
            "text = '\\U0001F50C DB BAGLANTI OLAYI (otomatik)\\n'"
            " + '\\U0001F4CC Servis: ' + (body.servis || 'Servis_A') + '\\n'"
            " + (ev === 'koptu'"
            " ? '\\U0001F534 Durum: Baglanti KOPTU\\n\\u2699\\uFE0F Aksiyon: Otomatik yeniden baglanma baslatildi'"
            " : '\\U0001F7E2 Durum: Baglanti KURULDU\\n\\u2699\\uFE0F Sistem normale dondu')"
            " + (body.detay ? ('\\n' + body.detay) : '');"
        ),
    },
    "CacheUyariWebhookWorkflow": {
        "webhook": "Cache Uyari Webhook",
        "fallback": (
            "const ev = body.event || '';"
            "text = '\\u26A1 ONBELLEK OLAYI (otomatik)\\n'"
            " + '\\U0001F4CC Servis: ' + (body.servis || 'Servis_B') + '\\n'"
            " + (ev === 'sisti'"
            " ? '\\U0001F534 Durum: Onbellek asiri sisti\\n\\u2699\\uFE0F Aksiyon: Onbellek otomatik temizleniyor'"
            " : '\\U0001F7E2 Durum: Onbellek normale dondu')"
            " + (body.detay ? ('\\n' + body.detay) : '');"
        ),
    },
    "LatencyUyariWebhookWorkflow": {
        "webhook": "Gecikme Webhook",
        "fallback": (
            "const ev = body.event || '';"
            "text = '\\u23F1 GECIKME OLAYI (otomatik)\\n'"
            " + '\\U0001F4CC Servis: ' + (body.servis || 'Servis_C') + '\\n'"
            " + (ev === 'aksiyon'"
            " ? '\\U0001F534 Durum: Yuksek gecikme \\u2014 servis yeniden baslatiliyor'"
            " : '\\U0001F7E1 Durum: Gecikme uyari esigini asti')"
            " + (body.gecikme_ms ? ('\\n\\U0001F4CA Gecikme: ' + body.gecikme_ms + ' ms') : '')"
            " + (body.detay ? ('\\n' + body.detay) : '');"
        ),
    },
}

PREP_NODE_NAME = "Mesaj Hazirla"


def build_prep_js(webhook_name: str, fallback_body: str) -> str:
    return (
        "// Gemini ciktisi varsa onu kullan; yoksa (rate limit/hata) ham mesajdan uret.\n"
        "const ai = (($json.output) || '').toString().trim();\n"
        "let text = ai;\n"
        "if (!text) {\n"
        f"  const body = ($('{webhook_name}').first().json.body) || {{}};\n"
        f"  {fallback_body}\n"
        "}\n"
        "return [{ json: { ...$json, output: text } }];"
    )


def find_node(nodes, predicate):
    for n in nodes:
        if predicate(n):
            return n
    return None


def main():
    con = sqlite3.connect(str(DB))
    con.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc).isoformat()

    for wid, cfg in WORKFLOWS.items():
        row = con.execute(
            "SELECT id, nodes, connections FROM workflow_entity WHERE id=?", (wid,)
        ).fetchone()
        if not row:
            print(f"ATLANDI (bulunamadi): {wid}")
            continue

        nodes = json.loads(row["nodes"])
        conns = json.loads(row["connections"])

        if any(n.get("name") == PREP_NODE_NAME for n in nodes):
            print(f"ATLANDI (zaten duzeltilmis): {wid}")
            continue

        agent = find_node(nodes, lambda n: str(n.get("type", "")).endswith(".agent"))
        telegram = find_node(nodes, lambda n: n.get("type") == "n8n-nodes-base.telegram")
        webhook = find_node(nodes, lambda n: n.get("type") == "n8n-nodes-base.webhook")
        if not (agent and telegram and webhook):
            print(f"ATLANDI (dugum eksik): {wid}")
            continue

        agent_name = agent["name"]
        telegram_name = telegram["name"]

        # 1) Hata toleransi
        agent["onError"] = "continueRegularOutput"
        telegram["onError"] = "continueRegularOutput"

        # 2) Mesaj Hazirla dugumu
        ax, ay = agent.get("position", [480, 300])
        prep = {
            "parameters": {
                "jsCode": build_prep_js(cfg["webhook"], cfg["fallback"]),
            },
            "id": f"prep-msg-{wid.lower()}",
            "name": PREP_NODE_NAME,
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [ax + 120, ay + 140],
        }
        nodes.append(prep)

        # 3) Yeniden baglama: Gemini.main -> Mesaj Hazirla -> Telegram
        conns[agent_name] = {
            "main": [[{"node": PREP_NODE_NAME, "type": "main", "index": 0}]]
        }
        conns[PREP_NODE_NAME] = {
            "main": [[{"node": telegram_name, "type": "main", "index": 0}]]
        }

        con.execute(
            "UPDATE workflow_entity SET nodes=?, connections=?, updatedAt=? WHERE id=?",
            (
                json.dumps(nodes, ensure_ascii=False),
                json.dumps(conns, ensure_ascii=False),
                now,
                wid,
            ),
        )
        print(f"DUZELTILDI: {wid}  (agent='{agent_name}' -> '{PREP_NODE_NAME}' -> '{telegram_name}')")

    con.commit()
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.commit()
    con.close()
    print("Tamamlandi.")


if __name__ == "__main__":
    main()
