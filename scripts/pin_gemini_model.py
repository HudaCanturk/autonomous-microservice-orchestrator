#!/usr/bin/env python3
"""Tum n8n workflow'larindaki Google Gemini node'larinda modeli ACIKCA sabitler.
Bos birakilan modelName, node varsayilanina dusup ileride sessizce degisebilir.
Bunu onlemek icin models/gemini-2.5-flash olarak sabitlenir.
ONEMLI: n8n DURDURULMUS olmali (docker compose stop n8n).
"""
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB = Path(__file__).resolve().parents[1] / "n8n_data" / "database.sqlite"
MODEL = "models/gemini-2.5-flash"
GEMINI_TYPE = "lmChatGoogleGemini"


def patch_nodes(nodes_json):
    """nodes JSON string -> (yeni JSON string, degisen node sayisi)."""
    nodes = json.loads(nodes_json)
    changed = 0
    for n in nodes:
        if GEMINI_TYPE in n.get("type", ""):
            params = n.setdefault("parameters", {})
            if params.get("modelName") != MODEL:
                params["modelName"] = MODEL
                changed += 1
    return json.dumps(nodes), changed


def main():
    c = sqlite3.connect(DB)
    c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    toplam = 0

    # 1) Aktif tanim: workflow_entity
    for wid, name, nodes in c.execute("SELECT id, name, nodes FROM workflow_entity").fetchall():
        new_nodes, ch = patch_nodes(nodes)
        if ch:
            c.execute(
                "UPDATE workflow_entity SET nodes=?, updatedAt=? WHERE id=?",
                (new_nodes, now, wid),
            )
            print(f"[entity] {name}: {ch} Gemini node sabitlendi -> {MODEL}")
            toplam += ch

    # 2) Versiyon gecmisi: workflow_history (n8n bazi surumlerde bunu calistirir)
    for vid, nodes in c.execute("SELECT versionId, nodes FROM workflow_history").fetchall():
        new_nodes, ch = patch_nodes(nodes)
        if ch:
            c.execute(
                "UPDATE workflow_history SET nodes=? WHERE versionId=?",
                (new_nodes, vid),
            )
            toplam += ch

    c.commit()
    c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    print(f"Toplam sabitlenen Gemini node: {toplam}")
    print("DONE")


if __name__ == "__main__":
    main()
