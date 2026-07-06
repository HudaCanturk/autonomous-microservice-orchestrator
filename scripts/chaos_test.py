#!/usr/bin/env python3
"""Ölçümlü Kaos / Kendini İyileştirme Testi.

Her senaryoda kasıtlı bir arıza üretir, sistemin OTONOM olarak (n8n + Gemini)
düzeltmesini bekler, kurtarma süresini ve başarı durumunu ölçer. Sonunda bir
başarı oranı tablosu yazar ve scripts/chaos_test_sonuc.md dosyasına kaydeder.

Çalıştırma (host üzerinden, servisler ayakta olmalı):
    python3 scripts/chaos_test.py
"""
import time
import datetime
import json
from pathlib import Path

import requests

A = "http://localhost:5001"  # Servis A (PostgreSQL)
B = "http://localhost:5002"  # Servis B (Redis)
C = "http://localhost:5003"  # Servis C (dayanıklılık)

RAPOR = Path(__file__).resolve().parent / "chaos_test_sonuc.md"


def _get(url, timeout=5):
    try:
        return requests.get(url, timeout=timeout)
    except Exception:
        return None


def bekle_kurtarma(kontrol, timeout, aralik=3):
    """kontrol() True dönene kadar bekler. (basari, gecen_saniye) döndürür."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if kontrol():
                return True, round(time.time() - t0, 1)
        except Exception:
            pass
        time.sleep(aralik)
    return False, round(time.time() - t0, 1)


# --- Senaryo tanımları: (ad, tetikleyici, kurtarma_kontrolu, timeout, kurtaran) ---

def senaryo_servis_c_cokme():
    _get(f"{C}/boz")  # process exit -> bağlantı düşer, hata normal
    def kurtuldu():
        r = _get(f"{C}/health")
        return r is not None and r.status_code == 200
    return bekle_kurtarma(kurtuldu, timeout=150)


def senaryo_ram_sizinti():
    # RAM'i 150MB eşiğinin üstüne çıkar (2x200MB)
    _get(f"{C}/leak?mb=200")
    _get(f"{C}/leak?mb=200")
    def kurtuldu():
        # restart sonrası leak listesi sıfırlanır -> simüle yük 0'a döner
        r = _get(f"{C}/api/status")
        if r is None:
            return False
        return r.json().get("ram_simulasyon_yuku_mb", 999) == 0
    return bekle_kurtarma(kurtuldu, timeout=150)


def senaryo_db_baglanti_kopar():
    _get(f"{A}/db/disconnect")
    def kurtuldu():
        r = _get(f"{A}/db/health")
        return r is not None and r.json().get("db") == "bagli"
    return bekle_kurtarma(kurtuldu, timeout=45)


def senaryo_cache_sisme():
    _get(f"{B}/cache/flood")
    def kurtuldu():
        r = _get(f"{B}/cache/stats")
        return r is not None and r.json().get("dbsize", 9999) < 500
    return bekle_kurtarma(kurtuldu, timeout=45)


SENARYOLAR = [
    ("Servis C ani çökme (/boz)", "Orkestratör + Gemini restart", senaryo_servis_c_cokme),
    ("Servis C RAM sızıntısı (+400MB)", "Orkestratör + Gemini restart", senaryo_ram_sizinti),
    ("Servis A DB bağlantı kopması", "DB Baglanti webhook + Gemini", senaryo_db_baglanti_kopar),
    ("Servis B önbellek şişmesi", "Cache webhook + Gemini evict", senaryo_cache_sisme),
]


def main():
    print("=" * 64)
    print("  OTONOM KENDİNİ İYİLEŞTİRME — ÖLÇÜMLÜ KAOS TESTİ")
    print("=" * 64)
    sonuclar = []
    for ad, kurtaran, fn in SENARYOLAR:
        print(f"\n▶ {ad}\n  Tetikleniyor... (kurtaran: {kurtaran})")
        basari, sure = fn()
        durum = "✅ KURTARILDI" if basari else "❌ KURTARILAMADI"
        print(f"  {durum} — süre: {sure} sn")
        sonuclar.append({"senaryo": ad, "kurtaran": kurtaran, "basari": basari, "sure_sn": sure})
        time.sleep(10)  # senaryolar arası dinlenme

    basarili = sum(1 for s in sonuclar if s["basari"])
    toplam = len(sonuclar)
    oran = round(basarili / toplam * 100, 1)
    sureler = [s["sure_sn"] for s in sonuclar if s["basari"]]
    ort_sure = round(sum(sureler) / len(sureler), 1) if sureler else 0

    print("\n" + "=" * 64)
    print(f"  SONUÇ: {basarili}/{toplam} senaryo otomatik düzeldi  (%{oran})")
    print(f"  Ortalama kurtarma süresi: {ort_sure} sn")
    print("=" * 64)

    # Markdown rapor
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    md = [f"# Kaos Testi Sonucu ({ts})", ""]
    md.append(f"**Başarı oranı:** {basarili}/{toplam} (%{oran})  ")
    md.append(f"**Ortalama kurtarma süresi:** {ort_sure} sn")
    md.append("")
    md.append("| Senaryo | Kurtaran mekanizma | Sonuç | Süre (sn) |")
    md.append("|---|---|---|---|")
    for s in sonuclar:
        md.append(f"| {s['senaryo']} | {s['kurtaran']} | {'✅' if s['basari'] else '❌'} | {s['sure_sn']} |")
    RAPOR.write_text("\n".join(md), encoding="utf-8")
    print(f"\nRapor kaydedildi: {RAPOR}")

    # Makine-okur çıktı
    print("\nJSON:", json.dumps({"oran": oran, "ort_sure": ort_sure, "sonuclar": sonuclar}, ensure_ascii=False))


if __name__ == "__main__":
    main()
