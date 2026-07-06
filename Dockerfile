# 1. Adım: Temel olarak Python'ın hafif bir sürümünü kullan
FROM python:3.9-slim

# 2. Adım: Konteyner içinde çalışacağımız klasörü belirle
WORKDIR /app

# 3. Adım: Malzeme listesini (requirements.txt) içeri kopyala
COPY requirements.txt .

# 4. Adım: Malzemeleri (kütüphaneleri) yükle
RUN pip install --no-cache-dir -r requirements.txt

# 5. Adım: Tüm kodlarımızı içeri kopyala
COPY . .

# 6. Adım: Servisimiz 5000. porttan konuşacak, bunu dünyaya duyur
EXPOSE 5000

# 7. Adım: Konteyner başladığında çalışacak komutu belirle
CMD ["python", "app.py"]