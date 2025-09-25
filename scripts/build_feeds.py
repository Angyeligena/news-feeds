import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os

OUTPUT_DIR = "data"

# Fuentes principales por país
SOURCES = {
    "venezuela": [
        "https://www.elnacional.com/feed/",
        "https://www.eluniversal.com/feed/",
        "https://ultimasnoticias.com.ve/feed/",
        "https://talcualdigital.com/feed/",
        "https://www.lapatilla.com/feed/"
    ],
    "panama": [
        "https://www.prensa.com/feed/",
        "https://elsiglo.com.pa/feed/",
        "https://www.panamaamerica.com.pa/feed/",
        "https://www.metrolibre.com/feed/",
        "https://www.critica.com.pa/feed/"
    ],
    "dominicana": [
        "https://www.diariolibre.com/rss/portada",
        "https://listindiario.com/feed/",
        "https://hoy.com.do/feed/",
        "https://www.elcaribe.com.do/feed/",
        "https://www.diariohispaniola.com/rss"
    ]
}

# Límite por país
LIMITS = {
    "venezuela": 10,
    "panama": 5,
    "dominicana": 5
}

def fetch_feed(url):
    try:
        return feedparser.parse(url)
    except Exception as e:
        print(f"Error con {url}: {e}")
        return None

def generate_country_feed(country, urls, limit):
    items = []
    cutoff = datetime.utcnow() - timedelta(days=1)

    for url in urls:
        feed = fetch_feed(url)
        if not feed or not feed.entries:
            continue
        for entry in feed.entries:
            if hasattr(entry, "published_parsed"):
                pubdate = datetime(*entry.published_parsed[:6])
                if pubdate < cutoff:
                    continue
            else:
                pubdate = datetime.utcnow()
            items.append({
                "title": entry.title,
                "link": entry.link,
                "date": pubdate
            })

    # Ordenar y limitar
    items = sorted(items, key=lambda x: x["date"], reverse=True)[:limit]

    # Generar RSS básico
    rss = f"""<?xml version="1.0" encoding="UTF-8" ?>
<rss version="2.0">
<channel>
<title>Noticias {country.title()}</title>
<link>https://github.com/</link>
<description>Feed generado automáticamente</description>
"""
    for item in items:
        rss += f"""
<item>
<title>{item['title']}</title>
<link>{item['link']}</link>
<pubDate>{item['date'].strftime('%a, %d %b %Y %H:%M:%S GMT')}</pubDate>
</item>
"""
    rss += "</channel></rss>"

    # Guardar
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(f"{OUTPUT_DIR}/{country}.xml", "w", encoding="utf-8") as f:
        f.write(rss)

def main():
    for country, urls in SOURCES.items():
        limit = LIMITS[country]
        generate_country_feed(country, urls, limit)

if __name__ == "__main__":
    main()
