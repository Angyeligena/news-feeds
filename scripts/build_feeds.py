import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os
from urllib.parse import urlparse

OUTPUT_DIR = "data"

# Fuentes principales por país
SOURCES = {
    "venezuela": [
        "https://www.elnacional.com/",               # scraping
        "https://talcualdigital.com/",               # scraping
        "https://efectococuyo.com/",               # scraping
        "https://www.bloomberglinea.com/latinoamerica/venezuela/",               # scraping
    ],
    "panama": [
        "https://www.prensa.com/",               # scraping
        "https://www.laestrella.com.pa/",        # scraping
        "https://www.bloomberglinea.com/latinoamerica/panama/",        # scraping
    ],
    "dominicana": [
        "https://www.diariolibre.com/rss/portada.xml",
        "https://listindiario.com/",        # scraping
        "https://www.elcaribe.com.do/",        # scraping
        "https://eldinero.com.do/",        # scraping
    ]
}

# Límite por país
LIMITS = {
    "venezuela": 10,
    "panama": 10,
    "dominicana": 10
}

def fetch_feed(url):
    """Descarga un feed RSS"""
    try:
        return feedparser.parse(url)
    except Exception as e:
        print(f"Error con {url}: {e}")
        return None

def scrape_site(url, limit=10):
    """Scrapea titulares principales de una portada"""
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        # Heurística genérica: titulares en h1/h2/a
        items = []
        for tag in soup.select("h1 a, h2 a")[:limit*2]:  # coger más de lo necesario
            title = tag.get_text(strip=True)
            link = tag.get("href")
            if not title or not link:
                continue
            if not link.startswith("http"):
                # construir URL absoluta
                domain = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
                link = domain + link
            items.append({
                "title": title,
                "link": link,
                "date": datetime.utcnow()
            })
        return items[:limit]

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return []

def generate_country_feed(country, urls, limit):
    items = []
    cutoff = datetime.utcnow() - timedelta(days=1)

    for url in urls:
        if url.endswith(".xml") or "feed" in url or "rss" in url:
            # Caso RSS
            feed = fetch_feed(url)
            if not feed or not feed.entries:
                continue
            for entry in feed.entries:
                if hasattr(entry, "published_parsed") and entry.published_parsed:
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
        else:
            # Caso scraping
            scraped = scrape_site(url, limit)
            items.extend(scraped)

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
        print(f"Generando feed para {country}...")
        generate_country_feed(country, urls, limit)

if __name__ == "__main__":
    main()
