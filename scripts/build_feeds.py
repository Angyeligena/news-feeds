# scripts/build_feeds.py
# Genera RSS diarios para Venezuela, Panamá y República Dominicana desde tus portadas
# Sin Google News. Con scraping + soporte de RSS nativo donde exista.

import os
import re
import html
import hashlib
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode

import requests
from bs4 import BeautifulSoup
import feedparser

OUTPUT_DIR = "data"
REQUEST_TIMEOUT = 20
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; news-feeds/1.0; +https://github.com/)"}

# ... tus SOURCES se quedan igual ...

# === Selectores por dominio (añade/ajusta estos) ===
SITE_SELECTORS = {
    # Venezuela
    "www.elnacional.com": ["article h2 a", "h3 a", ".headline a"],
    "talcualdigital.com": ["h2.entry-title a", "article h2 a"],
    "efectococuyo.com": ["h2 a[href]", "article h2 a"],
    "www.bloomberglinea.com": [
        "article a[href*='/venezuela/'] h2",
        "article a[href*='/panama/'] h2",
        "article h2 a",
        "article a[href] h2",
    ],
    # Panamá — SELECTORES MEJORADOS
    "www.prensa.com": [
        "h1 a[href]",
        "h2 a[href]",
        "article h2 a",
        "a[href^='https://www.prensa.com/']:not([href*='tag/'])",
    ],
    "www.laestrella.com.pa": [
        "h1 a[href]",
        "h2 a[href]",
        "article h2 a",
        "a[href^='https://www.laestrella.com.pa/']:not([href*='tag/'])",
    ],
    # Dominicana
    "listindiario.com": ["h2 a[href]", "h3 a[href]", "article h2 a"],
    "www.elcaribe.com.do": ["h2 a[href]", "article h2 a"],
    "eldinero.com.do": ["h2 a[href]", "article h2 a"],
}

# -- dentro de scrape_site(), tras construir items, añade filtro de calidad:
# (evita ruido de menús con textos cortos)
items = [it for it in items if len(it["title"]) >= 25]

# -- en make_rss(), AGREGA lastBuildDate y generator --
def make_rss(country: str, items: list[dict]) -> str:
    now_http = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    def esc(s: str) -> str:
        return html.escape(s or "", quote=True)
    parts = [
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
        "<rss version=\"2.0\">",
        "<channel>",
        f"  <title>Noticias {esc(country.title())}</title>",
        "  <link>https://github.com/</link>",
        "  <description>Feed generado automáticamente</description>",
        f"  <lastBuildDate>{now_http}</lastBuildDate>",
        "  <generator>news-feeds GitHub Action</generator>",
    ]
    
    for it in items:
        parts.extend(
            [
                "  <item>",
                f"    <title>{esc(it['title'])}</title>",
                f"    <link>{esc(it['link'])}</link>",
                f"    <pubDate>{it['date'].strftime('%a, %d %b %Y %H:%M:%S GMT')}</pubDate>",
                "  </item>",
            ]
        )
    parts.extend(["</channel>", "</rss>", ""])
    return "\n".join(parts)


def generate_country_feed(country: str, urls: list[str], limit: int) -> None:
    items: list[dict] = []
    cutoff = datetime.utcnow() - timedelta(days=1)

    for url in urls:
        if url.endswith(".xml") or "rss" in url.lower() or "feed" in url.lower():
            items.extend(fetch_rss(url, cutoff))
        else:
            items.extend(scrape_site(url, limit))

    # Ordenar, dedupe final y limitar
    items.sort(key=lambda x: x["date"], reverse=True)

    uniq, final = set(), []
    for it in items:
        key = hashlib.md5((norm_text(it["title"]) + "|" + urlparse(it["link"]).netloc).encode()).hexdigest()
        if key in uniq:
            continue
        uniq.add(key)
        final.append(it)
        if len(final) >= limit:
            break

    rss_xml = make_rss(country, final)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"{country}.xml")
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(rss_xml)
    print(f"[OK] {country}: {len(final)} items -> {out_path}")


def main():
    for country, urls in SOURCES.items():
        try:
            print(f"Generando feed para {country}...")
            generate_country_feed(country, urls, LIMITS[country])
        except Exception as e:
            print(f"[ERROR] {country}: {e}")


if __name__ == "__main__":
    main()

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
