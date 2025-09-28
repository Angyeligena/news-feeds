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
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; news-feeds/1.0; +https://github.com/)"
}

# === Fuentes por país (tus links) ===
SOURCES = {
    "venezuela": [
        "https://www.elnacional.com/",
        "https://talcualdigital.com/",
        "https://efectococuyo.com/",
        "https://www.bloomberglinea.com/latinoamerica/venezuela/",
    ],
    "panama": [
        "https://www.prensa.com/",
        "https://www.laestrella.com.pa/",
        "https://www.bloomberglinea.com/latinoamerica/panama/",
    ],
    "dominicana": [
        "https://www.diariolibre.com/rss/portada.xml",  # RSS nativo
        "https://listindiario.com/",
        "https://www.elcaribe.com.do/",
        "https://eldinero.com.do/",
    ],
}

# Límite de ítems por país
LIMITS = {"venezuela": 10, "panama": 10, "dominicana": 10}

# Selectores por dominio (más robusto que un selector genérico)
SITE_SELECTORS = {
    # Venezuela
    "www.elnacional.com": [
        "article h2 a",
        "h3 a",
        ".headline a",
    ],
    "talcualdigital.com": [
        "h2.entry-title a",
        "article h2 a",
    ],
    "efectococuyo.com": [
        "h2 a[href]",
        "article h2 a",
    ],
    "www.bloomberglinea.com": [
        "article a[href*='/venezuela/'] h2",
        "article a[href*='/panama/'] h2",
        "article h2 a",
        "article a[href] h2",
    ],
    # Panamá
    "www.prensa.com": [
        "h1 a[href]",
        "h2 a[href]",
        "article h2 a",
    ],
    "www.laestrella.com.pa": [
        "h2 a[href]",
        "article h2 a",
    ],
    # Dominicana
    "listindiario.com": [
        "h2 a[href]",
        "h3 a[href]",
        "article h2 a",
    ],
    "www.elcaribe.com.do": [
        "h2 a[href]",
        "article h2 a",
    ],
    "eldinero.com.do": [
        "h2 a[href]",
        "article h2 a",
    ],
}

# --- Utilidades ---

def clean_url(u: str) -> str:
    """Quita UTM y trackers comunes. Normaliza esquema/host."""
    try:
        p = urlparse(u)
        qs = [(k, v) for k, v in parse_qsl(p.query) if not k.lower().startswith("utm") and k.lower() not in {"gclid", "fbclid"}]
        return urlunparse((p.scheme or "https", p.netloc.lower(), p.path, "", urlencode(qs), ""))
    except Exception:
        return u


def norm_text(s: str) -> str:
    s = re.sub(r"\s+", " ", s or "").strip().lower()
    return s


def abs_url(base: str, href: str) -> str:
    if not href:
        return ""
    if href.startswith("//"):
        return "https:" + href
    return urljoin(base, href)


def fetch_html(url: str) -> BeautifulSoup | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return BeautifulSoup(r.content, "html.parser")
    except Exception as e:
        print(f"[WARN] Error HTML GET {url}: {e}")
        return None


def scrape_site(url: str, limit: int) -> list[dict]:
    """Scrapea titulares principales de una portada con selectores por dominio + fallback."""
    soup = fetch_html(url)
    if not soup:
        return []

    parsed = urlparse(url)
    host = parsed.netloc.lower()
    selectors = SITE_SELECTORS.get(host, []) + ["h1 a[href]", "h2 a[href]"]  # fallback al final

    seen_links = set()
    items: list[dict] = []

    for sel in selectors:
        # En Bloomberg Línea a veces el <h2> es el hijo; contemplamos ambos
        for tag in soup.select(sel):
            # Si el selector apunta al <h2>, intenta subir al <a>
            link = tag.get("href")
            text = tag.get_text(strip=True)
            if not link:
                a = tag.find("a", href=True)
                if a:
                    link = a["href"]
                    if not text:
                        text = a.get_text(strip=True)
            if not text or not link:
                continue

            full = abs_url(f"{parsed.scheme}://{parsed.netloc}/", link)
            if full in seen_links:
                continue
            seen_links.add(full)

            items.append(
                {
                    "title": text,
                    "link": full,
                    "date": datetime.utcnow(),
                    "source": host,
                }
            )
            if len(items) >= limit * 3:  # overfetch para dedupe posterior
                break
        if len(items) >= limit * 3:
            break

    # dedupe por (titulo normalizado + dominio)
    uniq, out = set(), []
    for it in items:
        key = hashlib.md5((norm_text(it["title"]) + "|" + urlparse(it["link"]).netloc).encode()).hexdigest()
        if key in uniq:
            continue
        uniq.add(key)
        it["link"] = clean_url(it["link"])
        out.append(it)

    return out[: limit * 2]  # aún sobredimensionado


def fetch_rss(url: str, cutoff_utc: datetime) -> list[dict]:
    """Lee RSS/Atom con requests (+UA) y feedparser."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        feed = feedparser.parse(r.content)
        out = []
        for e in feed.entries:
            # Título/enlace
            title = getattr(e, "title", "") or ""
            link = getattr(e, "link", "") or ""
            if not title or not link:
                continue
            # Fecha
            if getattr(e, "published_parsed", None):
                dt = datetime(*e.published_parsed[:6])
            elif getattr(e, "updated_parsed", None):
                dt = datetime(*e.updated_parsed[:6])
            else:
                dt = datetime.utcnow()
            if dt < cutoff_utc:
                continue
            out.append(
                {
                    "title": title,
                    "link": clean_url(link),
                    "date": dt,
                    "source": urlparse(link).netloc or urlparse(url).netloc,
                }
            )
        return out
    except Exception as e:
        print(f"[WARN] Error RSS GET {url}: {e}")
        return []


def make_rss(country: str, items: list[dict]) -> str:
    """Genera XML RSS 2.0 simple y válido; fuerza lastBuildDate para que Git vea cambios."""
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
