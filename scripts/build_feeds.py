# scripts/build_feeds.py
# Genera RSS diarios para Venezuela, Panamá y República Dominicana
# SIN Google News. Scraping + RSS nativo donde exista.
# Fuerza diffs diarios con <lastBuildDate> y comentario con run_id.

import os
import re
import html
import hashlib
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode

import requests
from bs4 import BeautifulSoup
import feedparser
from time import sleep

OUTPUT_DIR = "data"
REQUEST_TIMEOUT = 25
RETRIES = 2
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AngieNewsBot/1.0; +https://github.com/)"
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

# Selectores por dominio (más robustos que genérico)
SITE_SELECTORS = {
    # Venezuela
    "www.elnacional.com": ["article h2 a", "h3 a", ".headline a", "a[href*='/opinion/'] ~ h2 a"],
    "talcualdigital.com": ["h2.entry-title a", "article h2 a", "div.post-title h2 a"],
    "efectococuyo.com": ["article h2 a", "h2 a[href*='/']"],
    "www.bloomberglinea.com": [
        "article a[href*='/venezuela/'] h2",
        "article a[href*='/panama/'] h2",
        "article h2 a",
        "article a[href] h2",
    ],
    # Panamá
    "www.prensa.com": [
        "h1 a[href^='https://www.prensa.com/']",
        "h2 a[href^='https://www.prensa.com/']",
        "article h2 a[href^='https://www.prensa.com/']",
        "a[href^='https://www.prensa.com/']:not([href*='/tag/']):not([href*='autor'])",
    ],
    "www.laestrella.com.pa": [
        "h1 a[href^='https://www.laestrella.com.pa/']",
        "h2 a[href^='https://www.laestrella.com.pa/']",
        "article h2 a[href^='https://www.laestrella.com.pa/']",
        "a[href^='https://www.laestrella.com.pa/']:not([href*='/etiquetas/'])",
    ],
    # Dominicana
    "listindiario.com": ["h2 a[href]", "h3 a[href]", "article h2 a"],
    "www.elcaribe.com.do": ["h2 a[href]", "article h2 a", "a.post-title[href]"],
    "eldinero.com.do": ["h2 a[href]", "article h2 a", "a.post-title[href]"],
}

# ---------- Utilidades ----------

def clean_url(u: str) -> str:
    """Quita UTM y trackers comunes. Normaliza esquema/host."""
    try:
        p = urlparse(u)
        qs = [(k, v) for k, v in parse_qsl(p.query) if not k.lower().startswith("utm") and k.lower() not in {"gclid", "fbclid"}]
        return urlunparse((p.scheme or "https", p.netloc.lower(), p.path, "", urlencode(qs), ""))
    except Exception:
        return u

def norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()

def abs_url(base: str, href: str) -> str:
    if not href:
        return ""
    if href.startswith("//"):
        return "https:" + href
    return urljoin(base, href)

def get_html(url: str) -> bytes | None:
    for i in range(RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.content
        except Exception as e:
            print(f"[WARN] GET fail ({i+1}/{RETRIES+1}) {url}: {e}")
            sleep(1.5)
    return None

def fetch_html(url: str) -> BeautifulSoup | None:
    content = get_html(url)
    if not content:
        return None
    return BeautifulSoup(content, "html.parser")

def scrape_site(url: str, limit: int) -> list[dict]:
    """Scrapea una portada con selectores por dominio + fallbacks agresivos."""
    soup = fetch_html(url)
    if not soup:
        return []

    parsed = urlparse(url)
    host = parsed.netloc.lower()
    base = f"{parsed.scheme}://{parsed.netloc}/"
    selectors = SITE_SELECTORS.get(host, []) + [
        "h1 a[href]", "h2 a[href]", "article h2 a",  # fallback genérico
    ]

    seen_links = set()
    items: list[dict] = []

    for sel in selectors:
        for tag in soup.select(sel):
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
            full = abs_url(base, link)
            if full in seen_links:
                continue
            seen_links.add(full)
            items.append({"title": text, "link": full, "date": datetime.utcnow(), "source": host})
            if len(items) >= limit * 4:
                break
        if len(items) >= limit * 4:
            break

    # Filtro de calidad: evita menús y migas
    items = [it for it in items if len(it["title"]) >= 25]

    # Fallback ultra: pesca <a> del mismo dominio con título largo si no llegó nada
    if not items:
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            txt = a.get_text(strip=True)
            full = abs_url(base, href)
            if urlparse(full).netloc.lower() != host:
                continue
            if len(txt) < 35:
                continue
            items.append({"title": txt, "link": full, "date": datetime.utcnow(), "source": host})
            if len(items) >= limit * 2:
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

    return out[: limit * 2]

def fetch_rss(url: str, cutoff_utc: datetime) -> list[dict]:
    """Lee RSS/Atom con UA y feedparser, filtrando por fecha."""
    content = get_html(url)
    if not content:
        return []
    feed = feedparser.parse(content)
    out = []
    for e in feed.entries:
        title = getattr(e, "title", "") or ""
        link = getattr(e, "link", "") or ""
        if not title or not link:
            continue
        if getattr(e, "published_parsed", None):
            dt = datetime(*e.published_parsed[:6])
        elif getattr(e, "updated_parsed", None):
            dt = datetime(*e.updated_parsed[:6])
        else:
            dt = datetime.utcnow()
        if dt < cutoff_utc:
            continue
        out.append({
            "title": title,
            "link": clean_url(link),
            "date": dt,
            "source": urlparse(link).netloc or urlparse(url).netloc,
        })
    return out

def make_rss(country: str, items: list[dict]) -> str:
    """Genera RSS 2.0 con lastBuildDate y un comentario con timestamp para diferenciar runs."""
    now_http = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")

    def esc(s: str) -> str:
        return html.escape(s or "", quote=True)

    # Comentario para forzar diff incluso si el contenido coincide byte a byte
    build_comment = f"<!-- build {datetime.utcnow().isoformat()}Z -->"

    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        "<rss version=\"2.0\">",
        "<channel>",
        f"  <title>Noticias {esc(country.title())}</title>",
        "  <link>https://github.com/</link>",
        "  <description>Feed generado automáticamente</description>",
        f"  <lastBuildDate>{now_http}</lastBuildDate>",
        "  <generator>news-feeds GitHub Action</generator>",
        f"  {build_comment}",
    ]
    for it in items:
        parts.extend([
            "  <item>",
            f"    <title>{esc(it['title'])}</title>",
            f"    <link>{esc(it['link'])}</link>",
            f"    <pubDate>{it['date'].strftime('%a, %d %b %Y %H:%M:%S GMT')}</pubDate>",
            "  </item>",
        ])
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

    # Ordenar, dedupe final, limitar
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

    # Si a pesar de todo no hay items, mete un placeholder para que se vea en logs/HTML
    if not final:
        final = [{
            "title": f"No se pudieron extraer titulares para {country} en este run",
            "link": "https://example.com/",
            "date": datetime.utcnow(),
            "source": "generator"
        }]

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
