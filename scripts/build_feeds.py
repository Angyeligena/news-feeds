# scripts/build_feeds.py
# SIMPLE y estable:
# - Solo scraping (requests + bs4)
# - Fuentes por país (tus links)
# - Sin Bloomberg/Bloomberg Línea
# - Dedupe por URL
# - Mezcla sencilla por round-robin de fuentes (intenta diversidad)
# - Si no alcanza, rellena con lo que haya (sin placeholders)
# - Siempre escribe data/{pais}.xml con timestamp (el workflow verá cambios)

import os
import html
from datetime import datetime
from urllib.parse import urlparse, urljoin
import requests
from bs4 import BeautifulSoup

OUTPUT_DIR = "data"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SimpleScraper/1.0",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}
TIMEOUT = 20

# === TUS FUENTES (solo web) ===
SOURCES = {
    "venezuela": [
        "https://www.elnacional.com/",
        "https://talcualdigital.com/",
        "https://efectococuyo.com/",
    ],
    "panama": [
        "https://www.prensa.com/",
        "https://www.laestrella.com.pa/",
    ],
    "dominicana": [
        "https://listindiario.com/",
        "https://www.elcaribe.com.do/",
        "https://eldinero.com.do/",
    ],
}

LIMITS = {"venezuela": 10, "panama": 10, "dominicana": 10}

# Excluir por dominio (sin subdominios)
EXCLUDE_DOMAINS = {"bloomberg.com", "bloomberglinea.com"}

# ========= util =========
def abs_url(base: str, href: str) -> str:
    if not href:
        return ""
    if href.startswith("//"):
        return "https:" + href
    return urljoin(base, href)

def domain_of(url: str) -> str:
    host = (urlparse(url).netloc or "").lower()
    return host[4:] if host.startswith("www.") else host

# ========= scraping MUY simple =========
def scrape_home(url: str, max_items: int = 50) -> list[dict]:
    out = []
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        # reparar encoding básico
        if not r.encoding or r.encoding.lower() == "iso-8859-1":
            r.encoding = r.apparent_encoding or "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"

        # titulares básicos
        candidates = soup.select("h1 a[href], h2 a[href], article h2 a[href]")
        seen = set()
        for a in candidates:
            title = a.get_text(strip=True)
            href = a.get("href")
            if not title or not href:
                continue
            link = abs_url(base, href)
            dom = domain_of(link)
            if any(dom == d or dom.endswith("." + d) for d in EXCLUDE_DOMAINS):
                continue
            if link in seen:
                continue
            seen.add(link)
            # filtrar cosas obvias de etiquetas/autores
            path = urlparse(link).path.lower()
            if any(seg in path for seg in ("/tag/", "/etiqueta/", "/autor", "/author", "/category", "/categoria")):
                continue
            # título mínimamente decente
            if len(title) < 8:
                continue
            out.append({
                "title": title,
                "link": link,
                "domain": dom,
                "date": datetime.utcnow(),  # sin fecha real: usamos ahora
            })
            if len(out) >= max_items:
                break
    except Exception as e:
        print(f"[WARN] scrape_home fail {url}: {e}")
    return out

# ========= mezcla sencilla =========
def simple_mix(items_by_source: list[list[dict]], limit_total: int) -> list[dict]:
    """
    Round-robin por lista de fuentes.
    Si no alcanza el límite, rellena con lo que quede (aunque repita fuente).
    """
    picked = []
    # round-robin por índice
    idx = 0
    while len(picked) < limit_total:
        progressed = False
        for bucket in items_by_source:
            if idx < len(bucket):
                picked.append(bucket[idx])
                progressed = True
                if len(picked) >= limit_total:
                    break
        if not progressed:
            break  # no queda nada
        idx += 1

    # si faltan, mete siguientes de cada bucket en orden
    if len(picked) < limit_total:
        rest = []
        for bucket in items_by_source:
            if idx < len(bucket):
                rest.extend(bucket[idx:])
        for it in rest:
            picked.append(it)
            if len(picked) >= limit_total:
                break

    # dedupe por URL respetando el orden
    seen = set()
    final = []
    for it in picked:
        if it["link"] in seen:
            continue
        seen.add(it["link"])
        final.append(it)
        if len(final) >= limit_total:
            break
    return final

# ========= RSS writer =========
def make_rss(country: str, items: list[dict]) -> str:
    esc = lambda s: html.escape(s or "", quote=True)
    now_http = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss version="2.0">',
        '<channel>',
        f'  <title>Noticias {esc(country.title())}</title>',
        '  <link>https://github.com/</link>',
        '  <description>Feed generado automáticamente</description>',
        f'  <lastBuildDate>{now_http}</lastBuildDate>',
        '  <generator>news-feeds simple</generator>',
        f'  <!-- build {datetime.utcnow().isoformat()}Z -->',
    ]
    for it in items:
        parts += [
            '  <item>',
            f"    <title>{esc(it['title'])}</title>",
            f"    <link>{esc(it['link'])}</link>",
            f"    <pubDate>{it['date'].strftime('%a, %d %b %Y %H:%M:%S GMT')}</pubDate>",
            f"    <category>{esc(it['domain'])}</category>",
            '  </item>',
        ]
    parts += ['</channel>', '</rss>', '']
    return "\n".join(parts)

def write_feed(country: str, items: list[dict]):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"{country}.xml")
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(make_rss(country, items))
    print(f"[OK] wrote {path} ({len(items)} items)")

# ========= main =========
def generate_country(country: str, sources: list[str], limit: int):
    # scrape cada fuente -> buckets
    buckets = []
    for src in sources:
        items = scrape_home(src, max_items=50)
        buckets.append(items)
    # mezcla simple
    mixed = simple_mix(buckets, limit_total=limit)
    write_feed(country, mixed)

def main():
    for country, urls in SOURCES.items():
        try:
            generate_country(country, urls, LIMITS[country])
        except Exception as e:
            print(f"[FATAL] {country}: {e}")
            # aun así escribe un RSS vacío (con timestamp) para que el workflow pueda commitear
            write_feed(country, [])

if __name__ == "__main__":
    main()

