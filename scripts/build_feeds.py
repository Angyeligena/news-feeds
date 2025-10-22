# scripts/build_feeds.py
# Scraper simplificado para obtener todas las noticias disponibles en XML

import os, re, html, hashlib, traceback
from datetime import datetime
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode
from time import sleep

import requests
from bs4 import BeautifulSoup

OUTPUT_DIR = "data"
REQUEST_TIMEOUT = 25
RETRIES = 2
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AngieNewsBot/1.1; +https://github.com/)"}

# === FUENTES ===
SOURCES = {
    "venezuela": [
        "https://www.elnacional.com/venezuela/",
        "https://talcualdigital.com/noticias/",
        "https://efectococuyo.com/politica/",
    ],
    "panama": [
        "https://www.prensa.com/",
        "https://www.laestrella.com.pa/panama",
    ],
    "dominicana": [
        "https://www.diariolibre.com/actualidad/nacional",
        "https://listindiario.com/la-republica",
        "https://www.elcaribe.com.do/seccion/panorama/pais/",
        "https://eldinero.com.do/",
    ],
}

# Selectores específicos por URL
SITE_SELECTORS = {
    "https://www.elnacional.com/venezuela/": ["h2"],
    "https://efectococuyo.com/politica/": ["h2.entry-wrapper"],
    "https://talcualdigital.com/noticias/": ["h5"],
    "https://www.prensa.com/": ["h2"],
    "https://www.laestrella.com.pa/panama": ["h2 span.priority-content"],
    "https://www.diariolibre.com/actualidad/nacional": ["h3.text-md.sm\\:text-lg.mb-3"],
    "https://listindiario.com/la-republica": ["h2.c-article__title"],
    "https://www.elcaribe.com.do/seccion/panorama/pais/": ["h3.entry-title"],
    "https://eldinero.com.do/": ["h2.jeg_post_title"],
}

URL_ALLOWED_PREFIXES = {
    "https://www.prensa.com/": [
        "https://www.prensa.com/unidad-investigativa/",
        "https://www.prensa.com/sociedad/",
        "https://www.prensa.com/politica/",
    ],
    "https://www.laestrella.com.pa/panama": [
        "https://www.laestrella.com.pa/panama/",
    ],
    # opcionalmente, dejamos declarada la de RD por consistencia
    "https://www.elcaribe.com.do/seccion/panorama/pais/": [
        "https://www.elcaribe.com.do/seccion/panorama/pais/",
    ],
}

# ---------- Utilidades ----------
def log(msg: str): 
    print(msg, flush=True)

def clean_url(u: str) -> str:
    try:
        p = urlparse(u)
        qs = [(k, v) for k, v in parse_qsl(p.query)
              if not k.lower().startswith("utm") and k.lower() not in {"gclid","fbclid"}]
        return urlunparse((p.scheme or "https", p.netloc.lower(), p.path, "", urlencode(qs), ""))
    except Exception:
        return u

def norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()

def abs_url(page_url: str, href: str) -> str:
    if not href:
        return ""
    if href.startswith("//"):
        return "https:" + href
    return urljoin(page_url, href)

def get_html(url: str) -> bytes | None:
    for i in range(RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.content
        except Exception as e:
            log(f"[WARN] GET fail ({i+1}/{RETRIES+1}) {url}: {e}")
            sleep(1.2)
    return None

def fetch_html(url: str) -> BeautifulSoup | None:
    c = get_html(url)
    return BeautifulSoup(c, "html.parser") if c else None

# ---------- Scraping con selectores específicos ----------
def scrape_site(url: str) -> list[dict]:
    out: list[dict] = []
    try:
        soup = fetch_html(url)
        if not soup:
            log(f"[WARN] No HTML for {url}")
            return out
            
        parsed = urlparse(url)
        host, base = parsed.netloc.lower()
        
        # Obtener selectores específicos para esta URL
        selectors = SITE_SELECTORS.get(url, [])
        
        if not selectors:
            log(f"[WARN] No specific selectors for {url}, using fallback")
            selectors = ["h1 a[href]", "h2 a[href]", "h3 a[href]", "article h2 a"]

        seen = set()
        for sel in selectors:
            log(f"[INFO] Trying selector '{sel}' on {url}")
            hits = 0
            
            for tag in soup.select(sel):
                # Buscar el enlace dentro del elemento o en su padre
                link = None
                text = tag.get_text(strip=True)
                
                # Si el tag tiene href directamente
                if tag.name == 'a' and tag.get('href'):
                    link = tag.get('href')
                else:
                    # Buscar un enlace dentro del elemento
                    link_tag = tag.find('a', href=True)
                    if link_tag:
                        link = link_tag.get('href')
                
                if not text or not link:
                    continue
                    
                full = abs_url(url, link)
                if full in seen: 
                    continue

                # --- filtro por secciones si aplica ---
                allowed = URL_ALLOWED_PREFIXES.get(url)
                if allowed and not any(full.startswith(p) for p in allowed):
                    continue
                    
                seen.add(full)
                out.append({
                    "title": text,
                    "link": full,
                    "date": datetime.utcnow(),
                    "source": host,
                    "domain": host
                })
                hits += 1

            log(f"[INFO] {host} selector '{sel}' → {hits} items")

        # Filtro básico de calidad
        before = len(out)
        out = [it for it in out if len(it["title"]) >= 10]
        log(f"[INFO] {host} filter >=10 chars: {before} → {len(out)}")

        # Fallback si vacío - buscar cualquier enlace con texto
        if not out:
            log(f"[INFO] {host} using fallback selectors")
            for a in soup.select("a[href]"):
                href = a.get("href","")
                txt = a.get_text(strip=True)
                full = abs_url(url, href)
                
                if urlparse(full).netloc.lower() != host or len(txt) < 15: 
                    continue

                allowed = URL_ALLOWED_PREFIXES.get(url)
                if allowed and not any(full.startswith(p) for p in allowed):
                    continue
                    
                out.append({
                    "title": txt, 
                    "link": full, 
                    "date": datetime.utcnow(),
                    "source": host, 
                    "domain": host
                })

        # Dedupe por título normalizado
        seenk, dedup = set(), []
        for it in out:
            key = hashlib.md5(norm_text(it["title"]).encode()).hexdigest()
            if key in seenk: 
                continue
            seenk.add(key)
            it["link"] = clean_url(it["link"])
            dedup.append(it)
            
        log(f"[INFO] {host} final deduped: {len(dedup)} items")
        return dedup
        
    except Exception:
        log(f"[ERROR] scrape_site({url}) crashed:\n{traceback.format_exc()}")
        return out

# ---------- Generador XML ----------
def make_xml(country: str, items: list[dict]) -> str:
    now_http = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    build_comment = f"<!-- build {datetime.utcnow().isoformat()}Z -->"
    esc = lambda s: html.escape(s or "", quote=True)
    
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>', 
        '<rss version="2.0">', 
        '<channel>',
        f'  <title>Noticias {esc(country.title())} - Todas las disponibles</title>',
        '  <link>https://github.com/</link>',
        f'  <description>Feed con todas las noticias disponibles de {esc(country.title())} sin límites</description>',
        f'  <lastBuildDate>{now_http}</lastBuildDate>',
        '  <generator>simplified-news-scraper</generator>',
        f'  <totalItems>{len(items)}</totalItems>',
        f'  {build_comment}',
    ]
    
    for it in items:
        parts += [
            '  <item>',
            f"    <title>{esc(it['title'])}</title>",
            f"    <link>{esc(it['link'])}</link>",
            f"    <pubDate>{it['date'].strftime('%a, %d %b %Y %H:%M:%S GMT')}</pubDate>",
            f"    <category>{esc(it['domain'])}</category>",
            f"    <source>{esc(it['source'])}</source>",
            '  </item>',
        ]
    
    parts += ['</channel>', '</rss>', '']
    return "\n".join(parts)

def write_xml_feed(country: str, items: list[dict]):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"{country}.xml")
    xml = make_xml(country, items)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(xml)
    log(f"[OK] Wrote {path} ({len(items)} items)")

# ---------- Pipeline simplificado ----------
def generate_country_news(country: str, urls: list[str]):
    log(f"[RUN] Scraping {country} - all available news")
    all_items = []

    for url in urls:
        try:
            items = scrape_site(url)
            all_items.extend(items)
            log(f"[INFO] {country} source {url} → {len(items)} items")
        except Exception:
            log(f"[ERROR] {country} source {url} crashed:\n{traceback.format_exc()}")

    if not all_items:
        log(f"[WARN] {country}: no items found")
        return

    # Dedupe global por título
    seen_titles = set()
    unique_items = []
    for item in all_items:
        title_key = norm_text(item["title"])
        if title_key not in seen_titles:
            seen_titles.add(title_key)
            unique_items.append(item)

    log(f"[INFO] {country} total unique items: {len(unique_items)}")
    write_xml_feed(country, unique_items)

def main():
    for country, urls in SOURCES.items():
        try:
            generate_country_news(country, urls)
        except Exception:
            log(f"[FATAL] {country} crashed:\n{traceback.format_exc()}")

if __name__ == "__main__":
    main()
