# scripts/build_feeds.py
# Scraper simplificado para obtener todas las noticias disponibles

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

# Selectores por dominio
SITE_SELECTORS = {
    "www.elnacional.com": ["article h2 a", "h3 a", ".headline a"],
    "talcualdigital.com": ["h2.entry-title a", "article h2 a", "div.post-title h2 a"],
    "efectococuyo.com": ["article h2 a", "h2 a[href*='/']"],
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
    "listindiario.com": ["h2 a[href]", "h3 a[href]", "article h2 a"],
    "www.elcaribe.com.do": ["h2 a[href]", "article h2 a", "a.post-title[href]"],
    "eldinero.com.do": ["h2 a[href]", "article h2 a", "a.post-title[href]"],
    "www.diariolibre.com": [
        "h3 a[href*='/actualidad/']",
        "h2 a[href*='/actualidad/']",
        "article h3 a",
        "article h2 a",
        ".noticia h3 a",
        ".news-item h3 a",
        "a[href*='/actualidad/']:not([href*='/tag/']):not([href*='/autor/'])",
        "h3 a[href]",
        "h2 a[href]",
        "a[href*='/actualidad/']"
    ]
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
            log(f"[WARN] GET fail ({i+1}/{RETRIES+1}) {url}: {e}")
            sleep(1.2)
    return None

def fetch_html(url: str) -> BeautifulSoup | None:
    c = get_html(url)
    return BeautifulSoup(c, "html.parser") if c else None

# ---------- Scraping simplificado ----------
def scrape_site(url: str) -> list[dict]:
    out: list[dict] = []
    try:
        soup = fetch_html(url)
        if not soup:
            log(f"[WARN] No HTML for {url}")
            return out
            
        parsed = urlparse(url)
        host, base = parsed.netloc.lower(), f"{parsed.scheme}://{parsed.netloc}/"
        selectors = SITE_SELECTORS.get(host, []) + ["h1 a[href]", "h2 a[href]", "article h2 a"]

        seen = set()
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
                if full in seen: 
                    continue
                    
                seen.add(full)
                out.append({
                    "title": text,
                    "link": full,
                    "date": datetime.utcnow(),
                    "source": host,
                    "domain": host
                })

        log(f"[INFO] {host} selector '{sel}' → {len(out)} total")

        # Filtro básico de calidad (solo títulos muy cortos)
        before = len(out)
        out = [it for it in out if len(it["title"]) >= 10]
        log(f"[INFO] {host} filter >=10 chars: {before} → {len(out)}")

        # Fallback si vacío
        if not out:
            for a in soup.select("a[href]"):
                href = a.get("href","")
                txt = a.get_text(strip=True)
                full = abs_url(base, href)
                
                if urlparse(full).netloc.lower() != host or len(txt) < 15: 
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

# ---------- Output simplificado ----------
def write_news_data(country: str, items: list[dict]):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Archivo de texto simple
    txt_path = os.path.join(OUTPUT_DIR, f"{country}_news.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(f"=== NOTICIAS {country.upper()} ===\n")
        f.write(f"Total encontradas: {len(items)}\n")
        f.write(f"Fecha: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        for i, item in enumerate(items, 1):
            f.write(f"{i}. {item['title']}\n")
            f.write(f"   Link: {item['link']}\n")
            f.write(f"   Fuente: {item['source']}\n\n")
    
    # Archivo CSV
    csv_path = os.path.join(OUTPUT_DIR, f"{country}_news.csv")
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("titulo,link,fuente,fecha\n")
        for item in items:
            f.write(f'"{item["title"]}","{item["link"]}","{item["source"]}","{item["date"].isoformat()}"\n')
    
    log(f"[OK] Wrote {txt_path} and {csv_path} ({len(items)} items)")

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
    write_news_data(country, unique_items)

def main():
    for country, urls in SOURCES.items():
        try:
            generate_country_news(country, urls)
        except Exception:
            log(f"[FATAL] {country} crashed:\n{traceback.format_exc()}")

if __name__ == "__main__":
    main()
