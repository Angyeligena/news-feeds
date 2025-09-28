# scripts/build_feeds.py
# Scraping-only daily feeds (VE/PA/DO). No RSS. No Bloomberg. Fair mix with per-domain caps.
import os, re, html, hashlib, traceback, random
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode
from time import sleep

import requests
from bs4 import BeautifulSoup

OUTPUT_DIR = "data"
REQUEST_TIMEOUT = 25
RETRIES = 2
SLEEP_BETWEEN = 0.8  # ligera pausa entre intentos
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AngieNewsBot/2.0",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ================== TUS FUENTES (sin RSS) ==================
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

# LÍMITES POR PAÍS (salida final)
LIMITS = {"venezuela": 10, "panama": 10, "dominicana": 10}

# CAP inicial por dominio; si no alcanza el cupo total, subimos cap (3, 4, …) hasta llenar.
START_CAP_PER_DOMAIN = 2

# Overfetch para cada fuente (cuántos candidatos buscar por portada)
MAX_LINKS_PER_SOURCE = 40

# Filtro mínimo de calidad de titular
MIN_TITLE_LEN = 18

# Bloqueo de dominios no deseados
BLOCKED_DOMAINS = {"bloomberg.com", "bloomberglinea.com"}

# Selectores por dominio (más robustos) + fallback genérico
SITE_SELECTORS = {
    # Venezuela
    "www.elnacional.com": [
        "article h2 a", "article h3 a", "h2.entry-title a", "h3.entry-title a",
        ".headline a", ".post-title a", "h2 a[href*='/202']", "h3 a[href*='/202']",
    ],
    "talcualdigital.com": [
        "h2.entry-title a", "article h2 a", "div.post-title h2 a",
        ".jeg_post_title a", ".post-title a", "h3 a[href*='/']",
    ],
    "efectococuyo.com": [
        "article h2 a", "h2 a[href*='/']", "h3 a[href*='/']", ".jeg_post_title a",
        ".post-title a",
    ],
    # Panamá
    "www.prensa.com": [
        "article h2 a[href*='prensa.com']", "h1 a[href*='prensa.com']",
        "h2 a[href*='prensa.com']", ".headline a[href*='prensa.com']",
        "a[href*='prensa.com/noticias/']", "a[href*='prensa.com/sociedad/']",
        "a[href*='prensa.com/economia/']",
    ],
    "www.laestrella.com.pa": [
        "article h2 a[href*='laestrella.com.pa']", "h2 a[href*='laestrella.com.pa']",
        "h1 a[href*='laestrella.com.pa']", ".headline a[href*='laestrella.com.pa']",
        "a[href*='/panama/']", "a[href*='/economia/']", "a[href*='/vida-y-cultura/']",
    ],
    # Dominicana
    "listindiario.com": [
        "article h2 a", "h2 a[href*='listindiario.com']", "h3 a[href*='listindiario.com']",
        ".post-title a", ".headline a",
    ],
    "www.elcaribe.com.do": [
        "article h2 a", "h2 a[href*='elcaribe.com.do']", "a.post-title[href]",
        ".post-title a", ".headline a",
    ],
    "eldinero.com.do": [
        "article h2 a", "h2 a[href*='eldinero.com.do']", "a.post-title[href]",
        ".post-title a", ".headline a",
    ],
}

# ===== utilidades =====
def log(msg): print(msg, flush=True)

def strip_www(host: str) -> str:
    h = (host or "").strip().lower()
    return h[4:] if h.startswith("www.") else h

def clean_url(u: str) -> str:
    try:
        p = urlparse(u)
        qs = [(k, v) for k, v in parse_qsl(p.query)
              if not k.lower().startswith("utm") and k.lower() not in {"gclid", "fbclid"}]
        return urlunparse((p.scheme or "https", p.netloc.lower(), p.path.rstrip("/"), "", urlencode(qs), ""))
    except Exception:
        return u

def url_key(u: str) -> str:
    try:
        p = urlparse(clean_url(u))
        return f"{strip_www(p.netloc)}{p.path}"
    except Exception:
        return u

def norm_title(s: str) -> str:
    s = (s or "")
    s = re.sub(r"\b(LIVE|UPDATE|BREAKING|EN VIVO|ÚLTIMA HORA|ACTUALIZACIÓN)\b[:\-–]*\s*", "", s, flags=re.I)
    return re.sub(r"\s+", " ", s).strip().lower()

def abs_url(base: str, href: str) -> str:
    if not href: return ""
    if href.startswith("//"): return "https:" + href
    return urljoin(base, href)

def get_response(url: str):
    for i in range(RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            if not r.encoding or r.encoding.lower() == "iso-8859-1":
                r.encoding = r.apparent_encoding or "utf-8"
            return r
        except Exception as e:
            log(f"[WARN] GET fail ({i+1}/{RETRIES+1}) {url}: {e}")
            sleep(SLEEP_BETWEEN)
    return None

def to_soup(resp) -> BeautifulSoup | None:
    try:
        return BeautifulSoup(resp.text, "html.parser") if resp else None
    except Exception:
        return None

def extract_canonical(soup: BeautifulSoup, fallback_url: str) -> str:
    try:
        can = soup.find("link", rel=lambda x: x and "canonical" in x.lower())
        if can and can.has_attr("href"):
            return can["href"]
    except Exception:
        pass
    return fallback_url

def looks_like_article(link_url: str, host: str) -> bool:
    """Heurística simple para descartar portadas, etiquetas, autores."""
    p = urlparse(link_url)
    if strip_www(p.netloc) != strip_www(host):  # mantenemos internas; externas sólo si quieres ampliar
        # Si quieres permitir externas, comenta esta línea.
        return False
    path = p.path.lower()
    if any(seg in path for seg in ["/tag/", "/etiqueta/", "/autor", "/author", "/categoria", "/category", "/seccion", "/section"]):
        return False
    # Evitar páginas cortas tipo '/', '/home'
    if path in {"/", "", "/home"}:
        return False
    # Require formato con slug largo
    return len(path.strip("/").split("/")) >= 2

# ===== scraping =====
def scrape_home(url: str, limit: int) -> list[dict]:
    out = []
    resp = get_response(url)
    if not resp:
        log(f"[WARN] No HTML for {url}")
        return out
    soup = to_soup(resp)
    if not soup:
        log(f"[WARN] No soup for {url}")
        return out

    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}/"
    host = parsed.netloc.lower()
    selectors = SITE_SELECTORS.get(host, []) + [
        "article a[href]", "h1 a[href]", "h2 a[href]", "h3 a[href]",
        ".headline a[href]", ".post-title a[href]"
    ]

    seen = set()
    total = 0
    for sel in selectors:
        hits = 0
        for tag in soup.select(sel):
            href = tag.get("href")
            title = tag.get_text(strip=True)
            if not href or not title:
                continue
            full = abs_url(base, href)
            # filtra por aspecto de artículo
            if not looks_like_article(full, host):
                continue
            # normaliza por URL canónica si existe (requiere fetch rápido de cabecera HTML? evitamos coste)
            # Optamos por limpiar URL, que ya quita utm/fragmentos
            full = clean_url(full)
            k = url_key(full)
            if k in seen:
                continue
            seen.add(k)
            dom = strip_www(urlparse(full).netloc)
            if dom in BLOCKED_DOMAINS:
                continue
            if len(title) < MIN_TITLE_LEN:
                continue
            out.append({
                "title": title,
                "link": full,
                "date": datetime.utcnow(),   # sin timestamp; asume reciente
                "domain": dom,
                "urlk": k,
            })
            hits += 1; total += 1
            if total >= MAX_LINKS_PER_SOURCE:
                break
        log(f"[INFO] {host} selector '{sel}' → {hits}")
        if total >= MAX_LINKS_PER_SOURCE:
            break
    # dedupe interno por URL + (titulo+dominio)
    uniq_url, uniq_td, dedup = set(), set(), []
    for it in out:
        kurl = it["urlk"]
        ktd = hashlib.md5((norm_title(it["title"])+"|"+it["domain"]).encode()).hexdigest()
        if kurl in uniq_url or ktd in uniq_td: continue
        uniq_url.add(kurl); uniq_td.add(ktd); dedup.append(it)
    return dedup

# ===== mezcla con cap dinámico =====
def mix_with_dynamic_cap(items: list[dict], limit_total: int, start_cap: int) -> list[dict]:
    # dedupe global
    seen_url, seen_td, pool = se
