# scripts/build_feeds.py
# Feeds diarios PA/VE/DO SIN Google News.
# Cambio clave: cuotas por fuente EXACTAS + filtro de prefijo de path por fuente.

import os, re, html, hashlib, traceback, random
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode
from time import sleep

import requests, feedparser
from bs4 import BeautifulSoup

OUTPUT_DIR = "data"
REQUEST_TIMEOUT = 25
RETRIES = 2
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AngieNewsBot/1.1; +https://github.com/)"}

# === FUENTES con CUOTA (links exactos) ===
SOURCES = {
    "venezuela": [
        ("https://www.elnacional.com/venezuela/", 5),
        ("https://talcualdigital.com/noticias/", 5),
        ("https://efectococuyo.com/politica/", 5),
    ],
    "panama": [
        ("https://www.prensa.com/", 5),
        ("https://www.laestrella.com.pa/panama", 5),
    ],
    "dominicana": [
        ("https://www.diariolibre.com/actualidad/nacional", 3),
        ("https://listindiario.com/la-republica", 3),
        ("https://www.elcaribe.com.do/seccion/panorama/pais/", 3),
        ("https://eldinero.com.do/", 3),
    ],
}

# Límite total por país (suma de cuotas)
LIMITS = {"venezuela": 15, "panama": 10, "dominicana": 12}

# Cuota máxima por dominio dentro de cada país (ya no se usa para mezclar cuotas fijas, se deja por compatibilidad)
MAX_PER_DOMAIN = 4

# Selectores por dominio (para scraping HTML)
SITE_SELECTORS = {
    # Venezuela
    "www.elnacional.com": ["article h2 a", "article h3 a", "h1 a[href]", "h2 a[href]", ".headline a"],
    "talcualdigital.com": ["h2.entry-title a", "article h2 a", "div.post-title h2 a", ".jeg_post_title a", ".post-title a"],
    "efectococuyo.com": ["article h2 a", "h2 a[href*='/']", ".jeg_post_title a", ".post-title a"],

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
    "listindiario.com": ["h2 a[href]", "h3 a[href]", "article h2 a", ".post-title a"],
    # refuerzo para El Caribe:
    "www.elcaribe.com.do": ["h2 a[href]", "article h2 a", "a.post-title[href]", ".entry-title a[href]", ".td-module-title a[href]", ".c-post-card a[href]"],
    "eldinero.com.do": ["h2 a[href]", "article h2 a", "a.post-title[href]", ".entry-title a[href]"],
}

# ---------- Utilidades ----------
def log(msg: str): print(msg, flush=True)

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
    if not href: return ""
    if href.startswith("//"): return "https:" + href
    return urljoin(base, href)

def get_html(url: str) -> bytes | None:
    for i in range(RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            # asegurar decodificación correcta
            if not r.encoding or r.encoding.lower() == "iso-8859-1":
                r.encoding = r.apparent_encoding or "utf-8"
            return r.text.encode(r.encoding or "utf-8", errors="ignore")
        except Exception as e:
            log(f"[WARN] GET fail ({i+1}/{RETRIES+1}) {url}: {e}")
            sleep(1.2)
    return None

def fetch_html(url: str) -> BeautifulSoup | None:
    c = get_html(url)
    return BeautifulSoup(c, "html.parser") if c else None

def same_host_and_path_prefix(link: str, base_url: str) -> bool:
    """
    Acepta SOLO si:
    - El host es el mismo.
    - El path del link empieza con el path de la fuente (prefijo exacto) o es igual (sin barra final).
    """
    try:
        pl = urlparse(clean_url(link))
        pb = urlparse(clean_url(base_url))
        if pl.netloc.lower() != pb.netloc.lower():
            return False
        base_path = pb.path if pb.path.endswith("/") else pb.path + "/"
        return pl.path.startswith(base_path) or pl.path == pb.path
    except Exception:
        return False

# ---------- Scraping/RSS ----------
def scrape_site(url: str, quota: int) -> list[dict]:
    """
    Scrapea SOLO la URL dada y devuelve HASTA `quota` items que:
      - Sean del mismo host
      - Y cuyo path EMPIECE por el path de la fuente (link exacto + subrutas)
    """
    out: list[dict] = []
    try:
        soup = fetch_html(url)
        if not soup:
            log(f"[WARN] No HTML for {url}")
            return out
        parsed = urlparse(url)
        host, base = parsed.netloc.lower(), f"{parsed.scheme}://{parsed.netloc}/"
        selectors = SITE_SELECTORS.get(host, []) + ["h1 a[href]", "h2 a[href]", "article h2 a"]
