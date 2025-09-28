# scripts/build_feeds.py
# Feeds diarios PA/VE/DO SIN Google News.
# Cambio clave: cuotas por fuente + round-robin para no monopolizar por un solo dominio.

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

# === FUENTES (tus links) ===
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
        "https://www.diariolibre.com/rss/portada.xml",
        "https://listindiario.com/la-republica",
        "https://www.elcaribe.com.do/seccion/panorama/pais/",
        "https://eldinero.com.do/",
    ],
}

# Límite total por país
LIMITS = {"venezuela": 10, "panama": 10, "dominicana": 10}

# Cuota máxima por dominio dentro de cada país (tope duro por source)
MAX_PER_DOMAIN = 4

# Selectores por dominio (para scraping HTML)
SITE_SELECTORS = {
    # Venezuela
    "www.elnacional.com": ["article h2 a", "h3 a", ".headline a"],
    "talcualdigital.com": ["h2.entry-title a", "article h2 a", "div.post-title h2 a"],
    "efectococuyo.com": ["article h2 a", "h2 a[href*='/']"],
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
            return r.content
        except Exception as e:
            log(f"[WARN] GET fail ({i+1}/{RETRIES+1}) {url}: {e}")
            sleep(1.2)
    return None

def fetch_html(url: str) -> BeautifulSoup | None:
    c = get_html(url)
    return BeautifulSoup(c, "html.parser") if c else None

# ---------- Scraping/RSS ----------
def scrape_site(url: str, soft_limit: int) -> list[dict]:
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
            hits = 0
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
                if full in seen: continue
                seen.add(full)
                out.append({
                    "title": text,
                    "link": full,
                    "date": datetime.utcnow(),
                    "source": host,
                    "domain": host
                })
                hits += 1
                if len(out) >= soft_limit * 4: break
            log(f"[INFO] {host} selector '{sel}' → {hits}")
            if len(out) >= soft_limit * 4: break

        # Filtro calidad
        before = len(out)
        out = [it for it in out if len(it["title"]) >= 25]
        log(f"[INFO] {host} filter >=25 chars: {before} → {len(out)}")

        # Fallback si vacío
        if not out:
            fb = 0
            for a in soup.select("a[href]"):
                href = a.get("href",""); txt=a.get_text(strip=True); full=abs_url(base, href)
                if urlparse(full).netloc.lower()!=host or len(txt)<35: continue
                out.append({"title": txt, "link": full, "date": datetime.utcnow(),
                            "source": host, "domain": host})
                fb += 1
                if len(out) >= soft_limit * 2: break
            log(f"[INFO] {host} fallback anchors → {fb}")
        # Dedupe dentro de la fuente
        seenk, dedup = set(), []
        for it in out:
            key = hashlib.md5((norm_text(it["title"])+"|"+urlparse(it["link"]).netloc).encode()).hexdigest()
            if key in seenk: continue
            seenk.add(key)
            it["link"] = clean_url(it["link"])
            dedup.append(it)
        return dedup[: soft_limit * 2]
    except Exception:
        log(f"[ERROR] scrape_site({url}) crashed:\n{traceback.format_exc()}")
        return out

def fetch_rss(url: str, cutoff_utc: datetime, soft_limit: int) -> list[dict]:
    out: list[dict] = []
    try:
        c = get_html(url)
        if not c:
            log(f"[WARN] No RSS for {url}")
            return out
        feed = feedparser.parse(c)
        for e in feed.entries[: soft_limit * 6]:  # overfetch
            title = getattr(e,"title","") or ""
            link  = getattr(e,"link","") or ""
            if not title or not link: continue
            if getattr(e,"published_parsed", None):
                dt = datetime(*e.published_parsed[:6])
            elif getattr(e,"updated_parsed", None):
                dt = datetime(*e.updated_parsed[:6])
            else:
                dt = datetime.utcnow()
            if dt < cutoff_utc: continue
            dom = urlparse(link).netloc or urlparse(url).netloc
            out.append({"title": title, "link": clean_url(link), "date": dt,
                        "source": dom, "domain": dom})
        log(f"[INFO] RSS {url} → {len(out)} after cutoff")
        return out
    except Exception:
        log(f"[ERROR] fetch_rss({url}) crashed:\n{traceback.format_exc()}")
        return out

# ---------- Ensamblado con MIX por fuente ----------
def mix_by_domain(items: list[dict], limit_total: int, max_per_domain: int) -> list[dict]:
    """
    1) Dedupe global por (titulo normalizado + dominio)
    2) Bucket por dominio
    3) Round-robin entre dominios respetando max_per_domain
    """
    # 1) dedupe global
    seen, pool = set(), []
    for it in sorted(items, key=lambda x: x["date"], reverse=True):
        key = hashlib.md5((norm_text(it["title"])+"|"+it["domain"].lower()).encode()).hexdigest()
        if key in seen: continue
        seen.add(key)
        pool.append(it)

    # 2) bucket por dominio
    buckets = {}
    for it in pool:
        buckets.setdefault(it["domain"].lower(), []).append(it)

    # ordena cada bucket por fecha desc
    for d in buckets: buckets[d].sort(key=lambda x: x["date"], reverse=True)

    # 3) round-robin
    order = list(buckets.keys())
    # baraja el orden para evitar siempre el mismo arranque
    random.shuffle(order)
    picked = []
    domain_counts = {d: 0 for d in order}
    idx = 0
    while len(picked) < limit_total and order:
        d = order[idx % len(order)]
        if buckets[d] and domain_counts[d] < max_per_domain:
            picked.append(buckets[d].pop(0))
            domain_counts[d] += 1
        # Si ese dominio ya no tiene o llegó al tope, bórralo del ciclo
        if not buckets[d] or domain_counts[d] >= max_per_domain:
            order.pop(idx % len(order))
            # no incrementes idx si quitaste el actual
        else:
            idx += 1
    return picked

# ---------- RSS Writer ----------
def make_rss(country: str, items: list[dict]) -> str:
    now_http = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    build_comment = f"<!-- build {datetime.utcnow().isoformat()}Z -->"
    esc = lambda s: html.escape(s or "", quote=True)
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>', '<rss version="2.0">', '<channel>',
        f'  <title>Noticias {esc(country.title())}</title>',
        '  <link>https://github.com/</link>',
        '  <description>Feed generado automáticamente</description>',
        f'  <lastBuildDate>{now_http}</lastBuildDate>',
        '  <generator>news-feeds GitHub Action</generator>',
        f'  {build_comment}',
    ]
    for it in items:
        parts += [
            '  <item>',
            f"    <title>{esc(it['title'])}</title>",
            f"    <link>{esc(it['link'])}</link>",
            f"    <pubDate>{it['date'].strftime('%a, %d %b %Y %H:%M:%S GMT')}</pubDate>",
            # opcional: etiqueta de dominio/origen
            f"    <category>{esc(it['domain'])}</category>",
            '  </item>',
        ]
    parts += ['</channel>','</rss>','']
    return "\n".join(parts)

def write_feed(country: str, items: list[dict]):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"{country}.xml")
    xml = make_rss(country, items)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(xml)
    log(f"[OK] Wrote {path} ({len(items)} items)")

# ---------- Pipeline por país ----------
def generate_country_feed(country: str, urls: list[str], limit_total: int):
    log(f"[RUN] Generating {country} (limit {limit_total}, max/domain {MAX_PER_DOMAIN})")
    items, cutoff = [], datetime.utcnow() - timedelta(days=1)

    # 1) recolecta por fuente
    for url in urls:
        try:
            soft_limit = max(limit_total, 10)  # overfetch por fuente
            if url.endswith(".xml") or "rss" in url.lower() or "feed" in url.lower():
                got = fetch_rss(url, cutoff, soft_limit)
            else:
                got = scrape_site(url, soft_limit)
            # etiqueta dominio (por si falta)
            for it in got:
                it["domain"] = it.get("domain") or urlparse(it["link"]).netloc.lower()
            items += got
            log(f"[INFO] {country} source {url} → {len(got)} items")
        except Exception:
            log(f"[ERROR] {country} source {url} crashed:\n{traceback.format_exc()}")

    if not items:
        log(f"[WARN] {country}: no items at all → placeholder")
        items = [{
            "title": f"No se pudieron extraer titulares para {country} en este run",
            "link": "https://example.com/",
            "date": datetime.utcnow(),
            "source": "generator",
            "domain": "generator"
        }]

    # 2) mezcla justa por dominio (round-robin + tope por dominio)
    mixed = mix_by_domain(items, limit_total, MAX_PER_DOMAIN)

    # si por cualquier motivo quedó corto, rellena con el resto sin tope
    if len(mixed) < limit_total:
        log(f"[INFO] {country} mixed shortage: {len(mixed)}/{limit_total}. Backfilling…")
        # saca los que ya están
        chosen_ids = set(hashlib.md5((norm_text(x["title"])+"|"+x["domain"]).encode()).hexdigest() for x in mixed)
        remaining = []
        for it in sorted(items, key=lambda x: x["date"], reverse=True):
            key = hashlib.md5((norm_text(it["title"])+"|"+it["domain"]).encode()).hexdigest()
            if key in chosen_ids: continue
            remaining.append(it)
        mixed += remaining[: max(0, limit_total - len(mixed))]

    write_feed(country, mixed[:limit_total])

def main():
    random.seed()  # para variar el orden del round-robin
    for country, urls in SOURCES.items():
        try:
            generate_country_feed(country, urls, LIMITS[country])
        except Exception:
            log(f"[FATAL] {country} crashed:\n{traceback.format_exc()}")
            write_feed(country, [{
                "title": f"Error generando {country}",
                "link": "https://example.com/",
                "date": datetime.utcnow(),
                "source": "generator",
                "domain": "generator"
            }])

if __name__ == "__main__":
    main()
ChatGPT said:
Thought for 11s

Perfecto. Aquí tienes el script completo ajustado exactamente a lo que pediste:

Trabaja solo sobre las URLs exactas que indicas (no a nivel de dominio).

Aplica una cuota fija por cada fuente (5/5/5/5/5/3/3/3/3).

Para fuentes HTML, hace scraping simple de h1/h2/article h2 a.

Para la fuente RSS (diariolibre), usa feedparser.

Filtra los enlaces para que empiecen con el prefijo exacto de la fuente (misma URL base y mismo path o subpath).

Deduplica por URL y escribe siempre data/{pais}.xml.

Copia/pega sobre scripts/build_feeds.py:

# scripts/build_feeds.py
# Feeds diarios PA/VE/DO SIN Google News.
# Exact-match por URL (mismo host + prefijo de path) y CUOTAS por fuente.

import os, re, html, hashlib, traceback
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode
from time import sleep

import requests, feedparser
