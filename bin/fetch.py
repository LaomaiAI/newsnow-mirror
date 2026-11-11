#!/usr/bin/env python3
"""NewsNow 镜像抓取脚本。

由 cron 分两组运行: --group free 每 2 分钟抓 15 个免费源,
--group blockbeats 每 3 分钟只抓律动 (1 次 API 调用)。
不带 --group 时默认抓全部 (all)。NewsNow 频道逐个经 r.jina.ai
抓原站接口 (原站接口不带跨域许可头, 本机 curl 又被 Cloudflare 拦,
所以直接抓原站走不通, 走阅读代理转发)。
抓到的文本带一小段 markdown 头, 从 Markdown Content: 之后或
第一个 { 起提取纯 JSON 再解析, 校验通过后才落盘。
另有直连源直接抓官方接口或 RSS, 归一化成同一镜像格式后再落盘:
Odaily / CoinTelegraph / CoinDesk / Stocktwits / Yahoo / BWEnews
(t.me 公开页) / BlockBeats (付费 key, 三把轮换) / Linux.do 资源荟萃·
福利羊毛 (经 r.jina.ai)。(少数派 / Hacker News / Product Hunt 已于
2026-11-10 随卡片下线。)

安全规则:
- 单个平台失败不影响其他平台, 失败时保留上一次的好文件,
  绝不拿空文件或坏文件覆盖好文件。
- 写文件用临时文件加原子改名。
- 只有内容变化时才更新本地文件; 同步到 GitHub 时先比对远端
  内容, 相同就跳过, 不同才提交。

镜像结构保持原站 JSON 原样:
{"status","id","updatedTime","items":[{"id","title","mobileUrl"(或 url),...}]}
空间现有的解析代码不用改, 只换取数地址。
"""

import base64
import contextlib
import datetime
import fcntl
import hashlib
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

sys.path.insert(0, "/opt/muse/skills/custom-connector-setup/bin")
from dynamic_credentials import add_surrogate_to_request

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

# NewsNow 平台 ID 一个字都不许改, github-trending 与 qq-news 在原站报 500, 绝不用
# xueqiu 已于 2026-11-10 按用户指令移除 (只看美股改走 Stocktwits/Yahoo)
# tencent-hot 与 douyin 已于 2026-11-10 按用户指令移除
SOURCES = [
    ("cls-telegraph", "cls-telegraph.json"),
    ("wallstreetcn", "wallstreetcn.json"),
    ("github-trending-today", "github-trending-today.json"),
    ("juejin", "juejin.json"),
]

UA = {"User-Agent": "curl/8.5.0"}

GH_OWNER = "LaomaiAI"
GH_REPO = "newsnow-mirror"
GH_BRANCH = "main"


def log(msg):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def extract_json(text):
    marker = "Markdown Content:"
    if marker in text:
        text = text.split(marker, 1)[1].strip()
    start = text.find("{")
    if start < 0:
        raise ValueError("no JSON object found in response")
    end = text.rfind("}")
    if end < start:
        raise ValueError("no JSON object end found")
    return json.loads(text[start:end + 1])


def validate(data):
    """Keep only usable items (title + link); raise only if none remain.
    One malformed item (e.g. an ad without a link) must not drop the
    whole card for a round — custom fetchers already skip bad items
    one by one, so NewsNow sources do the same here."""
    if not isinstance(data, dict):
        raise ValueError("top level is not an object")
    items = data.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError("items missing or empty")
    good = []
    for it in items:
        if not isinstance(it, dict):
            continue
        if not it.get("title"):
            continue
        if not (it.get("mobileUrl") or it.get("url")):
            continue
        good.append(it)
    if not good:
        raise ValueError("items missing or empty")
    data["items"] = good
    return True


def fetch_one(sid):
    target = f"https://newsnow.busiyi.world/api/s?id={sid}&latest"
    url = "https://r.jina.ai/" + target
    last_err = None
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=30) as resp:
                text = resp.read().decode("utf-8", "replace")
            data = extract_json(text)
            validate(data)
            return data
        except Exception as e:  # noqa: BLE001 - retry then report
            last_err = e
            log(f"{sid}: attempt {attempt} failed: {e}")
            time.sleep(3 if attempt == 1 else 0)
    raise RuntimeError(f"fetch failed after 2 attempts: {last_err}")


def fetch_odaily():
    """Fetch Odaily newsflash directly.

    Public JSON API, no key, no jina needed (direct curl works).
    Normalizes to the same mirror format as NewsNow sources so the
    space parser needs no change.
    """
    url = "https://api.odaily.news/api/v1/newsflash?page=1&size=30&lang=zh-cn"
    last_err = None
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = json.loads(resp.read().decode("utf-8", "replace"))
            data_list = raw.get("data", {}).get("list")
            if not isinstance(data_list, list) or not data_list:
                raise ValueError("odaily list missing or empty")
            items = []
            for it in data_list:
                if not isinstance(it, dict):
                    continue
                title = it.get("title")
                link = it.get("link")
                if not title or not link:
                    continue
                items.append({
                    "id": it.get("id"),
                    "title": title,
                    "mobileUrl": link,
                    "url": link,
                    "pubDate": it.get("publishTimestamp"),
                })
            if not items:
                raise ValueError("odaily no usable items")
            return {
                "status": "cache",
                "id": "odaily",
                "updatedTime": int(time.time() * 1000),
                "items": items,
            }
        except Exception as e:  # noqa: BLE001 - retry then report
            last_err = e
            log(f"odaily: attempt {attempt} failed: {e}")
            time.sleep(3 if attempt == 1 else 0)
    raise RuntimeError(f"fetch failed after 2 attempts: {last_err}")


def _rss_pubdate_ms(text):
    try:
        dt = parsedate_to_datetime(text)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:  # noqa: BLE001
        return None


def fetch_rss(url, sid, size=30):
    """Fetch an RSS 2.0 feed and normalize to mirror format."""
    last_err = None
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
            root = ET.fromstring(raw)
            items = []
            for it in root.iter():
                if it.tag.rsplit("}", 1)[-1] != "item":
                    continue
                vals = {}
                for child in it:
                    name = child.tag.rsplit("}", 1)[-1]
                    if name in ("title", "link", "guid", "pubDate") and child.text:
                        vals.setdefault(name, child.text.strip())
                title = html.unescape(vals["title"]) if vals.get("title") else None
                link = vals.get("link")
                if not title or not link:
                    continue
                pub_ms = None
                if vals.get("pubDate"):
                    pub_ms = _rss_pubdate_ms(vals["pubDate"])
                items.append({
                    "id": vals.get("guid") or link,
                    "title": title,
                    "mobileUrl": link,
                    "url": link,
                    "pubDate": pub_ms,
                })
                if len(items) >= size:
                    break
            if not items:
                raise ValueError(f"{sid} rss no usable items")
            return {
                "status": "cache",
                "id": sid,
                "updatedTime": int(time.time() * 1000),
                "items": items,
            }
        except Exception as e:  # noqa: BLE001 - retry then report
            last_err = e
            log(f"{sid}: attempt {attempt} failed: {e}")
            time.sleep(3 if attempt == 1 else 0)
    raise RuntimeError(f"fetch failed after 2 attempts: {last_err}")


def fetch_cointelegraph():
    return fetch_rss("https://cointelegraph.com/rss", "cointelegraph", size=30)


def fetch_coindesk():
    return fetch_rss("https://www.coindesk.com/arc/outboundfeeds/rss/", "coindesk", size=30)


def fetch_stocktwits():
    """Stocktwits trending symbols (US discussion heat)."""
    url = "https://api.stocktwits.com/api/2/trending/symbols.json"
    last_err = None
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = json.loads(resp.read().decode("utf-8", "replace"))
            syms = raw.get("symbols")
            if not isinstance(syms, list) or not syms:
                raise ValueError("stocktwits symbols missing or empty")
            now_ms = int(time.time() * 1000)
            items = []
            for s in syms:
                if not isinstance(s, dict):
                    continue
                symbol = s.get("symbol")
                if not symbol:
                    continue
                name = s.get("title") or ""
                title = f"{symbol} {name}".strip()
                score = s.get("trending_score")
                watch = s.get("watchlist_count")
                info = ""
                if isinstance(score, (int, float)):
                    info = f"热度 {score:.1f}"
                if isinstance(watch, int):
                    info = f"{info} · {watch}人关注" if info else f"{watch}人关注"
                item = {
                    "id": symbol,
                    "title": title,
                    "mobileUrl": f"https://stocktwits.com/symbol/{symbol}",
                    "url": f"https://stocktwits.com/symbol/{symbol}",
                    "pubDate": now_ms,
                }
                if info:
                    item["extra"] = {"info": info}
                items.append(item)
                if len(items) >= 30:
                    break
            if not items:
                raise ValueError("stocktwits no usable items")
            return {
                "status": "cache",
                "id": "stocktwits",
                "updatedTime": now_ms,
                "items": items,
            }
        except Exception as e:  # noqa: BLE001 - retry then report
            last_err = e
            log(f"stocktwits: attempt {attempt} failed: {e}")
            time.sleep(3 if attempt == 1 else 0)
    raise RuntimeError(f"fetch failed after 2 attempts: {last_err}")


UA_BROWSER = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}


def fetch_yahoo_active():
    """Yahoo Finance most actives (US volume ranking)."""
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=most_actives&count=30"
    last_err = None
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(url, headers=UA_BROWSER)
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = json.loads(resp.read().decode("utf-8", "replace"))
            result = raw.get("finance", {}).get("result") or [{}]
            quotes = result[0].get("quotes")
            if not isinstance(quotes, list) or not quotes:
                raise ValueError("yahoo quotes missing or empty")
            now_ms = int(time.time() * 1000)
            items = []
            for q in quotes:
                if not isinstance(q, dict):
                    continue
                symbol = q.get("symbol")
                if not symbol:
                    continue
                name = q.get("shortName") or q.get("longName") or ""
                title = f"{symbol} {name}".strip()
                chg = q.get("regularMarketChangePercent")
                vol = q.get("regularMarketVolume")
                info = ""
                if isinstance(chg, (int, float)):
                    info = f"{chg:+.2f}%"
                if isinstance(vol, (int, float)):
                    vs = f"{vol / 1e6:.1f}M" if vol >= 1e6 else f"{vol / 1e3:.0f}K"
                    info = f"{info} · {vs}" if info else vs
                item = {
                    "id": symbol,
                    "title": title,
                    "mobileUrl": f"https://finance.yahoo.com/quote/{symbol}",
                    "url": f"https://finance.yahoo.com/quote/{symbol}",
                    "pubDate": now_ms,
                }
                if info:
                    item["extra"] = {"info": info}
                items.append(item)
                if len(items) >= 30:
                    break
            if not items:
                raise ValueError("yahoo no usable items")
            return {
                "status": "cache",
                "id": "yahoo-active",
                "updatedTime": now_ms,
                "items": items,
            }
        except Exception as e:  # noqa: BLE001 - retry then report
            last_err = e
            log(f"yahoo-active: attempt {attempt} failed: {e}")
            time.sleep(3 if attempt == 1 else 0)
    raise RuntimeError(f"fetch failed after 2 attempts: {last_err}")


def fetch_bwenews():
    """Scrape the public t.me/s/BWEnews page (Equation News, fastest Chinese crypto news)."""
    url = "https://t.me/s/BWEnews"
    last_err = None
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(url, headers=UA_BROWSER)
            with urllib.request.urlopen(req, timeout=30) as resp:
                page = resp.read().decode("utf-8", "replace")
            parts = re.split(r'data-post="BWEnews/(\d+)"', page)
            items = []
            for i in range(1, len(parts) - 1, 2):
                pid = parts[i]
                blk = parts[i + 1]
                tm = re.search(r'class="tgme_widget_message_text js-message_text"[^>]*>(.*?)</div>', blk, re.S)
                if not tm:
                    continue
                txt = re.sub(r'<[^>]+>', '', tm.group(1))
                txt = html.unescape(txt)
                txt = re.sub(r'\s+', ' ', txt).strip()
                if not txt:
                    continue
                if len(txt) > 500:
                    txt = txt[:500] + "..."
                m = re.search(r'<time datetime="([^"]+)"', blk)
                pub_ms = None
                if m:
                    try:
                        dt = datetime.datetime.fromisoformat(m.group(1).replace("Z", "+00:00"))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=datetime.timezone.utc)
                        pub_ms = int(dt.timestamp() * 1000)
                    except Exception:  # noqa: BLE001
                        pub_ms = None
                items.append({
                    "id": pid,
                    "title": txt,
                    "mobileUrl": f"https://t.me/BWEnews/{pid}",
                    "url": f"https://t.me/BWEnews/{pid}",
                    "pubDate": pub_ms,
                })
            if not items:
                raise ValueError("bwenews no usable items")
            items.reverse()
            items = items[:30]
            return {
                "status": "cache",
                "id": "bwenews",
                "updatedTime": int(time.time() * 1000),
                "items": items,
            }
        except Exception as e:  # noqa: BLE001 - retry then report
            last_err = e
            log(f"bwenews: attempt {attempt} failed: {e}")
            time.sleep(3 if attempt == 1 else 0)
    raise RuntimeError(f"fetch failed after 2 attempts: {last_err}")


def _blockbeats_once(connector):
    """One BlockBeats newsflash attempt with the given connector.
    Returns the parsed data dict. Raises the underlying exception
    (HTTPError keeps its .code, so the caller can spot a 429)."""
    req = urllib.request.Request(
        "https://api-pro.theblockbeats.info/v1/newsflash?size=30&page=1",
        headers=UA,
    )
    add_surrogate_to_request(req, connector, allowed_hosts=["api-pro.theblockbeats.info"])
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = json.loads(resp.read().decode("utf-8", "replace"))
    if raw.get("status") != 0:
        raise ValueError(f"blockbeats api status {raw.get('status')}: {raw.get('message')}")
    entries = raw.get("data", {}).get("data", [])
    items = []
    for e in entries:
        title = (e.get("title") or "").strip()
        link = (e.get("link") or "").strip()
        if not title or not link:
            continue
        pub_ms = None
        ct = e.get("create_time")
        if ct:
            try:
                dt = datetime.datetime.strptime(ct, "%Y-%m-%d %H:%M:%S")
                dt = dt.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=8)))
                pub_ms = int(dt.timestamp() * 1000)
            except Exception:  # noqa: BLE001
                pub_ms = None
        items.append({
            "id": str(e.get("id")),
            "title": title,
            "mobileUrl": link,
            "url": link,
            "pubDate": pub_ms,
        })
        if len(items) >= 30:
            break
    if not items:
        raise ValueError("blockbeats no usable items")
    return {
        "status": "cache",
        "id": "blockbeats",
        "updatedTime": int(time.time() * 1000),
        "items": items,
    }


BLOCKBEATS_KEYS = [
    ("custom.blockbeats", "primary"),
    ("custom.blockbeats-2", "secondary"),
    ("custom.blockbeats-3", "tertiary"),
]


def fetch_blockbeats():
    """Fetch BlockBeats Pro real-time newsflash. Tries the keys in order;
    on 429 (monthly quota exhausted) moves to the next key. Counts the call
    against the key that succeeded."""
    last_err = None
    for connector, label in BLOCKBEATS_KEYS:
        for attempt in (1, 2):
            try:
                data = _blockbeats_once(connector)
                month, counts = bump_blockbeats_usage(label)
                used = counts[label]
                others = ", ".join(f"{k} {v}" for k, v in counts.items() if k != label)
                log(f"blockbeats ({label}) usage this month ({month}): {used}/{BLOCKBEATS_MONTHLY_QUOTA} ({others})")
                if used >= BLOCKBEATS_WARN_AT:
                    log(f"BLOCKBEATS QUOTA WARNING: {label} {used}/{BLOCKBEATS_MONTHLY_QUOTA} used")
                return data
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    log(f"blockbeats: {label} quota exhausted (429), trying next key")
                    break
                if e.code in (401, 403):
                    log(f"blockbeats: {label} key rejected ({e.code}), trying next key")
                    break
                last_err = e
                log(f"blockbeats {label}: attempt {attempt} failed: {e}")
                time.sleep(3 if attempt == 1 else 0)
            except Exception as e:  # noqa: BLE001 - retry then report
                last_err = e
                log(f"blockbeats {label}: attempt {attempt} failed: {e}")
                time.sleep(3 if attempt == 1 else 0)
        else:
            raise RuntimeError(f"fetch failed after 2 attempts: {last_err}")
    raise RuntimeError("all blockbeats keys exhausted or rejected")


def fetch_linuxdo_rss(slug, cid, sid, size=30):
    """Fetch a Linux.do category RSS via r.jina.ai (Cloudflare blocks direct access).
    jina returns the RSS as markdown; parse the ### [title](url) headings and dates."""
    target = f"https://linux.do/c/{slug}/{cid}.rss"
    url = "https://r.jina.ai/" + target
    last_err = None
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=30) as resp:
                text = resp.read().decode("utf-8", "replace")
            marker = "Markdown Content:"
            if marker in text:
                text = text.split(marker, 1)[1]
            items = []
            for m in re.finditer(r"### \[(.*?)\]\((https://linux\.do/t/topic/\d+)\)", text):
                title = re.sub(r"\s+", " ", m.group(1)).strip()
                link = m.group(2)
                if not title or not link:
                    continue
                tail = text[m.end():m.end() + 400]
                dm = re.search(r"((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{2} \w{3} \d{4} \d{2}:\d{2}:\d{2} [+-]\d{4})", tail)
                pub_ms = None
                if dm:
                    pub_ms = _rss_pubdate_ms(dm.group(1))
                items.append({
                    "id": link,
                    "title": title,
                    "mobileUrl": link,
                    "url": link,
                    "pubDate": pub_ms,
                })
                if len(items) >= size:
                    break
            if not items:
                raise ValueError(f"{sid} linuxdo no usable items")
            return {
                "status": "cache",
                "id": sid,
                "updatedTime": int(time.time() * 1000),
                "items": items,
            }
        except Exception as e:  # noqa: BLE001 - retry then report
            last_err = e
            log(f"{sid}: attempt {attempt} failed: {e}")
            time.sleep(3 if attempt == 1 else 0)
    raise RuntimeError(f"fetch failed after 2 attempts: {last_err}")


def fetch_atom(url, sid, size=30):
    """Fetch an Atom feed and normalize to mirror format."""
    last_err = None
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
            root = ET.fromstring(raw)
            items = []
            for entry in root.iter():
                if entry.tag.rsplit("}", 1)[-1] != "entry":
                    continue
                title = None
                link = None
                pub = None
                for child in entry:
                    name = child.tag.rsplit("}", 1)[-1]
                    if name == "title" and child.text:
                        title = child.text.strip()
                    elif name == "link":
                        href = child.get("href")
                        rel = child.get("rel") or ""
                        if href and (link is None or rel == "alternate"):
                            link = href.strip()
                    elif name in ("published", "updated") and child.text:
                        if pub is None:
                            pub = child.text.strip()
                if not title or not link:
                    continue
                pub_ms = None
                if pub:
                    try:
                        dt = datetime.datetime.fromisoformat(pub.replace("Z", "+00:00"))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=datetime.timezone.utc)
                        pub_ms = int(dt.timestamp() * 1000)
                    except Exception:  # noqa: BLE001
                        pub_ms = None
                items.append({
                    "id": link,
                    "title": title,
                    "mobileUrl": link,
                    "url": link,
                    "pubDate": pub_ms,
                })
                if len(items) >= size:
                    break
            if not items:
                raise ValueError(f"{sid} atom no usable items")
            return {
                "status": "cache",
                "id": sid,
                "updatedTime": int(time.time() * 1000),
                "items": items,
            }
        except Exception as e:  # noqa: BLE001 - retry then report
            last_err = e
            log(f"{sid}: attempt {attempt} failed: {e}")
            time.sleep(3 if attempt == 1 else 0)
    raise RuntimeError(f"fetch failed after 2 attempts: {last_err}")


def fetch_linuxdo_resource():
    return fetch_linuxdo_rss("resource", 14, "linuxdo-resource", size=30)


def fetch_linuxdo_welfare():
    return fetch_linuxdo_rss("welfare", 36, "linuxdo-welfare", size=30)


def fetch_sspai():
    return fetch_rss("https://sspai.com/feed", "sspai", size=30)


def fetch_hn():
    return fetch_rss("https://news.ycombinator.com/rss", "hn", size=30)


def fetch_producthunt():
    return fetch_atom("https://www.producthunt.com/feed", "producthunt", size=30)


# --- Title translation (English -> Chinese) via AMD Token Factory ---
# Only for bwenews / cointelegraph / coindesk. (hn / producthunt were
# removed with their cards on 2026-11-10.)
# Translations are cached by md5 of the original title so a title is
# translated once and reused. If translation fails, the original English
# title is kept (feed still works).

# A cached translation is trusted forever (no TTL): a bad LLM translation
# stays until it is fixed by hand — delete that title's md5 entry from
# data/translation_cache.json and it will be re-translated next run.
TRANSLATION_CACHE_PATH = os.path.join(DATA_DIR, "translation_cache.json")
TRANSLATE_SOURCES = {"bwenews", "cointelegraph", "coindesk"}
AMD_MODELS = ["DeepSeek-V4-Flash", "DeepSeek-V4.1-Flash", "Qwen3.8-27B", "GLM-5.3-Flash"]


def load_translation_cache():
    try:
        with open(TRANSLATION_CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def save_translation_cache(cache):
    try:
        write_atomic(TRANSLATION_CACHE_PATH, json.dumps(cache, ensure_ascii=False).encode("utf-8"))
    except Exception as e:  # noqa: BLE001
        log(f"translation cache save fail: {e}")


# BlockBeats free quota: 10000 calls per calendar month (monthly reset).
# Count each successful newsflash call so we can warn before it runs out.
BLOCKBEATS_USAGE_PATH = os.path.join(DATA_DIR, "blockbeats_usage.json")
BLOCKBEATS_MONTHLY_QUOTA = 10000
BLOCKBEATS_WARN_AT = 9000


def bump_blockbeats_usage(key):
    """Count one successful BlockBeats API call against `key` ('primary',
    'secondary' or 'tertiary') for this calendar month (UTC).
    Returns (month, counts dict). Resets when the month changes.
    An old single-count file is read as primary."""
    month = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m")
    counts = {"primary": 0, "secondary": 0, "tertiary": 0}
    try:
        with open(BLOCKBEATS_USAGE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:  # noqa: BLE001
        data = {}
    if isinstance(data, dict) and data.get("month") == month:
        # Parse each key on its own: one corrupt value must not reset
        # the other two counters to zero.
        for k in counts:
            try:
                fallback = data.get("count", 0) if k == "primary" else 0
                counts[k] = int(data.get(k, fallback))
            except (TypeError, ValueError):
                counts[k] = 0
    if key in counts:
        counts[key] += 1
    try:
        write_atomic(BLOCKBEATS_USAGE_PATH, json.dumps({"month": month, **counts}, ensure_ascii=False).encode("utf-8"))
    except Exception as e:  # noqa: BLE001
        log(f"blockbeats usage save fail: {e}")
    return month, counts


def _amd_chat_translate_batch(titles):
    """Translate a list of headlines to Chinese, one LLM call per chunk of 8.
    Returns a dict {original: chinese} for successful ones."""
    result = {}
    for start in range(0, len(titles), 8):
        chunk = titles[start:start + 8]
        prompt = (
            "Translate each of the following English news headlines to Simplified Chinese. "
            "Return ONLY a JSON array of the Chinese translations, in the same order as the input, "
            "one per headline. Do not add any other text, explanation, or markdown.\n\n"
            + "\n".join(f"{i + 1}. {t}" for i, t in enumerate(chunk))
        )
        max_tokens = min(4096, max(1024, len(chunk) * 300))
        for model in AMD_MODELS:
            try:
                body = {
                    "model": model,
                    "max_tokens": max_tokens,
                    "messages": [{"role": "user", "content": prompt}],
                }
                data = json.dumps(body).encode()
                req = urllib.request.Request(
                    "https://developer.amd.com.cn/radeon/api/v1/chat/completions",
                    data=data, method="POST",
                )
                req.add_header("Content-Type", "application/json")
                add_surrogate_to_request(req, "custom.amd-radeon-cloud", allowed_hosts=["developer.amd.com.cn"])
                with urllib.request.urlopen(req, timeout=90) as resp:
                    raw = json.loads(resp.read().decode("utf-8", "replace"))
                content = raw["choices"][0]["message"]["content"]
                if not content:
                    continue
                content = content.strip()
                if content.startswith("```"):
                    content = re.sub(r"^```[a-zA-Z]*\s*", "", content)
                    content = re.sub(r"\s*```$", "", content)
                s = content.find("[")
                e = content.rfind("]")
                if s < 0 or e <= s:
                    continue
                arr = json.loads(content[s:e + 1])
                if isinstance(arr, list) and len(arr) == len(chunk):
                    for orig, zh in zip(chunk, arr):
                        if isinstance(zh, str) and zh.strip():
                            result[orig] = zh.strip()
                    break
            except Exception as ex:  # noqa: BLE001 - try next model
                log(f"translate batch {model} fail: {ex}")
                continue
    return result


def translate_items(items, cache):
    """Translate English titles to Chinese in place (batched, cached; falls back to original)."""
    to_translate = []
    for it in items:
        title = it.get("title") or ""
        if not title:
            continue
        if not re.search(r"[A-Za-z]", title):
            continue
        key = hashlib.md5(title.encode("utf-8")).hexdigest()[:16]
        zh = cache.get(key)
        if zh:
            if zh != title:
                it["orig_title"] = title
                it["title"] = zh
        else:
            to_translate.append(title)
    if not to_translate:
        return
    batch = _amd_chat_translate_batch(to_translate)
    for title, zh in batch.items():
        key = hashlib.md5(title.encode("utf-8")).hexdigest()[:16]
        cache[key] = zh
    for it in items:
        title = it.get("title") or ""
        zh = batch.get(title)
        if zh and zh != title:
            it["orig_title"] = title
            it["title"] = zh


# Custom sources fetched directly, not via NewsNow.
# Entry: (source id, local filename, fetch function)
CUSTOM_SOURCES = [
    ("odaily", "odaily.json", fetch_odaily),
    ("cointelegraph", "cointelegraph.json", fetch_cointelegraph),
    ("coindesk", "coindesk.json", fetch_coindesk),
    ("stocktwits", "stocktwits.json", fetch_stocktwits),
    ("yahoo-active", "yahoo-active.json", fetch_yahoo_active),
    ("bwenews", "bwenews.json", fetch_bwenews),
    ("blockbeats", "blockbeats.json", fetch_blockbeats),
    ("linuxdo-resource", "linuxdo-resource.json", fetch_linuxdo_resource),
    ("linuxdo-welfare", "linuxdo-welfare.json", fetch_linuxdo_welfare),
    # 2026-11-10 用户删卡: sspai / hn / producthunt 已下线 (内容不行)。
    # 抓取函数仍在 (fetch_sspai / fetch_hn / fetch_producthunt), 想加回
    # 时把下面三行取消注释即可。
    # ("sspai", "sspai.json", fetch_sspai),
    # ("hn", "hn.json", fetch_hn),
    # ("producthunt", "producthunt.json", fetch_producthunt),
]


def write_atomic(path, blob):
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "wb") as f:
        f.write(blob)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def gh_call(method, path, body=None):

    url = "https://api.github.com" + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if body is not None:
        req.add_header("Content-Type", "application/json")
    add_surrogate_to_request(req, "custom.github", allowed_hosts=["api.github.com"])
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            txt = resp.read().decode()
            return resp.status, json.loads(txt) if txt else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")[:500]
        try:
            return e.code, json.loads(raw or "{}")
        except Exception:  # noqa: BLE001
            return e.code, {"message": raw}


def _remote_blob(rel_path):
    """GET 远端文件, 返回 (sha, blob)。不存在返回 (None, None), 其他错误抛错。"""
    status, remote = gh_call(
        "GET", f"/repos/{GH_OWNER}/{GH_REPO}/contents/{rel_path}?ref={GH_BRANCH}"
    )
    if status == 404:
        return None, None
    if status != 200:
        raise RuntimeError(f"GET remote failed: HTTP {status} {remote}")
    try:
        blob = base64.b64decode(remote.get("content", ""))
    except Exception:  # noqa: BLE001
        blob = None
    return remote.get("sha"), blob


def sync_file(rel_path, blob):
    """把本地文件同步到 GitHub, 远端内容相同就跳过。返回动作描述。
    PUT 撞上 409 (远端在 GET 与 PUT 之间变了, 或远端读到旧版本) 时,
    重读一次远端再写一次, 再失败才报错、留待下轮重试。"""
    sha, remote_blob = _remote_blob(rel_path)
    if remote_blob == blob:
        return "github unchanged"
    body = {
        "message": f"mirror update: {rel_path}",
        "content": base64.b64encode(blob).decode(),
        "branch": GH_BRANCH,
    }
    if sha:
        body["sha"] = sha
    status, res = gh_call(
        "PUT", f"/repos/{GH_OWNER}/{GH_REPO}/contents/{rel_path}", body
    )
    if status == 409:
        # 远端在 GET 与 PUT 之间变了: 重读一次, 内容已相同就跳过,
        # 否则拿新 sha 再写一次。
        log(f"{rel_path}: PUT 409, re-read remote and retry once")
        sha, remote_blob = _remote_blob(rel_path)
        if remote_blob == blob:
            return "github unchanged"
        if sha:
            body["sha"] = sha
        else:
            body.pop("sha", None)
        status, res = gh_call(
            "PUT", f"/repos/{GH_OWNER}/{GH_REPO}/contents/{rel_path}", body
        )
    if status not in (200, 201):
        raise RuntimeError(f"PUT failed: HTTP {status} {res}")
    return "github updated"


def _items_key(data):
    """Deterministic key for a source's items, ignoring updatedTime.

    updatedTime is stamped with the fetch clock, so comparing the whole
    blob would mark every run as changed even when nothing new arrived.
    Comparing only the items makes the "unchanged" path actually trigger,
    so an unchanged file is not rewritten and not re-PUT to GitHub."""
    items = data.get("items", []) if isinstance(data, dict) else []
    return hashlib.md5(
        json.dumps(items, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


def _write_source(sid, fname, data):
    """Write one source file if its items changed. Returns True if written.

    Compares only the items (see _items_key). A write failure keeps the
    old file and returns False; an unreadable old file is rewritten."""
    path = os.path.join(DATA_DIR, fname)
    new_key = _items_key(data)
    if os.path.exists(path):
        try:
            with open(path, "rb") as f:
                old = json.loads(f.read().decode("utf-8", "replace"))
            if _items_key(old) == new_key:
                log(f"{sid}: unchanged ({len(data['items'])} items, updatedTime={old.get('updatedTime')})")
                return False
        except Exception:  # noqa: BLE001 - unreadable old file: rewrite it
            pass
    blob = (
        json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        + b"\n"
    )
    try:
        write_atomic(path, blob)
    except Exception as e:  # noqa: BLE001
        log(f"{sid}: WRITE FAIL {e}; keep previous file")
        return False
    log(f"{sid}: local updated ({len(data['items'])} items, updatedTime={data.get('updatedTime')})")
    return True


def main(group="all"):
    """Fetch one group and sync it. group: 'all' (every source, the old
    behaviour), 'free' (all sources except blockbeats, no API quota) or
    'blockbeats' (only blockbeats). Each group takes its own lock file so the
    2-minute free cron and the 3-minute blockbeats cron can run concurrently;
    the two groups touch disjoint state (translation cache vs usage counter)
    and disjoint files. "all" touches both groups, so it holds both group
    locks (free first, then blockbeats — always this order, so two "all"
    runs can never deadlock). The sync step pushes every file of this group:
    sync_file compares against the remote and skips identical files, so a
    failed sync is retried on the next run instead of being forgotten."""
    lock_paths = (
        [os.path.join(BASE_DIR, ".fetch-free.lock"), os.path.join(BASE_DIR, ".fetch-blockbeats.lock")]
        if group == "all"
        else [os.path.join(BASE_DIR, f".fetch-{group}.lock")]
    )
    with contextlib.ExitStack() as stack:
        for lp in lock_paths:
            lf = stack.enter_context(open(lp, "w"))
            try:
                fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                log("previous run still holding lock, exit")
                return 0

        # Only the free/all group uses translation; the blockbeats group must
        # not load or save the cache or it could clobber the free group's
        # concurrent updates with a stale copy.
        tcache = load_translation_cache() if group in ("all", "free") else {}

        if group in ("all", "free"):
            run_sources = SOURCES
        else:
            run_sources = []

        for sid, fname in run_sources:
            try:
                data = fetch_one(sid)
            except Exception as e:  # noqa: BLE001 - keep old file, next platform
                log(f"{sid}: FETCH FAIL {e}; keep previous file")
                time.sleep(0.2)
                continue
            _write_source(sid, fname, data)
            time.sleep(0.2)

        if group == "all":
            run_custom = list(CUSTOM_SOURCES)
        elif group == "free":
            run_custom = [(sid, fname, fn) for sid, fname, fn in CUSTOM_SOURCES if sid != "blockbeats"]
        elif group == "blockbeats":
            run_custom = [(sid, fname, fn) for sid, fname, fn in CUSTOM_SOURCES if sid == "blockbeats"]
        else:
            run_custom = []
        for sid, fname, fetch_fn in run_custom:
            try:
                data = fetch_fn()
            except Exception as e:  # noqa: BLE001 - keep old file, next source
                log(f"{sid}: FETCH FAIL {e}; keep previous file")
                time.sleep(0.2)
                continue
            if sid in TRANSLATE_SOURCES:
                try:
                    translate_items(data.get("items", []), tcache)
                except Exception as e:  # noqa: BLE001 - translation must never abort a run
                    log(f"{sid}: TRANSLATE FAIL {e}; keep English titles")
            _write_source(sid, fname, data)
            time.sleep(0.2)

        # 同步本组的全部文件。sync_file 会先比对远端、相同就跳过,
        # 所以失败的同步下轮会重试, 而不是只同步本轮写过的文件就丢下它。
        sync_files = [fname for _, fname in run_sources] + [fname for _, fname, _ in run_custom]
        for fname in sync_files:
            path = os.path.join(DATA_DIR, fname)
            if not os.path.exists(path):
                continue
            with open(path, "rb") as f:
                blob = f.read()
            try:
                res = sync_file(f"data/{fname}", blob)
                log(f"{fname}: {res}")
            except Exception as e:  # noqa: BLE001
                log(f"{fname}: GITHUB SYNC FAIL {e}; local kept, retry next run")
        if group in ("all", "free"):
            save_translation_cache(tcache)
        log("done")
        return 0


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NewsNow mirror fetcher")
    parser.add_argument(
        "--group",
        choices=["all", "free", "blockbeats"],
        default="all",
        help="which sources to fetch: all (default), free (all except blockbeats), blockbeats (only blockbeats)",
    )
    args = parser.parse_args()
    sys.exit(main(args.group))
