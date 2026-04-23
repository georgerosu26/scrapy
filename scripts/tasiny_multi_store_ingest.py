import argparse
import json
import os
import re
import sys
from datetime import datetime
from typing import Any
from urllib.request import Request, urlopen


def _clean(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _to_date(value: Any) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    m = re.match(r"^\d{4}-\d{2}-\d{2}", raw)
    if m:
        return m.group(0)
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return ""


def _fetch_text(url: str, timeout: int = 30) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "TasinyEventBot/1.0 (+https://tasiny.app)",
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        },
    )
    with urlopen(req, timeout=timeout) as res:
        return res.read().decode("utf-8", errors="replace")


def _is_event_node(node: Any) -> bool:
    if not isinstance(node, dict):
        return False
    t = node.get("@type")
    if isinstance(t, list):
        return any(str(v).lower() == "event" for v in t)
    return str(t).lower() == "event"


def _extract_event_nodes_from_jsonld(text: str) -> list[dict]:
    scripts = re.findall(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    out: list[dict] = []
    for script in scripts:
        raw = script.strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue

        nodes = payload if isinstance(payload, list) else [payload]
        for node in nodes:
            if isinstance(node, dict) and isinstance(node.get("@graph"), list):
                for g in node["@graph"]:
                    if _is_event_node(g):
                        out.append(g)
                continue
            if _is_event_node(node):
                out.append(node)
    return out


def _normalize_event(node: dict, source_url: str, source_name: str, source_type: str) -> dict | None:
    title = _clean(node.get("name"))
    if not title:
        return None
    start_date = _to_date(node.get("startDate"))
    if not start_date:
        return None
    end_date = _to_date(node.get("endDate")) or start_date

    location = node.get("location") or {}
    if isinstance(location, list):
        location = location[0] if location else {}
    venue = ""
    lat = None
    lon = None
    if isinstance(location, dict):
        venue = _clean(location.get("name"))
        geo = location.get("geo") or {}
        if isinstance(geo, dict):
            try:
                lat = float(geo.get("latitude"))
                lon = float(geo.get("longitude"))
            except Exception:
                lat = None
                lon = None

    event: dict[str, Any] = {
        "title": title,
        "venue": venue or None,
        "start_date": start_date,
        "end_date": end_date,
        "source_url": source_url,
        "source_name": source_name,
        "source_type": source_type,
        "confidence": 75,
        "event_type": "other",
        "raw_payload": node,
    }
    if lat is not None and lon is not None:
        event["latitude"] = lat
        event["longitude"] = lon
    return event


def crawl_sources_for_store(store: dict) -> list[dict]:
    events: list[dict] = []
    for src in store.get("sources", []):
        url = _clean(src.get("url"))
        if not url:
            continue
        source_name = _clean(src.get("source_name")) or "scrape_source"
        source_type = _clean(src.get("source_type")) or "scrape"
        try:
            html = _fetch_text(url)
            nodes = _extract_event_nodes_from_jsonld(html)
            for n in nodes:
                ev = _normalize_event(n, url, source_name, source_type)
                if ev:
                    events.append(ev)
        except Exception as exc:
            print(f"[WARN] source failed: {url} -> {exc}")
            continue
    return events


def post_ingestion(function_url: str, ingest_secret: str, store_id: str, events: list[dict], bearer_token: str = "") -> dict:
    payload = {
        "storeId": store_id,
        "events": events,
        "upsertDemandEvent": False,
        "maxDistanceKm": 10,
    }
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "x-ingest-secret": ingest_secret,
    }
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    req = Request(function_url, data=data, method="POST", headers=headers)
    with urlopen(req, timeout=60) as res:
        text = res.read().decode("utf-8", errors="replace")
        try:
            return json.loads(text)
        except Exception:
            return {"raw_response": text}


def load_stores_config(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and isinstance(data.get("stores"), list):
        return data["stores"]
    if isinstance(data, list):
        return data
    raise ValueError("Invalid config format. Expected {'stores': [...]} or [...].")


def main() -> int:
    parser = argparse.ArgumentParser(description="Tasiny multi-store event ingestion runner")
    parser.add_argument("--stores-file", default=os.getenv("TASINY_STORES_FILE", "stores.json"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    function_url = _clean(os.getenv("TASINY_FUNCTION_URL"))
    ingest_secret = _clean(os.getenv("TASINY_INGEST_SECRET"))
    bearer_token = _clean(os.getenv("TASINY_BEARER_TOKEN"))

    if not function_url:
        print("Missing TASINY_FUNCTION_URL")
        return 1
    if not ingest_secret:
        print("Missing TASINY_INGEST_SECRET")
        return 1

    stores = load_stores_config(args.stores_file)
    if not stores:
        print("No stores in config")
        return 1

    grand_total = 0
    for store in stores:
        store_id = _clean(store.get("storeId"))
        if not store_id:
            print("[WARN] Skipping store without storeId")
            continue

        events = crawl_sources_for_store(store)
        grand_total += len(events)
        print(f"[INFO] {store_id}: crawled {len(events)} candidate events")

        if args.dry_run:
            continue

        result = post_ingestion(function_url, ingest_secret, store_id, events, bearer_token)
        print(f"[INFO] {store_id}: ingestion response -> {json.dumps(result)}")

    print(f"[DONE] total crawled events across stores: {grand_total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
