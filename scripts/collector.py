import os, json, re, csv
from pathlib import Path
from datetime import datetime, timezone, timedelta
from telethon import TelegramClient
from telethon.sessions import StringSession

ROOT = Path(__file__).resolve().parents[1]
STORE_PATH = ROOT / "store.json"
STATE_PATH = ROOT / "state.json"

def normalize_channel(s: str) -> str:
    s = s.strip()
    if "t.me/" in s:
        s = "@" + s.split("t.me/")[-1].strip("/").split("?")[0]
    return s

def load_channels_csv(default_limit: int) -> list[tuple[str, int]]:
    p = ROOT / "channels.csv"
    rows: list[tuple[str, int]] = []
    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            first = row[0].strip().lower()
            if i == 0 and first in ("channel", "address", "url"):
                continue
            raw = row[0].strip()
            if not raw or raw.startswith("#"):
                continue
            ch = normalize_channel(raw)
            limit = default_limit
            if len(row) > 1 and row[1].strip():
                try:
                    limit = int(row[1].strip())
                except ValueError:
                    limit = default_limit
            rows.append((ch, limit))
    return rows

def load_patterns():
    data = json.loads((ROOT / "patterns.json").read_text(encoding="utf-8"))
    compiled = []
    for it in data.get("items", []):
        name = it["name"]
        rx = re.compile(it["regex"], re.IGNORECASE)
        compiled.append((name, rx))
    return compiled

def load_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))

def save_json(path: Path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def parse_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def write_output(name: str, items: list[str]) -> None:
    (ROOT / f"{name}.txt").write_text("\n".join(items) + ("\n" if items else ""), encoding="utf-8")

def prune_store(store: dict, cutoff: datetime) -> dict:
    new_store = {}
    for name, mapping in store.items():
        kept = {}
        for val, meta in mapping.items():
            try:
                seen = parse_dt(meta["seen_at"])
            except Exception:
                continue
            if seen >= cutoff:
                kept[val] = meta
        new_store[name] = kept
    return new_store

async def main():
    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]
    session_str = os.environ["TG_SESSION"]

    default_limit = int(os.environ.get("DEFAULT_LIMIT_PER_CHANNEL", "200"))
    ttl_hours = int(os.environ.get("ITEM_TTL_HOURS", "50"))

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=ttl_hours)

    channels = load_channels_csv(default_limit=default_limit)
    patterns = load_patterns()

    store = load_json(STORE_PATH, default={})
    state = load_json(STATE_PATH, default={})

    for name, _ in patterns:
        store.setdefault(name, {})

    async with TelegramClient(
        StringSession(session_str),
        api_id,
        api_hash,
        timeout=30,
        connection_retries=10,
        retry_delay=2,
    ) as client:
        for ch, per_limit in channels:
            last_id = int(state.get(ch, 0))
            max_id = last_id

            try:
                async for msg in client.iter_messages(ch, limit=per_limit):
                    if not msg.message:
                        continue
                    if msg.id <= last_id:
                        break

                    if msg.id > max_id:
                        max_id = msg.id

                    msg_dt = msg.date
                    if msg_dt.tzinfo is None:
                        msg_dt = msg_dt.replace(tzinfo=timezone.utc)
                    else:
                        msg_dt = msg_dt.astimezone(timezone.utc)

                    # Only keep items whose source message is within the TTL window
                    if msg_dt < cutoff:
                        continue

                    text = msg.message
                    for name, rx in patterns:
                        for m in rx.findall(text):
                            if isinstance(m, tuple):
                                m = m[0]
                            m = str(m).strip()
                            if not m:
                                continue
                            if m not in store[name]:
                                store[name][m] = {"seen_at": iso_z(msg_dt)}

                if max_id > last_id:
                    state[ch] = max_id

            except Exception as e:
                print(f"[WARN] channel={ch} error={e}")

    store = prune_store(store, cutoff=cutoff)

    for name, mapping in store.items():
        items_sorted = sorted(mapping.items(), key=lambda kv: parse_dt(kv[1]["seen_at"]), reverse=True)
        write_output(name, [val for val, _meta in items_sorted])

    save_json(STORE_PATH, store)
    save_json(STATE_PATH, state)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
