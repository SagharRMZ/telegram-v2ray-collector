import os
import json
import re
import csv
import asyncio
from pathlib import Path
from datetime import datetime, timezone, timedelta

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.errors.rpcerrorlist import UsernameInvalidError, UsernameNotOccupiedError
from telethon.tl.types import MessageEntityTextUrl


ROOT = Path(__file__).resolve().parents[1]
STORE_PATH = ROOT / "store.json"
STATE_PATH = ROOT / "state.json"


def normalize_channel(s: str) -> str:
    """
    Accepts:
      - https://t.me/SomeChannel
      - t.me/SomeChannel?start=...
      - @SomeChannel
      - SomeChannel
    Returns a normalized "@username" when possible.
    """
    s = (s or "").strip()
    if not s:
        return s

    if "t.me/" in s:
        s = s.split("t.me/")[-1]

    s = s.split("?")[0].strip().strip("/")

    if not s:
        return s

    if not s.startswith("@"):
        s = "@" + s

    return s


def load_channels_csv(default_limit: int) -> list[tuple[str, int]]:
    p = ROOT / "channels.csv"
    rows: list[tuple[str, int]] = []

    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue

            first = (row[0] or "").strip().lower()
            if i == 0 and first in ("channel", "address", "url"):
                continue

            raw = (row[0] or "").strip()
            if not raw or raw.startswith("#"):
                continue

            ch = normalize_channel(raw)
            limit = default_limit

            if len(row) > 1 and (row[1] or "").strip():
                try:
                    limit = int(row[1].strip())
                except ValueError:
                    limit = default_limit

            rows.append((ch, limit))

    return rows


def load_patterns() -> list[tuple[str, re.Pattern]]:
    data = json.loads((ROOT / "patterns.json").read_text(encoding="utf-8"))
    compiled: list[tuple[str, re.Pattern]] = []

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
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def write_output(name: str, items: list[str]) -> None:
    (ROOT / f"{name}.txt").write_text(
        "\n".join(items) + ("\n" if items else ""), encoding="utf-8"
    )


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


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def extract_candidate_texts(msg) -> list[str]:
    """
    Build a list of candidate strings to run regex on:
      - raw message text
      - entity URLs (MessageEntityTextUrl)
      - inline button URLs
    This prevents missing URLs that aren't in msg.message.
    """
    out: list[str] = []

    if getattr(msg, "message", None):
        out.append(msg.message)

    ents = getattr(msg, "entities", None)
    if ents:
        for e in ents:
            if isinstance(e, MessageEntityTextUrl) and getattr(e, "url", None):
                out.append(e.url)

    buttons = getattr(msg, "buttons", None)
    if buttons:
        for row in buttons:
            for b in row:
                url = getattr(b, "url", None)
                if url:
                    out.append(url)

    return out


def upsert_seen(store_bucket: dict, key: str, msg_dt: datetime) -> bool:
    """
    Insert new item or update 'seen_at' if this msg_dt is newer.
    Returns True if store changed.
    """
    prev = store_bucket.get(key)
    if prev is None:
        store_bucket[key] = {"seen_at": iso_z(msg_dt)}
        return True

    try:
        prev_dt = parse_dt(prev.get("seen_at", "1970-01-01T00:00:00Z"))
    except Exception:
        prev_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)

    if msg_dt > prev_dt:
        store_bucket[key] = {"seen_at": iso_z(msg_dt)}
        return True

    return False


async def safe_get_entity(client: TelegramClient, ch: str, entity_cache: dict) -> object | None:
    """
    Resolve channel username to entity once and cache it, to reduce ResolveUsernameRequest.
    """
    if ch in entity_cache:
        return entity_cache[ch]

    try:
        ent = await client.get_entity(ch)
        entity_cache[ch] = ent
        return ent
    except (UsernameInvalidError, UsernameNotOccupiedError) as e:
        print(f"[WARN] channel={ch} invalid/occupied: {e}")
        entity_cache[ch] = None
        return None
    except FloodWaitError as e:
        print(f"[WARN] FloodWait on get_entity({ch}): sleeping {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
        try:
            ent = await client.get_entity(ch)
            entity_cache[ch] = ent
            return ent
        except Exception as e2:
            print(f"[WARN] channel={ch} get_entity failed after wait: {e2}")
            entity_cache[ch] = None
            return None
    except Exception as e:
        print(f"[WARN] channel={ch} get_entity error: {e}")
        entity_cache[ch] = None
        return None


async def main():
    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]
    session_str = os.environ["TG_SESSION"]

    default_limit = int(os.environ.get("DEFAULT_LIMIT_PER_CHANNEL", "200"))
    ttl_hours = int(os.environ.get("ITEM_TTL_HOURS", "50"))

    # Rate-limit controls (tunable)
    sleep_between_channels = float(os.environ.get("SLEEP_BETWEEN_CHANNELS_SEC", "1.5"))
    sleep_between_messages = float(os.environ.get("SLEEP_BETWEEN_MESSAGES_SEC", "0.0"))
    max_channels = int(os.environ.get("MAX_CHANNELS_PER_RUN", "0"))  # 0 = unlimited

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=ttl_hours)

    channels = load_channels_csv(default_limit=default_limit)
    patterns = load_patterns()

    store = load_json(STORE_PATH, default={})
    state = load_json(STATE_PATH, default={})

    for name, _ in patterns:
        store.setdefault(name, {})

    entity_cache: dict[str, object | None] = {}

    async with TelegramClient(
        StringSession(session_str),
        api_id,
        api_hash,
        timeout=30,
        connection_retries=10,
        retry_delay=2,
    ) as client:
        if max_channels > 0:
            channels = channels[:max_channels]

        for ch, per_limit in channels:
            last_id = int(state.get(ch, 0))
            max_id = last_id

            ent = await safe_get_entity(client, ch, entity_cache)
            if ent is None:
                continue

            try:
                async for msg in client.iter_messages(ent, limit=per_limit):
                    if not getattr(msg, "message", None) and not getattr(msg, "entities", None) and not getattr(msg, "buttons", None):
                        continue

                    # Stop once we reach older messages already processed
                    if msg.id <= last_id:
                        break

                    if msg.id > max_id:
                        max_id = msg.id

                    msg_dt = _as_utc(msg.date)

                    # Keep only items whose source message is within TTL window
                    if msg_dt < cutoff:
                        continue

                    # Gather candidate strings (text + entity URLs + button URLs)
                    candidates = extract_candidate_texts(msg)

                    for candidate in candidates:
                        for name, rx in patterns:
                            # Use finditer to reliably get the full match (group(0))
                            for mat in rx.finditer(candidate):
                                m = mat.group(0).strip()
                                if not m:
                                    continue
                                upsert_seen(store[name], m, msg_dt)

                    # optional small pause to reduce rate-limits
                    if sleep_between_messages > 0:
                        await asyncio.sleep(sleep_between_messages)

                if max_id > last_id:
                    state[ch] = max_id

            except FloodWaitError as e:
                print(f"[WARN] FloodWait while reading {ch}: sleeping {e.seconds}s")
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                print(f"[WARN] channel={ch} error={e}")

            if sleep_between_channels > 0:
                await asyncio.sleep(sleep_between_channels)

    store = prune_store(store, cutoff=cutoff)

    for name, mapping in store.items():
        items_sorted = sorted(
            mapping.items(),
            key=lambda kv: parse_dt(kv[1]["seen_at"]),
            reverse=True,
        )
        write_output(name, [val for val, _meta in items_sorted])

    save_json(STORE_PATH, store)
    save_json(STATE_PATH, state)


if __name__ == "__main__":
    asyncio.run(main())    s = s.split("?")[0].strip().strip("/")

    if not s:
        return s

    if not s.startswith("@"):
        s = "@" + s

    return s


def load_channels_csv(default_limit: int) -> list[tuple[str, int]]:
    p = ROOT / "channels.csv"
    rows: list[tuple[str, int]] = []

    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue

            first = (row[0] or "").strip().lower()
            if i == 0 and first in ("channel", "address", "url"):
                continue

            raw = (row[0] or "").strip()
            if not raw or raw.startswith("#"):
                continue

            ch = normalize_channel(raw)
            limit = default_limit

            if len(row) > 1 and (row[1] or "").strip():
                try:
                    limit = int(row[1].strip())
                except ValueError:
                    limit = default_limit

            rows.append((ch, limit))

    return rows


def load_patterns() -> list[tuple[str, re.Pattern]]:
    data = json.loads((ROOT / "patterns.json").read_text(encoding="utf-8"))
    compiled: list[tuple[str, re.Pattern]] = []

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
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def write_output(name: str, items: list[str]) -> None:
    (ROOT / f"{name}.txt").write_text(
        "\n".join(items) + ("\n" if items else ""), encoding="utf-8"
    )


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


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def extract_candidate_texts(msg) -> list[str]:
    """
    Build a list of candidate strings to run regex on:
      - raw message text
      - entity URLs (MessageEntityTextUrl)
      - inline button URLs
    This prevents missing URLs that aren't in msg.message.
    """
    out: list[str] = []

    if getattr(msg, "message", None):
        out.append(msg.message)

    ents = getattr(msg, "entities", None)
    if ents:
        for e in ents:
            if isinstance(e, MessageEntityTextUrl) and getattr(e, "url", None):
                out.append(e.url)

    buttons = getattr(msg, "buttons", None)
    if buttons:
        for row in buttons:
            for b in row:
                url = getattr(b, "url", None)
                if url:
                    out.append(url)

    return out


def upsert_seen(store_bucket: dict, key: str, msg_dt: datetime) -> bool:
    """
    Insert new item or update 'seen_at' if this msg_dt is newer.
    Returns True if store changed.
    """
    prev = store_bucket.get(key)
    if prev is None:
        store_bucket[key] = {"seen_at": iso_z(msg_dt)}
        return True

    try:
        prev_dt = parse_dt(prev.get("seen_at", "1970-01-01T00:00:00Z"))
    except Exception:
        prev_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)

    if msg_dt > prev_dt:
        store_bucket[key] = {"seen_at": iso_z(msg_dt)}
        return True

    return False


async def safe_get_entity(client: TelegramClient, ch: str, entity_cache: dict) -> object | None:
    """
    Resolve channel username to entity once and cache it, to reduce ResolveUsernameRequest.
    """
    if ch in entity_cache:
        return entity_cache[ch]

    try:
        ent = await client.get_entity(ch)
        entity_cache[ch] = ent
        return ent
    except (UsernameInvalidError, UsernameNotOccupiedError) as e:
        print(f"[WARN] channel={ch} invalid/occupied: {e}")
        entity_cache[ch] = None
        return None
    except FloodWaitError as e:
        print(f"[WARN] FloodWait on get_entity({ch}): sleeping {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
        try:
            ent = await client.get_entity(ch)
            entity_cache[ch] = ent
            return ent
        except Exception as e2:
            print(f"[WARN] channel={ch} get_entity failed after wait: {e2}")
            entity_cache[ch] = None
            return None
    except Exception as e:
        print(f"[WARN] channel={ch} get_entity error: {e}")
        entity_cache[ch] = None
        return None


async def main():
    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]
    session_str = os.environ["TG_SESSION"]

    default_limit = int(os.environ.get("DEFAULT_LIMIT_PER_CHANNEL", "200"))
    ttl_hours = int(os.environ.get("ITEM_TTL_HOURS", "50"))

    # Rate-limit controls (tunable)
    sleep_between_channels = float(os.environ.get("SLEEP_BETWEEN_CHANNELS_SEC", "1.5"))
    sleep_between_messages = float(os.environ.get("SLEEP_BETWEEN_MESSAGES_SEC", "0.0"))
    max_channels = int(os.environ.get("MAX_CHANNELS_PER_RUN", "0"))  # 0 = unlimited

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=ttl_hours)

    channels = load_channels_csv(default_limit=default_limit)
    patterns = load_patterns()

    store = load_json(STORE_PATH, default={})
    state = load_json(STATE_PATH, default={})

    for name, _ in patterns:
        store.setdefault(name, {})

    entity_cache: dict[str, object | None] = {}

    async with TelegramClient(
        StringSession(session_str),
        api_id,
        api_hash,
        timeout=30,
        connection_retries=10,
        retry_delay=2,
    ) as client:
        if max_channels > 0:
            channels = channels[:max_channels]

        for ch, per_limit in channels:
            last_id = int(state.get(ch, 0))
            max_id = last_id

            ent = await safe_get_entity(client, ch, entity_cache)
            if ent is None:
                continue

            try:
                async for msg in client.iter_messages(ent, limit=per_limit):
                    if not getattr(msg, "message", None) and not getattr(msg, "entities", None) and not getattr(msg, "buttons", None):
                        continue

                    # Stop once we reach older messages already processed
                    if msg.id <= last_id:
                        break

                    if msg.id > max_id:
                        max_id = msg.id

                    msg_dt = _as_utc(msg.date)

                    # Keep only items whose source message is within TTL window
                    if msg_dt < cutoff:
                        continue

                    # Gather candidate strings (text + entity URLs + button URLs)
                    candidates = extract_candidate_texts(msg)

                    for candidate in candidates:
                        for name, rx in patterns:
                            # Use finditer to reliably get the full match (group(0))
                            for mat in rx.finditer(candidate):
                                m = mat.group(0).strip()
                                if not m:
                                    continue
                                upsert_seen(store[name], m, msg_dt)

                    # optional small pause to reduce rate-limits
                    if sleep_between_messages > 0:
                        await asyncio.sleep(sleep_between_messages)

                if max_id > last_id:
                    state[ch] = max_id

            except FloodWaitError as e:
                print(f"[WARN] FloodWait while reading {ch}: sleeping {e.seconds}s")
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                print(f"[WARN] channel={ch} error={e}")

            if sleep_between_channels > 0:
                await asyncio.sleep(sleep_between_channels)

    store = prune_store(store, cutoff=cutoff)

    for name, mapping in store.items():
        items_sorted = sorted(
            mapping.items(),
            key=lambda kv: parse_dt(kv[1]["seen_at"]),
            reverse=True,
        )
        write_output(name, [val for val, _meta in items_sorted])

    save_json(STORE_PATH, store)
    save_json(STATE_PATH, state)


if __name__ == "__main__":
    asyncio.run(main())    if "t.me/" in s:
        s = s.split("t.me/")[-1]
    s = s.split("?")[0].strip().strip("/")

    if not s:
        return s

    if not s.startswith("@"):
        s = "@" + s

    return s


def load_channels_csv(default_limit: int) -> list[tuple[str, int]]:
    p = ROOT / "channels.csv"
    rows: list[tuple[str, int]] = []

    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue

            first = (row[0] or "").strip().lower()
            if i == 0 and first in ("channel", "address", "url"):
                continue

            raw = (row[0] or "").strip()
            if not raw or raw.startswith("#"):
                continue

            ch = normalize_channel(raw)
            limit = default_limit

            if len(row) > 1 and (row[1] or "").strip():
                try:
                    limit = int(row[1].strip())
                except ValueError:
                    limit = default_limit

            rows.append((ch, limit))

    return rows


def load_patterns() -> list[tuple[str, re.Pattern]]:
    data = json.loads((ROOT / "patterns.json").read_text(encoding="utf-8"))
    compiled: list[tuple[str, re.Pattern]] = []

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


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)




def extract_candidate_texts(msg) -> list[str]:
    """Collect all text/URLs we might want to regex-scan from a Telegram message.

    - msg.message: plain text
    - MessageEntityTextUrl: URLs attached to clickable text (URL may not appear in msg.message)
    - msg.buttons / reply_markup: URLs behind inline keyboard buttons
    """
    out: list[str] = []

    if getattr(msg, "message", None):
        out.append(msg.message)

    # URLs attached to entities (e.g. 'click here' with an underlying URL)
    for e in (getattr(msg, "entities", None) or []):
        if isinstance(e, MessageEntityTextUrl) and getattr(e, "url", None):
            out.append(e.url)

    # Inline keyboard buttons with URL
    for row in (getattr(msg, "buttons", None) or []):
        for b in row:
            url = getattr(b, "url", None)
            if url:
                out.append(url)

    return out


async def safe_get_entity(client: TelegramClient, ch: str, entity_cache: dict) -> object | None:
    """
    Resolve channel username to entity once and cache it, to reduce ResolveUsernameRequest.
    """
    if ch in entity_cache:
        return entity_cache[ch]

    try:
        ent = await client.get_entity(ch)
        entity_cache[ch] = ent
        return ent
    except (UsernameInvalidError, UsernameNotOccupiedError) as e:
        print(f"[WARN] channel={ch} invalid/occupied: {e}")
        entity_cache[ch] = None
        return None
    except FloodWaitError as e:
        print(f"[WARN] FloodWait on get_entity({ch}): sleeping {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
        # retry once after sleeping
        try:
            ent = await client.get_entity(ch)
            entity_cache[ch] = ent
            return ent
        except Exception as e2:
            print(f"[WARN] channel={ch} get_entity failed after wait: {e2}")
            entity_cache[ch] = None
            return None
    except Exception as e:
        print(f"[WARN] channel={ch} get_entity error: {e}")
        entity_cache[ch] = None
        return None


async def main():
    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]
    session_str = os.environ["TG_SESSION"]

    default_limit = int(os.environ.get("DEFAULT_LIMIT_PER_CHANNEL", "200"))
    ttl_hours = int(os.environ.get("ITEM_TTL_HOURS", "50"))

    # Rate-limit controls (tunable)
    sleep_between_channels = float(os.environ.get("SLEEP_BETWEEN_CHANNELS_SEC", "1.5"))
    sleep_between_messages = float(os.environ.get("SLEEP_BETWEEN_MESSAGES_SEC", "0.0"))
    max_channels = int(os.environ.get("MAX_CHANNELS_PER_RUN", "0"))  # 0 = unlimited

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=ttl_hours)

    channels = load_channels_csv(default_limit=default_limit)
    patterns = load_patterns()

    store = load_json(STORE_PATH, default={})
    state = load_json(STATE_PATH, default={})

    for name, _ in patterns:
        store.setdefault(name, {})

    entity_cache: dict[str, object | None] = {}

    async with TelegramClient(
        StringSession(session_str),
        api_id,
        api_hash,
        timeout=30,
        connection_retries=10,
        retry_delay=2,
    ) as client:
        if max_channels > 0:
            channels = channels[:max_channels]

        for ch, per_limit in channels:
            last_id = int(state.get(ch, 0))
            max_id = last_id

            ent = await safe_get_entity(client, ch, entity_cache)
            if ent is None:
                continue

            try:
                async for msg in client.iter_messages(ent, limit=per_limit):
                    # Some messages have URLs only in entities/buttons (not in msg.message)
                    candidates = extract_candidate_texts(msg)
                    if not candidates:
                        continue

                    # Stop once we reach older messages already processed
                    if msg.id <= last_id:
                        break

                    if msg.id > max_id:
                        max_id = msg.id

                    msg_dt = _as_utc(msg.date)

                    # Keep only items whose source message is within TTL window
                    if msg_dt < cutoff:
                        continue

                    for candidate in candidates:
                        for name, rx in patterns:
                            for mat in rx.finditer(candidate):
                                m = mat.group(0).strip()
                                if not m:
                                    continue

                                prev = store[name].get(m)
                                # Always keep the *latest* seen_at for each item
                                if (prev is None) or (parse_dt(prev.get("seen_at", "1970-01-01T00:00:00Z")) < msg_dt):
                                    store[name][m] = {"seen_at": iso_z(msg_dt)}

                    # optional small pause to reduce rate-limits
                    if sleep_between_messages > 0:
                        await asyncio.sleep(sleep_between_messages)

                if max_id > last_id:
                    state[ch] = max_id

            except FloodWaitError as e:
                print(f"[WARN] FloodWait while reading {ch}: sleeping {e.seconds}s")
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                print(f"[WARN] channel={ch} error={e}")

            if sleep_between_channels > 0:
                await asyncio.sleep(sleep_between_channels)

    store = prune_store(store, cutoff=cutoff)

    for name, mapping in store.items():
        items_sorted = sorted(mapping.items(), key=lambda kv: parse_dt(kv[1]["seen_at"]), reverse=True)
        write_output(name, [val for val, _meta in items_sorted])

    save_json(STORE_PATH, store)
    save_json(STATE_PATH, state)


if __name__ == "__main__":
    asyncio.run(main())
    if "t.me/" in s:
        s = s.split("t.me/")[-1]
    s = s.split("?")[0].strip().strip("/")

    if not s:
        return s

    if not s.startswith("@"):
        s = "@" + s

    return s


def load_channels_csv(default_limit: int) -> list[tuple[str, int]]:
    p = ROOT / "channels.csv"
    rows: list[tuple[str, int]] = []

    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue

            first = (row[0] or "").strip().lower()
            if i == 0 and first in ("channel", "address", "url"):
                continue

            raw = (row[0] or "").strip()
            if not raw or raw.startswith("#"):
                continue

            ch = normalize_channel(raw)
            limit = default_limit

            if len(row) > 1 and (row[1] or "").strip():
                try:
                    limit = int(row[1].strip())
                except ValueError:
                    limit = default_limit

            rows.append((ch, limit))

    return rows


def load_patterns() -> list[tuple[str, re.Pattern]]:
    data = json.loads((ROOT / "patterns.json").read_text(encoding="utf-8"))
    compiled: list[tuple[str, re.Pattern]] = []

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


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)




def extract_candidate_texts(msg) -> list[str]:
    """Collect all text/URLs we might want to regex-scan from a Telegram message.

    - msg.message: plain text
    - MessageEntityTextUrl: URLs attached to clickable text (URL may not appear in msg.message)
    - msg.buttons / reply_markup: URLs behind inline keyboard buttons
    """
    out: list[str] = []

    if getattr(msg, "message", None):
        out.append(msg.message)

    # URLs attached to entities (e.g. 'click here' with an underlying URL)
    for e in (getattr(msg, "entities", None) or []):
        if isinstance(e, MessageEntityTextUrl) and getattr(e, "url", None):
            out.append(e.url)

    # Inline keyboard buttons with URL
    for row in (getattr(msg, "buttons", None) or []):
        for b in row:
            url = getattr(b, "url", None)
            if url:
                out.append(url)

    return out


async def safe_get_entity(client: TelegramClient, ch: str, entity_cache: dict) -> object | None:
    """
    Resolve channel username to entity once and cache it, to reduce ResolveUsernameRequest.
    """
    if ch in entity_cache:
        return entity_cache[ch]

    try:
        ent = await client.get_entity(ch)
        entity_cache[ch] = ent
        return ent
    except (UsernameInvalidError, UsernameNotOccupiedError) as e:
        print(f"[WARN] channel={ch} invalid/occupied: {e}")
        entity_cache[ch] = None
        return None
    except FloodWaitError as e:
        print(f"[WARN] FloodWait on get_entity({ch}): sleeping {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
        # retry once after sleeping
        try:
            ent = await client.get_entity(ch)
            entity_cache[ch] = ent
            return ent
        except Exception as e2:
            print(f"[WARN] channel={ch} get_entity failed after wait: {e2}")
            entity_cache[ch] = None
            return None
    except Exception as e:
        print(f"[WARN] channel={ch} get_entity error: {e}")
        entity_cache[ch] = None
        return None


async def main():
    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]
    session_str = os.environ["TG_SESSION"]

    default_limit = int(os.environ.get("DEFAULT_LIMIT_PER_CHANNEL", "200"))
    ttl_hours = int(os.environ.get("ITEM_TTL_HOURS", "50"))

    # Rate-limit controls (tunable)
    sleep_between_channels = float(os.environ.get("SLEEP_BETWEEN_CHANNELS_SEC", "1.5"))
    sleep_between_messages = float(os.environ.get("SLEEP_BETWEEN_MESSAGES_SEC", "0.0"))
    max_channels = int(os.environ.get("MAX_CHANNELS_PER_RUN", "0"))  # 0 = unlimited

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=ttl_hours)

    channels = load_channels_csv(default_limit=default_limit)
    patterns = load_patterns()

    store = load_json(STORE_PATH, default={})
    state = load_json(STATE_PATH, default={})

    for name, _ in patterns:
        store.setdefault(name, {})

    entity_cache: dict[str, object | None] = {}

    async with TelegramClient(
        StringSession(session_str),
        api_id,
        api_hash,
        timeout=30,
        connection_retries=10,
        retry_delay=2,
    ) as client:
        if max_channels > 0:
            channels = channels[:max_channels]

        for ch, per_limit in channels:
            last_id = int(state.get(ch, 0))
            max_id = last_id

            ent = await safe_get_entity(client, ch, entity_cache)
            if ent is None:
                continue

            try:
                async for msg in client.iter_messages(ent, limit=per_limit):
                    # Some messages have URLs only in entities/buttons (not in msg.message)
                    candidates = extract_candidate_texts(msg)
                    if not candidates:
                        continue

                    # Stop once we reach older messages already processed
                    if msg.id <= last_id:
                        break

                    if msg.id > max_id:
                        max_id = msg.id

                    msg_dt = _as_utc(msg.date)

                    # Keep only items whose source message is within TTL window
                    if msg_dt < cutoff:
                        continue

                    for candidate in candidates:
                        for name, rx in patterns:
                            for mat in rx.finditer(candidate):
                                m = mat.group(0).strip()
                                if not m:
                                    continue

                                prev = store[name].get(m)
                                # Always keep the *latest* seen_at for each item
                                if (prev is None) or (parse_dt(prev.get("seen_at", "1970-01-01T00:00:00Z")) < msg_dt):
                                    store[name][m] = {"seen_at": iso_z(msg_dt)}

                    # optional small pause to reduce rate-limits
                    if sleep_between_messages > 0:
                        await asyncio.sleep(sleep_between_messages)

                if max_id > last_id:
                    state[ch] = max_id

            except FloodWaitError as e:
                print(f"[WARN] FloodWait while reading {ch}: sleeping {e.seconds}s")
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                print(f"[WARN] channel={ch} error={e}")

            if sleep_between_channels > 0:
                await asyncio.sleep(sleep_between_channels)

    store = prune_store(store, cutoff=cutoff)

    for name, mapping in store.items():
        items_sorted = sorted(mapping.items(), key=lambda kv: parse_dt(kv[1]["seen_at"]), reverse=True)
        write_output(name, [val for val, _meta in items_sorted])

    save_json(STORE_PATH, store)
    save_json(STATE_PATH, state)


if __name__ == "__main__":
    asyncio.run(main())
    if "t.me/" in s:
        s = s.split("t.me/")[-1]
    s = s.split("?")[0].strip().strip("/")

    if not s:
        return s

    if not s.startswith("@"):
        s = "@" + s

    return s


def load_channels_csv(default_limit: int) -> list[tuple[str, int]]:
    p = ROOT / "channels.csv"
    rows: list[tuple[str, int]] = []

    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue

            first = (row[0] or "").strip().lower()
            if i == 0 and first in ("channel", "address", "url"):
                continue

            raw = (row[0] or "").strip()
            if not raw or raw.startswith("#"):
                continue

            ch = normalize_channel(raw)
            limit = default_limit

            if len(row) > 1 and (row[1] or "").strip():
                try:
                    limit = int(row[1].strip())
                except ValueError:
                    limit = default_limit

            rows.append((ch, limit))

    return rows


def load_patterns() -> list[tuple[str, re.Pattern]]:
    data = json.loads((ROOT / "patterns.json").read_text(encoding="utf-8"))
    compiled: list[tuple[str, re.Pattern]] = []

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


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)




def extract_candidate_texts(msg) -> list[str]:
    """Collect all text/URLs we might want to regex-scan from a Telegram message.

    - msg.message: plain text
    - MessageEntityTextUrl: URLs attached to clickable text (URL may not appear in msg.message)
    - msg.buttons / reply_markup: URLs behind inline keyboard buttons
    """
    out: list[str] = []

    if getattr(msg, "message", None):
        out.append(msg.message)

    # URLs attached to entities (e.g. 'click here' with an underlying URL)
    for e in (getattr(msg, "entities", None) or []):
        if isinstance(e, MessageEntityTextUrl) and getattr(e, "url", None):
            out.append(e.url)

    # Inline keyboard buttons with URL
    for row in (getattr(msg, "buttons", None) or []):
        for b in row:
            url = getattr(b, "url", None)
            if url:
                out.append(url)

    return out


async def safe_get_entity(client: TelegramClient, ch: str, entity_cache: dict) -> object | None:
    """
    Resolve channel username to entity once and cache it, to reduce ResolveUsernameRequest.
    """
    if ch in entity_cache:
        return entity_cache[ch]

    try:
        ent = await client.get_entity(ch)
        entity_cache[ch] = ent
        return ent
    except (UsernameInvalidError, UsernameNotOccupiedError) as e:
        print(f"[WARN] channel={ch} invalid/occupied: {e}")
        entity_cache[ch] = None
        return None
    except FloodWaitError as e:
        print(f"[WARN] FloodWait on get_entity({ch}): sleeping {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
        # retry once after sleeping
        try:
            ent = await client.get_entity(ch)
            entity_cache[ch] = ent
            return ent
        except Exception as e2:
            print(f"[WARN] channel={ch} get_entity failed after wait: {e2}")
            entity_cache[ch] = None
            return None
    except Exception as e:
        print(f"[WARN] channel={ch} get_entity error: {e}")
        entity_cache[ch] = None
        return None


async def main():
    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]
    session_str = os.environ["TG_SESSION"]

    default_limit = int(os.environ.get("DEFAULT_LIMIT_PER_CHANNEL", "200"))
    ttl_hours = int(os.environ.get("ITEM_TTL_HOURS", "50"))

    # Rate-limit controls (tunable)
    sleep_between_channels = float(os.environ.get("SLEEP_BETWEEN_CHANNELS_SEC", "1.5"))
    sleep_between_messages = float(os.environ.get("SLEEP_BETWEEN_MESSAGES_SEC", "0.0"))
    max_channels = int(os.environ.get("MAX_CHANNELS_PER_RUN", "0"))  # 0 = unlimited

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=ttl_hours)

    channels = load_channels_csv(default_limit=default_limit)
    patterns = load_patterns()

    store = load_json(STORE_PATH, default={})
    state = load_json(STATE_PATH, default={})

    for name, _ in patterns:
        store.setdefault(name, {})

    entity_cache: dict[str, object | None] = {}

    async with TelegramClient(
        StringSession(session_str),
        api_id,
        api_hash,
        timeout=30,
        connection_retries=10,
        retry_delay=2,
    ) as client:
        if max_channels > 0:
            channels = channels[:max_channels]

        for ch, per_limit in channels:
            last_id = int(state.get(ch, 0))
            max_id = last_id

            ent = await safe_get_entity(client, ch, entity_cache)
            if ent is None:
                continue

            try:
                async for msg in client.iter_messages(ent, limit=per_limit):
                    # Some messages have URLs only in entities/buttons (not in msg.message)
                    candidates = extract_candidate_texts(msg)
                    if not candidates:
                        continue

                    # Stop once we reach older messages already processed
                    if msg.id <= last_id:
                        break

                    if msg.id > max_id:
                        max_id = msg.id

                    msg_dt = _as_utc(msg.date)

                    # Keep only items whose source message is within TTL window
                    if msg_dt < cutoff:
                        continue

                    for candidate in candidates:
                        for name, rx in patterns:
                            for mat in rx.finditer(candidate):
                                m = mat.group(0).strip()
                                if not m:
                                    continue

                                prev = store[name].get(m)
                                # Always keep the *latest* seen_at for each item
                                if (prev is None) or (parse_dt(prev.get("seen_at", "1970-01-01T00:00:00Z")) < msg_dt):
                                    store[name][m] = {"seen_at": iso_z(msg_dt)}

                    # optional small pause to reduce rate-limits
                    if sleep_between_messages > 0:
                        await asyncio.sleep(sleep_between_messages)

                if max_id > last_id:
                    state[ch] = max_id

            except FloodWaitError as e:
                print(f"[WARN] FloodWait while reading {ch}: sleeping {e.seconds}s")
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                print(f"[WARN] channel={ch} error={e}")

            if sleep_between_channels > 0:
                await asyncio.sleep(sleep_between_channels)

    store = prune_store(store, cutoff=cutoff)

    for name, mapping in store.items():
        items_sorted = sorted(mapping.items(), key=lambda kv: parse_dt(kv[1]["seen_at"]), reverse=True)
        write_output(name, [val for val, _meta in items_sorted])

    save_json(STORE_PATH, store)
    save_json(STATE_PATH, state)


if __name__ == "__main__":
    asyncio.run(main())
    if "t.me/" in s:
        s = s.split("t.me/")[-1]
    s = s.split("?")[0].strip().strip("/")

    if not s:
        return s

    if not s.startswith("@"):
        s = "@" + s

    return s


def load_channels_csv(default_limit: int) -> list[tuple[str, int]]:
    p = ROOT / "channels.csv"
    rows: list[tuple[str, int]] = []

    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue

            first = (row[0] or "").strip().lower()
            if i == 0 and first in ("channel", "address", "url"):
                continue

            raw = (row[0] or "").strip()
            if not raw or raw.startswith("#"):
                continue

            ch = normalize_channel(raw)
            limit = default_limit

            if len(row) > 1 and (row[1] or "").strip():
                try:
                    limit = int(row[1].strip())
                except ValueError:
                    limit = default_limit

            rows.append((ch, limit))

    return rows


def load_patterns() -> list[tuple[str, re.Pattern]]:
    data = json.loads((ROOT / "patterns.json").read_text(encoding="utf-8"))
    compiled: list[tuple[str, re.Pattern]] = []

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


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)




def extract_candidate_texts(msg) -> list[str]:
    """Collect all text/URLs we might want to regex-scan from a Telegram message.

    - msg.message: plain text
    - MessageEntityTextUrl: URLs attached to clickable text (URL may not appear in msg.message)
    - msg.buttons / reply_markup: URLs behind inline keyboard buttons
    """
    out: list[str] = []

    if getattr(msg, "message", None):
        out.append(msg.message)

    # URLs attached to entities (e.g. 'click here' with an underlying URL)
    for e in (getattr(msg, "entities", None) or []):
        if isinstance(e, MessageEntityTextUrl) and getattr(e, "url", None):
            out.append(e.url)

    # Inline keyboard buttons with URL
    for row in (getattr(msg, "buttons", None) or []):
        for b in row:
            url = getattr(b, "url", None)
            if url:
                out.append(url)

    return out


async def safe_get_entity(client: TelegramClient, ch: str, entity_cache: dict) -> object | None:
    """
    Resolve channel username to entity once and cache it, to reduce ResolveUsernameRequest.
    """
    if ch in entity_cache:
        return entity_cache[ch]

    try:
        ent = await client.get_entity(ch)
        entity_cache[ch] = ent
        return ent
    except (UsernameInvalidError, UsernameNotOccupiedError) as e:
        print(f"[WARN] channel={ch} invalid/occupied: {e}")
        entity_cache[ch] = None
        return None
    except FloodWaitError as e:
        print(f"[WARN] FloodWait on get_entity({ch}): sleeping {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
        # retry once after sleeping
        try:
            ent = await client.get_entity(ch)
            entity_cache[ch] = ent
            return ent
        except Exception as e2:
            print(f"[WARN] channel={ch} get_entity failed after wait: {e2}")
            entity_cache[ch] = None
            return None
    except Exception as e:
        print(f"[WARN] channel={ch} get_entity error: {e}")
        entity_cache[ch] = None
        return None


async def main():
    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]
    session_str = os.environ["TG_SESSION"]

    default_limit = int(os.environ.get("DEFAULT_LIMIT_PER_CHANNEL", "200"))
    ttl_hours = int(os.environ.get("ITEM_TTL_HOURS", "50"))

    # Rate-limit controls (tunable)
    sleep_between_channels = float(os.environ.get("SLEEP_BETWEEN_CHANNELS_SEC", "1.5"))
    sleep_between_messages = float(os.environ.get("SLEEP_BETWEEN_MESSAGES_SEC", "0.0"))
    max_channels = int(os.environ.get("MAX_CHANNELS_PER_RUN", "0"))  # 0 = unlimited

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=ttl_hours)

    channels = load_channels_csv(default_limit=default_limit)
    patterns = load_patterns()

    store = load_json(STORE_PATH, default={})
    state = load_json(STATE_PATH, default={})

    for name, _ in patterns:
        store.setdefault(name, {})

    entity_cache: dict[str, object | None] = {}

    async with TelegramClient(
        StringSession(session_str),
        api_id,
        api_hash,
        timeout=30,
        connection_retries=10,
        retry_delay=2,
    ) as client:
        if max_channels > 0:
            channels = channels[:max_channels]

        for ch, per_limit in channels:
            last_id = int(state.get(ch, 0))
            max_id = last_id

            ent = await safe_get_entity(client, ch, entity_cache)
            if ent is None:
                continue

            try:
                async for msg in client.iter_messages(ent, limit=per_limit):
                    # Some messages have URLs only in entities/buttons (not in msg.message)
                    candidates = extract_candidate_texts(msg)
                    if not candidates:
                        continue

                    # Stop once we reach older messages already processed
                    if msg.id <= last_id:
                        break

                    if msg.id > max_id:
                        max_id = msg.id

                    msg_dt = _as_utc(msg.date)

                    # Keep only items whose source message is within TTL window
                    if msg_dt < cutoff:
                        continue

                    for candidate in candidates:
                        for name, rx in patterns:
                            for mat in rx.finditer(candidate):
                                m = mat.group(0).strip()
                                if not m:
                                    continue

                                prev = store[name].get(m)
                                # Always keep the *latest* seen_at for each item
                                if (prev is None) or (parse_dt(prev.get("seen_at", "1970-01-01T00:00:00Z")) < msg_dt):
                                    store[name][m] = {"seen_at": iso_z(msg_dt)}

                    # optional small pause to reduce rate-limits
                    if sleep_between_messages > 0:
                        await asyncio.sleep(sleep_between_messages)

                if max_id > last_id:
                    state[ch] = max_id

            except FloodWaitError as e:
                print(f"[WARN] FloodWait while reading {ch}: sleeping {e.seconds}s")
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                print(f"[WARN] channel={ch} error={e}")

            if sleep_between_channels > 0:
                await asyncio.sleep(sleep_between_channels)

    store = prune_store(store, cutoff=cutoff)

    for name, mapping in store.items():
        items_sorted = sorted(mapping.items(), key=lambda kv: parse_dt(kv[1]["seen_at"]), reverse=True)
        write_output(name, [val for val, _meta in items_sorted])

    save_json(STORE_PATH, store)
    save_json(STATE_PATH, state)


if __name__ == "__main__":
    asyncio.run(main())
        s = s.split("t.me/")[-1]
    s = s.split("?")[0].strip().strip("/")

    if not s:
        return s

    if not s.startswith("@"):
        s = "@" + s

    return s


def load_channels_csv(default_limit: int) -> list[tuple[str, int]]:
    p = ROOT / "channels.csv"
    rows: list[tuple[str, int]] = []

    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue

            first = (row[0] or "").strip().lower()
            if i == 0 and first in ("channel", "address", "url"):
                continue

            raw = (row[0] or "").strip()
            if not raw or raw.startswith("#"):
                continue

            ch = normalize_channel(raw)
            limit = default_limit

            if len(row) > 1 and (row[1] or "").strip():
                try:
                    limit = int(row[1].strip())
                except ValueError:
                    limit = default_limit

            rows.append((ch, limit))

    return rows


def load_patterns() -> list[tuple[str, re.Pattern]]:
    data = json.loads((ROOT / "patterns.json").read_text(encoding="utf-8"))
    compiled: list[tuple[str, re.Pattern]] = []

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


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def safe_get_entity(client: TelegramClient, ch: str, entity_cache: dict) -> object | None:
    """
    Resolve channel username to entity once and cache it, to reduce ResolveUsernameRequest.
    """
    if ch in entity_cache:
        return entity_cache[ch]

    try:
        ent = await client.get_entity(ch)
        entity_cache[ch] = ent
        return ent
    except (UsernameInvalidError, UsernameNotOccupiedError) as e:
        print(f"[WARN] channel={ch} invalid/occupied: {e}")
        entity_cache[ch] = None
        return None
    except FloodWaitError as e:
        print(f"[WARN] FloodWait on get_entity({ch}): sleeping {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
        # retry once after sleeping
        try:
            ent = await client.get_entity(ch)
            entity_cache[ch] = ent
            return ent
        except Exception as e2:
            print(f"[WARN] channel={ch} get_entity failed after wait: {e2}")
            entity_cache[ch] = None
            return None
    except Exception as e:
        print(f"[WARN] channel={ch} get_entity error: {e}")
        entity_cache[ch] = None
        return None


async def main():
    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]
    session_str = os.environ["TG_SESSION"]

    default_limit = int(os.environ.get("DEFAULT_LIMIT_PER_CHANNEL", "200"))
    ttl_hours = int(os.environ.get("ITEM_TTL_HOURS", "50"))

    # Rate-limit controls (tunable)
    sleep_between_channels = float(os.environ.get("SLEEP_BETWEEN_CHANNELS_SEC", "1.5"))
    sleep_between_messages = float(os.environ.get("SLEEP_BETWEEN_MESSAGES_SEC", "0.0"))
    max_channels = int(os.environ.get("MAX_CHANNELS_PER_RUN", "0"))  # 0 = unlimited

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=ttl_hours)

    channels = load_channels_csv(default_limit=default_limit)
    patterns = load_patterns()

    store = load_json(STORE_PATH, default={})
    state = load_json(STATE_PATH, default={})

    for name, _ in patterns:
        store.setdefault(name, {})

    entity_cache: dict[str, object | None] = {}

    async with TelegramClient(
        StringSession(session_str),
        api_id,
        api_hash,
        timeout=30,
        connection_retries=10,
        retry_delay=2,
    ) as client:
        if max_channels > 0:
            channels = channels[:max_channels]

        for ch, per_limit in channels:
            last_id = int(state.get(ch, 0))
            max_id = last_id

            ent = await safe_get_entity(client, ch, entity_cache)
            if ent is None:
                continue

            try:
                async for msg in client.iter_messages(ent, limit=per_limit):
                    if not msg.message:
                        continue

                    # Stop once we reach older messages already processed
                    if msg.id <= last_id:
                        break

                    if msg.id > max_id:
                        max_id = msg.id

                    msg_dt = _as_utc(msg.date)

                    # Keep only items whose source message is within TTL window
                    if msg_dt < cutoff:
                        continue

                    text = msg.message

                    any_match = False
                    for name, rx in patterns:
                        matches = rx.findall(text)
                        if matches:
                            any_match = True

                        for m in matches:
                            if isinstance(m, tuple):
                                m = m[0]
                            m = str(m).strip()
                            if not m:
                                continue
                            if m not in store[name]:
                                store[name][m] = {"seen_at": iso_z(msg_dt)}

                    # optional small pause to reduce rate-limits
                    if sleep_between_messages > 0:
                        await asyncio.sleep(sleep_between_messages)

                if max_id > last_id:
                    state[ch] = max_id

            except FloodWaitError as e:
                print(f"[WARN] FloodWait while reading {ch}: sleeping {e.seconds}s")
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                print(f"[WARN] channel={ch} error={e}")

            if sleep_between_channels > 0:
                await asyncio.sleep(sleep_between_channels)

    store = prune_store(store, cutoff=cutoff)

    for name, mapping in store.items():
        items_sorted = sorted(mapping.items(), key=lambda kv: parse_dt(kv[1]["seen_at"]), reverse=True)
        write_output(name, [val for val, _meta in items_sorted])

    save_json(STORE_PATH, store)
    save_json(STATE_PATH, state)


if __name__ == "__main__":
    asyncio.run(main())
