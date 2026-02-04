from telethon.sync import TelegramClient
from telethon.sessions import StringSession
import os

api_id = int(os.environ["TG_API_ID"])
api_hash = os.environ["TG_API_HASH"]

client = TelegramClient(
    StringSession(),
    api_id,
    api_hash,
    timeout=60,
    connection_retries=20,
    retry_delay=3,
)

with client:
    print("TG_SESSION=" + client.session.save())
