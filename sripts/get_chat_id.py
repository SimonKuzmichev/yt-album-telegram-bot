import os
import json
import urllib.request
import urllib.parse
from dotenv import load_dotenv


def api_get(method: str, token: str, params: dict | None = None) -> dict:
    base = f"https://api.telegram.org/bot{token}/{method}"
    if params:
        query = urllib.parse.urlencode(params)
        url = f"{base}?{query}"
    else:
        url = base

    with urllib.request.urlopen(url) as resp:
        data = resp.read().decode("utf-8")
    return json.loads(data)


def main() -> None:
    load_dotenv()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise SystemExit("BOT_TOKEN is not set. Put it into .env first.")

    # Get the latest updates (last messages sent to the bot)
    updates = api_get("getUpdates", token, {"limit": 10})
    print(json.dumps(updates, ensure_ascii=False, indent=2))

    # Try to extract chat_id from the newest message
    results = updates.get("result", [])
    if not results:
        print("\nNo updates found.")
        print("Send a new message to the bot (e.g. 'ping') and run this script again.")
        return

    last = results[-1]
    msg = last.get("message") or last.get("edited_message") or last.get("channel_post")
    if not msg:
        print("\nFound an update, but it doesn't contain a regular message object.")
        return

    chat = msg.get("chat", {})
    chat_id = chat.get("id")
    chat_type = chat.get("type")
    title = chat.get("title")
    username = chat.get("username")

    print("\n--- extracted ---")
    print("chat_id:", chat_id)
    print("chat_type:", chat_type)
    if title:
        print("title:", title)
    if username:
        print("username:", username)


if __name__ == "__main__":
    main()