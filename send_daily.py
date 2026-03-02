import asyncio

from src.picker import pick_random_album_no_repeat
from src.telegram_sender import send_album_message


async def main() -> None:
    # Pick a random album without repeats; refresh library if exhausted.
    album, refreshed = pick_random_album_no_repeat(
        auth_path="secrets/browser.json",
        library_limit=500,
    )

    # Optionally, you can prepend a line when a sync happened.
    # Keeping it simple: just send the album itself.
    await send_album_message(album)


if __name__ == "__main__":
    asyncio.run(main())