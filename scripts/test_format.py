import json
from src.formatting import album_message, album_url


def main() -> None:
    with open("data/albums_cache.json", "r", encoding="utf-8") as f:
        payload = json.load(f)

    album = payload["albums"][15]
    print(album)
    print("---")
    print("URL:", album_url(album))
    print("---")
    print(album_message(album))


if __name__ == "__main__":
    main()
