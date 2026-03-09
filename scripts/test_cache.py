from src.library import get_albums_with_cache

albums = get_albums_with_cache(
    auth_path="secrets/browser.json",
    refresh=True,
    limit=200,
)

print("albums:", len(albums))
print("sample:", albums[0])