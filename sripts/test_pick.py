from src.picker import pick_random_album_no_repeat
from src.formatting import album_message

for i in range(5):
    album, refreshed = pick_random_album_no_repeat(
        auth_path="secrets/browser.json",
        library_limit=200,  # adjust if you have a large library
    )
    print(f"\n--- pick {i+1} (refreshed={refreshed}) ---")
    print(album_message(album))