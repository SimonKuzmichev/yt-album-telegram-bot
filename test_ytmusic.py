from ytmusicapi import YTMusic

yt = YTMusic("secrets/browser.json")
albums = yt.get_library_albums(limit=5)
print(len(albums))
print(albums[0] if albums else "no albums")