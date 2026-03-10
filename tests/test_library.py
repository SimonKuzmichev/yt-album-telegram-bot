import json
import tempfile
import unittest
from pathlib import Path

from src.library import get_albums_with_cache, load_cached_albums
from src.providers import YTMusicProviderClient


class _FakeProviderClient:
    provider_name = "ytmusic"

    def __init__(self, albums):
        self.albums = albums
        self.calls = 0

    def validate_credentials(self) -> None:
        return None

    def list_saved_albums(self, limit=None):
        self.calls += 1
        return list(self.albums[:limit] if limit is not None else self.albums)

    def normalize_album(self, raw_album):
        return raw_album


class LoadCachedAlbumsTests(unittest.TestCase):
    def test_coerces_legacy_ytmusic_cache_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "albums.json"
            cache_path.write_text(
                json.dumps(
                    {
                        "updated_at": 123,
                        "albums": [
                            {
                                "title": "Discovery",
                                "artist": "Daft Punk",
                                "year": "2001",
                                "browseId": "MPREb_discovery",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            albums = load_cached_albums(str(cache_path), default_provider="ytmusic")

        self.assertEqual(len(albums or []), 1)
        self.assertEqual(albums[0]["provider"], "ytmusic")
        self.assertEqual(albums[0]["provider_album_id"], "MPREb_discovery")
        self.assertEqual(albums[0]["release_year"], 2001)
        self.assertEqual(albums[0]["url"], "https://music.youtube.com/browse/MPREb_discovery")


class GetAlbumsWithCacheTests(unittest.TestCase):
    def test_uses_cached_albums_before_provider_fetch(self) -> None:
        cached_album = {
            "provider": "ytmusic",
            "provider_album_id": "album-1",
            "title": "Agaetis byrjun",
            "artist": "Sigur Ros",
            "url": "https://example.com/albums/1",
            "release_year": 1999,
            "raw_payload_json": {"id": "album-1"},
        }
        provider_client = _FakeProviderClient(
            [
                {
                    "provider": "ytmusic",
                    "provider_album_id": "album-2",
                    "title": "Moon Safari",
                    "artist": "Air",
                    "url": "https://example.com/albums/2",
                    "release_year": 1998,
                    "raw_payload_json": {"id": "album-2"},
                }
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "albums.json"
            cache_path.write_text(
                json.dumps({"provider": "ytmusic", "updated_at": 123, "albums": [cached_album]}),
                encoding="utf-8",
            )

            albums = get_albums_with_cache(provider_client=provider_client, cache_path=str(cache_path), refresh=False)

        self.assertEqual(albums, [cached_album])
        self.assertEqual(provider_client.calls, 0)


class YTMusicProviderClientTests(unittest.TestCase):
    def test_normalize_album_returns_phase_two_shape(self) -> None:
        client = YTMusicProviderClient(auth_path="unused")

        album = client.normalize_album(
            {
                "title": "Dummy",
                "artists": [{"name": "Portishead"}],
                "year": "1994",
                "browseId": "MPREb_dummy",
                "thumbnails": [{"url": "https://example.com/thumb.jpg"}],
            }
        )

        self.assertIsNotNone(album)
        self.assertEqual(album["provider"], "ytmusic")
        self.assertEqual(album["provider_album_id"], "MPREb_dummy")
        self.assertEqual(album["title"], "Dummy")
        self.assertEqual(album["artist"], "Portishead")
        self.assertEqual(album["release_year"], 1994)
        self.assertEqual(album["url"], "https://music.youtube.com/browse/MPREb_dummy")
        self.assertEqual(album["raw_payload_json"]["browseId"], "MPREb_dummy")


if __name__ == "__main__":
    unittest.main()
