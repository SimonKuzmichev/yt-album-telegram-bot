from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

from src.providers import YTMusicProviderClient, build_provider_client


EXPECTED_NORMALIZED_KEYS = {
    "provider",
    "provider_album_id",
    "title",
    "artist",
    "url",
    "release_year",
    "raw_payload_json",
}


def assert_normalized_album_contract(album: dict[str, Any], *, provider: str) -> None:
    assert set(album) == EXPECTED_NORMALIZED_KEYS
    assert album["provider"] == provider
    assert isinstance(album["provider_album_id"], str)
    assert album["provider_album_id"]
    assert isinstance(album["title"], str)
    assert album["title"]
    assert isinstance(album["artist"], str)
    assert album["url"] is None or isinstance(album["url"], str)
    assert album["release_year"] is None or isinstance(album["release_year"], int)
    assert isinstance(album["raw_payload_json"], dict)


@pytest.mark.parametrize(
    ("raw_album", "expected_album_id", "expected_artist", "expected_year"),
    [
        (
            {
                "browseId": "MPREb_album_1",
                "title": "Discovery",
                "artists": [{"name": "Daft Punk"}],
                "year": "2001",
            },
            "MPREb_album_1",
            "Daft Punk",
            2001,
        ),
        (
            {
                "audioPlaylistId": "PLAYLIST-42",
                "title": "Random Access Memories",
                "artist": "Daft Punk",
                "year": "not-a-number",
            },
            "PLAYLIST-42",
            "Daft Punk",
            None,
        ),
    ],
)
def test_ytmusic_normalize_album_matches_contract(
    raw_album: dict[str, Any],
    expected_album_id: str,
    expected_artist: str,
    expected_year: int | None,
) -> None:
    client = build_provider_client(" YTMUSIC ")
    normalized = client.normalize_album(raw_album)

    assert normalized is not None
    assert_normalized_album_contract(normalized, provider="ytmusic")
    assert normalized["provider_album_id"] == expected_album_id
    assert normalized["artist"] == expected_artist
    assert normalized["release_year"] == expected_year
    assert normalized["raw_payload_json"] == raw_album


def test_provider_adapter_list_saved_albums_filters_invalid_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    raw_albums = [
        {"browseId": "album-1", "title": "Alive 2007", "artists": [{"name": "Daft Punk"}], "year": "2007"},
        {"browseId": "", "title": "Missing Id"},
        {"browseId": "album-2", "title": "Homework", "artist": "Daft Punk", "year": 1997},
    ]
    client = YTMusicProviderClient(credentials={"cookie_blob": "secret"})

    class FakeYTMusic:
        def get_library_albums(self, *, limit: int | None = None):
            assert limit == 25
            return raw_albums

    @contextmanager
    def fake_client_session():
        yield FakeYTMusic()

    monkeypatch.setattr(client, "_client_session", fake_client_session)

    albums = client.list_saved_albums(limit=25)

    assert [album["provider_album_id"] for album in albums] == ["album-1", "album-2"]
    for album in albums:
        assert_normalized_album_contract(album, provider="ytmusic")
