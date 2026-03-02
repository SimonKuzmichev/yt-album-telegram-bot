import random
from typing import Any, Dict, List, Optional, Tuple

from src.history import load_history, mark_sent, save_history, sent_set
from src.library import get_albums_with_cache

Album = Dict[str, Any]


def _eligible_albums(albums: List[Album]) -> List[Album]:
    # Filter out broken entries early
    return [a for a in albums if a.get("browseId") and a.get("title")]


def pick_random_album_no_repeat(
    auth_path: str,
    cache_path: str = "data/albums_cache.json",
    history_path: str = "data/sent_history.json",
    library_limit: Optional[int] = 500,
) -> Tuple[Album, bool]:
    """
    Returns:
      (album, did_refresh)

    Logic:
      1) Load cached album list (or fetch if missing).
      2) Load history of sent browseIds.
      3) Choose randomly from albums not in history.
      4) If none left, refresh library (sync) and reset history, then choose again.
    """
    did_refresh = False

    # Step 1: load albums (prefer cache)
    albums = get_albums_with_cache(
        auth_path=auth_path,
        cache_path=cache_path,
        refresh=False,
        limit=library_limit,
    )
    albums = _eligible_albums(albums)

    # Step 2: load sent history
    history = load_history(history_path)
    sent_ids = sent_set(history)

    # Step 3: pick from unsent
    unsent = [a for a in albums if str(a.get("browseId")) not in sent_ids]

    if not unsent:
        # Step 4: everything is exhausted -> refresh (sync) library and restart cycle
        did_refresh = True

        albums = get_albums_with_cache(
            auth_path=auth_path,
            cache_path=cache_path,
            refresh=True,          # force sync
            limit=library_limit,
        )
        albums = _eligible_albums(albums)

        # Reset history because we're starting a new cycle
        history = {"sent_browse_ids": []}
        sent_ids = set()

        unsent = [a for a in albums if str(a.get("browseId")) not in sent_ids]

        if not unsent:
            # If still empty, something is wrong with library fetch/auth or the library is empty
            raise RuntimeError("No eligible albums found after refresh. Check your library/auth data.")

    album = random.choice(unsent)

    # Persist history
    bid = str(album.get("browseId"))
    mark_sent(history, bid)
    save_history(history_path, history)

    return album, did_refresh