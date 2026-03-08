#!/usr/bin/env python3
"""Sync Spotify playlists to Tidal with incremental update support."""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import re
import sys
import threading
import time
import unicodedata
from pathlib import Path

import spotipy
from spotipy.oauth2 import SpotifyOAuth
import tidalapi

STATE_FILE = Path(__file__).parent / "sync_state.json"
VERBOSE = False


def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"playlists": {}}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def get_spotify_client():
    required = ["SPOTIFY_CLIENT_ID", "SPOTIFY_CLIENT_SECRET", "SPOTIFY_REDIRECT_URI"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        print(f"Missing environment variables: {', '.join(missing)}")
        print("Copy .env.example to .env and fill in your Spotify credentials.")
        sys.exit(1)

    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=os.environ["SPOTIFY_CLIENT_ID"],
        client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
        redirect_uri=os.environ["SPOTIFY_REDIRECT_URI"],
        scope="playlist-read-private playlist-read-collaborative",
    ))


def get_tidal_session():
    session = tidalapi.Session()
    # Try to load saved session first
    token_file = Path(__file__).parent / ".tidal_token.json"
    if token_file.exists():
        saved = json.loads(token_file.read_text())
        try:
            session.load_oauth_session(
                saved["token_type"],
                saved["access_token"],
                saved["refresh_token"],
                saved.get("expiry_time"),
            )
            if session.check_login():
                return session
        except Exception:
            pass

    # Fresh login
    login, future = session.login_oauth()
    print(f"\nOpen this URL to log in to Tidal:\n{login.verification_uri_complete}\n")
    print(f"Or go to {login.verification_uri} and enter code: {login.user_code}")
    future.result()

    if not session.check_login():
        print("Tidal login failed.")
        sys.exit(1)

    # Save session for next time
    token_file.write_text(json.dumps({
        "token_type": session.token_type,
        "access_token": session.access_token,
        "refresh_token": session.refresh_token,
        "expiry_time": session.expiry_time.isoformat() if session.expiry_time else None,
    }))
    print("Tidal login successful!\n")
    return session


def fetch_all_spotify_playlists(sp):
    playlists = []
    results = sp.current_user_playlists(limit=50)
    while results:
        playlists.extend(results["items"])
        results = sp.next(results) if results["next"] else None
    return playlists


def fetch_spotify_tracks(sp, playlist_id):
    """Return list of track dicts with metadata for matching."""
    tracks = []
    results = sp.playlist_tracks(playlist_id, limit=100)
    while results:
        for item in results["items"]:
            track = item.get("track")
            if not track or not track.get("name"):
                continue
            artists = [a["name"] for a in track.get("artists", []) if a.get("name")]
            isrc = track.get("external_ids", {}).get("isrc")
            tracks.append({
                "spotify_id": track["id"],
                "artist": artists[0] if artists else "",
                "artists": artists,
                "title": track["name"],
                "album": track.get("album", {}).get("name", ""),
                "isrc": isrc,
                "duration_ms": track.get("duration_ms", 0),
            })
        results = sp.next(results) if results["next"] else None
    return tracks


def normalize(text):
    """Normalize text for fuzzy comparison: strip accents, lowercase, normalize quotes."""
    # Replace smart quotes/apostrophes with standard ones
    text = text.replace("\u2018", "'").replace("\u2019", "'").replace("\u201c", '"').replace("\u201d", '"')
    text = unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode("ascii")
    return text.lower().strip()


def simplify(text):
    """Strip content after hyphens, brackets, parentheses (e.g. 'Song - Remastered 2020' -> 'Song')."""
    text = re.split(r"\s*[-(\[]", text)[0]
    return text.strip()


WRONG_VERSION_TAGS = {"instrumental", "acapella", "remix", "live", "acoustic", "karaoke"}


def has_wrong_version(sp_title, tidal_title):
    """Check if one title has a version tag the other doesn't."""
    sp_lower = normalize(sp_title)
    t_lower = normalize(tidal_title)
    for tag in WRONG_VERSION_TAGS:
        sp_has = tag in sp_lower
        t_has = tag in t_lower
        if sp_has != t_has:
            return True
    return False


def artists_match(sp_artists, tidal_artist_name):
    """Check if any Spotify artist matches the Tidal artist."""
    tidal_parts = {normalize(a.strip()) for a in re.split(r"[,&]", tidal_artist_name)}
    sp_parts = {normalize(a.strip()) for a in sp_artists}
    # Check for any intersection or substring match
    for sp_a in sp_parts:
        for t_a in tidal_parts:
            if sp_a in t_a or t_a in sp_a:
                return True
    return False


def duration_close(sp_ms, tidal_seconds):
    """Check if durations are within 3 seconds of each other."""
    if not sp_ms or not tidal_seconds:
        return True  # Skip check if data missing
    return abs((sp_ms / 1000) - tidal_seconds) <= 3


def tidal_search_with_retry(session, query, models, limit, max_retries=5):
    """Search Tidal with exponential backoff on rate limit errors."""
    for attempt in range(max_retries):
        try:
            return session.search(query, models=models, limit=limit)
        except Exception as e:
            if "too many" in str(e).lower() or "429" in str(e):
                wait = 2 ** attempt  # 1, 2, 4, 8, 16 seconds
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                raise
    return {}


def search_tidal_track(session, sp_track):
    """Search for a track on Tidal using ISRC, then name matching."""
    isrc = sp_track.get("isrc")
    artist = sp_track["artist"]
    title = sp_track["title"]

    try:
        # Phase 1: ISRC lookup (most accurate)
        if isrc:
            if VERBOSE:
                print(f"      ISRC lookup: {isrc}")
            results = tidal_search_with_retry(session, isrc, models=[tidalapi.media.Track], limit=5)
            candidates = results.get("tracks", results.get("top_hit", []))
            for track in candidates:
                if hasattr(track, "isrc") and track.isrc == isrc:
                    if VERBOSE:
                        print(f"      ISRC matched!")
                    return track
            if VERBOSE:
                print(f"      ISRC not found, falling back to search")

        # Build list of search queries to try (raw first, then normalized)
        simple_title = simplify(title)
        raw_query = f"{artist} {title}"
        norm_query = normalize(raw_query)
        queries = [raw_query]
        if norm_query != raw_query.lower().strip():
            queries.append(norm_query)
        if simple_title != title:
            queries.append(f"{artist} {simple_title}")
        # Title-only fallback (helps when artist name differs between platforms)
        queries.append(title)
        if simple_title != title:
            queries.append(simple_title)

        candidates = None
        for query in queries:
            results = tidal_search_with_retry(session, query, models=[tidalapi.media.Track], limit=10)
            candidates = results.get("tracks", results.get("top_hit", []))
            if candidates:
                if VERBOSE:
                    print(f"      Search query '{query}' returned {len(candidates)} results")
                break
            if VERBOSE:
                print(f"      Search query '{query}' returned no results")

        if not candidates:
            if VERBOSE:
                print(f"      No search results from Tidal")
            return None

        if VERBOSE:
            print(f"      Candidates from Tidal:")
            for c in candidates:
                c_artist = c.artist.name if c.artist else "?"
                c_isrc = c.isrc if hasattr(c, "isrc") else "?"
                print(f"        - {c_artist} - {c.name} (dur={c.duration}s, isrc={c_isrc})")

        sp_title_norm = normalize(title)
        sp_title_simple = normalize(simplify(title))
        sp_dur = sp_track["duration_ms"]
        if VERBOSE:
            print(f"      Matching: title_norm='{sp_title_norm}' title_simple='{sp_title_simple}' dur={sp_dur}ms")

        # Pass 1: Strict match (normalized title + artist + duration + no wrong version)
        for track in candidates:
            t_artist = track.artist.name if track.artist else ""
            t_title = normalize(track.name)
            if has_wrong_version(title, track.name):
                continue
            if not artists_match(sp_track["artists"], t_artist):
                continue
            if sp_title_norm in t_title or t_title in sp_title_norm:
                if duration_close(sp_track["duration_ms"], track.duration):
                    return track

        # Pass 2: Simplified title match (strips remaster/deluxe/etc.)
        for track in candidates:
            t_artist = track.artist.name if track.artist else ""
            t_title_simple = normalize(simplify(track.name))
            if has_wrong_version(title, track.name):
                continue
            if not artists_match(sp_track["artists"], t_artist):
                continue
            if sp_title_simple == t_title_simple:
                if duration_close(sp_track["duration_ms"], track.duration):
                    return track

        # Pass 3: Relaxed - artist match + title contains (no duration check)
        for track in candidates:
            t_artist = track.artist.name if track.artist else ""
            t_title = normalize(track.name)
            t_title_simple = normalize(simplify(track.name))
            if has_wrong_version(title, track.name):
                continue
            if not artists_match(sp_track["artists"], t_artist):
                continue
            if sp_title_simple in t_title or t_title_simple in sp_title_norm:
                return track

        # Pass 4: Title-only match with duration check (ignores artist name)
        for track in candidates:
            t_title = normalize(track.name)
            if has_wrong_version(title, track.name):
                continue
            if sp_title_norm == t_title or sp_title_simple == normalize(simplify(track.name)):
                if duration_close(sp_track["duration_ms"], track.duration):
                    if VERBOSE:
                        t_artist = track.artist.name if track.artist else "?"
                        print(f"      Pass 4 matched (title-only): {t_artist} - {track.name}")
                    return track

        return None
    except Exception as e:
        print(f"    Search error for '{artist} - {title}': {e}")
        return None


def sync_playlist(sp, session, spotify_playlist, state, dry_run=False):
    sp_id = spotify_playlist["id"]
    name = spotify_playlist["name"]
    playlist_state = state["playlists"].get(sp_id, {
        "tidal_playlist_id": None,
        "synced_spotify_track_ids": [],
        "unmatched": [],
    })

    print(f"\n{'='*60}")
    print(f"Playlist: {name}")
    print(f"{'='*60}")

    # Get all spotify tracks
    sp_tracks = fetch_spotify_tracks(sp, sp_id)
    already_synced = set(playlist_state["synced_spotify_track_ids"])
    new_tracks = [t for t in sp_tracks if t["spotify_id"] not in already_synced]

    if not new_tracks:
        print(f"  Already up to date ({len(sp_tracks)} tracks)")
        state["playlists"][sp_id] = playlist_state
        return

    print(f"  Total: {len(sp_tracks)} | Already synced: {len(already_synced)} | New: {len(new_tracks)}")

    if dry_run:
        for i, track in enumerate(new_tracks, 1):
            print(f"  [{i}/{len(new_tracks)}] {track['artist']} - {track['title']}")
        return

    # Get or create Tidal playlist
    tidal_playlist = None
    if playlist_state["tidal_playlist_id"]:
        try:
            tidal_playlist = session.playlist(playlist_state["tidal_playlist_id"])
        except Exception:
            print("  Previous Tidal playlist not found, creating new one...")

    if not tidal_playlist:
        tidal_playlist = session.user.create_playlist(name, name)
        playlist_state["tidal_playlist_id"] = tidal_playlist.id
        print(f"  Created Tidal playlist: {name}")

    # Search for new tracks in parallel
    matched_ids = []
    newly_unmatched = []
    lock = threading.Lock()

    def _search(track):
        tidal_track = search_tidal_track(session, track)
        time.sleep(0.5)  # Rate limit buffer
        return track, tidal_track

    completed = 0
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(_search, t): t for t in new_tracks
        }
        for future in as_completed(futures):
            track, tidal_track = future.result()
            completed += 1
            label = f"  [{completed}/{len(new_tracks)}] {track['artist']} - {track['title']}"
            with lock:
                if tidal_track:
                    matched_ids.append(tidal_track.id)
                    print(f"{label} -> matched")
                else:
                    newly_unmatched.append(f"{track['artist']} - {track['title']}")
                    print(f"{label} -> NOT FOUND")
                playlist_state["synced_spotify_track_ids"].append(track["spotify_id"])

    # Add matched tracks to Tidal playlist in smaller batches
    # Re-fetch playlist before each batch to get fresh ETag (avoids 412 errors)
    if matched_ids:
        added = 0
        batch_size = 20
        for i in range(0, len(matched_ids), batch_size):
            batch = matched_ids[i:i + batch_size]
            try:
                tidal_playlist = session.playlist(tidal_playlist.id)
                tidal_playlist.add(batch)
                added += len(batch)
            except Exception as e:
                print(f"  Error adding batch {i//batch_size + 1}: {e}")
                # Retry individually for failed batch
                for track_id in batch:
                    try:
                        tidal_playlist = session.playlist(tidal_playlist.id)
                        tidal_playlist.add([track_id])
                        added += 1
                    except Exception:
                        print(f"  Failed to add track {track_id}")
            time.sleep(0.5)
        print(f"  Added {added}/{len(matched_ids)} tracks to Tidal")

    if newly_unmatched:
        playlist_state["unmatched"].extend(newly_unmatched)
        print(f"  Could not find {len(newly_unmatched)} tracks on Tidal")

    state["playlists"][sp_id] = playlist_state


def parse_args():
    parser = argparse.ArgumentParser(description="Sync Spotify playlists to Tidal")
    parser.add_argument("--include-followed", action="store_true",
                        help="Also sync playlists you follow (by default only your own are synced)")
    parser.add_argument("--playlist", action="append", metavar="NAME",
                        help="Only sync playlists matching this name (can be used multiple times)")
    parser.add_argument("--unmatched", action="store_true",
                        help="Show all tracks that couldn't be found on Tidal")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be synced without making any changes")
    parser.add_argument("--verbose", action="store_true",
                        help="Show detailed matching info for debugging")
    return parser.parse_args()


def filter_playlists(playlists, sp, args):
    filtered = playlists

    if not args.include_followed:
        user_id = sp.current_user()["id"]
        filtered = [p for p in filtered if p["owner"]["id"] == user_id]

    if args.playlist:
        names = [n.lower() for n in args.playlist]
        filtered = [p for p in filtered if p["name"].lower() in names]

    return filtered


def main():
    # Load .env file manually if python-dotenv isn't installed
    env_file = Path(__file__).parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())

    args = parse_args()

    global VERBOSE
    VERBOSE = args.verbose

    print("Spotify -> Tidal Playlist Sync")
    print("=" * 40)

    state = load_state()
    sp = get_spotify_client()
    session = get_tidal_session()

    playlists = fetch_all_spotify_playlists(sp)
    print(f"\nFound {len(playlists)} Spotify playlists")

    playlists = filter_playlists(playlists, sp, args)
    if len(playlists) == 0:
        print("No playlists matched your filters.")
        return
    if not args.include_followed or args.playlist:
        print(f"Syncing {len(playlists)} playlist(s) after filtering")

    if args.dry_run:
        print("\n** DRY RUN — no changes will be made **")

    for playlist in playlists:
        sync_playlist(sp, session, playlist, state, dry_run=args.dry_run)
        if not args.dry_run:
            save_state(state)  # Save after each playlist in case of interruption

    # Print summary of all unmatched tracks
    total_unmatched = 0
    for sp_id, pstate in state["playlists"].items():
        if pstate["unmatched"]:
            total_unmatched += len(pstate["unmatched"])

    print(f"\n{'='*60}")
    print("SYNC COMPLETE")
    if total_unmatched:
        print(f"\n{total_unmatched} tracks could not be found on Tidal.")
        print("Run with --unmatched to see the full list.")

    if args.unmatched:
        print(f"\n{'='*60}")
        print("UNMATCHED TRACKS")
        print(f"{'='*60}")
        for sp_id, pstate in state["playlists"].items():
            if pstate["unmatched"]:
                name = sp_id
                for p in playlists:
                    if p["id"] == sp_id:
                        name = p["name"]
                        break
                print(f"\n  {name}:")
                for track in pstate["unmatched"]:
                    print(f"    - {track}")

    save_state(state)
    print("\nDone!")


if __name__ == "__main__":
    main()
