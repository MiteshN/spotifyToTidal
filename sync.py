#!/usr/bin/env python3
"""Sync Spotify playlists to Tidal with incremental update support."""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import spotipy
from spotipy.oauth2 import SpotifyOAuth
import tidalapi

STATE_FILE = Path(__file__).parent / "sync_state.json"


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
    """Return list of (artist, title, album) tuples."""
    tracks = []
    results = sp.playlist_tracks(playlist_id, limit=100)
    while results:
        for item in results["items"]:
            track = item.get("track")
            if not track or not track.get("name"):
                continue
            artist = track["artists"][0]["name"] if track["artists"] else ""
            tracks.append({
                "spotify_id": track["id"],
                "artist": artist,
                "title": track["name"],
                "album": track.get("album", {}).get("name", ""),
            })
        results = sp.next(results) if results["next"] else None
    return tracks


def search_tidal_track(session, artist, title):
    """Search for a track on Tidal, return track object or None."""
    query = f"{artist} {title}"
    try:
        results = session.search(query, models=[tidalapi.media.Track], limit=5)
        candidates = results.get("tracks", results.get("top_hit", []))
        if not candidates:
            return None

        # Try to find exact-ish match
        artist_lower = artist.lower()
        title_lower = title.lower()
        for track in candidates:
            t_artist = track.artist.name.lower() if track.artist else ""
            t_title = track.name.lower()
            if title_lower in t_title and artist_lower in t_artist:
                return track

        # Fall back to first result if artist matches
        for track in candidates:
            t_artist = track.artist.name.lower() if track.artist else ""
            if artist_lower in t_artist or t_artist in artist_lower:
                return track

        return None
    except Exception as e:
        print(f"    Search error for '{query}': {e}")
        return None


def sync_playlist(sp, session, spotify_playlist, state):
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

    # Search and add new tracks
    matched_ids = []
    newly_unmatched = []
    for i, track in enumerate(new_tracks, 1):
        print(f"  [{i}/{len(new_tracks)}] {track['artist']} - {track['title']}", end="")
        tidal_track = search_tidal_track(session, track["artist"], track["title"])
        if tidal_track:
            matched_ids.append(tidal_track.id)
            playlist_state["synced_spotify_track_ids"].append(track["spotify_id"])
            print(f" -> matched")
        else:
            newly_unmatched.append(f"{track['artist']} - {track['title']}")
            # Still mark as synced so we don't retry every run
            playlist_state["synced_spotify_track_ids"].append(track["spotify_id"])
            print(f" -> NOT FOUND")
        # Small delay to avoid rate limiting
        time.sleep(0.3)

    # Add matched tracks to Tidal playlist in batches
    if matched_ids:
        batch_size = 50
        for i in range(0, len(matched_ids), batch_size):
            batch = matched_ids[i:i + batch_size]
            try:
                tidal_playlist.add(batch)
            except Exception as e:
                print(f"  Error adding batch: {e}")
        print(f"  Added {len(matched_ids)} tracks to Tidal")

    if newly_unmatched:
        playlist_state["unmatched"].extend(newly_unmatched)
        print(f"  Could not find {len(newly_unmatched)} tracks on Tidal")

    state["playlists"][sp_id] = playlist_state


def parse_args():
    parser = argparse.ArgumentParser(description="Sync Spotify playlists to Tidal")
    parser.add_argument("--mine-only", action="store_true",
                        help="Only sync playlists you created (skip followed/saved playlists)")
    parser.add_argument("--playlist", action="append", metavar="NAME",
                        help="Only sync playlists matching this name (can be used multiple times)")
    parser.add_argument("--unmatched", action="store_true",
                        help="Show all tracks that couldn't be found on Tidal")
    return parser.parse_args()


def filter_playlists(playlists, sp, args):
    filtered = playlists

    if args.mine_only:
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
    if args.mine_only or args.playlist:
        print(f"Syncing {len(playlists)} playlist(s) after filtering")

    for playlist in playlists:
        sync_playlist(sp, session, playlist, state)
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
