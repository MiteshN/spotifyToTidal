# Spotify to Tidal Playlist Sync

Sync all your Spotify playlists to Tidal. Supports incremental updates — re-running the script only adds new songs that were added since the last sync.

## Setup

### 1. Create a Spotify Developer App

- Go to [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
- Click "Create App"
- Set the redirect URI to `http://localhost:8888/callback`
- Note your **Client ID** and **Client Secret**

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Spotify Credentials

```bash
cp .env.example .env
```

Edit `.env` and fill in your Spotify Client ID and Client Secret.

### 4. Run the Script

```bash
python sync.py
```

On the first run:
- A browser window will open for Spotify login
- A Tidal login URL will be printed in the terminal — open it and log in with your Tidal account

Both sessions are cached after the first login.

## Re-running (Incremental Sync)

Just run `python sync.py` again. The script tracks which songs have already been synced per playlist. Only new songs will be added.

## Viewing Unmatched Tracks

Some tracks may not be available on Tidal. To see the full list:

```bash
python sync.py --unmatched
```
