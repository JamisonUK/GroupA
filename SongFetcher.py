import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pprint

# Replace with your own client ID and client secret
client_id = 'b743f52e0250484e9ebf36fee403742f'
client_secret = 'a340a40b5220466d802f5e62ca81baeb'

# Initialize the Spotipy client with your credentials
client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
handler = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

query = 'live on spotify'
songs = handler.search(q=query, type='track', limit=1)

#storing all songs to list. Can be extended to be stored to other data pipeline too
live_songs = []
# Get first result and continue fetching is there is more song available
while True:
    try:
        for item in songs['tracks']['items']:
            if 'live' in item['name'].lower():
                live_songs.append(item)
        if songs['tracks']['next'] is not None:
            songs = handler.next(songs['tracks'])
        else:
            break
    except:
        continue
