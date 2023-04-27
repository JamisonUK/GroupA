import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# Replace with your own client ID and client secret
client_id = '3bfb338f97d24d5c9ee1a8e3ca30acdb'
client_secret = '4df7643c31a446619786caedbe4c0d57'

# Initialize the Spotipy client with your credentials
client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
handler = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

#prints out the track id of each song with the most popular
#song with that name on spotify
my_list = open("track_metadata.txt","r").readlines()
ids = []
for i1 in range(len(my_list)):
    ids.append(my_list[i1].split("|"))

for i2 in range(len(ids)):
    print(ids[i2][0],end=",")
    new_result = handler.search(type='track',
                        q=ids[i2][1],
                        limit=1)
    print(new_result['tracks']['items'][0]['uri'])



