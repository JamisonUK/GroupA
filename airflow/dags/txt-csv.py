def txt_to_csv(txt_file, csv_file):
        import pandas as pd
        df = pd.read_fwf(txt_file)
        f = df.to_csv(csv_file,  index=False)
        return f

#txt_to_csv('.\DataSet\kaggle_songs.txt','.\DataSet\kaggle_songs.csv')
txt_to_csv('.\DataSet\kaggle_users.txt','.\DataSet\kaggle_users.csv')
txt_to_csv('.\DataSet\kaggle_visible_evaluation_triplets.txt','.\DataSet\kaggle_visible_evaluation_triplets.csv')
txt_to_csv(r'.\DataSet\taste_profile_song_to_tracks.txt',r'.\DataSet\taste_profile_song_to_tracks.csv')
txt_to_csv(r'.\DataSet\track_metadata.txt',r'.\DataSet\track_metadata.csv')
txt_to_csv('.\DataSet\word_counts.txt','.\DataSet\word_counts.csv')