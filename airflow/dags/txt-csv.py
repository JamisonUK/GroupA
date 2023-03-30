def txt_to_csv(txt_file, csv_file):
        import pandas as pd
        df = pd.read_fwf(txt_file)
        f = df.to_csv(csv_file,  index=False)
        return f

txt_to_csv('.\DataSet\kaggle_songs.txt','.\DataSet\kaggle_songs.csv')