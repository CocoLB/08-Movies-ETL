# import dependencies
import json
import pandas as pd
import numpy as np
import re
import psycopg2
from sqlalchemy import create_engine
from config import db_password
import time
import sys
import traceback

# directory path
file_dir = "C:/Users/cocow/OneDrive/Documents/Data Projects/08-Movies-ETL/"



def ETLchallenge(wikijson, kagmeta, kagrating):
   
    # open and read the 3 files
    
    # ASSUMPTION: RIGHT 3 FILES NAMES AND PATHS - IF NOT,  ERROR MESSAGE, PROGRAM STOPS
    try:
        with open(f'{file_dir}{wikijson}', mode='r') as file:
            wiki_movies_raw = json.load(file)
        kaggle_metadata = pd.read_csv(f'{file_dir}{kagmeta}', low_memory = False )
        ratings = pd.read_csv(f'{file_dir}{kagrating}')  
    except FileNotFoundError:
        print("PLEASE WRITE THE CORRECT FILE NAMES OR IMPORT THEM FIRST IN THE RIGHT FOLDER")
        raise
    
    # clean and filter the wiki data first
    # we decided to keep only movies with a director  in the Imdb database
    wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]
    
    # ASSUMPTION: THE WIKIPEDIA FILES ALWAYS INCLUDES THESE COLUMNS - IF NOT WE'LL END UP WITH EMPTY TABLES
    if wiki_movies==[]:
        print("THE WIKIPEDIA FILE SEEMS NOT TO INCLUDE ANY MOVIES WITH A DIRECTOR OR AN IMDB LINK")
       
    # keep all aternate titles together in a new column, and change column names- NESTED FUNCTIONS
    # list of alternate languages
    list_alt_languages = ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune–Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']
    
    def clean_movie(movie):
        alt_titles = {}
        for key in list_alt_languages:
            if key in movie:
                alt_titles[key]=movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles
       
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')   
        
        return movie
    
    # ASSUMPTION: THERE ARE ALTERNATE TITLES, AND SOME OF THESE COLUMNS ARE INCLUDED - IF NOT, THE SAME FILE WILL BE RETURNED
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)
    
    # remove duplicate rows - checking with unique imdb id that we get from imdb link - IF ANY
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
   
    # keeping columns with less than 90% NaN
    # ASSUMPTION: THERE ARE ALWAYS SEVERAL COLUMNS WITH LESS THAN 90% NaN - IF NOT, PRINT AND KEEP THE UNCHANGED DF
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    if len(wiki_columns_to_keep)>0:
        wiki_movies_df = wiki_movies_df[wiki_columns_to_keep] 
    else:
        print("THERE ARE MORE THAN 90% NaNs IN EACH COLUMN!")
    
    # create function to parse dollars for box office and budget columns
    def parse_dollars(s):
        if type(s) != str:
            return np.nan
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            value = float(s) * 10**6
            return value
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            value = float(s) * 10**9
            return value
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
            s = re.sub('\$|,','', s)
            value = float(s)
            return value
        else:
            return np.nan
    
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    
    # parse box office column
    # ASSUMPTION: CELLS INCLUDE NUMBERS DATA IN ANY FORMAT - IF NONE, DATA CORRUPTED, DROP THE WHOLE COLUMN
    try:
        box_office = wiki_movies_df['Box office'].dropna() 
        box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
        box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars) 
        wiki_movies_df.drop('Box office', axis=1, inplace=True)
    except:
        traceback.print_exc()
        wiki_movies_df.drop('Box office', axis=1, inplace=True)
        print("BOX OFFICE COLUMN CORRUPTED, NO NUMBER IN IT - DROPPED")
    
    # parse budget column
    # ASSUMPTION: CELLS INCLUDE NUMBERS DATA IN ANY FORMAT - IF NONE, DATA CORRUPTED, DROP THE WHOLE COLUMN
    try:
        budget = wiki_movies_df['Budget'].dropna()
        budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        wiki_movies_df.drop('Budget', axis=1, inplace=True)
    except:
        traceback.print_exc()
        wiki_movies_df.drop('Budget', axis=1, inplace=True)
        print("BUDGET COLUMN CORRUPTED, NO NUMBER IN IT - DROPPED")
    
    # parse release date column
    # ASSUMPTION, CELLS INCLUDE SOME KIND OF DATE DATA - IF NONE, DATA CORRUPTED, DROP THE WHOLE COLUMN
    try:
        release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
        wiki_movies_df.drop('Release date', axis=1, inplace=True)
    except:
        traceback.print_exc()
        wiki_movies_df.drop('Release date', axis=1, inplace=True)
        print("RELEASE DATE COLUMN CORRUPTED, NO DATE IN IT - DROPPED")
    
    # parse running time column
    # ASSUMPTION, CELLS INCLUDE SOME KIND OF TIME DATA FORMATS - IF NONE, DATA CORRUPTED, DROP THE WHOLE COLUMN
    try:
        running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m').apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
        wiki_movies_df.drop('Running time', axis=1, inplace=True)
    except:
        traceback.print_exc()
        wiki_movies_df.drop('Running time', axis=1, inplace=True)
        print("RUNNING TIME COLUMN CORRUPTED, NO DATE IN IT - DROPPED")
    
    # wiki data cleaned and parsed
    # clean kaggle data - change types
    # keep non_adult movies and drop column
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    # change type of video column top bool
    # ASSUMPTION: CONTAINS ONLY TRUE AND FALSE VALUES - IF NOT,CORRUPTED, DROP IT
    try:
        kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    except:
        traceback.print_exc()
        kaggle_metadata = kaggle_metadata.drop('video', axis='columns')
        print("VIDEO COLUMN CORRUPTED, NO BOOLEAN - DROPPED")
    
    # convert numeric columns using "to_numeric()"
    # ASSUMPTION: THEY ALL INCLUDE NUMERICAL RIGHT VALUES/FORMAT - IF NOT,CORRUPTED, DROP IT
    try:
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    except:
        traceback.print_exc()
        kaggle_metadata = kaggle_metadata.drop('budget', axis='columns')
        print("BUDGET COLUMN CORRUPTED, NO NUMBER- DROPPED")
        
    try:    
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    except:
        traceback.print_exc()
        kaggle_metadata = kaggle_metadata.drop('id', axis='columns')
        print("id COLUMN CORRUPTED, NO Id- DROPPED")   
        
    try:   
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    except:
        traceback.print_exc()
        kaggle_metadata = kaggle_metadata.drop('popularity', axis='columns')
        print("POPULARITY COLUMN CORRUPTED- DROPPED")      
        
        
    # convert "release_date" column (already in standart format) to datetime
    # ASSUMPTION: ALREADY IN A DATE FORMAT - IF NOT,CORRUPTED, DROP IT
    try:
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    except:
        traceback.print_exc()
        kaggle_metadata = kaggle_metadata.drop('release_date', axis='columns')
        print("RELEASE DATE COLUMN CORRUPTED, NO DATE- DROPPED")
    
    #clean kaggle ratings
    # convert timestamp column to datetime
    # ASSUMPTION: ALREADY IN DATE FORMAT - IF NOT,CORRUPTED, DROP IT
    try:
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    except:
        traceback.print_exc()
        ratings = ratings.drop('timestamp', axis='columns')
        print("TIMESTAMP COLUMN CORRUPTED, NO DATE- DROPPED")
    
    # Data cleaned
    
    # Merge wiki movies and kaggle metadata
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    # drop redundant columns
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
   
    # function filling in missing data for a column pair and dropping the redundant column
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
        df.drop(columns=wiki_column, inplace=True)
    
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    
    # reorder the columns we only keep for easier read
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]
    # rename the columns
    movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)
    
    # wiki and kaggle metadata merged
    # merge with ratings
    # counting how many times each movie received a given rating
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count().rename({'userId':'count'}, axis=1) 
    # pivot the Data so movieId becomes the Index, columns are all the rating values  and rows the counts for each rating
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count().rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')
    # replace NaN values by 0
    rating_counts = rating_counts.fillna(0)
    # rename columns for easier understanding after merge
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    # left merge movies _df--> rating_counts
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    
    # Data ready to load
    # movies_df, movies_with_ratings_df and ratings
    # Connect to the Database
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)
    # import the Movie Data
    movies_df.to_sql(name='movies', con=engine, if_exists="replace")
    # import the Movies_with_ratings
    movies_with_ratings_df.to_sql(name='movies_ratings', con=engine, if_exists="replace")
    
    # import the Ratings Data
    # create a variable for the number of rows imported
    rows_imported = 0
    # get the start_time from time.time()
    start_time = time.time()
    for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
        # print out the range of rows that are being imported
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        if rows_imported == 0:
            data.to_sql(name='ratings', con=engine, if_exists='replace')
        else:    
            data.to_sql(name='ratings', con=engine, if_exists='append')
    
        # increment the number of rows imported by the size of 'data'
        rows_imported += len(data)

        # add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed')


ETLchallenge("wikipedia.movies.json", "movies_metadata.csv", "ratings.csv")        