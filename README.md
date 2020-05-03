# 08-Movies-ETL
Module 8: ETL - Extract, Transform, Load

## Project Overview
We've been tasked by Amazing Prime Videos to Extract ( Wikipedia and Kaggle data from their respective json and csv files), Transform, and Load clean datasets of movie data so they can sponsor a hackathon. 

## Resources
- Python 3.7.6, Anaconda 4.8.3, Jupyter Notebook, Pandas, Numpy, SQLalchemy
- Postgres 11.5, PgAdmin 4, SQL
- wikipedia.movie.json, movies_metada.csv, ratings.csv to be downloaded from Wikipedia, and Kaggle

## Summary
After extracting the 3 files, we followed the same iterative procees of Inspect-Plan-Execute cycles to clean, and transform each dataset before merging them, by
- getting rid of the corrupted data
- creating functions to clean the data
- removing duplicate rows
- writing Regex to find and parse the right data
- making sure each data column is in the right format

After the data was all cleaned, parsed and merged, we connectd to our SQL server, and moved into a PostgresQSL database - the ratings database being too large, we did it by chunks.

## Challenge Overview
Write a python script that performs all 3 ETL steps on the Wikipedia and Kaggle data in an automated way, while taking care and documenting assumptions that may arise.

## Challenge Summary
challenge.py contains the function ETLchallenge that takes in 3 arguments:
  - Wikipedia data (json file)
  - Kaggle metada (csv file)
  - MovieLens rating data from Kaggle (csv file)
  
We refactored our code, and removed all data analysis steps. Most assumptions are linked with the data being what we expect, and not corrupted. So each Transform step - transforming/cleaning/parsing a data column of interest - is included in a try/except block, dropping the whole column in case of fully corrupted data, and logging the error.

The loading process deletes the data from each table (or create a table if non-existant) before loading the new data.
We decided to load the movies-with-ratings data to help with the hackathon - people involved can still query the ratings table but the merging has already been done to help with their resources.

We created an automated pipeline that loads 3 clean tables at the end of this ETL process:
- Movies: table of movies with data info in more than 30 columns - 5/3/2020: 6052 rows, 31 columns
- Movies_ratings: table of same 6051 movies with info and their ratings - 5/3/2020: 6052 rows, 41 columns
- Ratings: massive table of individual ratings - 5/3/2020: 26,024,289 rows, 4 columns


