## Abstract

My project is intended for user who are new to Apache Spark 2.4.6. I tried to use most of the command and functions to get result. In this project simple SQL queries are performed in Spark Dataframe API. Various filter methods are used to filter required data from datasets. I used IMDB's (a reliable source to find rating) Weighted Rating method to get results.

## About Data

To collect datasets, I used Kaggle from where I get the database of MUBI SVOD Platform. MUBI could be described as the "Netflix for art movies". This data contains list of english movies and user's rating based on their social media MUBI API. I used two datasets which is in csv format. Fisrt one is 'movies' which contains data from all movies registered on MUBI. Second one is 'ratings' which contains data from ratings on MUBI for users who did not set their profile in private mode. This table goes back to 2008 and has about 15.5 million rows. I loaded these datasets into two Dataframes "raw_data_movies" and "raw_data_ratings" respectively.
Open file "Raw-Data.txt" to see dataframes and schema. The total size of data is 2.5 GB.
#### Problems with Dataframe "raw_data_movies":
* Column "movie_id" contains many missing value.
* Column "movie_title" contains many missing value.
* Column "movie_release_year" contains year in decimal format.
* Column "movie_title_language" contains some values other than english.
#### Problems with Dataframe "raw_data_ratings":
* Column "movie_id" contains many missing value.
* Various "user_id" are missing which needs to be filtered for authentic rating.
* Column "rating_score" contains various rows having value other than 0-9 like text or comment.
* There are vaious user who are not eligible to rate negative comments below 2.0.

## Methodology
My first approach is to filter out out the required data with proper schema. After that, I follow IMDB's Weighted Rating formulae to get the optimum result.
I used Weighted Rating formulae:
<br/> (v*R/(v+m))+(m*C/(v+m))
<br/> where,
<BR/> R = average rating for each movie.
<br/> v = number of votes for the movie.
<br/> m = minimum votes required to be listed in the Top 10 (i.e, 15000).
<br/> C = the mean vote across the whole report (i.e, 3.4).

## Code Breakup:
1. Movies dataset (in csv format) is loaded into Dataframe "raw_data_movies".
2. Rating dataset (in csv format) is loaded into Dataframe "raw_data_ratings".
3. Filter out the missing rows from "movie_id", "movie_title", "movie_release_year" and "movie_title_language"
4. Filter out english movies from column "movie_title_language".
5. Change year format from YYYY.0 to YYYY in column "movie_release_year". (See file "Movie-Filtered-Data.txt")
6. Casting "movies_id" as StringType
7. Renaming column "movie_id" to "movies_id".
8. In column "director_name", replacing Null value from N/A.
9. Filter columns "movies_id", "movie_title", "movie_release_year" and "director_name" into new Dataframe "movies".
10. Filter out the null values from column "movie_id" and "user_id".
11. Filter column "rating_score" rows having value not other than 0-9 like text or comment.
12. Filter column "user_eligible_for_trial" having boolean and null.
13. Casting column "rating_score" into DoubleType.
14. Replacing null from "False" in column "user_eligible_for_trial".
15. Creating new column "rating" for filter condition with Non-eligible users.(i.e., they can't rate minimum rating)
16. Grouping "movie_id" and creating two more column "avg_score" (i.e., R) and "rating_count" (i.e., v).
17. Filter minimum ratings count required to be in top 10 (i.e., 15000).
18. Adding new column "min_count" (i.e, m) usinh lit().
19. Finally creating new column "score" which is of weighted rating based on IMDB formulae into new Dataframe "ratings".
20. Joining with Inner Join two dataframe on "movies_id" and "movie_id".
21. Sorting the final score in descending order and limiting the dataframe upto 10.
22. Result....

## Conclusion

The whole project includes the leaning of Apache Spark 2.4.6 and use of most regular commands. I used my basic SQL knowledge to manipulate data and calculate the output. The output is attached with file "Result.txt".
