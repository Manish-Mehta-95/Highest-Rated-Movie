package com.movie.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}

object Ten_Highest_Rated_Movies extends App {
  val spark = SparkSession
    .builder()
    .appName("Ten_Highest_Rated_Movies")
    .master("local")
    .getOrCreate()

  //importing raw movies data from csv
  val raw_data_movies = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:/Spark Demo/mubi_movie_data.csv")

  //importing raw rating data from csv
  val raw_data_ratings = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:/Spark Demo/mubi_ratings_data.csv")

  //Filtering english movies from raw dataframe
  val movies = raw_data_movies
    .filter(col("movie_id").isNotNull &&
      col("movie_title").isNotNull &&
      col("movie_title_language") === "en" && //filtering english movies
      col("movie_release_year").isNotNull)
    .withColumn("movie_release_year",
      expr("substring(movie_release_year, 1, length(movie_release_year)-2)")) //change year from yyyy.0 to yyyy
    .withColumn("movie_id", raw_data_movies("movie_id")
      .cast(StringType))
    .withColumnRenamed("movie_id", "movies_id") // renaming column
    .na.fill("N/A", Seq("director_name")) // replacing null value from N/A
    .select("movies_id", "movie_title", "movie_release_year", "director_name")

  //Filtering ratings from raw dataframe
  val filtered_rating = raw_data_ratings
    .filter(col("movie_id").isNotNull &&
      col("user_id").isNotNull)
    .filter(col("rating_score")
      .rlike("^[0-9]" + "." + "[0-9]")) // Filtering ratings from string i.e., words/lines
    .filter(col("user_eligible_for_trial")
      .isin("True", "False", "Null")) //filtering boolean value or null
    .select("movie_id", "rating_score", "user_eligible_for_trial")
    .withColumn("rating_score", raw_data_ratings("rating_score")
      .cast(DoubleType))
    .na.fill("False", Seq("user_eligible_for_trial")) //replacing null from false
    .withColumn("rating", when(col("user_eligible_for_trial") === "False" && //condition
      col("rating_score") < "2.0", lit("2.0") //non eligible user cannot rate less than 2.0
      .cast(DoubleType))
      .otherwise(col("rating_score")))

  //Calculating final score
  val ratings = filtered_rating
    .groupBy("movie_id")
    .agg(round(avg("rating"), 2) //rounding off up to 2 decimal point
      .alias("avg_score"), count("user_eligible_for_trial")
      .alias("rating_count"))
    .filter(col("rating_count") > "15000") // minimum ratings count required to be in top 10
    .withColumn("min_count", lit("15000")) //adding column of min count
    .withColumn("score",
      round(col("rating_count") / (col("rating_count") + //formulae ((v/(v+m))*R)+((M/(V+M))*C)
        col("min_count")) * col("avg_score") +
        (col("min_count") / (col("rating_count") +
          col("min_count")) * 3.4), 2)) //average ratings of report is 3.4

  //Joining two dataframes
  val Result = ratings
    .join(movies, col("movie_id") === col("movies_id"), "inner") //joining dataframe
    .select(col("movie_id").alias("Movie_ID"),
      col("movie_title").alias("Movie_Name"),
      col("movie_release_year").alias("Release_Year"),
      col("director_name").alias("Directed_By"),
      col("score").alias("Final_Score"))
    .sort(desc("Final_Score"), asc("Release_Year")).limit(10) //limiting output

  //Result
  Result.show(false)
}
