package com.scala.task

object NewDay_Main_Exercise extends NewDay_dataframes {
  def main(args: Array[String]) : Unit ={

    val movie_rating_result = spark.sql("SELECT Title, MAX(Rating) as MaxRating, MIN(Rating) as MinRating, AVG(Rating) as AverageRating FROM mr_df group by Title")
    movie_rating_result.show(5)

    movies_df.coalesce(1).write.option("header", "true").parquet("./results/movie_data")
    ratings_df.coalesce(1).write.option("header", "true").parquet("./results/rating_data")
    movie_rating_result.coalesce(1).write.option("header", "true").parquet("./results/movie_rating_result")
  }

}
