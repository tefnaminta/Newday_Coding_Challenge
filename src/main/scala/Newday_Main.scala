package com.scala.task

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, LongType}
import org.apache.spark.sql.functions._


object Newday_Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("NewDay")
      .getOrCreate();

    val movie_schema = StructType(Array(
      StructField("MovieID",IntegerType),
      StructField("Title", StringType),
      StructField("Genres",StringType)
    ))

    val rating_schema = StructType(Array(
      StructField("UserID", IntegerType),
      StructField("MovieID", IntegerType),
      StructField("Rating", IntegerType),
      StructField("Timestamp", LongType)
    ))

    val movies_df = spark.read.option("delimiter","::").schema(movie_schema).csv(spark.read.textFile("./src/main/scala/ml-1m/movies.dat"))
    val ratings_df = spark.read.option("delimiter","::").schema(rating_schema).csv(spark.read.textFile("./src/main/scala/ml-1m/ratings.dat"))
    movies_df.coalesce(1).write.option("header", "true").json("./results/movie data")
    ratings_df.coalesce(1).write.option("header","true").json("./results/rating data")


    val movie_rating_df = movies_df.join(ratings_df, movies_df("MovieID") === ratings_df("MovieID"))
    movie_rating_df.createOrReplaceTempView("mr_df")
    val movie_rating_result = spark.sql("SELECT Title, MAX(Rating) as MaxRating, MIN(Rating) as MinRating, AVG(Rating) as AverageRating FROM mr_df group by Title")
    movie_rating_result.coalesce(1).write.option("header", "true").json("./results/movie_rating_result")


    val topthree_movies = spark.sql("select * from (select UserID, Title, Rating, row_number() over (partition by UserID order by Rating desc) as ranknum from mr_df)rnk where ranknum <=3")
    val top3movies_df = topthree_movies.groupBy("UserID").agg(collect_list("Title").name("Movie_Names"))
    top3movies_df.coalesce(1).write.option("header", "true").json("./results/top3movies_result")
  }
}

