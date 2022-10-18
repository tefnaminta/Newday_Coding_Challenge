package com.scala.task

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
class NewDay_dataframes {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("NewDay")
    .getOrCreate();

  val movie_schema = StructType(Array(
    StructField("MovieID", IntegerType),
    StructField("Title", StringType),
    StructField("Genres", StringType)
  ))

  val rating_schema = StructType(Array(
    StructField("UserID", IntegerType),
    StructField("MovieID", IntegerType),
    StructField("Rating", IntegerType),
    StructField("Timestamp", LongType)
  ))
  val movies_df = spark.read.option("delimiter", "::").schema(movie_schema).csv(spark.read.textFile("./src/main/scala/ml-1m/movies.dat"))
  val ratings_df = spark.read.option("delimiter", "::").schema(rating_schema).csv(spark.read.textFile("./src/main/scala/ml-1m/ratings.dat"))

  val movie_rating_df = movies_df.join(ratings_df, movies_df("MovieID") === ratings_df("MovieID"))
  movie_rating_df.createOrReplaceTempView("mr_df")
}
