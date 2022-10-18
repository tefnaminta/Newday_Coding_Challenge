package com.scala.task

import org.apache.spark.sql.functions.collect_list


object NewDay_Bonus_Exercise extends NewDay_dataframes {
  def main(args: Array[String]) : Unit ={

    val topthree_movies = spark.sql("select * from (select UserID, Title, Rating, row_number() over (partition by UserID order by Rating desc) as ranknum from mr_df)rnk where ranknum <=3")
    val top3movies_df = topthree_movies.groupBy("UserID").agg(collect_list("Title").name("Movie_Names"))
    top3movies_df.coalesce(1).write.option("header", "true").json("./results/top3movies_result")
    top3movies_df.show(5)
  }

}



