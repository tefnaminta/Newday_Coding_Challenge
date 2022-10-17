# Newday_Coding_Challenge

The task completed with the following:
1. Readed movies.csv and ratings.csv to spark dataframes.
2. Created a new dataframe, which contains the movies data and 3 new columns max, min and average rating for that movie from the ratings data.
3. BONUS: Created a new dataframe which contains each user’s (userId in the ratings data) top 3 movies based on their rating.
4. Written the original and new dataframes in json format.

The command to run jar:
spark-submit --master local[1] --class Newday_Main out/artifacts/Newday_Coding_Challenge_jar/Newday_Coding_Challenge.jar
