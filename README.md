# Spark Scala Task
## Introduction 
Analyzing [Video Game Sales dataset](https://www.kaggle.com/gregorut/videogamesales) using spark scala 
##  Content 
The repository includes three scala objects 

| Scala Object name | Description |
| ----------- | ----------- |
| DataFrameINFO | class to gain insights about data |
| GameSales | analyzing data using scala API |
|QueryGameScala| analyzing data using spark sql|

## Approach 
Answering the following questions :
1. Does the games industry grow? (sales vs years)
2. Game genre vs platform 
3. Game genre vs location (most popular in NA, EU, JP vs global)

## Code snaps 
| API | SQL |
| ----------- | ----------- |
| `games.groupBy("year").sum("Global_Sales").orderBy($"sum(Global_Sales)".desc).show(10)` | `val growth_max = spark.sql("SELECT Year, SUM (Global_Sales) as Total_Sales FROM games GROUP BY Year ORDER by Total_Sales DESC LIMIT 10 ")` |
| `games.groupBy("Genre","Platform").sum("Global_Sales").orderBy($"sum(Global_Sales)".desc).show(10)` | `val genre_vs_platform = spark.sql("SELECT Platform,Genre,SUM(Global_Sales) as total From games GROUP BY Platform,Genre order by total DESC limit 10")` |
|` games.groupBy("Genre").sum("Global_Sales").orderBy($"sum(Global_Sales)".desc).show(10)`| `val top_genre_location = spark.sql("SELECT Genre, SUM (Global_Sales)as Total,SUM (JP_Sales) as JP,SUM (EU_Sales) as EU FROM games GROUP BY Genre ORDER by Total DESC LIMIT 10 ").show()`|
