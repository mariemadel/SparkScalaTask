package scala.spark.first.task

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object QueryGamesScala {
  def main(args: Array[String]) {
     
    // Set the log level to only print errors
     Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Use new SparkSession interface in Spark 2.0
      val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
   
    // Load up the dataframe
    import spark.implicits._
    val df = spark.read.option("header",true).csv("../archive/vgsales.csv")
    df.createOrReplaceTempView("tempdf")
    val df2 = spark.sql("SELECT Name,Platform,Genre,Publisher ,INT(Year),FLOAT(NA_Sales),FLOAT(EU_Sales),FLOAT(JP_Sales),FLOAT(Other_Sales),FLOAT(Global_Sales),FLOAT(NA_Sales) from tempdf").cache()
    df2.printSchema()
    df2.createOrReplaceTempView("games")
    
    //industry growth 
    val growth_max = spark.sql("SELECT Year, SUM (Global_Sales) as Total_Sales FROM games GROUP BY Year ORDER by Total_Sales DESC LIMIT 10 ")
    val results_growth_max = growth_max.collect()
    println("\n \nindustry growth top 10 : ")
    results_growth_max.foreach(println)
    
    val growth_min = spark.sql("SELECT Year, SUM (Global_Sales) as Total_Sales FROM games GROUP BY Year ORDER by Total_Sales LIMIT 10 ")
    val results_growth_min = growth_min.collect()
    println("\n \nindustry growth lowest 10 : ")
    results_growth_min.foreach(println)
    
    //most profitable platform
    println("\n \n Globale sales for top 10 platforms : ")
    val top_platform = spark.sql("SELECT Platform ,SUM(Global_Sales) AS total  From games GROUP BY Platform order by total DESC limit 10")
    val results_top_platform = top_platform.collect()
    results_top_platform.foreach(println)
    
    //most profitable platform
    println("\n \n Globale sales for games genre : ")
    val top_genre = spark.sql("SELECT Genre, SUM (Global_Sales) as Total_Sales FROM games GROUP BY Genre ORDER by Total_Sales DESC LIMIT 10 ")
    val results_top_genre = top_genre.collect()
    results_top_genre.foreach(println)
    
    //Game genre vs platform
    println("\n \n top 10 profitable platform vs genre : ")
    val genre_vs_platform = spark.sql("SELECT Platform,Genre,SUM(Global_Sales) as total From games GROUP BY Platform,Genre order by total DESC limit 10")
    val results_genre_vs_platform = genre_vs_platform.collect()
    results_genre_vs_platform.foreach(println)
    
    //Game genre vs location (most popular in NA, EU, JP vs global)
    println("games's genre profits sorted by global profit")
    val top_genre_location = spark.sql("SELECT Genre, SUM (Global_Sales)as Total,SUM (JP_Sales) as JP,SUM (EU_Sales) as EU FROM games GROUP BY Genre ORDER by Total DESC LIMIT 10 ").show()
    println("games's genre profits sorted by Japan's profit")
    val top_genre_location_jp = spark.sql("SELECT Genre, SUM (Global_Sales)as Total,SUM (JP_Sales) as JP,SUM (EU_Sales) as EU FROM games GROUP BY Genre ORDER by JP DESC LIMIT 10 ").show()
    println("games's genre profits sorted by Europe's profit")
    val top_genre_location_eu = spark.sql("SELECT Genre, SUM (Global_Sales)as Total,SUM (JP_Sales) as JP,SUM (EU_Sales) as EU FROM games GROUP BY Genre ORDER by EU DESC LIMIT 10 ").show()
    
    spark.stop()
   }
  
}