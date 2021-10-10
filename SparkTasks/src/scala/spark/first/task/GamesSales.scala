package scala.spark.first.task

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
    

object GamesSales {
         case class Game(Rank:Int, Name:String,Platform:String, Year:Int,Genre:String,Publisher:String,NA_Sales:Float,EU_Sales:Float,JP_Sales:Float,Other_Sales:Float,Global_Sales:Float)

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
    val df2 = df.withColumn("NA_Sales",df("NA_Sales").cast("float"))
    .withColumn("EU_Sales",df("EU_Sales").cast("float"))
    .withColumn("JP_Sales",df("JP_Sales").cast("float"))
    .withColumn("Other_Sales",df("Other_Sales").cast("float"))
    .withColumn("Global_Sales",df("Global_Sales").cast("float"))
    .withColumn("Year",df("Year").cast("int"))
    df2.printSchema()
    df2.createOrReplaceTempView("games")
    import spark.implicits._
    val games = df2.cache()
    
    //industry growth 
    println("\n \nindustry growth top 10 : ")
    games.groupBy("year").sum("Global_Sales").orderBy($"sum(Global_Sales)".desc).show(10)
    println("\n \nindustry growth lowest 10 : ")
    games.groupBy("year").sum("Global_Sales").orderBy("sum(Global_Sales)").show(10)

    //most profitable platform
    println("\n \n Globale sales for top 10 platforms : ")
    games.groupBy("Platform").sum("Global_Sales").orderBy($"sum(Global_Sales)".desc).show(10)    
    //most profitable platform
    println("\n \n Globale sales for games genre : ")
    games.groupBy("Genre").sum("Global_Sales").orderBy($"sum(Global_Sales)".desc).show()  
    //Game genre vs platform
    println("\n \n top 10 profitable platform vs genre : ")
    games.groupBy("Genre","Platform").sum("Global_Sales").orderBy($"sum(Global_Sales)".desc).show(10)

  //Game genre vs location (most popular in EU, JP vs global)
    println("games's genre profits sorted by global profit")
    games.groupBy("Genre").sum("Global_Sales").orderBy($"sum(Global_Sales)".desc).show(10)
    println("games's genre profits sorted by Japan's profit")
    games.groupBy("Genre").sum("JP_Sales").orderBy($"sum(JP_Sales)".desc).show(10)
    println("games's genre profits sorted by Europe's profit")
    games.groupBy("Genre").sum("EU_Sales").orderBy($"sum(EU_Sales)".desc).show(10)

  spark.stop()
   }
}