package scala.spark.first.task

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object dataframeINFO {
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
    
    // print dataframe schema
    df.printSchema()
    
    // change data types 
    val df2 = df.withColumn("NA_Sales",df("NA_Sales").cast("float"))
    .withColumn("EU_Sales",df("EU_Sales").cast("float"))
    .withColumn("JP_Sales",df("JP_Sales").cast("float"))
    .withColumn("Other_Sales",df("Other_Sales").cast("float"))
    .withColumn("Global_Sales",df("Global_Sales").cast("float"))
    .withColumn("Year",df("Year").cast("int"))
    
    //print schema after edit
  df2.printSchema()
  //creat tempview
  df2.createOrReplaceTempView("games")
  
  // the study interval 
    val years = spark.sql("SELECT MAX(Year) FROM games union SELECT MIN(Year) FROM games")
    val results_years = years.collect()
    println("\n \nYears :")
    results_years.foreach(println)
    
  //distinct game genre
    val genre = spark.sql("SELECT DISTINCT genre FROM games")
    val results_genre = genre.collect()
    println("\n \nGenres : ")
    results_genre.foreach(println)
    
    //publishers count 
     val publisher = spark.sql("SELECT COUNT(Publisher) FROM games")
    val results_pub = publisher.collect()
    println("\n \nPublisher : ")
    results_pub.foreach(println)
    
    //distinct platforms 
    val platform = spark.sql("SELECT DISTINCT Platform FROM games")
    val results_plat = platform.collect()
    println("\n \nplatform : ")
    results_plat.foreach(println)
  spark.stop()
   }
  
}