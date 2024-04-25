/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.File

object SimpleApp {
  // DataFrames
  var appsDF: DataFrame = null
  var userReviewsDF: DataFrame = null
  
  def main(args: Array[String]): Unit = {
    // If not enough args, stop
    if (args.length < 2) {
      println("Expected path arguments: <googleplaystore.csv> <googleplaystore_user_reviews.csv>")
      return
    }

    // Get file paths from args
    val appsFile        = args(0)
    val userReviewsFile = args(1)
    
    // Initialize Spark
    val spark = SparkSession.builder
      .appName("Simple Application")
      .getOrCreate()

    // Initialize DataFrames
    this.appsDF        = spark.read.option("header", true).csv(appsFile)
    this.userReviewsDF = spark.read.option("header", true).csv(userReviewsFile)

    // Part 1
    // part1()

    // Part 2
    // part2()

    // Part 3
    // part3()

    // Part 4
    part4()

    // Part 5
    // part5()
    
    spark.stop()
  }
  
  def part1(): Unit = {
    Spark.getAverageSentimentPolarityByApp(userReviewsDF).show()
  }

  def part2(): Unit = {
    // TODO: improve, check https://sparkbyexamples.com/spark/spark-write-dataframe-single-csv-file/
    toCsvFolder( Spark.getAppsWithRatingGreaterOrEqual(appsDF, 4.0)
               , "best_apps.csv")
  }
  
  def part3(): Unit = {
    Spark.getSquashedApps(appsDF).show()
  }
  
  def part4(): Unit = {
    toParquetGzipFolder( Spark.getSquashedAppsWithAverageSentiment(appsDF, userReviewsDF)
                       , "googleplaystore_cleaned")
  }
  
  def part5(): Unit = {
    toParquetGzipFolder( Spark.getGooglePlayStoreMetrics(appsDF, userReviewsDF)
                       , "googleplaystore_metrics")
  }

  // Creates a folder named <outputFilePath>  with a single file
  def toCsvFolder(df: DataFrame, outputFilePath: String): Unit = {
    // If file/folder already exists, delete it
    val outputFile = new File(outputFilePath)
    Util.deleteRecursively(outputFile)

    df.coalesce(1)
      .write.options(Map("header"->"true", "delimiter"->"§"))
            .csv(outputFilePath)
  }

  // Creates a single file named <outputFilePath> by joining all partitions
  // def toCsvFileNaive(df: DataFrame, outputFilePath: String): Unit = {
  //   // TODO
  // }

  // Creates a single file named <outputFilePath> by joining all partition files
  // def toCsvFile(df: DataFrame, outputFilePath: String): Unit = {
  //   // TODO
  // }


  def toParquetGzipFolder(df: DataFrame, outputFilePath: String): Unit =  {
    // If file already exists, delete it
    val outputFile = new File(outputFilePath)
    Util.deleteRecursively(outputFile)

    df.coalesce(1)
      .write.option("compression", "gzip")
            .parquet(outputFilePath)
  }
}