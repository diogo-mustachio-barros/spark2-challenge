/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SimpleApp {
  
  // DataFrame headers
  val SENTIMENT_POLARITY_HEADER: String = "Sentiment_Polarity"
  val AVERAGE_SENTIMENT_POLARITY_HEADER: String = "Average_Sentiment_Polarity"
  val RATING_HEADER: String = "Rating"

  // DataFrames
  var appsDf: DataFrame = null
  var userReviewsDf: DataFrame = null
  
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
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    // Initialize DataFrames
    this.appsDf        = spark.read.option("header", true).csv(appsFile)
    this.userReviewsDf = spark.read.option("header", true).csv(userReviewsFile)

    // Part 1
    // part1()

    // Part 2
    part2()
    
    spark.stop()
  }
  
  def part1(): Unit = {
    getAverageSentimentPolarityByApp().show()
  }

  def getAverageSentimentPolarityByApp(): DataFrame = {
    return this.userReviewsDf
      .groupBy("App")
      .agg(
        coalesce(avg(SENTIMENT_POLARITY_HEADER), lit(0)).as(AVERAGE_SENTIMENT_POLARITY_HEADER))
  }



  def part2(): Unit = {
    // TODO: If file already exists, delete it

    val df = getAppsWithRatingGreaterOrEqual(4.0)
      .orderBy(desc(RATING_HEADER))
    
    // TODO: improve, check https://sparkbyexamples.com/spark/spark-write-dataframe-single-csv-file/
    toCsvFolder(df, "best_apps.csv")
  }
  
  def getAppsWithRatingGreaterOrEqual(rating: Double): DataFrame = {
    return this.appsDf
      .filter(col(RATING_HEADER) >= rating && col(RATING_HEADER) =!= Double.NaN)
  }

  // Creates a folder named <outputFilePath>  with a file for each spark partition
  def toCsvFolderPartitioned(df: DataFrame, outputFilePath: String): Unit = {
    df.write.options(Map("header"->"true", "delimiter"->","))
            .csv(outputFilePath)
  }

  // Creates a folder named <outputFilePath>  with a single file
  def toCsvFolder(df: DataFrame, outputFilePath: String): Unit = {
    df.coalesce(1)
      .write.options(Map("header"->"true", "delimiter"->","))
            .csv(outputFilePath)
  }

  // Creates a single file named <outputFilePath> by joining all partitions
  def toCsvFileNaive(df: DataFrame, outputFilePath: String): Unit = {
    // TODO
  }

  // Creates a single file named <outputFilePath> by joining all partition files
  def toCsvFile(df: DataFrame, outputFilePath: String): Unit = {
    // TODO
  }
}