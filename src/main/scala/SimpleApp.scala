/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SimpleApp {
  
  // DataFrame headers
  val SENTIMENT_POLARITY_HEADER: String = "Sentiment_Polarity"
  val AVERAGE_SENTIMENT_POLARITY_HEADER: String = "Average_Sentiment_Polarity"

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
    part1()
    
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
}