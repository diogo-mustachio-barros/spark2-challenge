/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SimpleApp {
  
  val SENTIMENT_POLARITY_HEADER = "Sentiment_Polarity"
  val AVERAGE_SENTIMENT_POLARITY_HEADER = "Average_Sentiment_Polarity"
  
  def main(args: Array[String]): Unit = {

    
    if (args.length < 2) {
      println("Expected path arguments: <googleplaystore.csv> <googleplaystore_user_reviews.csv>")
      return
    }

    val store_file = args(0)
    val user_reviews_file = args(1)
    
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    val df = spark.read
      .option("header",true)
      .csv(user_reviews_file);

    // Part 1
    part1(df).show();
    
    spark.stop()
  }
  
  def part1(df: DataFrame): DataFrame = {
      return df
        .groupBy("App")
        .agg(
          coalesce(avg(SENTIMENT_POLARITY_HEADER), lit(0)).as(AVERAGE_SENTIMENT_POLARITY_HEADER))
  }
}