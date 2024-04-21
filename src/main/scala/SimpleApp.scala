/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SimpleApp {
  
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
      return df.filter(col("Sentiment_Polarity").isNotNull)
        .groupBy("App")
        .agg(
          coalesce(avg("Sentiment_Polarity"), lit(0)).as("Average_Sentiment_Polarity"))
  }
}