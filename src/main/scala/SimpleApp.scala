/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession

object SimpleApp {
  
  def main(args: Array[String]): Unit = {
    
    if (args.length < 2) {
      println("Expected path arguments: <googleplaystore.csv> <googleplaystore_user_reviews.csv>")
      return
    }

    val store_file = args(0)
    val user_reviews_file = args(1)
    
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    
    // val logData = spark.read.textFile(logFile).cache()
    // val numAs = logData.filter(line => line.contains("a")).count()
    // val numBs = logData.filter(line => line.contains("b")).count()
    // println(s"Lines with a: $numAs, Lines with b: $numBs")

    val df = spark.read
      .option("header",true)
      .csv(user_reviews_file)
    
    df.printSchema()

    spark.stop()
  }
}