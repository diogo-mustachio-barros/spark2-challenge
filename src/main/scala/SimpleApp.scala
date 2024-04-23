/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window}
import org.apache.spark.sql.functions._
import java.io.File

object SimpleApp {
  
  // DataFrame headers
  val APP_HEADER: String = "App"
  val CATEGORY_HEADER: String = "Category"
  val RATING_HEADER: String = "Rating"
  val REVIEWS_HEADER: String = "Reviews"
  val SIZE_HEADER: String = "Size"
  val INSTALLS_HEADER: String = "Installs"
  val TYPE_HEADER: String = "Type"
  val PRICE_HEADER: String = "Price"
  val CONTENT_RATING_HEADER: String = "Content Rating"
  val GENRES_HEADER: String = "Genres"
  val LAST_UPDATED_HEADER: String = "Last Updated"
  val CURRENT_VERSION_HEADER: String = "Current Ver"
  val ANDROID_VERSION_HEADER: String = "Android Ver"

  val SENTIMENT_POLARITY_HEADER: String = "Sentiment_Polarity"
  val TRANSLATED_REVIEW_HEADER: String = "Translated_Review"
  val SENTIMENT_HEADER: String = "Sentiment"

  val AVERAGE_SENTIMENT_POLARITY_HEADER: String = "Average_Sentiment_Polarity"

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
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

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
    
    spark.stop()
  }
  
  def part1(): Unit = {
    getAverageSentimentPolarityByApp().show()
  }

  def getAverageSentimentPolarityByApp(): DataFrame = {
    return this.userReviewsDF
      .groupBy(APP_HEADER)
      .agg(
        coalesce(avg(SENTIMENT_POLARITY_HEADER), lit(0)).as(AVERAGE_SENTIMENT_POLARITY_HEADER))
  }



  def part2(): Unit = {
    val outputPath = "best_apps.csv"

    // If file/folder already exists, delete it
    val outputFile = new File(outputPath)
    Util.deleteRecursively(outputFile)

    val df = getAppsWithRatingGreaterOrEqual(4.0)
      .orderBy(desc(RATING_HEADER))
    
    // TODO: improve, check https://sparkbyexamples.com/spark/spark-write-dataframe-single-csv-file/
    toCsvFolder(df, outputPath)
  }
  
  def getAppsWithRatingGreaterOrEqual(rating: Double): DataFrame = {
    return this.appsDF
      .filter(col(RATING_HEADER) >= rating && col(RATING_HEADER) =!= Double.NaN)
  }

  // Creates a folder named <outputFilePath>  with a file for each spark partition
  def toCsvFolderPartitioned(df: DataFrame, outputFilePath: String): Unit = {
    df.write.options(Map("header"->"true", "delimiter"->"§"))
            .csv(outputFilePath)
  }

  // Creates a folder named <outputFilePath>  with a single file
  def toCsvFolder(df: DataFrame, outputFilePath: String): Unit = {
    df.coalesce(1)
      .write.options(Map("header"->"true", "delimiter"->"§"))
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



  def part3(): Unit = {
    getSquashedApps().show()
  }

  def getSquashedApps(): DataFrame = {
    val w = Window.partitionBy(APP_HEADER)

    val maxReviewsDF = appsDF
      .groupBy(APP_HEADER)
      .agg(max(REVIEWS_HEADER) as REVIEWS_HEADER)

    val categoriesDF = appsDF
      .groupBy(APP_HEADER)
      .agg(collect_set(CATEGORY_HEADER) as "Categories")

    val sizeStringToDouble = udf((size: String) => {
      if (size.endsWith("M")) {
        size.dropRight(1).toDouble
      } else if (size.endsWith("k")) {
        size.dropRight(1).toDouble / 1000
      } else {
        0.0
      }
    })

    val priceStringToEuro = udf((price: String) => {
      if (price.equals("0")) {
        0.0
      } else if (price.startsWith("$")) {
        // TODO: maybe round it to two decimal places?
        price.drop(1).toDouble * 0.9
      } else {
        0.0
      }
    })

    return this.appsDF
      // Squash apps to only the one with max reviews
      .join(maxReviewsDF, Seq(APP_HEADER, REVIEWS_HEADER), "inner")
      // Join with list of categories for each app
      .join(categoriesDF, APP_HEADER)
      // Modify other columns
      .withColumn(RATING_HEADER, col(RATING_HEADER).cast("double"))
      .withColumn(REVIEWS_HEADER, col(REVIEWS_HEADER).cast("long"))
      .withColumn(SIZE_HEADER, sizeStringToDouble(col(SIZE_HEADER)))
      .withColumn(PRICE_HEADER, priceStringToEuro(col(PRICE_HEADER)))
      .withColumn(GENRES_HEADER, split(col(GENRES_HEADER), ";"))
      .withColumn(LAST_UPDATED_HEADER, to_date(col(LAST_UPDATED_HEADER), "MMMM dd, yyyy"))
      // Only the columns we need by the order we want (+ renames)
      .select(
        col(APP_HEADER),
        col("Categories"),
        col(RATING_HEADER),
        col(REVIEWS_HEADER),
        col(SIZE_HEADER),
        col(INSTALLS_HEADER),
        col(TYPE_HEADER),
        col(PRICE_HEADER),
        col(CONTENT_RATING_HEADER) as "Content_Rating",
        col(GENRES_HEADER),
        col(LAST_UPDATED_HEADER) as "Last_Updated",
        col(CURRENT_VERSION_HEADER) as "Current_Version",
        col(ANDROID_VERSION_HEADER) as "Minimum_Android_Ver"
        )
  }


  def part4(): Unit = {
    val df = this.getSquashedApps().join(getAverageSentimentPolarityByApp(), APP_HEADER)
    val outputPath = "googleplaystore_cleaned"

    // If file already exists, delete it
    val outputFile = new File(outputPath)
    Util.deleteRecursively(outputFile)

    toParquetGzipFolder(df, outputPath)
  }

  def toParquetGzipFolder(df: DataFrame, outputFilePath: String): Unit =  {
    df.write.option("compression", "gzip")
            .parquet(outputFilePath)
  }
}