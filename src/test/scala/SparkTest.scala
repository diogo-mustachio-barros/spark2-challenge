import org.scalatest.funsuite.AnyFunSuite

import com.holdenkarau.spark.testing.DataFrameSuiteBase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

class SparkTest extends AnyFunSuite with DataFrameSuiteBase {

    val DELTA: Double = 0.0001

    val APPS_SCHEMA = StructType(List(
        StructField(Spark.APP_HEADER, StringType, true), 
        StructField(Spark.CATEGORY_HEADER, StringType, true),
        StructField(Spark.RATING_HEADER, StringType, true),
        StructField(Spark.REVIEWS_HEADER, StringType, true),
        StructField(Spark.SIZE_HEADER, StringType, true),
        StructField(Spark.INSTALLS_HEADER, StringType, true),
        StructField(Spark.TYPE_HEADER, StringType, true),
        StructField(Spark.PRICE_HEADER, StringType, true),
        StructField(Spark.CONTENT_RATING_HEADER, StringType, true),
        StructField(Spark.GENRES_HEADER, StringType, true),
        StructField(Spark.LAST_UPDATED_HEADER, StringType, true),
        StructField(Spark.CURRENT_VERSION_HEADER, StringType, true),
        StructField(Spark.ANDROID_VERSION_HEADER, StringType, true)
    ))

    val USER_REVIEWS_SCHEMA = StructType(List(
        StructField(Spark.APP_HEADER, StringType, true), 
        StructField(Spark.TRANSLATED_REVIEW_HEADER, StringType, true),
        StructField(Spark.SENTIMENT_HEADER, StringType, true),
        StructField(Spark.SENTIMENT_POLARITY_HEADER, StringType, true),
        StructField(Spark.SENTIMENT_SUBJECTIVITY_HEADER, StringType, true)
    ))

    test("Spark.getAverageSentimentPolarityByApp.validNeutral") {
        // Input data for the DataFrame
        val inputData = Seq(
            Row("App 1", "", "Positive",  "0.5", "nan"),
            Row("App 1", "", "Negative", "-0.5", "0.6")
        )

        val expectedData = Seq(
            Row("App 1", 0.0)
        )

        // Create DataFrames
        val inputDF = spark.createDataFrame(
            spark.sparkContext.parallelize(inputData),
            USER_REVIEWS_SCHEMA
        )

        assertDataFrameEquals( createAverageSentimentPolarityByAppDF(expectedData)
                             , Spark.getAverageSentimentPolarityByApp(inputDF)
                             )
    }

    test("Spark.getAverageSentimentPolarityByApp.valid") {
        // Input data for the DataFrame
        val inputData = Seq(
            Row("App 2", "", "Neutral" , "0.0", "0.1"),
            Row("App 2", "", "Positive", "1.0", "0.4")
        )

        val expectedData = Seq(
            Row("App 2", 0.5)
        )

        // Create DataFrame
        val inputDF = spark.createDataFrame(
            spark.sparkContext.parallelize(inputData),
            USER_REVIEWS_SCHEMA
        )

        assertDataFrameApproximateEquals( createAverageSentimentPolarityByAppDF(expectedData)
                                        , Spark.getAverageSentimentPolarityByApp(inputDF)
                                        , DELTA
                                        )
    }

    test("Spark.getAverageSentimentPolarityByApp.zero") {
        // Input data for the DataFrame
        val inputData = Seq(
            Row("App 3", "nan", "nan"     , "nan" , "nan")
        )

        val expectedData = Seq(
            Row("App 3", 0.0)
        )

        // Create DataFrame
        val inputDF = spark.createDataFrame(
            spark.sparkContext.parallelize(inputData),
            USER_REVIEWS_SCHEMA
        )

        assertDataFrameEquals( createAverageSentimentPolarityByAppDF(expectedData)
                             , Spark.getAverageSentimentPolarityByApp(inputDF)
                             )
    }

    test("Spark.getAverageSentimentPolarityByApp.partial") {
        // Input data for the DataFrame
        val inputData = Seq(
            Row("App 4", ""   , "nan"     , "nan" , "nan"),
            Row("App 4", ""   , "Negative", "1"   , "1.0")
        )

        val expectedData = Seq(
            Row("App 4", 1.0)
        )

        // Create DataFrame
        val inputDF = spark.createDataFrame(
            spark.sparkContext.parallelize(inputData),
            USER_REVIEWS_SCHEMA
        )

        assertDataFrameApproximateEquals( createAverageSentimentPolarityByAppDF(expectedData)
                                        , Spark.getAverageSentimentPolarityByApp(inputDF)
                                        , DELTA
                                        )
    }

    // 4. one row nan
    // 5. 4 rows for each above cases, eq and high rating are ordered

    test("Spark.getAppsWithRatingGreaterOrEqual.less") {
        // Input data for the DataFrame
        val inputData = Seq(
            Row("Photo Editor", "ART_AND_DESIGN", "2.1", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up")
        )

        // Create DataFrame
        val inputDF = createAppsDF(inputData)

        assertDataFrameEquals( createEmptyDataFrameWithSchema(APPS_SCHEMA)
                             , Spark.getAppsWithRatingGreaterOrEqual(inputDF, 3.5)
                             )
    }

    test("Spark.getAppsWithRatingGreaterOrEqual.equal") {
        // Input data for the DataFrame
        val inputData = Seq(
            Row("Photo Editor", "ART_AND_DESIGN", "3.5", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up")
        )

        // Create DataFrame
        val inputDF = createAppsDF(inputData)

        assertDataFrameEquals( inputDF
                             , Spark.getAppsWithRatingGreaterOrEqual(inputDF, 3.5)
                             )
    }

    test("Spark.getAppsWithRatingGreaterOrEqual.more") {
        // Input data for the DataFrame
        val inputData = Seq(
            Row("Photo Editor", "ART_AND_DESIGN", "4.2", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up")
        )

        // Create DataFrame
        val inputDF = createAppsDF(inputData)

        assertDataFrameEquals( inputDF
                             , Spark.getAppsWithRatingGreaterOrEqual(inputDF, 3.5)
                             )
    }

    test("Spark.getAppsWithRatingGreaterOrEqual.nan") {
        // Input data for the DataFrame
        val inputData = Seq(
            Row("Photo Editor", "ART_AND_DESIGN", "nan", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up")
        )

        // Create DataFrame
        val inputDF = createAppsDF(inputData)

        assertDataFrameEquals( createEmptyDataFrameWithSchema(APPS_SCHEMA)
                             , Spark.getAppsWithRatingGreaterOrEqual(inputDF, 3.5)
                             )
    }

    test("Spark.getAppsWithRatingGreaterOrEqual.order") {
        // Input data for the DataFrame
        val inputData = Seq(
            Row("Photo Editor 3", "ART_AND_DESIGN", "4.3", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up"),
            Row("Photo Editor 1", "ART_AND_DESIGN", "4.0", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up"),
            Row("Photo Editor 4", "ART_AND_DESIGN", "4.6", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up"),
            Row("Photo Editor 2", "ART_AND_DESIGN", "4.2", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up")
        )

        val expectedData = Seq(
            Row("Photo Editor 4", "ART_AND_DESIGN", "4.6", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up"),
            Row("Photo Editor 3", "ART_AND_DESIGN", "4.3", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up"),
            Row("Photo Editor 2", "ART_AND_DESIGN", "4.2", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up"),
            Row("Photo Editor 1", "ART_AND_DESIGN", "4.0", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up")
        )

        // Create DataFrame
        val inputDF = createAppsDF(inputData)

        assertDataFrameEquals( createAppsDF(expectedData)
                             , Spark.getAppsWithRatingGreaterOrEqual(inputDF, 3.5)
                             )
    }

    def createAverageSentimentPolarityByAppDF(data: Seq[Row]): DataFrame = {
        spark.createDataFrame(
            spark.sparkContext.parallelize(data),
            StructType(List(
                StructField(Spark.APP_HEADER, StringType, true), 
                StructField(Spark.AVERAGE_SENTIMENT_POLARITY_HEADER, DoubleType, false)
            ))
        )
    }

    def createAppsDF(data: Seq[Row]): DataFrame = {
        spark.createDataFrame(
            spark.sparkContext.parallelize(data),
            APPS_SCHEMA
        )
    }

    def createEmptyDataFrameWithSchema(schema: StructType): DataFrame = {
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    }
}