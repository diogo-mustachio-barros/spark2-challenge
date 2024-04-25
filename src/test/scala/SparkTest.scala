import org.scalatest.funsuite.AnyFunSuite

import com.holdenkarau.spark.testing.DataFrameSuiteBase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

class SparkTest extends AnyFunSuite with DataFrameSuiteBase {

    val DELTA: Double = 0.0001

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

        assertDataFrameEquals( createAverageSentimentPolarityByAppDFExpected(expectedData)
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

        assertDataFrameApproximateEquals( createAverageSentimentPolarityByAppDFExpected(expectedData)
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

        assertDataFrameEquals( createAverageSentimentPolarityByAppDFExpected(expectedData)
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

        assertDataFrameApproximateEquals( createAverageSentimentPolarityByAppDFExpected(expectedData)
                                        , Spark.getAverageSentimentPolarityByApp(inputDF)
                                        , DELTA
                                        )
    }

    def createAverageSentimentPolarityByAppDFExpected(expectedData: Seq[Row]): DataFrame = {
        spark.createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            StructType(List(
                StructField(Spark.APP_HEADER, StringType, true), 
                StructField(Spark.AVERAGE_SENTIMENT_POLARITY_HEADER, DoubleType, false)
            ))
        )
    }
}