import org.scalatest.funsuite.AnyFunSuite

import com.holdenkarau.spark.testing.DataFrameSuiteBase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

class SparkTest extends AnyFunSuite with DataFrameSuiteBase {

    // var spark: SparkSession = null
    // var appsDF: DataFrame = null
    // var userReviewsDF: DataFrame = null
    
    // override def beforeAll(): Unit = {
    //     spark = SparkSession.builder()
    //         .appName("DataFrameInitialization")
    //         .getOrCreate()

    //     val data = List(
    //         Row("1", "John", "30"),
    //         Row("2", "Alice", "25"),
    //         Row("3", "Bob", "35")
    //     )

    //     val schema = StructType(
    //         List(
    //             StructField("id", StringType, nullable = true),
    //             StructField("name", StringType, nullable = true),
    //             StructField("age", StringType, nullable = true)
    //         )
    //     )

    //     appsDF = spark.createDataFrame(data.asJava, schema)
    // }

    // test ("DFs equal") {
    //     val expected=sc.parallelize(List(
    //       Row("Wodehouse","Sales",250,1),
    //       Row("Steinbeck","Sales",100,2),
    //       Row("Hemingway","IT",349,1),
    //       Row("Woolf","IT",99,2)
    //     ))

    //     val schema=StructType(
    //       List(
    //       StructField("emp",StringType,true),
    //       StructField("dept",StringType,true),
    //       StructField("sal",IntegerType,false),
    //       StructField("rank",IntegerType,true)
    //       )
    //     )

    //     val e2=sqlContext.createDataFrame(expected,schema)
    //     val actual=ScratchPad.get_data_frame(sqlContext.sparkSession)
    //     assertDataFrameEquals(e2,actual)
    //   }

    // override def afterAll(): Unit = {
    //     spark.stop()
    // }

    val DELTA: Double = 0.0001

    val USER_REVIEWS_SCHEMA = StructType(List(
        StructField(Spark.APP_HEADER, StringType, true), 
        StructField(Spark.TRANSLATED_REVIEW_HEADER, StringType, true),
        StructField(Spark.SENTIMENT_HEADER, StringType, true),
        StructField(Spark.SENTIMENT_POLARITY_HEADER, StringType, true),
        StructField(Spark.SENTIMENT_SUBJECTIVITY_HEADER, StringType, true)
    ))

    test("simple test") {
        val sqlCtx = sqlContext
        import sqlCtx.implicits._

        val input1 = sc.parallelize(List[(Int, Double)]((1, 1.1), (2, 2.2), (3, 3.3))).toDF
        val input2 = sc.parallelize(List[(Int, Double)]((1, 1.2), (2, 2.3), (3, 3.4))).toDF
        assertDataFrameApproximateEquals(input1, input2, 0.11) // equal

        // intercept[org.scalatest.exceptions.TestFailedException] {
        //     assertDataFrameApproximateEquals(input1, input2, 0.05) // not equal
        // }
    }

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