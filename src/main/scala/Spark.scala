import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Spark {
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
    
    def getSquashedApps(appsDF: DataFrame): DataFrame = {
        val w = Window.partitionBy(APP_HEADER)

        val maxReviewsDF = appsDF
            .groupBy(APP_HEADER)
            .agg(max(REVIEWS_HEADER) as REVIEWS_HEADER)

        val categoriesDF = appsDF
            .groupBy(APP_HEADER)
            .agg(collect_set(CATEGORY_HEADER) as "Categories")


        // User defined functions to apply to columns
        val safeDoubleCast = udf(Util.safeParseDouble _)

        val parseSize = udf(Util.parseSizeInMB _)

        val parsePrice = udf(
        Util.parseDollarPrice(_: String)
            .map(Util.dollarsToEuros)
        )


        appsDF
            // Squash apps to only the one with max reviews
            .join(maxReviewsDF, Seq(APP_HEADER, REVIEWS_HEADER), "inner")
            // Join with list of categories for each app
            .join(categoriesDF, APP_HEADER)
            // Modify other columns
            .withColumn(RATING_HEADER, safeDoubleCast(col(RATING_HEADER)))
            .withColumn(REVIEWS_HEADER, col(REVIEWS_HEADER).cast("long"))
            .withColumn(SIZE_HEADER, parseSize(col(SIZE_HEADER)))
            .withColumn(PRICE_HEADER, parsePrice(col(PRICE_HEADER)))
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

    def getAverageSentimentPolarityByApp(userReviewsDF: DataFrame): DataFrame = {
        userReviewsDF
            .groupBy(APP_HEADER)
            .agg(
                coalesce(avg(SENTIMENT_POLARITY_HEADER), lit(0)).as(AVERAGE_SENTIMENT_POLARITY_HEADER))
    }

    def getSquashedAppsWithAverageSentiment(appsDF: DataFrame, userReviewsDF: DataFrame): DataFrame = {
        getSquashedApps(appsDF)
            .join(getAverageSentimentPolarityByApp(userReviewsDF), APP_HEADER)
    }
    
    def getAppsWithRatingGreaterOrEqual(appsDF: DataFrame, rating: Double): DataFrame = {
        appsDF
            .filter(col(RATING_HEADER) >= rating && col(RATING_HEADER) =!= Double.NaN)
            .orderBy(desc(RATING_HEADER))
    }

    def getGooglePlayStoreMetrics(appsDF: DataFrame, userReviewsDF: DataFrame): DataFrame = {
        this.getSquashedApps(appsDF)
            .join(getAverageSentimentPolarityByApp(userReviewsDF), APP_HEADER)
            .withColumn("Genre", explode(col(GENRES_HEADER)))
            .groupBy("Genre")
            .agg( count(APP_HEADER) as "Count"
                , avg(RATING_HEADER) as "Average_Rating"
                , avg(AVERAGE_SENTIMENT_POLARITY_HEADER) as AVERAGE_SENTIMENT_POLARITY_HEADER)
    }
}
