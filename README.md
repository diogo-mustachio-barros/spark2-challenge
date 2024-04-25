# Requirements
This project is based on Scala 2.11.12 and Spark 2.4.8.
It also depends on the `spark-submit` command to execute code, so be sure to have the correct 
    version installed.

# Structure
Main code is in `src\main\scala`. Main is in `SimpleApp.scala`, while `Util.scala` has utility 
    functions and `Spark.scala` contains dataframe-related functions.

Tests for both non-main classes are located in `src\test\scala`.

# How to run
A Makefile is used to run every aspect of the project by leveraging `sbt` and 
    `spark-submit`.

To test, run `make test` **or** `sbt test`. 

To build a `.jar` executable, run `make build` **or** `sbt package`

To run this code directly, run `make run` **or** 

    spark-submit --class SimpleApp --master local[4]\
    target/scala-2.11/simple-project_2.11-1.0.jar\
    data/google-play-store-apps/googleplaystore.csv\
    data/google-play-store-apps/googleplaystore_user_reviews.cs

**!! Attention !!** `make run` compiles before executing, but the above command **does not**
    so be sure to do `sbt package` before `spark-submit`.

# Notes
- This project always runs locally, but it could be easily changed through a main argument
- Decided to not round any doubles because it's only for readibility and may loose some data.
- Enunciate only referenced app size may not have an `"M"` at the end, but it can have a `"k"` or 
    can even be `"Varies with device"`. For this reason I interpreted them as to be size in 
    megabytes, size in kilobytes (converted to megabytes) and null value. Furthermore, I also 
    added an option in case of gigabyte sizes (for `"G"`).
- I opted to not implement tests for file related functions, as they are non-essencial.
- Property-based testing was considered, but it seems like non-deterministic testing is useful in
    uncovering bugs instead of testing target test cases.
- Spark testing is incomplete and very undocumented due to time constraints