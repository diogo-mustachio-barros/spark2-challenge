APP_NAME=SimpleApp
MAIN_ARGS=data/google-play-store-apps/googleplaystore.csv data/google-play-store-apps/googleplaystore_user_reviews.csv

test:
	sbt test

build:
	sbt package

run: build
	spark-submit --class $(APP_NAME) --master local[4] target/scala-2.11/simple-project_2.11-1.0.jar $(MAIN_ARGS)