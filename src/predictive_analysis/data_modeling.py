import numpy as np
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, mean, udf, expr, explode
import os
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import DoubleType
import subprocess

from src.utils.mongo_utils import MongoDBUtils
import matplotlib.pyplot as plt

class DataModeling:

    def __init__(self, logger, vm_host, mongodb_port, persistent_db, formatted_db, exploitation_db):
        self.logger = logger
        self.vm_host = vm_host
        self.mongodb_port = mongodb_port
        self.persistent_db = persistent_db
        self.formatted_db = formatted_db
        self.exploitation_db = exploitation_db

        try:
            # Create a SparkSession
            self.spark = SparkSession.builder \
                .master("local[*]") \
                .appName("Unify Lookup District") \
                .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
                .getOrCreate()
        except Exception as e:
            self.logger.error(
                f"An error occurred during the creation of the Spark Configuration during the creation of DataModeling class."
                f" This is the error: {e}")

    def get_data_from_formatted_to_exploitation(self):
        # Read idealista_reconciled collection and select relevant columns
        idealista_df = MongoDBUtils.read_collection(
            self.logger,
            self.spark,
            self.vm_host,
            self.mongodb_port,
            self.formatted_db,
            "idealista_reconciled"
        ).select("_id", "size", "rooms", "bathrooms", "latitude", "longitude", "exterior", "floor", "has360",
            "has3DTour", "hasLift", "hasPlan", "hasStaging", "hasVideo","neighborhood_id", "numPhotos", "price") \
            .filter(col("municipality") == "Barcelona") \
            .filter(col("neighborhood_id").isNotNull())


        # Read income_reconciled collection and select relevant columns
        income_df = MongoDBUtils.read_collection(
            self.logger,
            self.spark,
            self.vm_host,
            self.mongodb_port,
            self.formatted_db,
            "income_reconciled"
        ).select("_id", "info.year", "info.RFD")

        # Read buildin_age_reconciled collection and select relevant columns
        buildin_age_df = MongoDBUtils.read_collection(
            self.logger,
            self.spark,
            self.vm_host,
            self.mongodb_port,
            self.formatted_db,
            "building_age_reconciled"
        ).select("_id", "info.year", "info.mean_age")

        # Join the three dataframes on district_id
        joined_df = idealista_df.join(
            income_df,
            idealista_df["neighborhood_id"] == income_df["_id"],
            "left"
        ).drop(income_df["_id"]).withColumnRenamed("year", "income_year")

        joined_df = joined_df.join(
            buildin_age_df,
            joined_df["neighborhood_id"] == buildin_age_df["_id"],
            "left"
        ).drop(buildin_age_df["_id"]).withColumnRenamed("year", "building_year")

        self.logger.info('Data sources joined successfully.')

        # Save the joined dataframe to a new collection in MongoDB
        MongoDBUtils.write_to_collection(
            self.logger,
            self.vm_host,
            self.mongodb_port,
            self.exploitation_db,
            "model_collection",
            joined_df
        )

    def calculate_mean(self, array):
        return sum(array) / len(array) if array else None

    def preprocess_and_train_model(self):

        df = MongoDBUtils.read_collection(
            self.logger,
            self.spark,
            self.vm_host,
            self.mongodb_port,
            self.exploitation_db,
            "model_collection"
        )

        # Drop unnecessary columns
        columns_to_drop = ['_id', 'income_year', 'building_year']
        df = df.drop(*columns_to_drop)

        # Calculate the mean of the 'mean_age' column
        df = df.withColumn('mean_age', expr(
            'aggregate(mean_age, CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) / size(mean_age)'))

        # Calculate the mean of the 'RFD' column
        df = df.withColumn('RFD', expr(
            'aggregate(RFD, CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) / size(RFD)'))

        # Convert boolean columns to string and index them
        boolean_columns = ['exterior', 'has360', 'has3DTour', 'hasLift', 'hasPlan', 'hasStaging', 'hasVideo']

        for column_name in boolean_columns:
            df = df.withColumn(column_name, col(column_name).cast("string"))

        # Get the column types
        column_types = df.dtypes

        # Print the column types
        # for column_name, column_type in column_types:
        #     print(f"Column: {column_name}, Type: {column_type}")

        # Convert numeric columns to appropriate types
        numeric_cols = ["RFD", "bathrooms", "floor", "latitude", "longitude", "mean_age",
                        "numPhotos", "rooms", "size"]
        for column in numeric_cols:
            df = df.withColumn(column, df[column].cast("double"))

        # Convert string columns to numeric using StringIndexer
        string_cols = ["exterior", "has360", "has3DTour", "hasLift", "hasPlan",
                       "hasStaging", "hasVideo", "neighborhood_id"]

        #indexers = [StringIndexer(inputCol=column, outputCol=column + "_index").fit(df) for column in string_cols]
        indexers = [StringIndexer(inputCol=column, outputCol=column+"_index", handleInvalid="skip").fit(df) for column
                    in string_cols]
        pipeline = Pipeline(stages=indexers)
        df = pipeline.fit(df).transform(df)

        df.dropna()


        # Assemble features into a single vector column
        feature_cols = numeric_cols + [column + "_index" for column in string_cols]

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
        df = assembler.transform(df)

        # Split the data into training and testing sets
        (training_data, testing_data) = df.randomSplit([0.7, 0.3])

        # Create the Random Forest Regressor model
        rf = RandomForestRegressor(featuresCol="features", labelCol="price", maxBins=64)

        # Train the model
        model = rf.fit(training_data)

        # Make predictions on the testing data
        predictions = model.transform(testing_data)

        # Evaluate the model
        evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)

        # Additional evaluation metrics
        mae_evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="mae")
        mae = mae_evaluator.evaluate(predictions)

        mse_evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="mse")
        mse = mse_evaluator.evaluate(predictions)

        r2_evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="r2")
        r2 = r2_evaluator.evaluate(predictions)

        # Print the evaluation metrics
        self.logger.info(f"Root Mean Squared Error (RMSE): {rmse}")
        self.logger.info(f"Mean Absolute Error (MAE): {mae}", )
        self.logger.info(f"Mean Squared Error (MSE): {mse}")
        self.logger.info(f"R-squared (R2): {r2}")

        # Plotting the predicted vs. actual prices
        actual_prices = predictions.select("price").rdd.flatMap(lambda x: x).collect()
        predicted_prices = predictions.select("prediction").rdd.flatMap(lambda x: x).collect()

        plt.figure(figsize=(8, 6))
        plt.scatter(actual_prices, predicted_prices, color='#648E9C', alpha=0.5, label='Actual Price')
        plt.scatter(actual_prices, predicted_prices, color='#9C648E', alpha=0.5, label='Predicted Price')
        plt.plot([min(actual_prices), max(actual_prices)], [min(actual_prices), max(actual_prices)], color='r',
                 linestyle='--')
        plt.xlabel("Actual Price (in thousands of euros)")
        plt.ylabel("Predicted Price (in thousands of euros)")
        plt.title("Actual vs. Predicted Prices")
        plt.legend()
        plt.tight_layout()
        plt.show()

        importances = model.featureImportances
        feature_names = [column.replace("_index", "") for column in feature_cols]

        plt.figure(figsize=(8, 6))
        plt.barh(np.arange(len(importances)), importances, align="center", color='#648E9C')
        plt.yticks(np.arange(len(importances)), feature_names)
        plt.xlabel("Feature Importance")
        plt.ylabel("Feature")
        plt.title("Feature Importances")
        plt.tight_layout()
        plt.show()

        return model