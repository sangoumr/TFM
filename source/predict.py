##
# @package predict.py
# Predict the political position of each country's news from the last date of appointment of the
# president to today, received from function get_dataset_last_news_weight(), loading trained best
# model saved, and save results combined with old presidentials terms.

from datetime import datetime, timedelta
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml import PipelineModel
from TFM.source.get_dataset import get_dataset_last_news_weight
from TFM.source.constants import (
    FILE_PRESI,
    PATH_HDFS_BEST_MODEL,
    FILE_PRESIDENTS_N_PREDICTION,
    FILE_PROBABILITY_PREDICTION
    )
##
# @brief Predict the political position of each country's news from the last date of appointment of
# the president to today, received from function get_dataset_last_news_weight(), loading trained
# best model saved.
# @details The predict label that model return, is for grouped news by day for each country, so to
# get summary do a final count by label iso_code, to obtain the majority prediction, from las
# appointment to today. Expand old presidential term and union with predictions in CSV saved as
# local to visualize evolution in Power BI maps.
#
# @param my_pspark (SparkSession): Spark sesion.
#
# @return None
def predict_last_news(my_spark) -> None:
    """ Predict political position of news, received from function get_dataset_last_news_weight(),
        loading trained best model saved.
        The predict label that model return, is for grouped news by day for each country, so to
        get summary do a final count by label iso_code, to obtain the majority prediction, from las
        appointment to today. Expand old presidential term and union with predictions in CSV saved
        as local to visualize evolution in Power BI maps.

    Args:
        my_pspark (SparkSession): Spark sesion.
    """
    try:
        my_spark.sparkContext.setLogLevel("ERROR")

        # Get df with last news of all countries to predict.
        df_last_news_test = get_dataset_last_news_weight(my_spark)

        # Load trained model.
        model = PipelineModel.load(PATH_HDFS_BEST_MODEL)
        prediccions = model.transform(df_last_news_test)

        # Show predictions.
        print("## Show predictions from last news (by day):")
        prediccions.show(n=5)

        print(f"## Show count labels and probability of predictions by country today {datetime.now()}:")
        # Count labels predicted by iso_code and pivot and show.
        df = prediccions.groupBy("iso_code") \
                .pivot("prediction") \
                .count() \
                .orderBy("iso_code")
        # Get probabilities array [prob 0, prob 1] and prediction to save csv to Power BI
        df_prob = df.withColumn(
            "total", F.col("`0.0`") + F.col("`1.0`")
        ).withColumn(
            "prob_0", (F.col("`0.0`") / F.col("total"))
        ).withColumn(
            "prob_1", (F.col("`1.0`") / F.col("total"))
        ).withColumn(
            "probability", F.array(  
                F.round(F.col("prob_0"), 3),
                F.round(F.col("prob_1"), 3)
            )
        ).withColumn(
            "prediction", F.when(F.col("prob_0") > F.col("prob_1"), F.lit(0))
                           .when(F.col("prob_0") < F.col("prob_1"), F.lit(1))
                           .otherwise(F.lit(2))
        ).withColumn("posicio_politica",
                     F.when(F.col("prediction") == '0',"esquerra")
                     .when(F.col("prediction") == '1', "dreta")
                     .otherwise("empat")
        ).withColumn("day_prediction", F.current_date())
        df_prob.show(n=30, truncate=False)
        df_prop_final = df_prob.select("iso_code",
                                       "day_prediction",
                                       "probability",
                                       "prediction",
                                       "posicio_politica")

        df_prop_final.toPandas().to_csv(FILE_PROBABILITY_PREDICTION, index=False)

        # Add new cols like FILE_PRESI and rename others, cast label to int.
        resultat_final = df_prop_final.withColumn("nomenament", F.current_date()) \
                                      .withColumn("nom", F.lit("PREDICTION")) \
                                      .withColumnRenamed("prediction", "label") \
                                      .withColumn("label", F.col("label").cast("int"))

        # Reorder cols like FILE_PRESI
        resultat_final = resultat_final.select(
            "iso_code",
            "nom",
            "nomenament",
            "posicio_politica",
            "label"
            )
        print(f"## Show predictions by country of today {datetime.now()}:")
        resultat_final.show(n=30)

        # Read presidents file and add predictions to save in local data Cleaned to use in
        # Power Bi visualization.
        df = my_spark.read.option("header", True).csv("file://"+FILE_PRESI)
        df = df.withColumn("nomenament_date", F.to_date(F.col("nomenament"), "dd-MM-yyyy"))

        # Expand period from nomenament to next one to visualize better the evolution in mapa
        # Power BI.
        # Get window for iso_code ordered by date of nomenament
        window_spec = Window.partitionBy("iso_code").orderBy("nomenament_date")

        # Get next Date of nomenament
        df = df.withColumn("next_nomenament_date", F.lead("nomenament_date").over(window_spec))

        # if last nomenament insert yesterday.
        df = df.withColumn(
            "next_nomenament_date",
            F.when(
                F.col("next_nomenament_date").isNull(),
                F.date_sub(F.current_date(), 1)
            ).otherwise(F.col("next_nomenament_date"))
        )

        # Extend the presidential term.
        def generate_dates(start_date, end_date):
            dates = []
            if start_date is None or end_date is None:
                return dates
            current_date = start_date
            while current_date <= end_date:
                dates.append(current_date.strftime("%d-%m-%Y"))
                current_date += timedelta(days=1)  # sumem un dia cada cop
            return dates

        # Instance UDF function.
        generate_dates_udf = F.udf(generate_dates, ArrayType(StringType()))

        # Aply function to expand dates.
        df = df.withColumn(
            "expanded_dates",
            generate_dates_udf(F.col("nomenament_date"), F.col("next_nomenament_date"))
        )

        # Return a new row for each element givent in array.
        df_exploded = df.withColumn("nomenament", F.explode("expanded_dates"))
        print("## Sample of expanded presidential term:")
        df_exploded.show(n=10)

        # Subselect cols.
        result = df_exploded.select(
            "iso_code",
            "nom",
            "nomenament",
            "posicio_politica",
            "label"
        )

        # Union predictions and Save in local
        df_contat = result.unionByName(resultat_final)
        df_concat_pd = df_contat.toPandas()
        df_concat_pd.to_csv(FILE_PRESIDENTS_N_PREDICTION, index=False)

    except KeyboardInterrupt:
        print("KeyboardInterrupt in Predict last news.")
        pass
    except Exception as e:
        print('ERROR - Predict last news: ',e)
