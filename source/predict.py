##
# @package predict.py
# Predict political position of news, received from function get_dataset_last_news_weight(),
# loading trained best model saved.

from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel
from TFM.source.get_dataset import get_dataset_last_news_weight
from TFM.source.constants import (
    FILE_PRESI,
    PATH_HDFS_BEST_MODEL,
    FILE_PRESIDENTS_N_PREDICTION
    )
##
# @brief Predict political position of news, received from function get_dataset_last_news_weight(),
# loading trained best model saved.
# @details The predict label that model return, is for grouped news by day for each country, so to
# get summary do a final count by label iso_code, to obtain the majority prediction, from las
# appointment to today.
#
# @param my_pspark (SparkSession): Spark sesion.
#
# @return None
def predict_last_news(my_spark) -> None:
    """ Predict political position of news, received from function get_dataset_last_news_weight(),
        loading trained best model saved.
        The predict label that model return, is for grouped news by day for each country, so to
        get summary do a final count by label iso_code, to obtain the majority prediction, from las
        appointment to today.

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

        print(f"## Show count labels of predictions by country today {datetime.now()}:")
        # Count labels predicted by iso_code and pivot and show.
        prediccions.groupBy("iso_code") \
            .pivot("prediction") \
            .count() \
            .orderBy("iso_code").show(n=30)

        # Final count by label iso_code, to obtain the majority prediction.
        recompte = prediccions.groupBy("iso_code", "prediction").count()
        # Set window by iso_code sorting by descending count, to break the tie.
        window = Window.partitionBy("iso_code").orderBy(F.col("count").desc(),
                                                        F.col("prediction").asc())
        # assign row number for each iso_code registers.
        recompte_ranked = recompte.withColumn("rank", F.row_number().over(window))
        # Select first registres if if there is a tie.
        resultat = recompte_ranked.filter(F.col("rank") == 1).select("iso_code",
                                                                     "prediction",
                                                                     "count")

        # Add new cols like FILE_PRESI and rename other
        resultat_final = resultat.withColumn("nomenament", F.current_date()) \
                                    .withColumn("nom", F.lit("PREDICTION")) \
                                    .withColumn("posicio_politica",
                                                F.when(F.col("prediction") == '0',"esquerra")
                                                .when(F.col("prediction") == '1', "dreta")
                                                .otherwise("desconeguda")
                                                )
        resultat_final = resultat_final.withColumnRenamed("prediction", "label")
        # Cast label to int.
        resultat_final = resultat_final.withColumn("label", F.col("label").cast("int"))

        # Reorder cols like FILE_PRESI
        resultat_final = resultat_final.select(
            "iso_code",
            "Nom",
            "nomenament",
            "posicio_politica",
            "label"
            )
        print(f"## Show predictions by country of today {datetime.now()}:")
        resultat_final.show(n=30)

        # Read presidents file and add predictions to save in local data Cleaned to use in
        # Power Bi visualization.
        df = my_spark.read.option("header", True).csv("file://"+FILE_PRESI)
        df_contat = df.unionByName(resultat_final)

        df_concat_pd = df_contat.toPandas()
        df_concat_pd.to_csv(FILE_PRESIDENTS_N_PREDICTION, index=False)

    except KeyboardInterrupt:
        print("KeyboardInterrupt in Predict last news.")
        pass
    except Exception as e:
        print('ERROR - Predict last news: ',e)
