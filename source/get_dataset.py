##
# @package get_dataset.py
# Prepare the datasets that combines features and labels, for each day of each country news.
#
# The features will be all tokens of each day for country, from last appointment to next one, or
# today for last news, including calculatd weight (weight up if day of news are close next
# appointment in old news, or today for last news) and the labels will be the political position.
import os
import subprocess
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window
from TFM.source.constants import (
    FILE_PRESI,
    PATH_HDFS_NEWS_CLEANED,
    DAYS_DIF_FROM_ELECTIONS,
    PATH_PRESIDENTS
    )

##
# @brief Combine files of presidents of each country in one
# @details to use when generate dataset to train model and to predict, in case some country had new
# election and consequently new president.
#
# @return none.
def combine_files_presidents() -> None:
    """ Combine files of presidents of each country in one, to use when generate dataset to train
        model and to predict, in case some country had new election and consequently new president.
    """

    try:
        # Get list of files name presidents in local directory.
        list_file_presi = subprocess.check_output("ls "+PATH_PRESIDENTS+ \
                                                "presidents-* | awk '{print $NF}'",
                                                    shell=True).decode("utf-8").splitlines()

        # list to add DataFrames
        dfs = []

        for fitxer in list_file_presi:
            # Read file presidents of country.
            df = pd.read_csv(fitxer)

            # ADD iso code.
            codi_iso = os.path.basename(fitxer).split("-")[1].split(".")[0]
            df.insert(0, "pais_iso", codi_iso)

            # Rename columns to normalize.
            df.rename(columns={"pais_iso":"iso_code",
                            "Nom": "nom",
                            "Nomenament": "nomenament",
                            "Partit": "posicio_politica"}, inplace=True)

            dfs.append(df)

        # Merge all DataFrames.
        df_total = pd.concat(dfs, ignore_index=True)

        # Show unique values to check.
        #valors_distints = df_total['posicio_politica'].unique()
        #print(valors_distints)

        # To lower.
        df_total['posicio_politica'] = df_total['posicio_politica'].str.lower()

        # Replace inconsistent data to unify.
        df_total['posicio_politica'] = df_total['posicio_politica'].replace({
            'eesquerra': 'esquerra',
            'independente': 'centre',
            'independent': 'centre',
            'derecha': 'dreta',
            'centre': 'centre',
            'cento': 'centre',
            'cente': 'centre',
        })

        # Show unique values to check.
        #valors_distints = df_total['posicio_politica'].unique()
        #print(valors_distints)
        #print(df_total.tail())

        # Discretize the column 'political_position'.
        mapping = {'esquerra': 0, 'dreta': 1, 'centre': 2}
        df_total['label'] = df_total['posicio_politica'].map(mapping)

        # Save combined presidents of all countries CSV
        df_total.to_csv(FILE_PRESI, index=False)

        print(f"\n ## Dimensions of combined presidents file for all countries: {df_total.shape}")

    except Exception as e:
        print('ERROR - Combine presidents files: ',e)


##
# @brief Prepare dataset of old news for US to train model to prodecit political position.
# @details Filter news by pubDate from last appointment (presidential elections) date for each country
# to next one, add same label of last appointment and weight, where Weight will be upp if the publication
# date is close to the next appointment. Calculate based on the number of
# days presidential elections.
#
# @param my_pspark (SparkSession): Spark sesion.
# @param path_hdfs_news (str): path where found the old news.
# @param  iso_code (str, optional): country iso code. Defaults to 'us'.
#
# @return DataFrame weighted, ready to train model.
def get_dataset_old_news_weight(my_spark, path_hdfs_news: str, iso_code: str = 'us') -> DataFrame:
    """ Prepare dataset of old news for US to train model to prodecit political position.
        Filter news by pubDate from last appointment (presidential elections) date for each country
        to next one, add same label of last appointment and weight, where Weight will be upp if the
        publication date is close to the next appointment. Calculate based on the number of
        days presidential elections.

    Args:
        my_pspark (SparkSession): Spark sesion.
        path_hdfs_news (str): path where found the old news.
        iso_code (str, optional): country iso code. Defaults to 'us'.

    Returns:
        DataFrame: dataset prepared to train model.
    """

    # Read presidents file to select days of US presidents and labels of political position.
    combine_files_presidents() # combine first in case new elections
    df = my_spark.read.option("header", True).csv("file://"+FILE_PRESI)

    # Cast to date nomenament.
    df = df.withColumn("nomenament", F.to_date(F.col("nomenament"), "dd-MM-yyyy"))
    # Subtract some days to exclude news from after the elections until the date of appointment.
    df = df.withColumn("nomenament", F.date_sub(F.col("nomenament"), DAYS_DIF_FROM_ELECTIONS))

    # Filter USA iso code US.
    df_filtrat = df.filter(F.col("iso_code") == iso_code)

    # Define window by date.
    window_nom_date = Window.orderBy("nomenament")
    # Add before date  appointment.
    df_filtrat = df_filtrat.withColumn("before_nomenament",
                                       F.lag("nomenament").over(window_nom_date))

    # Calculate days betwen  appointment.
    df_filtrat = df_filtrat.withColumn("days_before_apointment",
                                       F.date_diff(F.col("nomenament"),
                                                   F.col("before_nomenament")))

    # Cast string label to int.
    df_filtrat = df_filtrat.withColumn("label", F.col("label").cast("int"))
    # remove registers with date null in before_nomenament.
    df_filtrat = df_filtrat.filter(F.col("before_nomenament").isNotNull())
    # Show sample.
    df_filtrat.show(n=5)


    # Get cleaned old new, cast pubDate to date and filter by iso_code.
    df_old_news_cln = my_spark.read.parquet(path_hdfs_news)
    df_old_news_cln = df_old_news_cln.filter(F.col("iso_code") == iso_code)
    df_old_news_cln = df_old_news_cln.withColumn("pubDate", F.to_date(F.col("pubDate"),
                                                                      "yyyy-MM-dd"))
    df_old_news_cln.show(n=5)

    df_final = None

    for row in df_filtrat.collect():
        iso_code = row['iso_code']
        date_start = row['before_nomenament']
        date_end = row['nomenament']
        label = row['label']

        print(f"Preparing weighted samples from {date_start} to {date_end}: label: {label}")

        # Filter between dates range.
        df_filtrat_data = df_old_news_cln.filter((F.col("pubDate") > F.lit(date_start))
                                                 & (F.col("pubDate") < F.lit(date_end)))

        # Calculate days to next appointment.
        df_filtrat_data = df_filtrat_data.withColumn("day_to_app", F.datediff(F.lit(date_end),
                                                                              F.col("pubDate")))

        # Calculate weight, more weight if close to next appointment.
        df_ponderat = df_filtrat_data.withColumn("weight", 1 / (1 + F.col("day_to_app") \
                                                                .cast("double")))

        # Add label, Word Count, dates between appointments.
        df_ponderat = df_ponderat.withColumns({
            "word_count": F.size(F.split(F.col("words_text"), " ")),
            "before_app": F.lit(date_start),
            "next_app": F.lit(date_end),
            "label": F.lit(label)
        })

        # Union df results: filtered and weighted days news.
        if df_final is None:
            df_final = df_ponderat
        else:
            df_final = df_final.union(df_ponderat)

    # Show sample.
    print("## Total records in the final dataset: ",df_final.count())
    df_final.show(n=5)

    return df_final

##
# @brief Prepare dataset of last news for all countries to predict next political position.
# @details Filter news by pubDate from last appointment (presidential elections) date for each
# country till today, add label same of last appointment and weight. Weight upp if the publication
# date is close to the next appointment, in this case today. Calculate based on the number of days
# since the last presidential election.
#
# @param my_pspark (SparkSession): Spark sesion.
#
# @return DataFrame prepared to prediction.
def get_dataset_last_news_weight(my_spark) -> DataFrame:
    """ Prepare dataset of last news for all countries to predict next political position.
        Filter news by pubDate from last appointment (presidential elections) date for each country
        till today, add label same of last appointment and weight. Weight upp if the publication
        date is close to the next appointment, in this case today. Calculate based on the number of
        days since the last presidential election.

    Args:
        my_spark (SparkSession): Spark sesion.

    Returns:
        DataFrame:  DataFrame prepared to prediction.
    """

    # Read presidents file
    combine_files_presidents() # combine first in case new elections
    df = my_spark.read.option("header", True).csv("file://"+FILE_PRESI)

    # Cast to date nomenament
    df = df.withColumn("nomenament", F.to_date(F.col("nomenament"), "dd-MM-yyyy"))

    # Window to get last appointment for iso_code
    window = Window.partitionBy("iso_code").orderBy(F.col("nomenament").desc())

    # Add col number and filter == 1, getting las appointment of all countries.
    df_row_num = df.withColumn("row_num", F.row_number().over(window))
    last_appointments = df_row_num.filter(F.col("row_num") == 1).drop("row_num")

    # Show sample.
    print("## Show the last appointments (presidential elections) of each country:")
    last_appointments.show(truncate=False)

    # Add today's date simulating the upcoming presidential elections.
    df_date = last_appointments.withColumn("today", F.current_date())

    # Calculate days betwen last presidential elections and today.
    df_date_diff = df_date.withColumn("days_last_apointment", F.date_diff(F.col("today"),
                                                                           F.col("nomenament")))

    # Cast string label to int.
    df_filtrat = df_date_diff.withColumn("label", F.col("label").cast("int"))
    # Show sample.
    print("## Show days from last presidential elections to today of each country:")
    df_filtrat.show(n=30)

    # Get cleaned last news and cast PubDate to date.
    df_last_news_cln = my_spark.read.parquet(PATH_HDFS_NEWS_CLEANED)
    df_last_news_cln = df_last_news_cln.withColumn("pubDate", F.to_date(F.col("pubDate"),
                                                                      "yyyy-MM-dd"))
    print("## Show the sample of cleaned news in NLP, that will be used to create a weighted dataset:")
    df_last_news_cln.show(n=5)

    df_final = None

    # For last appointment (presidential elections) in each country.
    for row in df_filtrat.collect():
        iso_code = row['iso_code']
        date_start = row['nomenament']
        date_end = row['today']
        label = row['label']

        # Filter by iso_code to calculate weight.
        df_last_news_filtered = df_last_news_cln.filter(F.col("iso_code") == iso_code)

        print(f"## Preparing weighted samples [{iso_code}] from last " \
              f"appointment {date_start} to {date_end}: label: {label}")

        # Filter between dates range.
        df_filtrat_data = df_last_news_filtered.filter((F.col("pubDate") > F.lit(date_start))
                                                 & (F.col("pubDate") <= F.lit(date_end)))

        # Calculate days to next appointment.
        df_filtrat_data = df_filtrat_data.withColumn("day_last_app", F.datediff(F.lit(date_end),
                                                                              F.col("pubDate")))

        # Calculate weight, weight upp if the publication date is close to the next appointment,
        # in this case today. Calculate based on the number of days since the last presidential
        # election, calculated in the previous step.
        df_ponderat = df_filtrat_data.withColumn("weight",
                                                 1 / (1 + F.col("day_last_app").cast("double")))

        # Add label, Word Count, dates between appointments.
        df_ponderat = df_ponderat.withColumns({
            "word_count": F.size(F.split(F.col("words_text"), " ")),
            "before_app": F.lit(date_start),
            "next_app": F.lit(date_end),
            "label": F.lit(label)
        })

        df_ponderat.show(n=5)

        # Union df results: filtered and weighted days news.
        if df_final is None:
            df_final = df_ponderat
        else:
            df_final = df_final.union(df_ponderat)


    # Show sample.
    print("## Total records in the final test dataset to predict: ",df_final.count())
    df_final.show(n=5)

    return df_final.select('iso_code', 'pubDate', 'words_text', 'day_last_app', 'weight', 'label')
