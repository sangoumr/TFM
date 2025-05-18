##
# @package npl_news.py
# Process news using NLP for model training and prediction, as well as for use in Power BI visualization.
#
import os
import csv
from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import TimestampType
from pyspark.sql.utils import AnalysisException
from sparknlp.base import DocumentAssembler, Pipeline
from sparknlp.annotator import (
    Tokenizer,
    Normalizer,
    LemmatizerModel,
    StopWordsCleaner,
    BertForTokenClassification
    )
import pyspark.sql.functions as F
from TFM.source.constants import BERT_MODEL, NER_EXCLUDE, EXCLUDE_WORDS

##
# @brief Preprocess old news and last news for training models and to visualize in Power BI.
# @details Create a Document Assembler, Tokenizer, LemmatizerModel, normalizer, StopWordsCleaner, and
#        Named Entity Recognition (Bert model trained) objects, and add everything to a pipeline.
#        After run pipeline for each country directory given as a list, to get last date of news
#        processed and start to process news till today. Then remove excluded NER, convert tokens to
#        lower, remove some words about time and www, http,..., group tokens of news of each country
#        by day. Finally save then into hdfs and local csv to use in Power bi visualization.
#
# @param my_pspark (SparkSession): Spark sesion.
# @param list_dirs_countries (list): List of dirs to be processed.
# @param col_desc (str): Collumn name to be processed.
# @param hdfs_file_cleaned (str): hdfs path to save news processed in parquet format.
# @param local_file (str): Local path and file name to add news processed.
def nlp_proces_news(my_spark, list_dirs_countries: list, col_desc: str, hdfs_file_cleaned: str,
                    local_file: str)-> None:
    """ Preprocess news for training models and to visualize in Power BI.
        
        Create a Document Assembler, Tokenizer, LemmatizerModel, normalizer, StopWordsCleaner, and
        Named Entity Recognition (Bert model trained) objects, and add everything to a pipeline.
        After run pipeline for each country directory given as a list, to get last date of news
        processed and start to process news till today. Then remove excluded NER, convert tokens to
        lower, remove some words about time and www, http,..., group tokens of news of each country
        by day. Finally save them into hdfs and local csv to use in Power bi visualization.

    Args:
        my_pspark (SparkSession): Spark sesion.
        list_dirs_countries (list): List of dirs to be processed.
        col_desc (str): Collumn name to be processed.
        hdfs_file_cleaned (str): hdfs path to save news processed in parquet format
        local_file (str): Local path and file name to add news processed.

    Returns:
        None
    """

    # Set initial path.
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    try:

        # Transforms raw texts to `document` annotation
        document_assembler = (
            DocumentAssembler()
            .setInputCol(col_desc)
            .setOutputCol("document")
        )

        # Gets the tokens of the text.
        tokenizer = (
            Tokenizer()
            .setInputCols(["document"])
            .setOutputCol("token")
        )
        # Get Lemma of tokens.
        lemmatizer = (
            LemmatizerModel.pretrained()
            .setInputCols(["token"])
            .setOutputCol("lemma")
        )

        # Normalizes the tokens to remove all dirty characters from text following a regex pattern,
        # remove special characters, punctuation.
        normalizer = (
            Normalizer() \
            .setInputCols(["lemma"]) \
            .setOutputCol("normalized") \
            .setMinLength(3)
        )

        # Remove stopwords.
        stop_words = StopWordsCleaner.pretrained('stopwords_en', 'en')\
            .setInputCols(["normalized"]) \
            .setOutputCol("clean_SW_Tokens") \
            .setCaseSensitive(False)

        # Get Named Entity Recognition.
        token_classifier = BertForTokenClassification.load(BERT_MODEL) \
            .setInputCols(["document","clean_SW_Tokens"]) \
            .setOutputCol("ner")

        # Instance Pipeline setting stages.
        pipeline_norm = Pipeline(stages=[document_assembler,
                                         tokenizer,
                                         lemmatizer,
                                         normalizer,
                                         stop_words,
                                         token_classifier])

        for folder in list_dirs_countries:
            # Read origilal news.
            df_old_news_f = my_spark.read.parquet(folder)
            iso_code = df_old_news_f.first()["iso_code"]

            # Check if date is timestamp cast to date.
            if isinstance(df_old_news_f.schema["pubDate"].dataType, TimestampType):
                df_old_news_f = df_old_news_f.withColumn("pubDate", F.to_date(F.col("pubDate")))

            df_old_news_f.printSchema()

            try:
                # Upload processed news to get the latest date by county.
                df_proceced_news = my_spark.read.parquet(hdfs_file_cleaned)
                df_proceced_news.printSchema()
                max_date = df_proceced_news.filter(F.col("iso_code") == iso_code) \
                    .agg(F.max(F.col("pubDate")).alias("latest_date")).collect()[0]["latest_date"]

            except AnalysisException:
                # Get the oldest news date from pubDate.
                max_date = date(1970,1,1)
                min_date = df_old_news_f.filter(F.col("iso_code") == iso_code) \
                    .agg(F.min(F.col("pubDate")).alias("latest_date")).collect()[0]["latest_date"]
                if min_date > max_date: max_date = min_date

            # Filter by the oldest data if it has been processed, and if it has been processed by
            # the last processed until end of year.
            years_add = 1
            max_date_plus_y = max_date + relativedelta(years=years_add)
            df_old_news_f = df_old_news_f.filter((F.col("pubDate") > F.lit(max_date))
                                                    & (F.col("pubDate") < F.lit(max_date_plus_y)))
            records_found = df_old_news_f.count()
            print(max_date, max_date_plus_y, records_found)

            print(f"## Records found in country [{iso_code}] to be processed: ",records_found)

            if records_found > 0:
                # Show sample.
                df_old_news_f.show(n=3)
                df_old_news_f.select(F.col(col_desc)).show(n=3, truncate=False)

                model = pipeline_norm.fit(df_old_news_f)
                result = model.transform(df_old_news_f)

                # Show sample results of Pipeline.
                print("### 0: Pipeline transforms: \n",result.columns)
                result.select(F.col("token.result").alias("token"),
                            F.col("lemma.result").alias("lemma"), 
                            F.col("normalized.result").alias("norm"), 
                            F.col("clean_SW_Tokens.result").alias("clean_SW_Tokens"),
                            F.col("ner.result").alias("ner")
                            ).show(n=3, truncate=False)

                # Add to columns with results of tokens withou stop words and NER.
                pipeline_df = result.withColumn("cleanTokens_result",
                                                F.expr("transform(clean_SW_Tokens, x -> x.result)")
                                                ) \
                                    .withColumn("ner_result", F.expr("transform(ner, x -> x.result)"
                                                                     ))
                print("### 1: Get results of clean_SW_Tokens and NER: \n",pipeline_df.columns)
                pipeline_df.select("iso_code","pubDate",
                                "cleanTokens_result","ner_result").show(n=3, truncate=False)

                # Combine new columns and remove excluded NER.
                pipeline_df = pipeline_df.withColumn("tokens_n_ner",
                                                    F.arrays_zip("cleanTokens_result", "ner_result"
                                                                 ))
                print("### 3: cleanToken and NER combined:\n",pipeline_df.columns)
                pipeline_df.select("iso_code","pubDate","tokens_n_ner").show(n=3, truncate=False)

                pipeline_df = pipeline_df.withColumn(
                    "filtered_tokens",
                    F.expr(f"filter(tokens_n_ner, x -> NOT x.ner_result rlike '{NER_EXCLUDE}')")
                )
                print("### 4: ner excluded: \n",pipeline_df.columns)
                pipeline_df.select("iso_code","pubDate","filtered_tokens").show(n=3, truncate=False)

                # Convert tokens to lower.
                pipeline_df = pipeline_df.withColumn("lower_token",
                            F.expr("transform(filtered_tokens, x -> lower(x.cleanTokens_result))"))
                print("### 5: to lower: \n",pipeline_df.columns)
                pipeline_df.select("iso_code","pubDate","lower_token").show(n=3, truncate=False)

                # Exclude some words about time and www, http,...
                pipeline_df = pipeline_df.withColumn(
                    "final_token",
                    F.expr(f"filter(lower_token, x -> NOT x rlike '{EXCLUDE_WORDS}')")
                )
                print("### 6: filter webs (www...): \n",pipeline_df.columns)
                pipeline_df.select("iso_code","pubDate","final_token").show(n=3, truncate=False)

                # Group tokens by day.
                group_date_df = pipeline_df.groupBy(["iso_code","pubDate"]
                                                    ).agg(F.flatten(F.collect_list("final_token")
                                                                    ).alias("all_tokens"))
                print("### 7: group tokens by day: \n",group_date_df.columns)
                group_date_df.show(n=3, truncate=False)

                # Convert list of tokens to string  # Save new features for visualization in
                # Power Bi.
                df_tokens_str = group_date_df.select(
                    "iso_code",
                    "pubDate",
                    F.concat_ws(" ", "all_tokens").alias("words_text")
                    )
                print("### 8: concat tokens: \n",df_tokens_str.columns)
                df_tokens_str.show(n=3, truncate=False)

                # Save new features processed appening as parquet in hdfs.
                print("### 9: Save into HDFS as parquet: ",hdfs_file_cleaned)
                df_tokens_str.write.mode("append").parquet(hdfs_file_cleaned)

                # append news processed to local csv.
                file_exists = os.path.isfile(local_file)

                # Opend file in append mode to add new processeed news.
                print("### 10: Save into local CSV: ",local_file)
                with open(local_file, mode="a", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)

                    if not file_exists:
                        writer.writerow(['iso_code', 'pubDate', 'words_text'])

                    # Add last news cleaned.
                    for row in df_tokens_str.toLocalIterator():
                        writer.writerow(row)

    except Exception as e:
        print('ERROR - Npl: ',e)
