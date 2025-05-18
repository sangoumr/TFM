##
# @package classification_news.py
# Pretrained model to classification news to visualize in Power BI summary concerns.
#
import os
import csv
from sparknlp.base import DocumentAssembler, Pipeline
from sparknlp.annotator import (
    Tokenizer,
    DistilBertForSequenceClassification)
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from TFM.source.constants import DISTILBERT_MODEL, FILE_CONCERNS_SUMMARY

##
# @brief Pretrained model to classificate news.
# @details Classification news to visualize in Power BI summary concerns, using Pipline and
# pretrained model, and save results on local CSV.
#
# @param df_news (DataFrame): News to be classified
# @param title (str): Collumn name of news title.
# @param description (str): Collumn name of news description.
#
# @return None
def classify_news(df_news: DataFrame, title: str, description: str) -> None:
    """ Classification news to visualize in Power BI summary concerns, using Pipline and pretrained
        model, and save results on local CSV.

    Args:
        df_news (DataFrame): News to be classified
        title (str): Collumn name of news title.
        description (str): Collumn name of news description.
    """
    try:

        document_assembler = DocumentAssembler() \
            .setInputCol('text_news') \
            .setOutputCol('document')

        tokenizer = Tokenizer() \
            .setInputCols(['document']) \
            .setOutputCol('token')

        sequence_classifier = DistilBertForSequenceClassification \
            .load(DISTILBERT_MODEL) \
            .setInputCols(['token', 'document']) \
            .setOutputCol('class') \
            .setCaseSensitive(True) \
            .setMaxSentenceLength(512)

        pipeline = Pipeline(stages=[
            document_assembler,
            tokenizer,
            sequence_classifier
            ])

        # Cast to date.
        df_news = df_news.withColumn("pubDate", F.to_date(F.col("pubDate")))
        df_news_na = df_news.na.drop()
        # Concat title and description in case desc was null.
        df_news_sel = df_news_na.select("iso_code",
                                        "pubDate",
                                        F.concat(F.col(title), F.lit(" "),
                                                F.col(description)).alias("text_news"))

        result = pipeline.fit(df_news_sel).transform(df_news_sel)
        print("## Sample result classification news:")
        df_final = result.select("iso_code", "pubDate", F.col("class.result")[0])
        df_final.show(n=4, truncate=False)

        # Append news processed to local csv.
        file_exists = os.path.isfile(FILE_CONCERNS_SUMMARY)

        # Opend file in append mode to add new processeed news.
        print("##: Save summary concerns into local CSV: ",FILE_CONCERNS_SUMMARY)
        with open(FILE_CONCERNS_SUMMARY, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(['iso_code', 'pubDate', 'summary_concern'])

            # Add last news cleaned.
            for row in df_final.toLocalIterator():
                writer.writerow(row)

        print("\nNews have been classified and saved.")

    except Exception as e:
        print('ERROR - Classifying news: ',e)
