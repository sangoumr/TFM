from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from sparknlp.base import DocumentAssembler, Pipeline
from sparknlp.annotator import (
    Tokenizer,
    Normalizer,
    LemmatizerModel,
    StopWordsCleaner,
    BertForTokenClassification
)
from constants import *
from collections import Counter
import sparknlp
import pyspark.sql.functions as F



def nlp_proces_news(path_news: str)-> bool:

    try:
        npl_spark = sparknlp.start()

        # Create my_spark
        my_spark = SparkSession.builder \
                    .appName("Npl news") \
                    .getOrCreate()
        # Llegeix la columna com a string
        df_old_news_f = my_spark.read.parquet(path_news)

        # Filtra per la data concreta
        df_old_news_f = df_old_news_f.filter((F.month("pubDate") == 2) & (F.year("pubDate") == 2025))
        
        df_old_news_f.printSchema()
        
        print(df_old_news_f.count())  # Mostra les primeres files
        df_old_news_f.show(n=3)
        df_old_news_f.select(F.col('description')).show(n=3, truncate=False)
        
        # Transforms raw texts to `document` annotation
        document_assembler = (
            DocumentAssembler()
            .setInputCol("description")
            .setOutputCol("document")
        )

        # Gets the tokens of the text
        tokenizer = (
            Tokenizer()
            .setInputCols(["document"])
            .setOutputCol("token")
        )

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
        
        # Remove stopwords
        stop_words = StopWordsCleaner.pretrained('stopwords_en', 'en')\
            .setInputCols(["normalized"]) \
            .setOutputCol("clean_SW_Tokens") \
            .setCaseSensitive(False)

        # Get Named Entity Recognition
        tokenClassifier  = BertForTokenClassification.load("file:///home/roser/models_npl/berttest") \
            .setInputCols(["document","clean_SW_Tokens"]) \
            .setOutputCol("ner") \

        
        pipeline_norm = Pipeline(stages=[document_assembler, tokenizer, lemmatizer, normalizer, stop_words, tokenClassifier])
        
        model = pipeline_norm.fit(df_old_news_f)
        result = model.transform(df_old_news_f)

        print("### 0: Pipeline transforms: \n",result.columns)
        result.select(F.col("token.result").alias("token"),
                    F.col("lemma.result").alias("lemma"), 
                    F.col("normalized.result").alias("norm"), 
                    F.col("clean_SW_Tokens.result").alias("clean_SW_Tokens"),  
                    F.col("ner.result").alias("ner")
                    ).show(n=3, truncate=False)


        pipelineDF = result.withColumn("cleanTokens_result", F.expr("transform(clean_SW_Tokens, x -> x.result)")) \
                            .withColumn("ner_result", F.expr("transform(ner, x -> x.result)")) 
        print("### 1: Get results of clean_SW_Tokens and NER: \n",pipelineDF.columns)
        pipelineDF.select("iso_code","pubDate","cleanTokens_result","ner_result").show(n=3, truncate=False)
        

        pipelineDF = pipelineDF.withColumn("tokens_n_ner", F.arrays_zip("cleanTokens_result", "ner_result"))  
        print("### 3: cleanToken and NER combined:\n",pipelineDF.columns)
        pipelineDF.select("iso_code","pubDate","tokens_n_ner").show(n=3, truncate=False)

        pipelineDF = pipelineDF.withColumn(
            "filtered_tokens",
            F.expr(f"filter(tokens_n_ner, x -> NOT x.ner_result rlike '{NER_EXCLUDE}')")
        )
        print("### 4: ner excluded: \n",pipelineDF.columns)
        pipelineDF.select("iso_code","pubDate","filtered_tokens").show(n=3, truncate=False)

        pipelineDF = pipelineDF.withColumn("lower_token", F.expr("transform(filtered_tokens, x -> lower(x.cleanTokens_result))"))
        print("### 5: to lower: \n",pipelineDF.columns)
        pipelineDF.select("iso_code","pubDate","lower_token").show(n=3, truncate=False)

        pipelineDF = pipelineDF.withColumn(
            "final_token",
            F.expr(f"filter(lower_token, x -> NOT x rlike '{EXCLUDE_WORDS}')")
        )    
        print("### 6: filter webs (www...): \n",pipelineDF.columns)
        pipelineDF.select("iso_code","pubDate","final_token").show(n=3, truncate=False)

        # Save new features for visualization in Power Bi
        # First group tokens by day
        group_date_DF = pipelineDF.groupBy(["iso_code","pubDate"]).agg(F.flatten(F.collect_list("final_token")).alias("all_tokens"))
        print("### 7: group tokens by day: \n",group_date_DF.columns)
        group_date_DF.show(n=3, truncate=False)

        # UDF amb top 10 més freqüents, ordenades descendentment
        list_schema = ArrayType(StructType([
            StructField("word", StringType(), True),
            StructField("count", IntegerType(), True)
        ]))
        
        def word_count_top(tokens):
            # Recompte de tokens ordenats descendentment amb límit de 10
            if tokens is None:
                return []
            counts = Counter(tokens)
            sorted_counts = counts.most_common(LIMIT_WORD_COUNT_DAY)
            return [[word, count] for word, count in sorted_counts]

        # Enregistrar la UDF
        word_count_udf = F.udf(word_count_top, list_schema)

        # Aplicar-la
        df_count_tokens = group_date_DF.withColumn("wordcount_top10", word_count_udf("all_tokens"))
        print("### 8: 10 most common words : \n",group_date_DF.columns)
        df_count_tokens.select("iso_code","pubDate","wordcount_top10").show(n=3, truncate=False)

        my_spark.stop()
        npl_spark.stop()
    except KeyboardInterrupt:
        my_spark.stop()
        npl_spark.stop()
        pass
    except Exception as e: 
        my_spark.stop()
        npl_spark.stop()
        print('ERROR - Npl: ',e)



result = nlp_proces_news("/TFM/old_news")        
