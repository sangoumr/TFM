from pyspark.sql import SparkSession
import sparknlp
# Import the required modules and classes
from sparknlp.base import DocumentAssembler, Pipeline
from sparknlp.annotator import (
    Tokenizer,
    BertForTokenClassification
)
import pyspark.sql.functions as F

try:
    npl_spark = sparknlp.start()

    documentAssembler = DocumentAssembler() \
        .setInputCol('text') \
        .setOutputCol('document')
    
    tokenizer = Tokenizer() \
        .setInputCols(['document']) \
        .setOutputCol('token')

    tokenClassifier  = BertForTokenClassification.load("file:///home/roser/models_npl/berttest") \
        .setInputCols(["document","token"]) \
        .setOutputCol("ner")

    pipeline = Pipeline().setStages([documentAssembler, tokenizer, tokenClassifier])
    data = npl_spark.createDataFrame([["Scientists and legal experts from Europe, Asia, Donald Trump, Latin America and the United States, meeting this weekend at the University of Rhode Island, heard pleas yesterday for a new approach to the idea of national rights over the sea bed and ocean floor."]]).toDF("text")
    pipelineModel = pipeline.fit(data)
    pipelineDF = pipelineModel.transform(data)

    # Rename features before zip because it assing same name
    pipelineDF = pipelineDF.withColumn("token_result", F.expr("transform(token, x -> x.result)")) \
                        .withColumn("ner_result", F.expr("transform(ner, x -> x.result)"))
    print(pipelineDF.columns)
    # Zip cols
    zipped_df = pipelineDF.select(F.explode(F.arrays_zip("token_result", "ner_result")).alias("zipped"))

    # and select 
    final_df = zipped_df.select(
        F.col("zipped.token_result").alias("token"),
        F.col("zipped.ner_result").alias("ner")
    )
    filter_df = final_df.filter(~F.col("ner").rlike(".*-PER"))

    # Mostrem el resultat
    filter_df.show(truncate=False)
    
    npl_spark.stop()

except Exception as e: 

    print('ERROR - Npl: ',e)
    