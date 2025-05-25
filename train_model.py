##
# @package train_model.py
# Train the Logistic Regression, and Naive Bayes prediction model as a base, with old news from the
# New York Times from the last 50 years.
#
import os
from pyspark.sql import SparkSession
import pandas as pd
from sklearn.metrics import classification_report
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
from pyspark.ml import PipelineModel
from pyspark.sql.window import Window
from pyspark.ml.feature import Tokenizer, CountVectorizer, IDF
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from sparknlp.base import Pipeline
from TFM.source.get_dataset import get_dataset_old_news_weight
from TFM.source.constants import (
    PATH_HDFS_OLD_NEWS_CLEANED,
    PATH_HDFS_BEST_MODEL,
    FILE_METRICS_BEST_MODEL,
    FILE_CONCERNS_WORDS
    )

##
# @brief Train the Logistic Regression, and Naive Bayes prediction model as a base, with old
# news from the New York Times from the last 50 years.
# @details Create a Tokenizer, CountVectorizer, TF-IDF, a logistic regression object, and add everything
# to a pipeline.
# Also make the same pipeline with Naive Bayes.
# And tune de params with grid and CrossValidator for bouth classivfiers with train set, and
# compare test set predictions with AUC metric.
# Since the best results are given by the Logistic Regression Model with the cross validation
# of the parameter search, this will be the model that is kept for the predictions of the news
# that are downloaded daily.
#
# @param df_final (DataFrame): Dataset to use in train models.
#
# @return None.
def get_models_weight(df_final: DataFrame) -> None:
    """ Train the Logistic Regression, and Naive Bayes prediction model as a base, with old news
        from the New York Times from the last 50 years.

        Create a Tokenizer, CountVectorize, TF-IDF, a logistic regression object, and add everything to
        a pipeline.
        Also make the same pipeline with Naive Bayes.
        And tune de params with grid and CrossValidator for bouth classivfiers with train set, and
        compare test set predictions with AUC metric.

        Since the best results are given by the Logistic Regression Model with the cross validation
        of the parameter search, this will be the model that is kept for the predictions of the news
        that are downloaded daily.

    Args:
        df_final (DataFrame): Dataset to use in train models.
    """
    # Tokenizer text to get: num features, prepare count Vectorizer, and convert hashed symbols
    # to TF-IDF.
    tokenizer = Tokenizer(inputCol="words_text",
                          outputCol="words")
    df_tokenized = tokenizer.transform(df_final)

    # Get count Vectorizet to get vocaulary.
    cv = CountVectorizer(inputCol="words",
                         outputCol="countV")
    cv_model = cv.fit(df_tokenized)
    # Get vocabulary.
    vocabulari = cv_model.vocabulary
    # Get numFeatures.
    num_features = len(vocabulari)
    print(f"## Number of unique words in vocabulary: {num_features}")
    df_tokenized.show(n=5)

    # Convert hashed symbols to TF-IDF.
    tf_idf = IDF(inputCol='countV',
                 outputCol='features')

    # Create a logistic regression object and add everything to a pipeline.
    logistic = LogisticRegression(weightCol="weight",
                                  featuresCol="features",
                                  labelCol="label",
                                  predictionCol="prediction")
    pipeline = Pipeline(stages=[tokenizer, cv, tf_idf, logistic])

    # Split the data into training and testing sets.
    news_train, news_test = df_final.randomSplit([0.8, 0.2], seed=13)

    # Comptar etiquetes al dataset de training.
    print("## Label count in news_train::")
    news_train.groupBy("label").count().show()

    # Comptar etiquetes al dataset de test.
    print("## Label count in news_test:")
    news_test.groupBy("label").count().show()

    # Create a classifier object and train on training data.
    model = pipeline.fit(news_train)

    print("### Logistic Regression Confusion Matrix:")
    # Create predictions for the testing data and show confusion matrix.
    predict = model.transform(news_test)
    predict.groupBy('label', 'prediction').count().show()

    print("### Logistic Regression Tuning ###")
    # Create parameter grid.
    params = ParamGridBuilder()

    # Add grid for countVectorizer parameters and logistic regression parameters, and build.
    params = params.addGrid(cv.vocabSize, [1000, 10000,  num_features]) \
                .addGrid(logistic.regParam, [0.001, 0.01, 0.1]).build()

    print('Number of models to be tested: ', len(params))
    evaluator = BinaryClassificationEvaluator()
    # Create cross-validator for logistic regression.
    crossval = CrossValidator(estimator=pipeline,
                              estimatorParamMaps=params,
                              evaluator=evaluator,
                              numFolds=5)

    # Run cross-validation, and choose the best set of parameters.
    cv_model_l = crossval.fit(news_train)

    # Make predictions on test documents. cvModel uses the best model found (lrModel).
    prediction_cv_l = cv_model_l.transform(news_test)

    print("### Logistic Regression (CV) - Confusion Matrix:")
    # Create predictions for the testing data and show confusion matrix.
    prediction_cv_l.groupBy('label', 'prediction').count().show()

    print("## Sample of predict Logistic Regression (CV)")
    selected = prediction_cv_l.select('label', 'prediction', "probability")
    for row in selected.collect()[:5]:
        print(row)

    # Create model Naive Bayes.
    nb = NaiveBayes(weightCol="weight",
                    featuresCol="features",
                    labelCol="label",
                    predictionCol="prediction")

    pipeline_nb = Pipeline(stages=[tokenizer, cv, tf_idf, nb])

    model_nb = pipeline_nb.fit(news_train)
    prediction_nb = model_nb.transform(news_test)
    print("### Naive Bayes - Confusion Matrix:")
    # Create predictions for the testing data and show confusion matrix.
    prediction_nb.groupBy('label', 'prediction').count().show()

    # Construction of the parameter grid.
    param_grid_nb = ParamGridBuilder() \
        .addGrid(cv.vocabSize, [1000, 10000, num_features]) \
        .addGrid(nb.smoothing, [0.5, 1.0, 1.5]) \
        .addGrid(nb.modelType, ["multinomial", "gaussian"]) \
        .build()


    print('Number of models to be tested: ', len(param_grid_nb))
    # CrossValidator for Naive Bayes.
    crossval_nb = CrossValidator(estimator=pipeline_nb,
                        estimatorParamMaps=param_grid_nb,
                        evaluator=evaluator,
                        numFolds=5)

    # Run cross-validation, selecting the best parameters and model.
    cv_model_nb = crossval_nb.fit(news_train)

    # Make predictions on test documents. cvModel uses the best model found.
    prediction_nv = cv_model_nb.transform(news_test)
    print("### Naive Bayes (CV) - Confusion Matrix:")
    # Create predictions for the testing data and show confusion matrix.
    prediction_nv.groupBy('label', 'prediction').count().show()

    # Instance metrics
    evaluator_auc = BinaryClassificationEvaluator(metricName="areaUnderROC")
    evaluator_f1 = MulticlassClassificationEvaluator(metricName="f1")

    max_auc = 0
    # Compare AUC on testing data
    def eval_model(name, cors_val, models, dataset):
        nonlocal max_auc
        predictions = models.transform(dataset)

        # Final count by label, groub by iso_code, between appointments, and label, to obtain the
        # majority prediction in electoral period, in USA usrually 4 years.
        predictions_period_group = predictions.groupBy("iso_code",
                                                 "before_app",
                                                 "next_app",
                                                 "label",
                                                 "prediction").count()

        # Get winner label for each period and calcultate F1-Score
        # Set window grouped and ordered to get winner prediction.
        window_perid = Window.partitionBy("iso_code",
                                           "before_app",
                                           "next_app",
                                           "label"
                                           ).orderBy(F.col("count").desc(),
                                                     F.col("prediction").asc())

        # Add row ID to get winner label.
        predictions_period = predictions_period_group.withColumn("row_num",
                                                                 F.row_number().over(window_perid))
        # Get winner labe (have more days of news in count).
        predictions_period = predictions_period.filter(F.col("row_num") == 1).drop("row_num")

        # Get metrics.
        auc = evaluator_auc.evaluate(predictions)
        f1_score = evaluator_f1.evaluate(predictions)
        f1_score_period = evaluator_f1.evaluate(predictions_period)

        print(f"\n### {name:<40} AUC: {auc:.4f} F1-Score: {f1_score:.4f} " \
              f"F1-Score-period: {f1_score_period:.4f}")

        print("\n## Report metrics of prediction by day:")
        print(classification_report(predictions.select("label").rdd.flatMap(lambda x: x).collect(),
                                    predictions.select("prediction").rdd.flatMap(lambda x: x).collect()))
        print("\n## Report metrics of prediction grouped by election period")
        print(classification_report(predictions_period.select("label").rdd.flatMap(lambda x: x).collect(),
                                    predictions_period.select("prediction").rdd.flatMap(lambda x: x).collect()))
        
        print("## Sample of prediction by days:")
        predictions.select("iso_code", "pubDate", "label", "probability", "prediction").show(n=10)
        print("## Sample of prediction grouped by election period and counting label predicted:")
        predictions_period_group.show(n=30)
        print("\n## Sample of election period with label and prediction winner: ")
        predictions_period.show(n=30)

        if auc > max_auc:
            max_auc = auc
            if cors_val:
                # Save best model
                models.bestModel.write().overwrite().save(PATH_HDFS_BEST_MODEL)
            else:
                # Save model
                models.write().overwrite().save(PATH_HDFS_BEST_MODEL)
            # Save metrics of best model as csv to show in visualization Power BI
            # Calcular mètriques
            metrics = {
                "Model": name,
                "AUC": max_auc,
                "F1-Score_day": f1_score,
                "F1-Score_period": f1_score_period,

            }
            # To Pandas and save as CSV to show in Power Bi visualization.
            df_metrics = pd.DataFrame([metrics])
            df_metrics.to_csv(FILE_METRICS_BEST_MODEL, index=False)

    eval_model("Naive Bayes", False, model_nb, news_test)
    eval_model("Naive Bayes (CV)", True, cv_model_nb, news_test)
    eval_model("Logistic Regression", False, model, news_test)
    eval_model("Logistic Regression (CV)", True ,cv_model_l, news_test)
    


def get_words_coefficients()-> None:
    """ Get concerns words and its coefficients from best model, and save as CSV to show in
        visualization.
    """    
    # 1. Carrega el model entrenat
    model_entrenat = PipelineModel.load(PATH_HDFS_BEST_MODEL)

    # 2. Extreu el CountVectorizerModel i LogisticRegressionModel
    cv_model = model_entrenat.stages[1]  
    lr_model = model_entrenat.stages[3]  

    # Get Vocabulary
    vocab = cv_model.vocabulary

    # Get Coeficients
    coefficients = lr_model.coefficients.toArray()

    # Get DataFrame convining vocabulary and its coefficients
    weights_words = pd.DataFrame({
        "word": vocab,
        "weigh": coefficients
    })

    # Show 10 first negative and positive words
    print("\n## More positive words of model: \n",
          weights_words[["word", "weigh"]].sort_values("weigh", ascending=False) \
            .head(10).to_string(index=False))
    print("\n## More negative words of model: \n",
          weights_words[["word", "weigh"]].sort_values("weigh", ascending=True) \
            .head(10).to_string(index=False))
    weights_words.to_csv(FILE_CONCERNS_WORDS, index=False)


try:

    # Set initial path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Crear la SparkSession
    my_spark = SparkSession.builder.appName("prepare_dataset").getOrCreate()
    my_spark.sparkContext.setLogLevel("ERROR")

    # Get Dataset from old news to train models and save the best
    df_final_old_news = get_dataset_old_news_weight(my_spark,
                                                    PATH_HDFS_OLD_NEWS_CLEANED,
                                                    iso_code='us')
    get_models_weight(df_final_old_news)

    # Get and save new words and its coeficients to use in Power Bi visualization.
    get_words_coefficients()

    my_spark.stop()

except KeyboardInterrupt:
    my_spark.stop()
    print("Spark session closed. KeyboardInterrupt.")
    pass
except Exception as e:
    my_spark.stop()
    print('ERROR - Train model: ',e)
