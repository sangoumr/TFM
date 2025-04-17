##
# @package train_model.py
# Train the Logistic Regression, and Naive Bayes prediction model as a base, with old news from the
# New York Times from the last 50 years.
#
import os
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.feature import Tokenizer, CountVectorizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from sparknlp.base import Pipeline
from TFM.source.get_dataset import get_dataset_old_news_weight
from TFM.source.constants import (
    PATH_HDFS_OLD_NEWS_CLEANED,
    PATH_HDFS_BEST_MODEL,
    FILE_METRICS_BEST_MODEL
    )

##
# @brief Train the Logistic Regression, and Naive Bayes prediction model as a base, with old
# news from the New York Times from the last 50 years.
# @details Create a Tokenizer, HashingTF, TF-IDF, a logistic regression object, and add everything to
# a pipeline.
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

        Create a Tokenizer, HashingTF, TF-IDF, a logistic regression object, and add everything to
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
    # Tokenizer text to get: num features, prepare hashing tricks, and convert hashed symbols
    # to TF-IDF.
    tokenizer = Tokenizer(inputCol="words_text", outputCol="words")
    df_tokenized = tokenizer.transform(df_final)

    # Get count Vectorizet to get vocaulary
    cv = CountVectorizer(inputCol="words", outputCol="features")
    cv_model = cv.fit(df_tokenized)
    # Get vocabulary.
    vocabulari = cv_model.vocabulary
    # Get numFeatures
    num_features = len(vocabulari)
    print(f"## Number of unique words in vocabulary: {num_features}")
    df_tokenized.show(n=5)

    # Apply the hashing trick
    has_tf = HashingTF(inputCol='words', outputCol='hash')

    # Convert hashed symbols to TF-IDF
    tf_idf = IDF(inputCol='hash', outputCol='features')

    # Create a logistic regression object and add everything to a pipeline
    logistic = LogisticRegression(weightCol="weight")
    pipeline = Pipeline(stages=[tokenizer, has_tf, tf_idf, logistic])

    # Split the data into training and testing sets
    news_train, news_test = df_final.randomSplit([0.8, 0.2], seed=13)

    # Comptar etiquetes al dataset de training
    print("## Label count in news_train::")
    news_train.groupBy("label").count().show()

    # Comptar etiquetes al dataset de test
    print("## Label count in news_test:")
    news_test.groupBy("label").count().show()

    # Create a classifier object and train on training data.
    model = pipeline.fit(news_train)

    print("### Logistic Regresion Confusion Matrix:")
    # Create predictions for the testing data and show confusion matrix.
    predict = model.transform(news_test)
    predict.groupBy('label', 'prediction').count().show()

    print("### Logistic Regresion Tuning ###")
    # Create parameter grid
    params = ParamGridBuilder()

    # Add grid for hashing trick parameters and logistic regression parameters, and build.
    params = params.addGrid(has_tf.numFeatures, [1000, 10000,  num_features]) \
                .addGrid(logistic.regParam, [0.001, 0.01, 0.1]).build()

    print('Number of models to be tested: ', len(params))
    evaluator = BinaryClassificationEvaluator()
    # Create cross-validator for logistic
    crossval = CrossValidator(estimator=pipeline,
                              estimatorParamMaps=params,
                              evaluator=evaluator,
                              numFolds=5)

    # Run cross-validation, and choose the best set of parameters.
    cv_model_l = crossval.fit(news_train)

    # Make predictions on test documents. cvModel uses the best model found (lrModel).
    prediction_cv_l = cv_model_l.transform(news_test)
    print("### Logistic Regresion (CV) - Confusion Matrix:")
    # Create predictions for the testing data and show confusion matrix.
    prediction_cv_l.groupBy('label', 'prediction').count().show()

    print("## Sample of predict Logistic Regression (CV)")
    selected = prediction_cv_l.select('label', 'prediction', "probability")
    for row in selected.collect()[:5]:
        print(row)

    # Create model Naive Bayes
    nb = NaiveBayes(weightCol="weight",
                    featuresCol="features",
                    labelCol="label",
                    predictionCol="prediction")

    pipeline_nb = Pipeline(stages=[tokenizer, has_tf, tf_idf, nb])

    model_nb = pipeline_nb.fit(news_train)
    prediction_nb = model_nb.transform(news_test)
    print("### Naive Bayes - Confusion Matrix:")
    # Create predictions for the testing data and show confusion matrix.
    prediction_nb.groupBy('label', 'prediction').count().show()

    # Construcció de grid de paràmetres
    param_grid_nb = ParamGridBuilder() \
        .addGrid(has_tf.numFeatures, [100, 1000, num_features]) \
        .addGrid(nb.smoothing, [0.5, 1.0, 1.5]) \
        .addGrid(nb.modelType, ["multinomial", "gaussian"]) \
        .build()


    print('Number of models to be tested: ', len(param_grid_nb))
    # CrossValidator for Naive Bayes
    crossval_nb = CrossValidator(estimator=pipeline_nb,
                        estimatorParamMaps=param_grid_nb,
                        evaluator=evaluator,
                        numFolds=5)

    # Run cross-validation, selecting the best parameters and model.
    cv_model_nb = crossval_nb.fit(news_train)

    # Make predictions on test documents. cvModel uses the best model found (lrModel).
    prediction_nv = cv_model_nb.transform(news_test)
    print("### Naive Bayes (CV) - Confusion Matrix:")
    # Create predictions for the testing data and show confusion matrix.
    prediction_nv.groupBy('label', 'prediction').count().show()

    # Instance metrics
    evaluator_auc = BinaryClassificationEvaluator(metricName="areaUnderROC")

    # Compare AUC on testing data
    def eval_model(name, models, dataset):
        auc = evaluator_auc.evaluate(models.transform(dataset))
        print(f"{name:<40} AUC: {auc:.4f}")


    eval_model("\n## Logistic Regression", model, news_test)
    eval_model("## Logistic Regression (CV)", cv_model_l, news_test)
    eval_model("## Naive Bayes", model_nb, news_test)
    eval_model("## Naive Bayes (CV)", cv_model_nb, news_test)

    # Save best model
    cv_model_l.bestModel.write().overwrite().save(PATH_HDFS_BEST_MODEL)

    # Save metrics of best model as csv to show in visualization Power BI
    # Calcular mètriques
    metrics = {
        "Model": "Logistic Regression (CV)",
        "AUC": evaluator_auc.evaluate(cv_model_l.transform(news_test)),
    }

    # To Pandas and save as CSV to show in Power Bi visualization.
    df_metrics = pd.DataFrame([metrics])
    df_metrics.to_csv(FILE_METRICS_BEST_MODEL, index=False)



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

    my_spark.stop()

except KeyboardInterrupt:
    my_spark.stop()
    print("Spark session closed. KeyboardInterrupt.")
    pass
except Exception as e:
    my_spark.stop()
    print('ERROR - Train model: ',e)
