from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import os
import time

spark = SparkSession.builder \
    .appName("KafkaSparkML") \
    .getOrCreate()

current_dir = os.path.dirname(os.path.abspath(__file__))
batch_folder_path = os.path.join(current_dir, "../batch")
model_save_path = os.path.join(current_dir, "models")

def load_and_preprocess_data(batch_file_path):
    # Load the data
    df = spark.read.csv(batch_file_path, header=True, inferSchema=True)

    # Index the label column 'diabetes' for classification
    label_indexer = StringIndexer(inputCol="diabetes", outputCol="label").fit(df)
    df = label_indexer.transform(df)

    # Define categorical columns to index
    categorical_columns = ['gender', 'smoking_history']
    indexers = [StringIndexer(inputCol=col, outputCol=col + "_index").fit(df) for col in categorical_columns]
    
    # Apply indexing transformations for categorical columns
    for indexer in indexers:
        df = indexer.transform(df)

    # Drop original categorical columns
    df = df.drop(*categorical_columns)
    
    # Define feature columns
    feature_columns = [
        'age', 'hypertension', 'heart_disease', 'bmi', 'HbA1c_level', 
        'blood_glucose_level', 'gender_index', 'smoking_history_index'
    ]
    
    # Assemble feature columns into a single 'features' vector
    assembler = VectorAssembler(inputCols=feature_columns, outputCol='features')
    df = assembler.transform(df)
    df = df.select('features', 'label')
    return df

def train_and_save_model(df, model_name):
    # Split the data into training and testing sets
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=1234)
    
    # Define and train the Gradient-Boosted Tree model
    gbt = GBTClassifier(featuresCol='features', labelCol='label', maxIter=50, seed=1)
    model = gbt.fit(train_data)

    # Make predictions and evaluate the model
    predictions = model.transform(test_data)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    f1_score = evaluator_f1.evaluate(predictions)

    # Print model information and save the model
    print(f"Model saved at {model_save_path}/{model_name}")
    print(f"Model Accuracy: {accuracy:.2f}")
    print(f"F1 Score: {f1_score:.2f}")
    
    # Save the trained model
    model_dir = os.path.join(model_save_path, model_name)
    model.write().overwrite().save(model_dir)

def process_batches():
    # List batch files and process each batch
    batch_files = sorted(os.listdir(batch_folder_path))
    batch_count = 0
    
    for batch_file in batch_files:
        batch_file_path = os.path.join(batch_folder_path, batch_file)
        df = load_and_preprocess_data(batch_file_path)
        model_name = f"model_{batch_count + 1}"
        train_and_save_model(df, model_name)
        
        batch_count += 1
        print(f"Batch {batch_count} processed, and model {model_name} has been trained")
        
        time.sleep(5)

if __name__ == "__main__":
    process_batches()
    spark.stop()
