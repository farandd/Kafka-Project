from flask import Flask, request, jsonify
from pyspark.ml.classification import GBTClassificationModel
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os

app = Flask(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SparkApp") \
    .master("local[*]") \
    .getOrCreate()

MODEL_DIR = "spark/models/"

# List to store prediction history
prediction_history = []

def load_model(model_name):
    model_dir = os.path.join(MODEL_DIR, model_name)
    return GBTClassificationModel.load(model_dir) if os.path.exists(model_dir) else None

# Prepare features for the model based on the new variables
def prepare_features(data):
    features = [
        data.get("age", 0),
        data.get("hypertension", 0),
        data.get("heart_disease", 0),
        data.get("bmi", 0),
        data.get("HbA1c_level", 0),
        data.get("blood_glucose_level", 0),
        data.get("gender_index", 0),
        data.get("smoking_history_index", 0)
    ]
    return Vectors.dense(features)

@app.route("/prediction/<model_id>", methods=["POST"])
def predict(model_id):
    data = request.json
    input_vector = prepare_features(data)

    model_name = 'model_' + model_id
    model = load_model(model_name)

    if not model:
        return jsonify({"error": f"Model {model_id} not found"}), 404

    # Convert the input vector into a DataFrame
    input_df = spark.createDataFrame([Row(features=input_vector)])

    # Perform prediction using transform()
    prediction_df = model.transform(input_df)
    prediction = prediction_df.collect()[0].prediction  # Extract prediction

    # Save to prediction history
    prediction_history.append({"model_id": model_id, "input": data, "prediction": int(prediction)})

    return jsonify({"model": int(model_id), "diabetes": int(prediction)})

@app.route("/history", methods=["GET"])
def get_prediction_history():
    return jsonify(prediction_history)

@app.route("/batch-prediction/<model_id>", methods=["POST"])
def batch_prediction(model_id):
    data_list = request.json.get("data", [])

    model_name = 'model_' + model_id
    model = load_model(model_name)

    if not model:
        return jsonify({"error": f"Model {model_id} not found"}), 404

    predictions = []
    for data in data_list:
        input_vector = prepare_features(data)

        # Convert input to DataFrame
        input_df = spark.createDataFrame([Row(features=input_vector)])

        # Perform prediction using transform()
        prediction_df = model.transform(input_df)
        prediction = prediction_df.collect()[0].prediction

        result = {
            "input": data,
            "diabetes": int(prediction)
        }
        predictions.append(result)

    return jsonify({
        "model": model_id,
        "predictions": predictions
    })

if __name__ == "__main__":
    app.run(debug=True)

    spark.stop()
