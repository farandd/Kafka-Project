# Kafka Project

| NRP        | Anggota Kelompok           |
|------------|----------------------------|
| 5027231033 | Raditya Hardian S.         |
| 5027231084 | Farand Febriansyah         |
| 5027231088 | Veri Rahman                |

## Project Big Data and Lakehouse - Diabetes Prediction with Kafka & Spark ML
Proyek ini mensimulasikan pemrosesan data stream menggunakan Apache Kafka dan Apache Spark ML untuk membuat model prediksi diabetes. Data di-stream dari Kafka Producer ke Kafka Server dan diproses dalam batch oleh Kafka Consumer, kemudian digunakan oleh Spark untuk melatih beberapa model Machine Learning dan diakses melalui API.

---

# 1. Persiapan

## 1.1 Setup
Gunakan `docker-compose.yml` berikut untuk menyiapkan Apache Kafka dan Zookeeper.

```yaml
services:
  zookeeper:
    image: "bitnami/zookeeper:latest"
    container_name: zookeeper
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes
    ports:
      - "2181:2181"

  kafka:
    image: "bitnami/kafka:latest"
    container_name: kafka
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_LISTENERS=PLAINTEXT://:9092
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092
    ports:
      - "9092:9092"
    depends_on:
      - zookeeper
```

Jalankan container Docker dengan perintah berikut:
```
docker-compose up --build
```

## 1.2 Membuat Topik Kafka
Masuk ke container Kafka:
```
docker exec -it kafka bash
```

Buat topik bernama `server-kafka`:
```
kafka-topics.sh --create --topic server-kafka --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Cek apakah topik sudah berhasil dibuat:
```
kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

# 2. Streaming Data dengan Kafka

## 2.1 Kafka Producer
`producer.py` akan membaca dataset dan mengirimkan data per baris ke Kafka dengan jeda acak. Jalankan perintah berikut:
```
python3 kafka/producer.py
```

## 2.2 Kafka Consumer
Buka terminal baru dan jalankan `consumer.py` untuk membaca data dari Kafka dan menyimpannya dalam batch:
```
python3 kafka/consumer.py
```

Output akan disimpan dalam folder `batch/` sesuai dengan jumlah data yang diterima per batch.

---

# 3. Training Model dengan Spark ML

## 3.1 Menyiapkan Lingkungan
Aktifkan virtual environment:
```
python3 -m venv venv
source venv/bin/activate
```

## 3.2 Melatih Model
Script `spark_script.py` akan memproses setiap batch data dari folder `batch/`, melakukan preprocessing, melatih model, dan menyimpannya di folder `models/`.

Jalankan perintah berikut untuk melatih model:
```
python3 spark/spark_script.py
```

Model yang dilatih akan disimpan dengan nama `model_1`, `model_2`, dan seterusnya, sesuai dengan batch yang diproses.

---

# 4. API dan Endpoint

## 4.1 Menjalankan API
Jalankan API untuk mengakses model melalui endpoint:
```
python3 api/api.py
```
API akan berjalan pada `http://localhost:5000`.

## 4.2 Endpoint Prediction
### Endpoint `/prediction/<model_id>`
Endpoint ini menerima input JSON dan menghasilkan prediksi dari model yang dipilih.

#### Contoh Request (Model 1)
```
curl -X POST http://localhost:5000/prediction/1 \
 -H "Content-Type: application/json" \
 -d '{
        "age": 65,
        "hypertension": 1,
        "heart_disease": 1,
        "bmi": 30.5,
        "HbA1c_level": 8.0,
        "blood_glucose_level": 160,
        "gender_index": 1,
        "smoking_history_index": 2
    }'
```

#### Contoh Response
```
{
  "diabetes": 1,
  "model": 1
}
```

## 4.3 Endpoint History
### Endpoint `/history`
Mengembalikan riwayat prediksi yang telah dilakukan.

#### Contoh Request
```
curl -X GET http://localhost:5000/history
```

#### Contoh Response
```
[
  {
    "model_id": "1",
    "input": {
      "age": 65,
      "hypertension": 1,
      "heart_disease": 1,
      "bmi": 30.5,
      "HbA1c_level": 8.0,
      "blood_glucose_level": 160,
      "gender_index": 1,
      "smoking_history_index": 2
    },
    "prediction": 1
  }
]
```

## 4.4 Endpoint Batch Prediction
### Endpoint `/batch-prediction/<model_id>`
Mengambil daftar input JSON dan menghasilkan prediksi untuk setiap input.

#### Contoh Request
```
curl -X POST http://localhost:5000/batch-prediction/1 \
 -H "Content-Type: application/json" \
 -d '{
        "data": [
            {
                "age": 65,
                "hypertension": 1,
                "heart_disease": 1,
                "bmi": 30.5,
                "HbA1c_level": 8.0,
                "blood_glucose_level": 160,
                "gender_index": 1,
                "smoking_history_index": 2
            },
            {
                "age": 45,
                "hypertension": 0,
                "heart_disease": 0,
                "bmi": 25.3,
                "HbA1c_level": 6.5,
                "blood_glucose_level": 120,
                "gender_index": 0,
                "smoking_history_index": 1
            }
        ]
    }'
```

#### Contoh Response
```
{
  "model": "1",
  "predictions": [
    {
      "input": {
        "age": 65,
        "hypertension": 1,
        "heart_disease": 1,
        "bmi": 30.5,
        "HbA1c_level": 8.0,
        "blood_glucose_level": 160,
        "gender_index": 1,
        "smoking_history_index": 2
      },
      "prediction": 1
    },
    {
      "input": {
        "age": 45,
        "hypertension": 0,
        "heart_disease": 0,
        "bmi": 25.3,
        "HbA1c_level": 6.5,
        "blood_glucose_level": 120,
        "gender_index": 0,
        "smoking_history_index": 1
      },
      "prediction": 0
    }
  ]
}
```
