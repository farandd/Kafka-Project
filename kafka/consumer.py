from kafka import KafkaConsumer
from json import loads

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'server-kafka',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Buffer and batch control
buffer_size = 1500  # Adjust batch size as needed
batch = 0
counter = 0

# Updated header with new column names
header = 'gender,age,hypertension,heart_disease,smoking_history,bmi,HbA1c_level,blood_glucose_level,diabetes\n'

# Open the first batch file
output = open(f'../batch/batch{batch}.csv', 'w', encoding='utf-8')
output.write(header)
output.flush()  # Ensure header is written immediately

for message in consumer:
    data = message.value
    row = ','.join(str(data.get(col, '')) for col in header.strip().split(',')) + '\n'
    output.write(row)
    output.flush()  # Force write each row to disk
    print(data)
    counter += 1

    # Check if buffer size is reached to create a new batch file
    if counter >= buffer_size:
        output.close()  # Close current file
        batch += 1      # Increment batch number
        counter = 0     # Reset counter
        
        # Open a new batch file for the next batch of messages
        output = open(f'../batch/batch{batch}.csv', 'w', encoding='utf-8')
        output.write(header)
        output.flush()  # Ensure header is written immediately
