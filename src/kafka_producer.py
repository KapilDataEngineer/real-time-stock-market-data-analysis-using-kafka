from kafka import KafkaProducer
import pandas as pd
import json

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Read CSV
df = pd.read_csv('indexProcessed.csv')

# Convert dictionary to JSON string, then bytes
for i in range(10):
    line = df.sample(1).to_dict(orient='records')[0]
    producer.send('test4', value=json.dumps(line).encode('utf-8'))
    
producer.flush()  # Ensure message is sent

print(f"Message sent: {line}")
