from kafka import KafkaProducer
import pandas as pd
import time

bootstrap_servers = ['localhost:9092']
topicName = 'csv_data'

df = pd.read_csv('/home/am/T7/stream_test_set.csv/part-00000-b2ee3482-dd41-43f0-83b4-d1ca623a81f0-c000.csv', nrows=20)

producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
for _, row in df.iterrows():  
    message = str(row.values.tolist())  
    producer.send(topicName, message.encode('utf-8'))
    print(message)
    time.sleep(1)
producer.close()