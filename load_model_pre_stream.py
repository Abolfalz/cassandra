from pyspark.ml.clustering import KMeansModel
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import unix_timestamp
from pyspark.ml.feature import StringIndexer
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
#from pyspark.sql.functions import createDataFrame
#import pyspark.sql.types.DataType
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from datetime import datetime


spark = SparkSession.builder.appName("LoadStream").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

model = KMeansModel.load("/home/am/T8/K3_means_Model")

bootstrap_servers = ['localhost:9092']
topicName = 'csv_data'

consumer = KafkaConsumer(topicName, bootstrap_servers = bootstrap_servers, auto_offset_reset = 'latest')


schema = StructType([   
    StructField("Lat", FloatType(),True),
    StructField("Lon", FloatType(),True),
    StructField("Date/Time_unix", IntegerType(),True),
    StructField("BaseIndex", IntegerType(), True)
])


# Connect to Cassandra
cl = Cluster(['127.0.0.1'])
session = cl.connect('ate')

# Delete Table
session.execute("DROP TABLE tableForLocation")

# Create Table
session.execute("CREATE TABLE tableForLocation (id int, DataTime timestamp, Lat double, Lon double, location tuple<float, float>, Base double, ClusterId int, PRIMARY KEY (location,id))")
print('OK')

id_row = 0

for message in consumer:
    message_str = message.value.decode('utf-8')
    data_list = message_str.split(', ')
    lat = float(data_list[1])
    lon = float(data_list[2])
    datetime_unix = int(data_list[4])
    bi = data_list[5]
    base_index = float(bi.split('.')[0])

    data = spark.createDataFrame([(datetime_unix ,lat ,lon ,base_index)], ["Date/Time_unix", "Lat", "Lon", "BaseIndex"])
    vectorAssembler = VectorAssembler(inputCols=["Date/Time_unix", "Lat", "Lon", "BaseIndex"], outputCol="features")
    data = vectorAssembler.transform(data).select('features')

    predictions = model.transform(data)
    row = predictions.collect()
    insert_query = f"INSERT INTO tableForLocation (id, DataTime, Lat, Lon, location, Base, ClusterId) VALUES ({id_row}, '{datetime.fromtimestamp(row[0][0][0])}', {row[0][0][1]},{row[0][0][2]}, {(row[0][0][1],row[0][0][2])}, {row[0][0][3]}, {row[0][1]})"
    session.execute(insert_query)
    id_row += 1 
   
    if id_row == 20:
        consumer.close()

print('\n\n\n')

# Print Data In Table
result = session.execute("SELECT * FROM saveDataStream")
for row in result:
    print(row)