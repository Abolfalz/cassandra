from pyspark.ml.clustering import KMeansModel
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import unix_timestamp
from pyspark.ml.feature import StringIndexer
import matplotlib.pyplot as plt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from datetime import datetime


# Create Spark Session
spark = SparkSession.builder.appName("LoadStream").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print('\n\n\n\n\n\n\n\n\n\n')



# Load Model
model = KMeansModel.load("/home/am/T8/K3_means_Model")

# Load Test Data
data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/home/am/T8/stream_test_set.csv/part-00000-b2ee3482-dd41-43f0-83b4-d1ca623a81f0-c000.csv")

# VectorAssembler
vectorAssembler = VectorAssembler(inputCols=["Date/Time_unix", "Lat", "Lon", "BaseIndex"], outputCol="features")
test_data = vectorAssembler.transform(data).select('features')

# Prediction
predictions = model.transform(test_data)


# Sava In DataBase
# Connect to Cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()


# Create New Database 
session.execute("CREATE KEYSPACE ate WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true")

# Connect to Database
session = cluster.connect('ate')

# Create New Table
session.execute("CREATE TABLE mytable (id int, DataTime timestamp, Lat double, Lon double, Base double, ClusterId int, PRIMARY KEY ((ClusterId), id))")


# Insert Data And Predictions in Table
id_row = 1
for row in predictions.collect():
    session.execute("INSERT INTO mytable (id, DataTime, Lat, Lon, Base, ClusterId) VALUES (%s, %s, %s, %s,%s, %s)", (id_row, datetime.fromtimestamp(row[0][0]), row[0][1], row[0][2], row[0][3], row[1]))
    id_row += 1

print('---------------------------------------------------------------------------------')


# Print Data Saved In Table - 20 Rows
result = session.execute("SELECT * FROM mytable LIMIT 20")
for row in result:
    print(row)



def count_cluster():
    result = session.execute("SELECT ClusterId, COUNT(*) FROM mytable GROUP BY ClusterId")
    for row in result:
        print(row)

count_cluster()



def service_in_week():
    result = session.execute("SELECT * FROM mytable WHERE DataTime >= '2014-08-03T00:00:00.000Z' AND DataTime <= '2014-08-11T00:00:00.000Z' ALLOW FILTERING")
    for row in result:
            print(row)

service_in_week()



def Top_cluster_work_on_week():
    result = session.execute("SELECT clusterid, COUNT(*) as count FROM mytable WHERE datatime >= '2014-08-19T00:00:00.000Z' AND datatime <= '2014-08-27T00:00:00.000Z' GROUP BY clusterid ALLOW FILTERING")
    for row in result:
            print(row)

Top_cluster_work_on_week()





def creat_new_table_for_query():
    session.execute("CREATE TABLE tableForLocation (id int, DataTime timestamp, Lat double, Lon double, location tuple<float, float>, Base double, ClusterId int, PRIMARY KEY (location,id))")

creat_new_table_for_query()



def insert_to_new_table():
    result = session.execute("SELECT * FROM mytable")
    for row in result:
        print(row.datatime)
        insert_query = f"INSERT INTO tableForLocation (id, DataTime, Lat, Lon, location, Base, ClusterId) VALUES ({row.id}, '{row.datatime}', {row.lat}, {row.lon}, {(row.lat,row.lon)}, {row.base}, {row.clusterid})"
        session.execute(insert_query)   

insert_to_new_table()




def hight_traffic_in_ten_days_ago():
    l = []
    a_query ="SELECT location, COUNT(*) AS count FROM tableForLocation WHERE datatime >= '2014-08-21T00:00:00.000Z' AND datatime <= '2014-08-31T23:59:00.000Z' GROUP BY location ALLOW FILTERING"
    result = session.execute(a_query)
    for row in result:
        l.append([row.location,row.count])
    
    sorted_list = sorted(l, key=lambda x: x[1], reverse=True)
    for item in sorted_list[:10]:
        print(item)

hight_traffic_in_ten_days_ago()