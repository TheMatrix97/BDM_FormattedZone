import json
import os
from process import Process
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from utils.properties_parser import parse_properties


class KafkaStreamingProcess(Process):
    def __init__(self):
        super().__init__()
        self.server = parse_properties('kafka')['kafka.server']
        self.topic = parse_properties('kafka')['kafka.topic']
        self.consumer = KafkaConsumer(self.topic, bootstrap_servers=self.server)
        self.monetuser = parse_properties('monetdb')['database.user']
        self.monetpassw = parse_properties('monetdb')['database.password']
    
    def run_process(self):
        

        #for record in self.consumer:
        #    print(record.value)
        
        
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
        spark = SparkSession.builder.master("local[*]").appName("datasource - kafka").config('spark.driver.extraClassPath',
                './drivers/monetdb-jdbc-3.2.jre8.jar').getOrCreate()

        df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", self.server) \
            .option("subscribe", self.topic).load()

        df = df.selectExpr("CAST(value AS STRING)")
        # https://stackoverflow.com/questions/39235704/split-spark-dataframe-string-column-into-multiple-columns
        split_col = split(df['value'], ',')
        df = df.withColumn('date', split_col.getItem(0))
        df = df.withColumn('neighborhood', split_col.getItem(1))
        df = df.withColumn('price', split_col.getItem(2))

        
        # Example output b'2022-05-22 12:43:23.276091,Q3751076,371515'
        #query = df.writeStream.outputMode("append").format("console").start()
        #query.awaitTermination()
        
        column_types = "date TIMESTAMP, price INT"

        properties = {
            "user": parse_properties('monetdb')['database.user'],
            "password": parse_properties('monetdb')['database.password'],
            "driver": "org.monetdb.jdbc.MonetDriver",
            "batchsize": 10000
        }
        
        # Example stream to jdbc
        def foreach_batch_function(df, epoch_id):
            df.write.format("jdbc").mode('append').options(
                url=f"jdbc:monetdb://{parse_properties('monetdb')['database.host']}:50000/mydb",
                dbtable='kafka_data', 
                createTableColumnTypes=column_types, **properties).save()
  
        res = df.writeStream.foreachBatch(foreach_batch_function).start()   
        res.awaitTermination()

        # Ask Sergi -> Do we need to store this information? or just for realtime validation of the model
        
    
