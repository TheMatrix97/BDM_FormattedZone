import json
import os
from process import Process
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from utils.properties_parser import parse_properties


class KafkaStreamingProcess(Process):
    def __init__(self):
        super().__init__()
        self.server = parse_properties('kafka')['kafka.server']
        self.topic = parse_properties('kafka')['kafka.topic']
        self.consumer = KafkaConsumer(self.topic, bootstrap_servers=self.server)
    
    def run_process(self):
        

        for record in self.consumer:
            print(record.value)
        
        """
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
        spark = SparkSession.builder.master("local[*]").appName("datasource - kafka").config('spark.driver.extraClassPath',
                './drivers/monetdb-jdbc-3.2.jre8.jar').getOrCreate()

        df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", self.server) \
            .option("subscribe", self.topic).load()

        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        
        # Example output b'2022-05-22 12:43:23.276091,Q3751076,371515'
        query = df.writeStream.outputMode("append").format("console").start()
        query.awaitTermination()
        """

        """
        # Example stream to jdbc
        def foreach_batch_function(df, epoch_id):
            df.format("jdbc").option("url", "url")\
            .option("dbtable","test").option("user","postgres")\
            .option("password", "password").save()
  
        df5.writeStream.foreachBatch(foreach_batch_function).start()   
        """

        # Ask Sergi -> Do we need to store this information? or just for realtime validation of the model
        
    
