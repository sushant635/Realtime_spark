from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.context import SparkContext

import time

kafka_topic_name_order = "orderstopic"
kafka_topic_name_customer = "customertopic"
kafka_bootstrap_servers = "localhost:9092"
customer_data_file_path = "/home/sushant/workarea/data/customers.csv"

mysql_host_name = "localhost"
mysql_port_no = "3306"
mysql_database_name = "sales_db"
mysql_driver_class = "com.mysql.jbdc.Driver"
mysql_table_name = "total_sales_by_source_state"
mysql_user = "root"
mysql_password = "sushant"
mysql_jddc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name


cassandra_host_name = "localhost"
cassandra_port_no = "9042"
cassandra_keyspace_name = "sales_ks"
cassandra_table_name_order = "orders_tbl"
cassandra_table_name_customer = "customer"

def save_to_cassandra(current_df,epoc_id):
    print("Printing epoc_id: ",epoc_id)

    current_df.write.format("org.apache.spark.sql.cassandra")\
            .mode("append")\
            .option(table=cassandra_table_name_order, keyspace=cassandra_keyspace_name)\
            .save()

    print("Cassandra table save: "+str(epoc_id))


def save_to_mysql(current_df,epoc_id):
    db_credentials = {"user":mysql_user,"password":mysql_password,
                        "driver":mysql_driver_class}

    
    print("Printing epoc_id: ",epoc_id)

    processed_at = time.strftime("%Y-%m-%d %H:%M:%s")

    current_df_final = current_df.withColumn("processed_at",list(processed_at))\
        .withColumn("batch_id",list(epoc_id))

    current_df_final.write.jdbc(url=mysql_jddc_url,
            table=mysql_table_name,mode="append",
            properties=db_credentials)

    print("Mysql data save:"+str(epoc_id))






if __name__ == "__main__":
    print("Welcome to Spark")
    print("Data processing Application Started...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession\
            .builder\
            .appName("Pyspark straming application with kafka and cassandra")\
            .master("local")\
            .config("spark.jars","file:///usr/share/java/jsr166e-1.1.0.jar, file:///usr/share/java/spark-cassandra-connector-2.4.0-s_2.11.jar, file:///usr/share/java/mysql-connector-java-5.1.45.jar, file:///usr/share/java/spark-sql-kafka-0-10_2.12-3.3.0.jar, file:///usr/share/java/kafka-clients-3.2.0.jar")\
            .config("spark.executor.extraClassPath","file:///usr/share/java/jsr166e-1.1.0.jar, file:///usr/share/java/spark-cassandra-connector-2.4.0-s_2.11.jar, file:///usr/share/java/mysql-connector-java-5.1.45.jar, file:///usr/share/java/spark-sql-kafka-0-10_2.12-3.3.0.jar, file:///usr/share/java/kafka-clients-3.2.0.jar")\
            .config("spark.executor.extraLibrary", "file:///usr/share/java/jsr166e-1.1.0.jar, file:///usr/share/java/spark-cassandra-connector-2.4.0-s_2.11.jar, file:///usr/share/java/mysql-connector-java-5.1.45.jar, file:///usr/share/java/spark-sql-kafka-0-10_2.12-3.3.0.jar, file:///usr/share/java/kafka-clients-3.2.0.jar")\
            .config("spark.driver.extraClassPath", "file:///usr/share/java/jsr166e-1.1.0.jar, file:///usr/share/java/spark-cassandra-connector-2.4.0-s_2.11.jar, file:///usr/share/java/mysql-connector-java-5.1.45.jar, file:///usr/share/java/spark-sql-kafka-0-10_2.12-3.3.0.jar, file:///usr/share/java/kafka-clients-3.2.0.jar")\
            .config('spark.cassandra.connectio.host', cassandra_host_name)\
            .config('spark.cassandra.connection.port', cassandra_port_no)\
            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    #SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)


    order_df = spark.readStream.format("kafka")\
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
                .option("subscribe", kafka_topic_name_order)\
                .option("startingoffsets", "latest").load()

    print("Printing schema of orders_df: ")
    order_df.printSchema()

    order_df1 = order_df.selectExpr("CAST(value AS STRING)","timestamp")
    #order_df1.printSchema()

    order_schema = StructType().add("order_id", StringType()).add("created_at", StringType()) \
                .add("discount", StringType()).add("product_id", StringType()).add("quantity", StringType())\
                .add("subtotal", StringType()).add("tax", StringType()).add("total", StringType())\
                .add("order_id", StringType()).add("customer_id",StringType())

    order_df2 = order_df1.select(from_json(col("value"), order_schema)\
            .alias("orders"), "timestamp")

    order_df3 = order_df2.select("orders.*", "timestamp")

    order_df1.writeStream.trigger(processingTime='15 seconds')\
        .outputMode("update")\
        .foreachBatch(save_to_cassandra).start()

    #order_df3.show(5,False)
    

    customer_df = spark.read.csv(customer_data_file_path,header=True, inferSchema=True)
    customer_df.printSchema()
    customer_df.show(5,False)

    order_df4 = order_df3.join(customer_df, order_df3.customer_id == customer_df.ID, how='inner')
    print("Printing Schema of order_df4: ")
    order_df4.printSchema()

    order_df5 = order_df4.groupBy("source","state")\
                .agg({'total':'sum'}).select("source","state",col("sum(total)").alias("total_sum_amount"))

    print("printing Schema of order_dfs: ")
    order_df5.printSchema()

    trans_detail_write_stream =order_df5.writeStream\
            .trigger(processingTime='15 seconds')\
            .outputMode("update").option("truncate","false")\
            .format("console").start()


    order_df5.writeStream.trigger(processingTime='15 seconds')\
            .outputMode("update")\
            .foreachBatch(save_to_mysql).start()


    trans_detail_write_stream.awaitTermination()


    print("pyspark streaming application with kafka completed ...")



