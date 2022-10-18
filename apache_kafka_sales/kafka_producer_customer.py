import time
from kafka import KafkaProducer
from json import dumps
import pandas as pd


KAFKA_TOPIC_NAME_CONS ="customertopic"
KAFKA_BOOTSTRAP_SERVER_CONS = "localhost:9092"

if __name__ == "__main__":
    print('Kafka producer Application Started ...')
    kafka_producer_obj = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                       value_serializer=lambda x:dumps(x).encode('utf-8'))
    file_path = "/home/sushant/workarea/data/customers.csv"

    order_pd_df = pd.read_csv(file_path)
    print(order_pd_df.head(1))

    orders_list =order_pd_df.to_dict(orient="records")

    print(orders_list[0])

    for order in orders_list:
        message = order
        print('Message to be sent: ',message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS,message)
        time.sleep(1)
