import boto3
import os, json
import csv
import datetime
import logging
import configparser

configParser = configparser.RawConfigParser()   
configFilePath = r'config.ini'
configParser.read(configFilePath)

region = configParser.get('kinesis', 'region_name')
access_key = configParser.get('kinesis', 'aws_access_key_id')
secret_key = configParser.get('kinesis', 'aws_secret_access_key')

os.environ["AWS_ACCESS_KEY_ID"] = access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key

my_stream_name = 'iot_eval8_kds_team2'
logging.warning("Connecting to data stream : " + my_stream_name + "..")

class KProducer(object):

    def __init__(self):
        self.kinesis_client = boto3.client('kinesis', region_name=region)
        self.response = self.kinesis_client.describe_stream(StreamName=my_stream_name)
        logging.warning("Kinesis Client : " + str(self.kinesis_client))

    def put_orders_to_stream(self, orderid, order, property_timestamp):
        logging.warning("Sending orders data to stream..")
        put_response = self.kinesis_client.put_record(
                            StreamName=my_stream_name,
                            #Data=json.dumps(order),
                            Data=str(order),
                            PartitionKey=str(orderid))
        logging.warning("Response :: " + str(put_response))

    def put_bottrips_to_stream(self, tripid, order, property_timestamp):
        logging.warning("Sending trips data to stream..")
        put_response = self.kinesis_client.put_record(
                            StreamName=my_stream_name,
                            Data=json.dumps(order),
                            PartitionKey=str(tripid))
        logging.warning("Response :: " + str(put_response))

producer = KProducer()

orders = csv.DictReader(open("./train_data/orderProducts_clean"))  
producer.put_orders_to_stream( 'order_id', 'order_id,prod_id,qty', None)
for row in orders:
    print(str(dict(row)))
    Data=json.dumps(row)
    x = json.loads(Data)
    row = "\n" + x['order_id']+","+x['prod_id']+","+x['qty']
    print(row)
    producer.put_orders_to_stream( x['order_id'], row, None)

# Following section can be used for adding data for trips
# trips = csv.DictReader(open("trips_clean.csv"))clear

# for row in trips:
#     producer.put_bottrips_to_stream( row['tripID'], row, None)