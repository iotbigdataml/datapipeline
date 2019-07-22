import boto3
import os, json
import csv
import datetime
import logging

os.environ["AWS_ACCESS_KEY_ID"] = "AKIA5EE3PFKR6JYH7DDQ"
os.environ["AWS_SECRET_ACCESS_KEY"] = "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"

my_stream_name = 'iot_eval8_kds_team2'
logging.warning("Connecting to data stream : " + my_stream_name + "..")

class KProducer(object):

    def __init__(self):
        self.kinesis_client = boto3.client('kinesis', region_name='us-east-2')
        #self.response = kinesis_client.describe_stream(StreamName=my_stream_name)
        logging.warning("Kinesis Client : " + str(self.kinesis_client))

    def put_orders_to_stream(self, orderid, order, property_timestamp):
        logging.warning("Sending orders data to stream..")
        put_response = self.kinesis_client.put_record(
                            StreamName=my_stream_name,
                            Data=json.dumps(order),
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

orders = csv.DictReader(open("orders_clean.csv"))

for row in orders:
    producer.put_orders_to_stream( row['Order_ID'], row, None)

trips = csv.DictReader(open("trips_clean.csv"))

for row in trips:
    producer.put_bottrips_to_stream( row['tripID'], row, None)