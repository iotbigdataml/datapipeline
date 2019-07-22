from kinesis_producer import KinesisProducer
import csv

config = dict(
    aws_region='us-east-1',
    buffer_size_limit=100000,
    buffer_time_limit=0.2,
    kinesis_concurrency=1,
    kinesis_max_retries=10,
    record_delimiter='\n',
    stream_name='KINESIS_STREAM_NAME',
    )

k = KinesisProducer(config=config)

with open('orders_clean.csv', newline='') as csvfile:
    records = csv.reader(csvfile, delimiter=' ', quotechar='|')

for record in records:
    k.send(record)

k.close()
k.join()