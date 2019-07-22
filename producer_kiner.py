from kiner.producer import KinesisProducer

p = KinesisProducer('stream-name', batch_size=500, max_retries=5, threads=10)

for i in range(10000):
    p.put_record(i)

p.close()