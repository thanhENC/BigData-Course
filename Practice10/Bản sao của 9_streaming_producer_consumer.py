
import kafka
print(kafka.__version__)
from kafka import KafkaProducer, KafkaConsumer
from time import sleep
KAFKA_URL = 'localhost:9092' # kafka broker
KAFKA_TOPIC = 'sida3_sdtest_topic' # topic name

# ASSUMING THAT the topic exist

# write to the topic
producer = KafkaProducer(bootstrap_servers=[KAFKA_URL])
for i in range(200):
    producer.send(KAFKA_TOPIC, ('Show me' + str(i)).encode() )
producer.flush()

# read from the topic
# auto_offset_reset='earliest', # auto_offset_reset is needed when offset is not found, it's NOT what we need here
consumer = KafkaConsumer(KAFKA_TOPIC,bootstrap_servers=[KAFKA_URL],max_poll_records=2,group_id='sida3')

# (!?) wtf, why we need this to get partitions assigned
# AssertionError: No partitions are currently assigned if poll() is not called
consumer.poll()
consumer.seek_to_beginning()

# also AssertionError: No partitions are currently assigned if poll() is not called
print('partitions of the topic: ',consumer.partitions_for_topic(KAFKA_TOPIC))

from kafka import TopicPartition
print('before poll() x2: ')
print(consumer.position(TopicPartition(KAFKA_TOPIC, 0)))
print(consumer.position(TopicPartition(KAFKA_TOPIC, 1)))

# (!?) sometimes the first call to poll() returns nothing and doesnt change the offset
messages = consumer.poll()
sleep(1)
messages = consumer.poll()

print('after poll() x2: ')
print(consumer.position(TopicPartition(KAFKA_TOPIC, 0)))
print(consumer.position(TopicPartition(KAFKA_TOPIC, 1)))

print('messages: ', messages)