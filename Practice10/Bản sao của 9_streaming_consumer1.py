# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer

# Import sys module
import sys

# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name from where the message will recieve
topicName = 'MyNewTopic'

# Initialize consumer variable
consumer = KafkaConsumer (topicName, auto_offset_reset='earliest',group_id='None',bootstrap_servers = bootstrap_servers)

# Read and print message from consumer
for msg in consumer:
    print("Topic Name: ",msg.topic, "Message:",msg.value)

# Terminate the script
sys.exit()
# from kafka import KafkaConsumer
# from json import loads

# consumer = KafkaConsumer(
#     'MyNewTopic',
#     bootstrap_servers=['localhost:9092'],
#     auto_offset_reset = 'earliest',
#     group_id=None,
# )

# print('Listening')
# for msg in consumer:
#     print(msg)
    
    
    
    
    
    
    
    