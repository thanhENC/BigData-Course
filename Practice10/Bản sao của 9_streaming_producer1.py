
print("--*--"*20)
# Import KafkaProducer from Kafka library
from kafka import KafkaProducer

# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name where the message will publish
topicName = 'MyNewTopic'

# Initialize producer variable
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer.flush()

# Publish text in defined topic
producer.send(topicName, b'This topic will sent to...')
print("--*--"*20)
# Print message
print("Message Sent")
producer.close()