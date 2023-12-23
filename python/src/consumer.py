from kafka import KafkaConsumer
import sys

### Setting up the Python consumer
bootstrap_servers = ['18.211.252.152:9092']
topicName = 'transactions-topic-verified'
consumer = KafkaConsumer (topicName,bootstrap_servers = bootstrap_servers, auto_offset_reset = 'earliest')   
## auto_offset_reset = earliest : read messages from the oldest messages
## auto_offset_reset = latest : read from the last message since consumer started listening

### Reading the message from consumer
try:
    for message in consumer:
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
        
except KeyboardInterrupt:
    sys.exit()