from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer

# consume messages
client = KafkaClient("localhost:9092")
consumer = SimpleConsumer(client,
                          "tetst-group",
                          "my-topic")
for message in consumer:
    print(message)

