import pika
import json
import os


dataJson = os.environ.get("dataJson")
data = json.load(open(dataJson, 'r'))



connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.exchange_declare(
    exchange="person",
    exchange_type="direct"
)




channel.basic_publish(
    exchange="person",
    routing_key="person.notify",
    body=json.dumps(data)
    )
       
   


print("[x] sent notify message")



connection.close()


