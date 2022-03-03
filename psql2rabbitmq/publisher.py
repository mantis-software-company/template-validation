import pika
import json
import os


dataJson = os.environ.get("dataJson")



connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.exchange_declare(
    exchange="product",
    exchange_type="direct"
)




channel.basic_publish(
    exchange="product",
    routing_key="product.notify",
    body=json.dumps(dataJson)
    )
       
   


print("[x] sent notify message")



connection.close()



