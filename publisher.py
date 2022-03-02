import pika
import json
import os



path = os.path.basename("data.json")




connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.exchange_declare(
    exchange="product",
    exchange_type="direct"
)




channel.basic_publish(
    exchange="product",
    routing_key="product.notify",
    body=json.dumps(path)
    )
       
   


print("[x] sent notify message")



connection.close()



