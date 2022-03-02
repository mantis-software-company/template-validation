import pika
import json
from script import validatorCheck
import os



SCHEMA_PATH = os.environ.get("SCMA")



connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

queue = channel.queue_declare("product.notify")
queue_name = queue.method.queue

channel.queue_bind(
    exchange="product",
    queue=queue_name ,
    routing_key="product.notify"

)


def callback(ch,method,properties,body):
    payload=json.loads(body)

    validatorCheck(payload,SCHEMA_PATH)
    
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(on_message_callback=callback,queue=queue_name)

print("[*] waiting  for notfiy message to exit press ctrl+c")

channel.start_consuming()


