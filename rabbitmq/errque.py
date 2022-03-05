import pika
import json


connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

queue = channel.queue_declare("error.notify")
queue_name = queue.method.queue

channel.queue_bind(
    exchange="error",
    queue=queue_name ,
    routing_key="error.notify"

)


def callback(ch,method,properties,body):
    payload=json.loads(body)
    print("error datalar listelenıyor")
    print(payload)

    
    ch.basic_ack(delivery_tag=method.delivery_tag)
    

channel.basic_consume(on_message_callback=callback,queue=queue_name)

print("[*] waiting  for err data")

channel.start_consuming()


