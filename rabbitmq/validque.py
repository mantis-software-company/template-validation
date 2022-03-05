import pika
import json


connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

queue = channel.queue_declare("valid.notify")
queue_name = queue.method.queue

channel.queue_bind(
    exchange="valid",
    queue=queue_name ,
    routing_key="valid.notify"

)


def callback(ch,method,properties,body):
    payload=json.loads(body)
    print("valid datalar listelenıyor")
    print(payload)

    
    ch.basic_ack(delivery_tag=method.delivery_tag)
    

channel.basic_consume(on_message_callback=callback,queue=queue_name)

print("[*] waiting  for valid data")

channel.start_consuming()


