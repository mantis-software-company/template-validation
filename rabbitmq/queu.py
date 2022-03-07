import pika
import json
import sys

commandReq = sys.argv[1]
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

queue = channel.queue_declare(commandReq)
queue_name = queue.method.queue

channel.queue_bind(
    exchange=commandReq,
    queue=queue_name ,
    routing_key=commandReq

)


def callback(ch,method,properties,body):
    payload=json.loads(body)
   
    print(payload)

    
    ch.basic_ack(delivery_tag=method.delivery_tag)
    

channel.basic_consume(on_message_callback=callback,queue=queue_name)


print('waiting  data for ' + commandReq )

channel.start_consuming()



