
from script import validatorCheck
import pika
from pika.exchange_type import ExchangeType
import os
import json
import sys



SCHEMA_PATH = os.environ.get("SCMA")




commandReq = sys.argv[1] 

def on_message(ch,method,properties,body):
    payload=json.loads(body)
    validatorCheck(payload,SCHEMA_PATH)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():

    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))

    channel = connection.channel()
    queue = channel.queue_declare(commandReq)
    queue_name = queue.method.queue
   
    channel.queue_bind(
        exchange=commandReq,
        queue=queue_name ,
        routing_key=commandReq

    )
    
    channel.basic_consume(on_message_callback=on_message,queue=queue_name)
    print("çıkmak için ctrl c ")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()

    connection.close()


if __name__ == '__main__':
    main()