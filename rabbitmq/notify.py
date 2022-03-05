

import functools
from script import validatorCheck
import pika
from pika.exchange_type import ExchangeType
import os
import json


SCHEMA_PATH = os.environ.get("SCMA")


def on_message(ch,method,properties,body):
    payload=json.loads(body)
    validatorCheck(payload,SCHEMA_PATH)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():

    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))

    channel = connection.channel()
    queue = channel.queue_declare("person.notify")
    queue_name = queue.method.queue
   
    channel.queue_bind(
        exchange="person",
        queue=queue_name ,
        routing_key="person.notify"

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