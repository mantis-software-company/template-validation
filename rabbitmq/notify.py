
import functools
import pika
from pika.exchange_type import ExchangeType

from script import validatorCheck
import os
import json

SCHEMA_PATH = os.environ.get("SCMA")


class Consumer(object):
  
    EXCHANGE = "person"
    EXCHANGE_TYPE = ExchangeType.direct
    QUEUE = "person"
    ROUTING_KEY = "person.notify"

    def __init__(self, amqp_url):
       
    
        self._connection = None
        self._channel = None
       
        self._consumer_tag = None
        self._url = amqp_url
        self._consuming = False
       
     

    def connect(self):
      
        return pika.BlockingConnection(pika.ConnectionParameters(self._url),  on_open_callback=self.on_connection_open)


    def on_connection_open(self):
       
       
        self.open_channel()


    def open_channel(self):
       
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        
       
        self._channel = channel
        self.setup_queue(self.QUEUE)


       
    def setup_queue(self, queue_name):
     
       
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(queue=queue_name, callback=cb)

    def on_queue_declareok(self, userdata):
      
        queue_name = userdata
       
       
        self._channel.queue_bind(
            queue_name,
            self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            )

        self.start_consuming()


    def start_consuming(self):
       
       
        self._consumer_tag = self._channel.basic_consume(
            self.QUEUE, self.on_message)
      

  

    def on_message(self,  basic_deliver, body):
        # payload=json.loads(body)
        # validatorCheck(payload,SCHEMA_PATH)
        self.acknowledge_message(basic_deliver.delivery_tag)
        print("[*] waiting  for notfiy message to exit press ctrl+c")

    def acknowledge_message(self, delivery_tag):
        
        self._channel.basic_ack(delivery_tag)

        

 
    def run(self):
        self._connection = self.connect()

   

class ReconConsumer(object):
    

    def __init__(self, amqp_url):
        self._reconnect_delay = 0
        self._amqp_url = amqp_url
        self._consumer = Consumer(self._amqp_url)

    def run(self):
        self._consumer.run()  # classdakı consumer 
                
        



def main():
    
    amqp_url = 'localhost'
    consumer = ReconConsumer(amqp_url)
    consumer.run()

if __name__ == '__main__':
    main()