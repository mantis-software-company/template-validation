import pika
import json
import os
from setting import DATA_JSON,PERSON

PERSON = str(PERSON)


with open(DATA_JSON) as f:
  data = json.load(f)


connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()
  



def fetch_and_publish():
  try:

    channel.exchange_declare(
          exchange=PERSON,
          exchange_type="direct"
        )

        
    channel.basic_publish(
          exchange=PERSON,
          routing_key=PERSON,
          body=json.dumps(data)
          )
            
        
    print("[x] sent notify message")

  except Exception as e:
    print(e)

def main():
  while True :
    try:

      fetch_and_publish()

    except Exception as e:
      print(e)
      break


if __name__ == '__main__':
    main()

  
 



