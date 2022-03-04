

from marshmallow import  ValidationError, validates
import json
from importlib.machinery import SourceFileLoader
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()


def erIndexContrl(errIndex,scmaData) :
    newErrArr = []
   
  
    for index  in errIndex :
        newErrArr.append(scmaData[index])


    for index  in sorted(errIndex, reverse=True) :
        scmaData.pop(index)

    def printQueue(newErrArr,scmaData):
        channel.exchange_declare(
        exchange="error",
        exchange_type="direct"

            )


        channel.basic_publish(
        exchange="error",
        routing_key="error.notify",
        body=json.dumps(newErrArr)
        #  errordata kuyruga ıletılecek
        )

       

        channel.exchange_declare(
        exchange="valid",
        exchange_type="direct"

            )


        channel.basic_publish(
        exchange="valid",
        routing_key="valid.notify",
        body=json.dumps(scmaData)
     
        )
        print("[x] sent err data")
        print("[x] sent valid data")
   
    printQueue(newErrArr,scmaData)
  

def validatorCheck(data,SCHEMA_PATH) :

    foo = SourceFileLoader("module.name", SCHEMA_PATH+".py").load_module()

    try :

       
        schema = foo.Schemas(many=True)
            
        schema.load(data,partial=True)
        
    
    except ValidationError as err:
       
        errIndex = err.messages.keys()
        scmaData = err.data
        
      
        erIndexContrl(errIndex,scmaData)
        

   




       
  

   

           

        
    


