

from msilib.schema import Error
from marshmallow import  ValidationError, validates
import json
from importlib.machinery import SourceFileLoader
import pika
from setting import ERROR,VALİD

ERROR = str(ERROR)
VALİD = str(VALİD)


connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()



def queuContrl(errIndex,scmaData) :
   
    def findError(n):
        return scmaData[n]

    def findValid(n):
        return  scmaData.pop(n)

   
    resultErr = list(map(findError, errIndex))
    list(map(findValid,sorted(errIndex,reverse=True)))  
 

    def printQueue(resultErr,scmaData):
        
     

        channel.basic_publish(
        exchange=ERROR,
        routing_key=ERROR,
        body=json.dumps(resultErr)
      
        )

    
        channel.basic_publish(
        exchange=VALİD,
        routing_key=VALİD,
        body=json.dumps(scmaData)
     
        )
        print("[x] sent err data")
        print("[x] sent valid data")
   
    printQueue(resultErr,scmaData)
  

def validatorCheck(data,SCHEMA_PATH) :

    foo = SourceFileLoader("module.name", SCHEMA_PATH+".py").load_module()

    try :

       
        schema = foo.Schemas(many=True)
            
        scmaData = schema.load(data,partial=True)
        errIndex = []



    
    except ValidationError as err:
       
        errIndex = err.messages.keys() 
        scmaData = err.data
        
      
    queuContrl(errIndex,scmaData)
        

   




       
  

   

           

        
    


