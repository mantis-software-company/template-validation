

from marshmallow import  ValidationError, validates
import json
from importlib.machinery import SourceFileLoader
import pika



connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()



def IndexContrl(errIndex,scmaData) :
   
    def findError(n):
        return scmaData[n]

    def findValid(n):
        return  scmaData.pop(n)

   
    resultErr = list(map(findError, errIndex))
    resultValid = list(map(findValid,sorted(errIndex,reverse=True)))  

   

    def printQueue(resultErr,resultValid):
        
     

        channel.basic_publish(
        exchange="error",
        routing_key="error",
        body=json.dumps(resultErr)
      
        )

    
        channel.basic_publish(
        exchange="valid",
        routing_key="valid",
        body=json.dumps(resultValid)
     
        )
        print("[x] sent err data")
        print("[x] sent valid data")
   
    printQueue(resultErr,resultValid)
  

def validatorCheck(data,SCHEMA_PATH) :

    foo = SourceFileLoader("module.name", SCHEMA_PATH+".py").load_module()

    try :

       
        schema = foo.Schemas(many=True)
            
        result = schema.load(data,partial=True)
        errIndex = []
        IndexContrl(errIndex,result)



    
    except ValidationError as err:
       
        errIndex = err.messages.keys()
        scmaData = err.data
        
      
        IndexContrl(errIndex,scmaData)
        

   




       
  

   

           

        
    


