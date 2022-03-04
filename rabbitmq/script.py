

from marshmallow import  ValidationError, validates
import json
from importlib.machinery import SourceFileLoader



def erIndexContrl(errIndex,scmaData) :
    newErrArr = []
   
  
    for index  in errIndex :
        newErrArr.append(scmaData[index])


    for index  in sorted(errIndex, reverse=True) :
        scmaData.pop(index)


    print(scmaData) 
    print("Hatasız ********** yukarıdakıler")
    print(newErrArr) 
       

    
 
  
        

    
   

def validatorCheck(data,SCHEMA_PATH) :

    foo = SourceFileLoader("module.name", SCHEMA_PATH+".py").load_module()

    try :

       
        schema = foo.Schemas(many=True)
            
        schema.load(data,partial=True)
        
    
    except ValidationError as err:
       
        errIndex = err.messages.keys()
        scmaData = err.data
      
        erIndexContrl(errIndex,scmaData)
        

   



       
  

   

           

        
    


