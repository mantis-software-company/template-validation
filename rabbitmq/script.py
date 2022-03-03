

from marshmallow import  ValidationError
import json
from importlib.machinery import SourceFileLoader



def erIndexContrl(errIndex,scmaData) :


    for indexErr in errIndex :
        print(indexErr)  
        # burada sırasıyla verdıgımız datanın 0 ve 1 rıncı ındexlerını baska yere arraye atarız gerı kalanlarda hatasız olanlar 

def validatorCheck(data,SCHEMA_PATH) :

    foo = SourceFileLoader("module.name", SCHEMA_PATH+".py").load_module()

    try :

       
        schema = foo.Schemas(many=True)
            
        schema.load(data)
        
    
    except ValidationError as err:
       
        errIndex = err.messages.keys()
        scmaData = err.data
        erIndexContrl(errIndex,scmaData)
        

   



       
  

   

           

        
    


