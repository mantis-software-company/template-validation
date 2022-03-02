

from marshmallow import  ValidationError
import json
from importlib.machinery import SourceFileLoader



def readData(data):
    with open(data, 'r') as f:
        jsons = f.read()
        return json.loads(jsons)


def validatorCheck(data,SCHEMA_PATH) :
    foo = SourceFileLoader("module.name", SCHEMA_PATH+".py").load_module()

    arrData = readData(data) 

    try :

        for jsonDta in arrData:
            schema = foo.Schemas()
        
            schema.load(jsonDta)

           
    except ValidationError as err:
        print(err)
        print(err.valid_data)
            

       
  

   

           

        
    


