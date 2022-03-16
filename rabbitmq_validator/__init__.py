import asyncio
from marshmallow import  ValidationError, validates
import aio_pika
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool
import json
from functools import partial
from importlib.machinery import SourceFileLoader
import os

async def main(config=None) -> None:
    loop = asyncio.get_event_loop()

    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust("amqp://guest:guest@localhost/")

    connection_pool: Pool = Pool(get_connection, max_size=2, loop=loop)

    async def get_channel() -> aio_pika.Channel:
        async with connection_pool.acquire() as connection:
            return await connection.channel()

    channel_pool: Pool = Pool(get_channel, max_size=2, loop=loop)
    queue_name = "leb_adenem"


    
    def queuContrl(errIndex,scmaData) :
    
        def findError(n):
            return scmaData[n]

        def findValid(n):
            return  scmaData.pop(n)

    
        resultErr = list(map(findError, errIndex))
        list(map(findValid,sorted(errIndex,reverse=True)))  
        return resultErr
       
      
  
        
    def validatorCheck(data,SCHEMA_PATH) :

        foo = SourceFileLoader("module.name", SCHEMA_PATH+".py").load_module()

        try :
                
        
            schema = foo.Schemas(many=True)
                
            scmaData = schema.load(data,partial=True)
            errIndex = []



        
        except ValidationError as err:
        
            errIndex = err.messages.keys() 
            scmaData = err.data
            
        
        snc=queuContrl(errIndex,scmaData)
        return snc

    def readData():
        dataJson = config.get("data_json")
        scmaPath = config.get("schema_path")
        with open(dataJson) as f:
            data = json.load(f)
            sncValidation = validatorCheck(data,scmaPath)
            return sncValidation





    async def consume(consumer_id) -> None:
        
        
        async with channel_pool.acquire() as channel:  # type: aio_pika.Channel
           
           
            queue = await channel.declare_queue(
                queue_name, durable=False, auto_delete=False,
            )
               
        while True :
            try:
                m = await queue.get(timeout=300 * 10)
                message = m.body.decode('utf-8')
             

                try :
                    j = json.loads(message)
                    print(j)
                except Exception as e:
                    print("error" % (e,))
                    raise e
                m.ack()
                

        
            except aio_pika.exceptions.QueueEmpty:
                
                print("Consumer %s: Queue empty. Stopping." % consumer_id)
                break
            
         

               
           
           
           
         
    async def publish(data) -> None:
        async with channel_pool.acquire() as channel:  
       
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(data).encode(),
                   
                ),
             
                queue_name,
            )

    if config is None:
        config = {
            "data_json":os.environ.get("dataJson"),
            "schema_path":os.environ.get("SCMA"),
            
        }


    async with connection_pool, channel_pool:
        consumer_pool = []
        print("consumer started")
        for i in range(1):
            consumer_pool.append(consume(consumer_id=i))

        await asyncio.gather(*consumer_pool)


if __name__ == "__main__":
    asyncio.run(main())