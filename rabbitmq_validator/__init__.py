import asyncio
from marshmallow import  ValidationError, validates
import aio_pika
from aio_pika.pool import Pool
import sys
import os
import json
from functools import partial
from importlib.machinery import SourceFileLoader



async def main(config=None,consumer_pool_size=10) :
    loop = asyncio.get_event_loop()
   
   

    async def get_connection() :
        return await aio_pika.connect("amqp://guest:guest@localhost/", loop=loop)

    connection_pool: Pool = Pool(get_connection, max_size=consumer_pool_size, loop=loop)

    async def get_channel():
        async with connection_pool.acquire() as connection:
            return await connection.channel()


    channel_pool: Pool = Pool(get_channel, max_size=consumer_pool_size, loop=loop)
  


    
    async def publish(data,channel) :
       
        exchange = await channel.get_exchange(config.get("mq_target_exchange"))

        
        await exchange.publish(
            aio_pika.Message(
                    body=json.dumps(data).encode(),
                   
                ),
               config.get("mq_target_routing_key")
            )


        
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





    async def consume(consumer_id) :
        async with channel_pool.acquire() as channel:  
           
            queue = await channel.declare_queue(
                config.get("mq_source_queue"), durable=False, auto_delete=False
            )
           
            
            while True:
               
                try:
                  
                    m = await queue.get(timeout=300 * consumer_pool_size)
                    
                    message = m.body.decode('utf-8')
                    j = json.loads(message)
                   
                    try:
                       
                        data_value = await loop.run_in_executor(None, partial(readData))
                        
                        await publish(json.dumps(data_value), channel=channel)
                        print(j)
                      
                    except Exception as e:
                        print(e)
                        raise e
                    # m.ack()
                except aio_pika.exceptions.QueueEmpty:
                    print("Consumer %s: Queue empty. Stopping." % consumer_id)
                    break
                    


    if config is None:
        config = {
            "data_json":os.environ.get("dataJson"),
            "schema_path":os.environ.get("SCMA"),
            "mq_source_queue": os.environ.get('MQ_SOURCE_QUEUE'),
            "mq_target_exchange": os.environ.get('MQ_TARGET_EXCHANGE'),
            "mq_target_routing_key": os.environ.get("MQ_TARGET_ROUTING_KEY")
            
        }

    
    async with connection_pool, channel_pool:
        consumer_pool = []
        print("Consumers started")
        for i in range(consumer_pool_size):
            consumer_pool.append(consume(consumer_id=i))

        await asyncio.gather(*consumer_pool)
        
    
if __name__ == '__main__':
    asyncio.run(main())

   
   