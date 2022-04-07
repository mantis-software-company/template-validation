import asyncio
from pickle import FALSE, TRUE
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


    if config is None:
        config = {
            "data_json":os.environ.get("DATA_JSON"),
            "schema_path":os.environ.get("SCHEMA_PATH"),
            "mq_data_valid": os.environ.get('MQ_DATA_VALID'),
            "mq_data_invalid": os.environ.get('MQ_DATA_INVALID'),
            "consumer_pool_size": os.environ.get("CONSUMER_POOL_SIZE"),
            "connect_address": os.environ.get("CONNECT_ADDRESS"),
            "mq_data_queue": os.environ.get("MQ_DATA_QUEUE"),
        
        }


    if "consumer_pool_size" in config:
        if config.get("consumer_pool_size"):
            try:
                consumer_pool_size = int(config.get("consumer_pool_size"))
            except TypeError as e:
                print("Invalid pool size: %s" % (consumer_pool_size,))
                   
                raise e

    
    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust("amqp://guest:guest@127.0.0.1/",loop=loop)
        
   

    connection_pool: Pool = Pool(get_connection, max_size=consumer_pool_size, loop=loop)

    async def get_channel() -> aio_pika.Channel:
        async with connection_pool.acquire() as connection:
            return await connection.channel()

    channel_pool: Pool = Pool(get_channel, max_size=consumer_pool_size, loop=loop)
   

    async def queuContrl(errIndex,scmaData,channel,queuInvalid,queuValid) :
        def findError(n):
            return scmaData[n]

        def findValid(n):
            return  scmaData.pop(n)

    
        resultErr = list(map(findError, errIndex))
        list(map(findValid,sorted(errIndex,reverse=True)))  
        
        await channel.default_exchange.publish(
            aio_pika.Message(
                    body=json.dumps(resultErr).encode(),
                   
                ),
             
                queuInvalid,
            )


        await channel.default_exchange.publish(
            aio_pika.Message(
                    body=json.dumps(scmaData).encode(),
                   
                ),
             
                queuValid,
            )

      
    async def validatorCheck(data,SCHEMA_PATH,channel,queuInvalid,queuValid) :

        foo = SourceFileLoader("module.name", SCHEMA_PATH+".py").load_module()

        try :
            schema = foo.Schemas(many=True)
                
            scmaData = schema.load(data,partial=True)
            errIndex = []

        except ValidationError as err:
        
            errIndex = err.messages.keys() 
            scmaData = err.data
        await queuContrl(errIndex,scmaData,channel,queuInvalid,queuValid)
       

    async def readData(dataJson,channel,queuInvalid,queuValid):
       
        scmaPath = config.get("schema_path")
        with open(dataJson) as f:
            data = json.load(f)
            await validatorCheck(data,scmaPath,channel,queuInvalid,queuValid)
           

    async def consume(consumer_id) -> None:
        
        async with channel_pool.acquire() as channel:
           
            queue = await channel.declare_queue(
                config.get("mq_data_queue"), durable=False, auto_delete=False,
            )
           

            async with queue.iterator() as queue_iter: 
                    async for message in queue_iter:
                        msg = json.loads(message.body)
                        await readData(dataJson=msg,channel=channel,queuInvalid=config.get("mq_data_invalid"), queuValid=config.get("mq_data_valid"))
                        await message.ack()
                
    async def publish(data,queue) -> None:
        
        async with channel_pool.acquire() as channel:  
            
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(data).encode(),
                   
                ),
             
                queue,
            )

    async with connection_pool, channel_pool:
        consumer_pool = []
        print("consumer started")
        for i in range(consumer_pool_size):
            consumer_pool.append(consume(consumer_id=i))
        
        await publish(data=config.get("data_json"),queue=config.get("mq_data_queue")) 
        await asyncio.gather(*consumer_pool)
        


if __name__ == "__main__":
    asyncio.run(main())