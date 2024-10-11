import asyncio
from contextlib import asynccontextmanager
from itertools import product
import logging
from aiokafka import AIOKafkaProducer,AIOKafkaConsumer
from fastapi import Depends, FastAPI,HTTPException
from pydantic import Field
from sqlalchemy import create_engine
from sqlmodel import SQLModel,Field,Session
from typing import Annotated, Optional
from inventory_service import settings
from aiokafka.admin import AIOKafkaAdminClient , NewTopic
from inventory_service import inventory_pb2



class Products(SQLModel,table = True):
    id : int = Field(default=None,primary_key=True)
    name : str = Field()
    price : int = Field()
    quantity : int = Field()
    
    # it is for relationship b/w product and availabe stock in inventory
class inventoryItems(SQLModel):
    product_id: int = Field ()   
    quantity: int = Field()

connection_string=str(settings.DATABASE_URL)
engine = create_engine(connection_string, echo=True, pool_pre_ping=True)

def create_tables():
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session


async def create_topic():
    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=settings.BOOTSTRAP_SERVER)
    await admin_client.start()
    topic_list = [NewTopic(name=settings.KAFKA_ORDER_TOPIC,
                           num_partitions=2, replication_factor=1)]
    try:
        await admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{settings.KAFKA_ORDER_TOPIC}' created successfully")
    except Exception as e:
        print(f"Failed to create topic '{settings.KAFKA_ORDER_TOPIC}': {e}")
    finally:
        await admin_client.close()

async def consume_messages():
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        settings.KAFKA_ORDER_TOPIC,
        bootstrap_servers=settings.BOOTSTRAP_SERVER,
        group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        auto_offset_reset='earliest',
        # session_timeout_ms=30000,  # Example: Increase to 30 seconds
        # max_poll_records=10,
    )
    # Start the consumer.
    await consumer.start()
    print("consumer started")
    try:
        with Session(engine) as session:
        # Continuously listen for messages.
            async for msg in consumer:
                if msg.value is not None:
                    try:
                        print(msg.value)
                        product = inventory_pb2.product()
                        
                        product.ParseFromString(msg.value)            
                        data_from_producer=session.get(Products, product.id)
                        print("message received from kafka")
                        if data_from_producer:

                            print("updating quantity")
                            data_from_producer.quantity  += product.quantity
                            session.add(data_from_producer)
                            session.commit()
                            session.refresh(data_from_producer)
                            print(f'''Stored Protobuf data in database with ID: {data_from_producer.id}''')
                        

                        else:
                                    logging.warning(f"Product with ID {product.id} not found for update")

                    except:
                        logging.error(f"Error deserializing messages")
                else :
                    print("Msg has no value")
    except Exception as e: 
        logging.error(f"Error consuming messages: {e}")
    finally:
        # Ensure to close the consumer when done.
            await consumer.stop()



@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Fastapi app started...")
    print('Creating Tables')
    create_tables()
    await create_topic()
    print("Tables Created")
    loop = asyncio.get_event_loop()
    task = loop.create_task(consume_messages())
    yield
    task.cancel()
    await task

async def kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVER)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()

app = FastAPI(lifespan=lifespan,
              title="Zia Mart inventory Service...",
              version='1.0.0'
              )


@app.get('/')
async def root():
   return{"welcome to zia mart","product_service"}


@app.post('/product/{product_id}',response_model=inventoryItems)
async def change_quantity(product_id: int,Quantity:int,
                          producer : Annotated[AIOKafkaProducer, Depends(kafka_producer)],
                          session : Annotated[Session,Depends(get_session)] ):
    product = session.get(Products,product_id)
    if product:
        
 
        new_quantity = inventory_pb2.product()
        new_quantity.id = product_id
        new_quantity.quantity = Quantity
        

        serializedtostring = new_quantity.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC,serializedtostring)

        return inventoryItems(product_id=product.id, quantity=Quantity)
        
    else:
        raise HTTPException(status_code=404, detail="Product not found")

# end point for retreave the quantity of product
@app.get('/products/inventory/{product_id}', response_model=inventoryItems)
async def Get_Product_Quantity( id: int,  session : Annotated[Session, Depends(get_session)]):
    product = session.get(Products, id)
    if not product:
        raise HTTPException(status_code=404, detail="No Product was found With that ID...")
    return inventoryItems(product_id= product.id , quantity = product.quantity)

# end point for update inventory

# @app.post('/products/inventory/{product_id}', response_model=inventoryItems)
# async def Update_Quantity(
#     id: int,
#     quantity: int,
#     producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
#     # current_user: Usertoken = Depends(get_current_user),
#     session : Annotated[Session, Depends(get_session)]):

#     product = session.get(Products, id)
#     if product:
        
#         new_product = inventory_pb2.product()
#         new_product.id = id
#         new_product.quantity = quantity
#         serialized_product = new_product.SerializeToString()
#         await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)

#         return inventoryItems(product_id=product.id, quantity=product.quantity)
   


