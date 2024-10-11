from order_service import settings
from aiokafka import AIOKafkaConsumer,AIOKafkaProducer
from sqlmodel import SQLModel,Session,create_engine,Field,select
from aiokafka .admin import AIOKafkaAdminClient,NewTopic
import logging
from fastapi import Depends, FastAPI, BackgroundTasks, HTTPException, Request
from contextlib import asynccontextmanager
from . import order_pb2
import asyncio
from typing import Annotated , List,Optional
from pydantic import BaseModel
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
# this class is use for product
class Products(SQLModel,table = True):
    id : int = Field(default=None,primary_key=True)
    name : str = Field()
    price : int = Field()
    quantity : int = Field()
# this class is use for create new table for orde
class Orders(SQLModel,table = True):
    id : int = Field(default=None,primary_key=True)
    user_name : str = Field()
    user_id : int = Field()
    product_id: int =Field()
    product_name: str =Field()
    quantity : int = Field()
    total : int = Field()
    # this class is use to get data from user it is not use for new table
class order(SQLModel):  
    id : int = Field(default=None,primary_key=True)
    quantity : int = Field()
    product_id: int =Field()

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
    print(f"Connecting to Kafka at {settings.BOOTSTRAP_SERVER}...")
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
        


# async def create_topic():
#     admin_client = AIOKafkaAdminClient(
#         bootstrap_servers=settings.BOOTSTRAP_SERVER)
#     await admin_client.start()
#     topic_list = [NewTopic(name=settings.KAFKA_ORDER_TOPIC,
#                            num_partitions=2, replication_factor=1)]
#     try:
#         await admin_client.create_topics(new_topics=topic_list, validate_only=False)
#         print(f"Topic '{settings.KAFKA_ORDER_TOPIC}' created successfully")
#     except Exception as e:
#         print(f"Failed to create topic '{settings.KAFKA_ORDER_TOPIC}': {e}")
#     finally:
#         await admin_client.close()

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
                        product = order_pb2.order()
                        
                        product.ParseFromString(msg.value)            
                        data_from_producer=Orders(id=product.id, product_name= product.product_name, product_id = product.product_id , user_name = product.user_name,user_id = product.user_id, quantity = product.quantity, total = product.total)
                        
                        
                        session.add(data_from_producer)
                        session.commit()
                        session.refresh(data_from_producer)
                        print(f'''Stored Protobuf data in database with ID: {data_from_producer.id}''')

                        db_product = session.get(Products, product.product_id)
                        db_product.quantity += -(product.quantity)
                        
                        session.add(db_product)
                        session.commit()
                        session.refresh(db_product)

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
              title="Zia Mart User Service...",
              version='1.0.0'
              )


@app.get('/')
async def root():
   return{"welcome to zia mart","order_service"}

async def getproduct(session:Session, 
                     product_id:int):
    product = session.get(Products, product_id)
    if product:
        return product
    else:
        raise HTTPException(status_code=401, detail="product not found")

@app.post('/products', response_model=order)
async def add_new_product(
    new_order: order, 
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    
    
    pb_order = order_pb2.order()
    pb_order.id = new_order.id
    pb_order.product_id = new_order.product_id
    pb_order.quantity = new_order.quantity
    pb_order.user_id = 1122
    pb_order.user_name = "zawar"
    chackquantity = await getproduct(session=Session(engine), product_id=new_order.product_id)
    if chackquantity is not None:
        if chackquantity.quantity >=new_order.quantity and  chackquantity.quantity !=0:
            pb_order.product_name = chackquantity.name
            pb_order.total = chackquantity.price * new_order.quantity    
            
            serialized_order = pb_order.SerializeToString()

            # Send the serialized product to Kafka
            await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_order)

            return new_order
        else:
             raise HTTPException (status_code=400, detail="not enough quantity avilable in inventory")

    else:
        raise HTTPException (status_code=400, detail=" quantity not avialable")
    

def get_order(session: Session, order_id: int):
    return session.get(Orders, order_id) 
 
@app.get('/orders/{order_id}', response_model=Orders)
async def Get_Order(
    order_id: int,
    db: Session = Depends(get_session)
):
    product_data = get_order(session=db, order_id=order_id)
    if product_data:
        return product_data
    else:
        raise HTTPException(status_code=404, detail="No Order was found With that ID...")

# Post endpoint to fetch all products
@app.get('/orders', response_model=List[Orders])
async def Get_All_Orders(
    session: Annotated[Session, Depends(get_session)]
):
    all_products = session.exec(select(Orders)).all()
    return all_products
   
        