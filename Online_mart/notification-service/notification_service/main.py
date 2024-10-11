
from email import message
from typing import Optional

from pydantic import BaseModel, Field
from notification_service import setting
import asyncio
from contextlib import asynccontextmanager
import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlmodel import SQLModel, Session
from aiokafka.admin import AIOKafkaAdminClient , NewTopic
from notification_service import notification_pb2



from twilio.rest import Client

account_sid = 'AC86e0cc084432ad550aff3be85ab50cfb'
auth_token = 'a71167fc1746bc17058541bc0b702fec'
client = Client(account_sid, auth_token)

async def sendNotification():
    message = client.messages.create(
    from_='whatsapp:+14155238886',
    content_sid='HXb5b62575e6e4ff6129ad7c8efe1f983e',
    content_variables='{"1":"12/1","2":"3pm"}',
    to='whatsapp:+923418876882'
    )

    print(message.sid)


class Notification (BaseModel):
    message : str 
    

# class Order(SQLModel, table=True):
#     id:Optional[int] = Field(default= None, primary_key= True)
#     product_id : int = Field()
#     product_name : str =Field()
#     user_id : int = Field()
#     quantity : int = Field()
#     total_price : float = Field()

# class Orders(SQLModel):
#     id:Optional[int] = Field(default= None, primary_key= True)
#     product_id : int =Field()
#     user_id : int = Field()
#     quantity : int = Field()

connection_string=str(setting.DATABASE_URL)
engine = create_engine(connection_string, echo=True, pool_pre_ping=True)

def create_tables():
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session


async def create_topic():
    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=setting.BOOTSTRAP_SERVER)
    await admin_client.start()
    topic_list = [NewTopic(name=setting.KAFKA_ORDER_TOPIC,
                           num_partitions=2, replication_factor=1)]
    try:
        await admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{setting.KAFKA_ORDER_TOPIC}' created successfully")
    except Exception as e:
        print(f"Failed to create topic '{setting.KAFKA_ORDER_TOPIC}': {e}")
    finally:
        await admin_client.close()




async def consume_messages():
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        setting.KAFKA_ORDER_TOPIC,
        bootstrap_servers=setting.BOOTSTRAP_SERVER,
        group_id=setting.KAFKA_CONSUMER_GROUP_ID,
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
                        # print(msg.value)
                        product = notification_pb2.product()
                        
                        product.ParseFromString(msg.value)            
                        data_from_producer=session.get(product.id,product.product_id,product.product_name,product.user_id,product.quantiy,product.total_price)
                        # await sendNotification()
                        print("message received from kafka")
                        
            
                    except Exception as e:
                        print(f"Error processing  messages")
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
    # await sendNotification()
    loop = asyncio.get_event_loop()
    task = loop.create_task(consume_messages())
    yield
    task.cancel()
    await task

async def kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=setting.BOOTSTRAP_SERVER)
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
   return{"welcome to zia mart","notification_service"}

