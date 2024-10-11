import asyncio
from contextlib import asynccontextmanager
from typing import Annotated, List, Optional
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import Field
from sqlalchemy import select
from sqlmodel import Session,create_engine,SQLModel
from user_service import settings
from fastapi import  Depends, FastAPI, BackgroundTasks, HTTPException, Request
from aiokafka import AIOKafkaConsumer,AIOKafkaProducer
from aiokafka .admin import AIOKafkaAdminClient,NewTopic
import logging
from . import user_pb2
from jose import JWTError, jwt
from user_service.authentication import SECRET_KEY, ALGORITHM, create_access_token, get_password_hash, verify_password
from user_service.models import User, Edit

connection_string = str(settings.DATABASE_URL)
engine = create_engine(connection_string,echo=True,pool_pre_ping = True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Fastapi app started...")
    print('Creating Tables')
    createtable()
    print("Tables Created")
    await create_topic()
    loop = asyncio.get_event_loop()
    task = loop.create_task(consume_messages())
    yield
    task.cancel()
    await task

# create tables and store data in tables
def createtable():
    SQLModel.metadata.create_all(engine)
# session is use to data base session manage and help to improve profamance 
def get_session():
    with Session(engine) as session :
        yield session  


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

def get_current_user(token: str = Depends(oauth2_scheme), session: Session = Depends(get_session)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_email = payload.get("sub")
        if user_email is None:
            raise HTTPException(status_code=401, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    user = session.query(User).filter(User.email == user_email).first()
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
    
    return user

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
                        print("zawar teak ha")
                        product = user_pb2.user()
                        product.ParseFromString(msg.value)            
                        data_from_producer=User(id=User.id,
                        username= User.name, email= User.email, password=User.password)
                        print("zawar ok")
                        if product.type == user_pb2.Operation.CREATE:
                                print("tk ha ider tk")
                                session.add(data_from_producer)
                                
                                session.commit()
                                session.refresh(data_from_producer)
                                print(f'''Stored Protobuf data in database with ID: {data_from_producer.id}''')

                        elif product.type== user_pb2.Operation.DELETE:
                                product_to_delete = session.get(User, product.id)
                                if product_to_delete:
                                    session.delete(product_to_delete)
                                    session.commit()
                                    logging.info(f"Deleted product with ID: {product.id}")
                                else:
                                    logging.warning(f"Product with ID {product.id} not found for deletion")
                        elif product.type== user_pb2.Operation.PUT:
                                db_product = session.get(User, product.id)
                                if db_product:
                                    db_product.id = product.id
                                    db_product.name = product.name
                                    db_product.price = product.price
                                    session.add(db_product)
                                    session.commit()
                                    session.refresh(db_product)
                                    logging.info(f"Updated product with ID: {product.id}")
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
   return{"welcome to zia mart","user_service"}


@app.post('/Register_Admin', response_model=User)
async def Create_User(
    New_user:User,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    
        hashed_password = get_password_hash(New_user.password)

        pb_product = user_pb2.user()
        pb_product.id = New_user.id
        pb_product.username = New_user.name
        pb_product.email = New_user.email
        pb_product.password = hashed_password
        pb_product.type = user_pb2.Operation.CREATE
        serialized_product = pb_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)

        return New_user
    
def get_user_by_username(session: Session, username: str) -> Optional[User]:
    user = session.exec(select(User).where(User.name == username)).first()
    return user    
def authenticate_user(session: Session, username: str, password: str) ->      Optional[User]:
    user = get_user_by_username(session, username=username)
    if not user:
        return None
    if not verify_password(password, user.password):
        return None
    return user
    
# GET token to fetch all products
@app.post('/token', response_model=User)
async def login_user(form_data: OAuth2PasswordRequestForm = Depends(),
 db: Session = Depends(get_session)):
    user = authenticate_user(session=db, username=form_data.username, password=form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Invalid User Credentials...")
    access_token = create_access_token(data={"id": user.id, "name": user.name, "email": user.email, })
    return  {"access_token": access_token, "token_type": "bearer"}
    
# Post endpoint to fetch all products
@app.get('/users', response_model=List[User])
async def get_all_users(session: Annotated[Session, Depends(get_session)], current_user: Annotated[User, Depends(get_current_user)]):
    print(current_user)
       
    all_users = session.exec(select(User)).all()
    return all_users

@app.get('/users/{user_id}', response_model=User)
async def get_user(user_id: int, db: Session = Depends(get_session),
                   ):
    user = db.get(User, user_id)
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

            
    return user
@app.delete('/users/{user_id}')
async def delete_user(
    user_id: int,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
    
):
        pb_product = user_pb2.user()
        pb_product.id=user_id
        pb_product.type=user_pb2.Operation.DELETE
        serialized_product = pb_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)
        return {"message": "User Deleted"}

@app.put('/Edit/{user_id}',response_model=Edit)
async def Edits (user_id : int ,
                Update : Edit,
                producer : Annotated[AIOKafkaProducer,Depends(kafka_producer)],
                session : Annotated[Session,Depends(get_session)]):
    
    edit_user = session.get(User,user_id)
    if edit_user is None:
        raise HTTPException(status_code=404,detail="Please provide valid id")
    
        
    else:
        hashed_password = get_password_hash(Update.password)

        pb_product = user_pb2.user()
        pb_product.id = user_id
        pb_product.username = Update.name
        pb_product.email = Update.email
        pb_product.password = hashed_password
        
       
        serialized_product = pb_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)

        return Update