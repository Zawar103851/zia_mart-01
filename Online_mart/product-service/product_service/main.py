from product_service import settings
from aiokafka import AIOKafkaConsumer,AIOKafkaProducer
from sqlmodel import Session,select
from aiokafka .admin import AIOKafkaAdminClient,NewTopic
import logging
from fastapi import Depends, FastAPI, BackgroundTasks, HTTPException, Request
from contextlib import asynccontextmanager
from . import product_pb2
import asyncio
from typing import Annotated , List,Optional
from pydantic import BaseModel
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from product_service.model import Products
from product_service.db import Session,create_tables, get_session
from product_service.kafka import create_topic,consume_messages,kafka_producer


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



app = FastAPI(lifespan=lifespan,
              title="Zia Mart User Service...",
              version='1.0.0'
              )


@app.get('/')
async def root():
   return{"welcome to zia mart","product_service"}



@app.post('/products', response_model=Products)
async def add_new_product(
    new_product: Products, 
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    
    pb_product = product_pb2.product()
    pb_product.id = new_product.id
    pb_product.name = new_product.name
    pb_product.price = new_product.price
    pb_product.quantity = new_product.quantity
    pb_product.type = product_pb2.Operation.CREATE
    serialized_product = pb_product.SerializeToString()

    # Send the serialized product to Kafka
    await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)

    return new_product


# function
@app.get('/products/{product_id}', response_model=Products)
async def Get_Products( product_id: int, db: Session = Depends(get_session)):
    product_data = Get_Products(session=db, product_id=product_id)
    if not product_data:
        raise HTTPException(status_code=404, detail="No Product was found With that ID...")
    return product_data

@app.get('/products', response_model=List[Products])
async def Get_All_Products(session: Annotated[Session, Depends(get_session)]):
    all_products = session.exec(select(Products)).all()
    return all_products


@app.delete('/products/{product_id}')
async def Delete_Product(
    product_id: int, 
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
    db: Session = Depends(get_session)
):
    # Fetch the product to delete from the database
    product_data = Get_Products(session=db, product_id=product_id)
    if not product_data:
        raise HTTPException(status_code=404, detail="Product not found.")

    # Delete the product from the database
    db.delete(product_data)
    db.commit()

    # Prepare delete event for Kafka using Protobuf
    pb_product = product_pb2.product()
    pb_product.id = product_id
    pb_product.name = product_data.name
    pb_product.price = product_data.price
    pb_product.added_by = "Admin"  # Consistent with the rest of the app

    serialized_product = pb_product.SerializeToString()

    # Send delete event to Kafka
    await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)

    return {"message": f"Product with id {product_id} deleted successfully."}


