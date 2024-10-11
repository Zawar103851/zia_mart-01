from product_service import settings
from aiokafka import AIOKafkaConsumer,AIOKafkaProducer
from sqlmodel import Session,select
from aiokafka .admin import AIOKafkaAdminClient,NewTopic
import logging
from product_service.db import engine
from product_service.model import Products
from . import product_pb2




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
                        product = product_pb2.product()
                        
                        product.ParseFromString(msg.value)            
                        data_from_producer=Products(id=product.id, name= product.name, price=product.price, quantity = product.quantity)
                        
                        if product.type == product_pb2.Operation.CREATE:
                                session.add(data_from_producer)
                                session.commit()
                                session.refresh(data_from_producer)
                                print(f'''Stored Protobuf data in database with ID: {data_from_producer.id}''')

                        elif product.type== product_pb2.Operation.DELETE:
                                product_to_delete = session.get(Products, product.id)
                                if product_to_delete:
                                    session.delete(product_to_delete)
                                    session.commit()
                                    logging.info(f"Deleted product with ID: {product.id}")
                                else:
                                    logging.warning(f"Product with ID {product.id} not found for deletion")
                        elif product.type== product_pb2.Operation.PUT:
                                db_product = session.get(Products, product.id)
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
