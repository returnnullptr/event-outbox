from pathlib import Path
from typing import AsyncIterator

import dynaconf
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dynaconf import LazySettings
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)

from event_outbox import EventOutbox


@pytest.fixture
def config() -> LazySettings:
    config = dynaconf.Dynaconf(
        environments=True,
        settings_files=[
            Path(__file__).parent / "config.toml",
            Path(__file__).parent / "config.local.toml",
        ],
        merge_enabled=True,
    )
    config.configure(FORCE_ENV_FOR_DYNACONF="test")
    return config


@pytest.fixture
async def topic(config: LazySettings) -> str:
    return config.kafka.producer.topic


@pytest.fixture
async def mongo_client(config: LazySettings) -> AsyncIterator[AsyncIOMotorClient]:
    client = AsyncIOMotorClient(config.mongo.connection_string, tz_aware=True)
    yield client
    client.close()


@pytest.fixture
async def mongo_db(
    mongo_client: AsyncIOMotorClient,
) -> AsyncIterator[AsyncIOMotorDatabase]:
    db = mongo_client.get_default_database()
    await mongo_client.drop_database(db)
    yield db


@pytest.fixture
def outbox(
    config: LazySettings,
    mongo_db: AsyncIOMotorDatabase,
) -> AsyncIOMotorCollection:
    return mongo_db[config.mongo.collection_outbox]


@pytest.fixture
async def kafka_producer(
    config: LazySettings,
) -> AsyncIterator[AIOKafkaProducer]:
    async with AIOKafkaProducer(
        bootstrap_servers=config.kafka.bootstrap_servers,
        enable_idempotence=True,
        acks="all",
    ) as producer:
        yield producer


@pytest.fixture
async def kafka_consumer(config: LazySettings) -> AsyncIterator[AIOKafkaConsumer]:
    async with AIOKafkaConsumer(
        *config.kafka.consumer.topics,
        bootstrap_servers=config.kafka.bootstrap_servers,
        group_id=config.kafka.consumer.group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    ) as consumer:
        await consumer.seek_to_end()
        yield consumer


@pytest.fixture
async def event_outbox(
    config: LazySettings,
    mongo_client: AsyncIOMotorClient,
    mongo_db: AsyncIOMotorDatabase,
    kafka_producer: AIOKafkaProducer,
    kafka_consumer: AIOKafkaConsumer,
) -> EventOutbox:
    event_outbox = EventOutbox(
        mongo_client,
        kafka_producer,
        kafka_consumer,
        mongo_db=mongo_db,
        mongo_collection_outbox=config.mongo.collection_outbox,
        mongo_collection_inbox=config.mongo.collection_inbox,
    )
    await event_outbox.create_indexes()
    return event_outbox
