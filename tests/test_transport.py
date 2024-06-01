import asyncio
import uuid
from contextlib import suppress
from typing import Any, AsyncIterator, Literal
from unittest.mock import AsyncMock

import pymongo.errors
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorDatabase,
)

from event_outbox import EventTransport, Event, EventHandler


@pytest.fixture
async def mongo_connection_string() -> str:
    # TODO: Configure local test environment
    return "mongodb://localhost:27018/test?directConnection=true"


@pytest.fixture
async def mongo_collection_outbox() -> str:
    # TODO: Configure local test environment
    return "test_outbox"


@pytest.fixture
async def kafka_bootstrap_servers() -> str:
    # TODO: Configure local test environment
    return "localhost:9092"


@pytest.fixture
async def kafka_test_topic() -> str:
    # TODO: Configure local test environment
    return f"async-mongo-kafka-test-topic-{uuid.uuid4().hex}"


@pytest.fixture
async def kafka_consumer_group_id() -> str:
    # TODO: Configure local test environment
    return "test_consumer_group"


@pytest.fixture
async def mongo_client() -> AsyncIterator[AsyncIOMotorClient]:
    client = AsyncIOMotorClient(
        "mongodb://localhost:27018/test?directConnection=true",
        tz_aware=True,
    )
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
async def kafka_producer(
    kafka_bootstrap_servers: str,
) -> AsyncIterator[AIOKafkaProducer]:
    # TODO: Configure broker:
    #   min.insync.replicas = len(replicas) - 1
    async with AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        enable_idempotence=True,
        acks="all",
    ) as kafka_producer:
        yield kafka_producer


@pytest.fixture
async def kafka_consumer(
    kafka_bootstrap_servers: str,
    kafka_test_topic: str,
    kafka_consumer_group_id: str,
) -> AsyncIterator[AIOKafkaConsumer]:
    async with AIOKafkaConsumer(
        kafka_test_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        group_id=kafka_consumer_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    ) as kafka_consumer:
        yield kafka_consumer


class ExpectedEvent(Event):
    content_schema: Literal["ExpectedEvent"] = "ExpectedEvent"


class UnexpectedEvent(Event):
    content_schema: Literal["UnexpectedEvent"] = "UnexpectedEvent"


class TransactionFailure(Exception):
    pass


async def test_kafka_delivery(
    mongo_client: AsyncIOMotorClient,
    mongo_db: AsyncIOMotorDatabase,
    kafka_producer: AIOKafkaProducer,
    kafka_consumer: AIOKafkaConsumer,
    kafka_test_topic: str,
) -> None:
    async_mongo_kafka = EventTransport(
        mongo_client,
        kafka_producer,
        kafka_consumer,
        mongo_db=mongo_db,
    )

    event_handled = asyncio.Event()

    async def event_handler(event: Event, session: AsyncIOMotorClientSession) -> None:
        actual_event = ExpectedEvent.model_validate(event, from_attributes=True)
        assert expected_event == actual_event
        assert isinstance(session, AsyncIOMotorClientSession)
        assert session is not mongo_session
        event_handled.set()

    async with await mongo_client.start_session() as mongo_session:
        async with async_mongo_kafka.event_listener(mongo_session) as event_listener:
            expected_event = ExpectedEvent(topic=kafka_test_topic)
            event_listener.event_occurred(expected_event)

    async with async_mongo_kafka.run_event_handler(event_handler):
        await event_handled.wait()


async def test_mongo_transactional_outbox(
    mongo_client: AsyncIOMotorClient,
    mongo_db: AsyncIOMotorDatabase,
    kafka_producer: AIOKafkaProducer,
    kafka_consumer: AIOKafkaConsumer,
    kafka_test_topic: str,
    mongo_collection_outbox: str,
) -> None:
    async_mongo_kafka = EventTransport(
        mongo_client,
        kafka_producer,
        kafka_consumer,
        mongo_db=mongo_db,
        mongo_collection_outbox=mongo_collection_outbox,
    )

    async with await mongo_client.start_session() as session:
        with suppress(TransactionFailure):
            async with async_mongo_kafka.event_listener(session) as listener:
                unexpected_event = UnexpectedEvent(topic=kafka_test_topic)
                listener.event_occurred(unexpected_event)
                raise TransactionFailure

        async with async_mongo_kafka.event_listener(session) as listener:
            expected_event = ExpectedEvent(topic=kafka_test_topic)
            listener.event_occurred(expected_event)

    await asyncio.sleep(1)

    assert await mongo_db[mongo_collection_outbox].find_one(
        {
            "_id": expected_event.model_dump(
                mode="json",
                include={"topic", "content_schema", "idempotency_key"},
            )
        }
    )
    assert not await mongo_db[mongo_collection_outbox].find_one(
        {
            "_id": unexpected_event.model_dump(
                mode="json",
                include={"topic", "content_schema", "idempotency_key"},
            )
        }
    )


async def test_outbox_duplicate_event_error(
    mongo_client: AsyncIOMotorClient,
    mongo_db: AsyncIOMotorDatabase,
    kafka_producer: AIOKafkaProducer,
    kafka_consumer: AIOKafkaConsumer,
    kafka_test_topic: str,
    mongo_collection_outbox: str,
) -> None:
    async_mongo_kafka = EventTransport(
        mongo_client,
        kafka_producer,
        kafka_consumer,
        mongo_db=mongo_db,
        mongo_collection_outbox=mongo_collection_outbox,
    )
    with pytest.raises(pymongo.errors.BulkWriteError):
        async with await mongo_client.start_session() as session:
            with suppress(TransactionFailure):
                async with async_mongo_kafka.event_listener(session) as listener:
                    duplicate_event = ExpectedEvent(topic=kafka_test_topic)
                    listener.event_occurred(duplicate_event)
                    listener.event_occurred(duplicate_event)


async def test_transactional_inbox_deduplicate_events(
    mongo_client: AsyncIOMotorClient,
    mongo_db: AsyncIOMotorDatabase,
    kafka_producer: AIOKafkaProducer,
    kafka_consumer: AIOKafkaConsumer,
    kafka_test_topic: str,
    mongo_collection_outbox: str,
) -> None:
    async_mongo_kafka = EventTransport(
        mongo_client,
        kafka_producer,
        kafka_consumer,
        mongo_db=mongo_db,
        mongo_collection_outbox=mongo_collection_outbox,
    )

    duplicate_event = ExpectedEvent(topic=kafka_test_topic)
    duplicate_count = 2
    for _ in range(duplicate_count):
        await async_mongo_kafka.kafka_producer.send_and_wait(
            duplicate_event.topic,
            duplicate_event.model_dump_json().encode(),
            partition=0,
        )

    event_handler = AsyncMock(spec=EventHandler)

    async with async_mongo_kafka.run_event_handler(event_handler):
        await asyncio.sleep(1)

    event_handler.assert_called_once()


async def test_transactional_inbox_retry_handle(
    mongo_client: AsyncIOMotorClient,
    mongo_db: AsyncIOMotorDatabase,
    kafka_producer: AIOKafkaProducer,
    kafka_consumer: AIOKafkaConsumer,
    kafka_test_topic: str,
    mongo_collection_outbox: str,
) -> None:
    async_mongo_kafka = EventTransport(
        mongo_client,
        kafka_producer,
        kafka_consumer,
        mongo_db=mongo_db,
        mongo_collection_outbox=mongo_collection_outbox,
    )

    expected_event = ExpectedEvent(topic=kafka_test_topic)
    await async_mongo_kafka.kafka_producer.send_and_wait(
        expected_event.topic,
        expected_event.model_dump_json().encode(),
        partition=0,
    )

    first_time_handled = asyncio.Event()
    second_time_handled = asyncio.Event()

    async def event_handler(*_: Any, **__: Any) -> None:
        if not first_time_handled.is_set():
            first_time_handled.set()
            raise NotImplementedError
        else:
            second_time_handled.set()

    async with async_mongo_kafka.run_event_handler(event_handler):
        await second_time_handled.wait()
