import asyncio
from contextlib import suppress
from datetime import timedelta
from typing import Any, AsyncIterator, Literal
from unittest.mock import AsyncMock

import pymongo.errors
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dynaconf import LazySettings
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)

from event_outbox import Event, EventHandler, EventOutbox


class ExpectedEvent(Event):
    content_schema: Literal["ExpectedEvent"] = "ExpectedEvent"


class UnexpectedEvent(Event):
    content_schema: Literal["UnexpectedEvent"] = "UnexpectedEvent"


class TransactionFailure(Exception):
    pass


async def test_event_delivery(
    event_outbox: EventOutbox,
    mongo_client: AsyncIOMotorClient,
    topic: str,
) -> None:
    event_handled = asyncio.Event()

    async def event_handler(event: Event, session: AsyncIOMotorClientSession) -> None:
        actual_event = ExpectedEvent.model_validate(event, from_attributes=True)
        assert expected_event == actual_event
        assert isinstance(session, AsyncIOMotorClientSession)
        assert session is not mongo_session
        event_handled.set()

    async with await mongo_client.start_session() as mongo_session:
        async with event_outbox.event_listener(mongo_session) as event_listener:
            expected_event = ExpectedEvent(topic=topic)
            event_listener.event_occurred(expected_event)

    async with event_outbox.run_event_handler(event_handler):
        await event_handled.wait()


async def test_transactional_outbox(
    event_outbox: EventOutbox,
    mongo_client: AsyncIOMotorClient,
    outbox_collection: AsyncIOMotorCollection,
    topic: str,
) -> None:
    async with await mongo_client.start_session() as session:
        with suppress(TransactionFailure):
            async with event_outbox.event_listener(session) as listener:
                unexpected_event = UnexpectedEvent(topic=topic)
                listener.event_occurred(unexpected_event)
                raise TransactionFailure

        async with event_outbox.event_listener(session) as listener:
            expected_event = ExpectedEvent(topic=topic)
            listener.event_occurred(expected_event)

    assert await outbox_collection.find_one(
        {"payload": expected_event.model_dump(mode="json")},
    )
    assert not await outbox_collection.find_one(
        {"payload": unexpected_event.model_dump(mode="json")},
    )


async def test_transactional_outbox_duplicate_event_error(
    event_outbox: EventOutbox,
    mongo_client: AsyncIOMotorClient,
    topic: str,
) -> None:
    async with await mongo_client.start_session() as session:
        with pytest.raises(pymongo.errors.BulkWriteError):
            async with event_outbox.event_listener(session) as listener:
                duplicate_event = ExpectedEvent(topic=topic)
                listener.event_occurred(duplicate_event)
                listener.event_occurred(duplicate_event)


async def test_invalidate_mongo_change_stream(
    mongo_client: AsyncIOMotorClient,
    mongo_db: AsyncIOMotorDatabase,
    event_outbox: EventOutbox,
    topic: str,
) -> None:
    event_handled = asyncio.Event()

    async def event_handler(_: Event, __: AsyncIOMotorClientSession) -> None:
        event_handled.set()

    async with event_outbox.run_event_handler(event_handler):
        await mongo_client.drop_database(mongo_db)

        async with await mongo_client.start_session() as mongo_session:
            async with event_outbox.event_listener(mongo_session) as event_listener:
                unexpected_event = UnexpectedEvent(topic=topic)
                event_listener.event_occurred(unexpected_event)

        await event_handled.wait()


async def test_transactional_inbox_deduplicate_events(
    event_outbox: EventOutbox,
    mongo_client: AsyncIOMotorClient,
    mongo_db: AsyncIOMotorDatabase,
    topic: str,
) -> None:
    duplicate_event = ExpectedEvent(topic=topic)
    for _ in range(2):
        await event_outbox._kafka_producer.send_and_wait(
            duplicate_event.topic,
            duplicate_event.model_dump_json().encode(),
            partition=0,
        )

    event_handler = AsyncMock(spec=EventHandler)

    async with event_outbox.run_event_handler(event_handler):
        await asyncio.sleep(1)

    event_handler.assert_called_once()


async def test_transactional_inbox_retry_handle(
    event_outbox: EventOutbox,
    topic: str,
) -> None:
    expected_event = ExpectedEvent(topic=topic)
    await event_outbox._kafka_producer.send_and_wait(
        expected_event.topic,
        expected_event.model_dump_json().encode(),
        partition=0,
    )

    first_time_handled = asyncio.Event()
    second_time_handled = asyncio.Event()

    async def event_handler(*_: Any, **__: Any) -> None:
        if not first_time_handled.is_set():
            first_time_handled.set()
            raise TransactionFailure
        else:
            second_time_handled.set()

    async with event_outbox.run_event_handler(event_handler):
        await second_time_handled.wait()


async def test_create_listener_in_event_handler(
    event_outbox: EventOutbox,
    topic: str,
) -> None:
    expected_event = ExpectedEvent(topic=topic)
    await event_outbox._kafka_producer.send_and_wait(
        expected_event.topic,
        expected_event.model_dump_json().encode(),
        partition=0,
    )

    event_handled = asyncio.Event()

    async def event_handler(_: Event, session: AsyncIOMotorClientSession) -> None:
        async with event_outbox.event_listener(session):
            event_handled.set()

    async with event_outbox.run_event_handler(event_handler):
        await event_handled.wait()


async def test_change_event_expiration(
    event_outbox: EventOutbox,
    event_expiration_seconds: int,
) -> None:
    event_outbox._mongo_event_expiration = timedelta(
        seconds=event_expiration_seconds + 1
    )
    await event_outbox.create_indexes()


async def test_change_stream_invalidation(
    mongo_client: AsyncIOMotorClient,
    mongo_db: AsyncIOMotorDatabase,
    event_outbox: EventOutbox,
    event_expiration_seconds: int,
    topic: str,
) -> None:
    event_handled = asyncio.Event()

    async def event_handler(_: Event, __: AsyncIOMotorClientSession) -> None:
        event_handled.set()

    async with event_outbox.run_event_handler(event_handler):
        await mongo_client.drop_database(mongo_db)
        await event_outbox.create_indexes()
        async with await mongo_client.start_session() as mongo_session:
            async with event_outbox.event_listener(mongo_session) as event_listener:
                event_listener.event_occurred(UnexpectedEvent(topic=topic))
        await event_handled.wait()


@pytest.fixture
async def another_event_outbox(
    mongo_client: AsyncIOMotorClient,
    mongo_db: AsyncIOMotorDatabase,
    kafka_producer: AIOKafkaProducer,
    config: LazySettings,
) -> AsyncIterator[EventOutbox]:
    async with AIOKafkaConsumer(
        *config.kafka.consumer.topics,
        bootstrap_servers=config.kafka.bootstrap_servers,
        group_id=config.kafka.consumer.group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    ) as another_kafka_consumer:
        await another_kafka_consumer.seek_to_end()
        yield EventOutbox(
            mongo_client,
            kafka_producer,
            another_kafka_consumer,
            mongo_db=mongo_db,
            mongo_collection_outbox=config.mongo.collection_outbox,
            mongo_collection_inbox=config.mongo.collection_inbox,
        )


async def test_change_stream_segregation(
    event_outbox: EventOutbox,
    another_event_outbox: EventOutbox,
    mongo_client: AsyncIOMotorClient,
    topic: str,
) -> None:
    events_number = 3
    async with await mongo_client.start_session() as mongo_session:
        async with event_outbox.event_listener(mongo_session) as event_listener:
            for partition_key in range(events_number):
                event_listener.event_occurred(
                    ExpectedEvent(topic=topic, partition_key=partition_key)
                )

    handled_events = set()
    all_events_handled = asyncio.Event()

    async def event_handler(event: Event, __: AsyncIOMotorClientSession) -> None:
        handled_events.add(event.partition_key)
        if len(handled_events) == events_number:
            all_events_handled.set()

    async with (
        event_outbox.run_event_handler(event_handler),
        another_event_outbox.run_event_handler(event_handler),
    ):
        await all_events_handled.wait()


@pytest.mark.async_timeout(10)
async def test_partition_assignment_changed(
    mongo_client: AsyncIOMotorClient,
    event_outbox: EventOutbox,
    config: LazySettings,
    topic: str,
) -> None:
    event_handled = asyncio.Event()

    async def event_handler(_: Event, __: AsyncIOMotorClientSession) -> None:
        event_handled.set()

    async with event_outbox.run_event_handler(event_handler):
        async with AIOKafkaConsumer(
            *config.kafka.consumer.topics,
            bootstrap_servers=config.kafka.bootstrap_servers,
            group_id=config.kafka.consumer.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        ) as another_kafka_consumer:
            await asyncio.sleep(1)
            partition_key = next(iter(another_kafka_consumer.assignment())).partition

        async with await mongo_client.start_session() as mongo_session:
            async with event_outbox.event_listener(mongo_session) as event_listener:
                event_listener.event_occurred(
                    ExpectedEvent(topic=topic, partition_key=partition_key)
                )

        await event_handled.wait()
