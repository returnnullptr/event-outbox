import asyncio
from contextlib import suppress
from typing import Any, Literal
from unittest.mock import AsyncMock

import pymongo.errors
import pytest
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
    outbox: AsyncIOMotorCollection,
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

    assert await outbox.find_one(
        {"payload": expected_event.model_dump(mode="json")},
    )
    assert not await outbox.find_one(
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
        await event_outbox.kafka_producer.send_and_wait(
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
    await event_outbox.kafka_producer.send_and_wait(
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
    await event_outbox.kafka_producer.send_and_wait(
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
