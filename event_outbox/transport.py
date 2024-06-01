import asyncio
import logging
import uuid
from asyncio import CancelledError
from collections import deque
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from enum import IntEnum
from typing import Any, AsyncIterator, Deque, Mapping, Protocol

import pymongo.errors
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from bson import Timestamp
from motor.motor_asyncio import (
    AsyncIOMotorChangeStream,
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorDatabase,
)
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

__all__ = ["Event", "EventListener", "EventTransport", "EventHandler"]


class Event(BaseModel):
    topic: str
    content_schema: str
    idempotency_key: str = Field(default_factory=lambda: uuid.uuid4().hex)
    occurred_at: AwareDatetime = Field(default_factory=lambda: datetime.now(tz=UTC))

    model_config = ConfigDict(extra="allow")


class EventListener:
    def __init__(self, events: Deque[Event]) -> None:
        self.__events = events

    def event_occurred(self, event: Event) -> None:
        self.__events.append(event)


class EventHandler(Protocol):
    async def __call__(
        self,
        event: Event,
        mongo_session: AsyncIOMotorClientSession,
        /,
    ) -> None:
        pass


class EventTransport:
    def __init__(
        self,
        mongo_client: AsyncIOMotorClient,
        kafka_producer: AIOKafkaProducer,
        kafka_consumer: AIOKafkaConsumer,
        *,
        mongo_db: AsyncIOMotorDatabase | None = None,
        mongo_collection_outbox: str = "transactional-outbox",
        mongo_collection_inbox: str = "transactional-inbox",
    ) -> None:
        self.mongo_client = mongo_client
        self.mongo_db = (
            mongo_db
            if mongo_db is not None
            else self.mongo_client.get_default_database()
        )
        self.mongo_collection_outbox = mongo_collection_outbox
        self.mongo_collection_inbox = mongo_collection_inbox
        self.kafka_consumer = kafka_consumer
        self.kafka_producer = kafka_producer

    def event_listener(
        self, mongo_session: AsyncIOMotorClientSession
    ) -> AbstractAsyncContextManager[EventListener]:
        async def event_listener() -> AsyncIterator[EventListener]:
            events: Deque[Event] = deque()
            listener = EventListener(events)
            async with mongo_session.start_transaction():
                yield listener
                documents = []
                while events:
                    event = events.popleft()
                    document = {
                        "_id": event.model_dump(
                            mode="json",
                            include={"topic", "content_schema", "idempotency_key"},
                        ),
                        "payload": event.model_dump(
                            mode="json",
                            exclude={"topic", "content_schema", "idempotency_key"},
                        ),
                        "published": False,
                    }
                    documents.append(document)
                if documents:
                    await self.mongo_db[self.mongo_collection_outbox].insert_many(
                        documents,
                        session=mongo_session,
                    )

        return asynccontextmanager(event_listener)()

    def run_event_handler(
        self, handler: EventHandler
    ) -> AbstractAsyncContextManager[None]:
        async def run_event_handler() -> AsyncIterator[None]:
            async with self._subscribe_to_mongo_change_stream(
                # TODO: Load state to database
                start_after=None,
                start_at_operation_type=None,
            ) as change_stream:
                producer_loop = asyncio.create_task(
                    self._produce_messages(
                        change_stream,
                        heartbeat=timedelta(minutes=5),
                    ),
                )
                consumer_loop = asyncio.create_task(
                    self._consume_messages(handler),
                )
                try:
                    yield
                finally:
                    consumer_loop.cancel()
                    producer_loop.cancel()
                    with suppress(CancelledError):
                        await asyncio.gather(consumer_loop, producer_loop)

        return asynccontextmanager(run_event_handler)()

    def _subscribe_to_mongo_change_stream(
        self,
        start_after: Mapping[str, Any] | None,
        start_at_operation_type: datetime | None,
    ) -> AbstractAsyncContextManager[AsyncIOMotorChangeStream]:
        async def generator_func() -> AsyncIterator[AsyncIOMotorChangeStream]:
            if start_after:
                try:
                    async with self.mongo_db[self.mongo_collection_outbox].watch(
                        [{"$match": {"operationType": {"$in": ["insert"]}}}],
                        start_after=start_after,
                    ) as change_stream:
                        yield change_stream
                except pymongo.errors.OperationFailure as ex:
                    if ex.code in _FailedToResumeChangeStream:
                        logging.getLogger(__name__).warning(
                            "Failed to start change stream of collection %r after %r.",
                            self.mongo_collection_outbox,
                            start_after,
                            exc_info=True,
                        )
                    else:
                        raise

            if start_at_operation_type:
                logging.getLogger(__name__).info(
                    "Start change stream of collection %r at operation time: %r",
                    self.mongo_collection_outbox,
                    start_at_operation_type.isoformat(),
                )
                try:
                    async with self.mongo_db[self.mongo_collection_outbox].watch(
                        [{"$match": {"operationType": {"$in": ["insert"]}}}],
                        start_at_operation_time=Timestamp(start_at_operation_type, 1),
                    ) as change_stream:
                        yield change_stream
                except pymongo.errors.OperationFailure as ex:
                    if ex.code in _FailedToResumeChangeStream:
                        logging.getLogger(__name__).critical(
                            "Failed to start change stream of collection %r "
                            "at operation time: %r",
                            self.mongo_collection_outbox,
                            start_at_operation_type.isoformat(),
                            exc_info=True,
                        )
                    else:
                        raise

            oldest_document = await self.mongo_db[
                self.mongo_collection_outbox
            ].find_one(
                {},
                {"payload.occurred_at": True},
                sort={"payload.occurred_at": pymongo.ASCENDING},
            )
            async with self.mongo_db[self.mongo_collection_outbox].watch(
                [{"$match": {"operationType": {"$in": ["insert"]}}}],
                start_at_operation_time=(
                    Timestamp(
                        datetime.fromisoformat(
                            oldest_document["payload"]["occurred_at"]
                        ),
                        1,
                    )
                    if oldest_document
                    else None
                ),
            ) as change_stream:
                yield change_stream

        return asynccontextmanager(generator_func)()

    async def _produce_messages(
        self,
        change_stream: AsyncIOMotorChangeStream,
        *,
        heartbeat: timedelta,
    ) -> None:
        while True:
            get_change_event: asyncio.Task[Mapping[str, Any]] = asyncio.create_task(
                anext(change_stream)
            )
            try:
                while True:
                    try:
                        async with asyncio.timeout(heartbeat.total_seconds()):
                            change_event = await asyncio.shield(get_change_event)
                    except TimeoutError:
                        # TODO: Save `start_at_operation_type` to database
                        continue
                    else:
                        break
            except asyncio.CancelledError:
                get_change_event.cancel()

            document = change_event["fullDocument"]
            event = Event.model_validate(document["_id"] | document["payload"])

            async with await self.mongo_client.start_session() as mongo_session:
                async with mongo_session.start_transaction():
                    # TODO: Save `change_stream.resume_token` to database
                    # TODO: Save `start_at_operation_type` to database
                    published_at = datetime.now(tz=UTC)
                    document = await self.mongo_db[
                        self.mongo_collection_outbox
                    ].find_one_and_update(
                        {"_id": document["_id"], "published": False},
                        {"$set": {"published": True, "published_at": published_at}},
                        session=mongo_session,
                    )
                    if not document:
                        logging.getLogger(__name__).info(
                            "Already published event %r from %r collection skipped",
                            change_event["_id"],
                            self.mongo_collection_outbox,
                            exc_info=True,
                        )
                        continue

                    await self.kafka_producer.send_and_wait(
                        event.topic,
                        event.model_dump_json().encode(),
                        partition=0,
                        timestamp_ms=int(published_at.timestamp()),
                    )

    async def _consume_messages(self, handler: EventHandler) -> None:
        handled_kafka_messages = []
        async with await self.mongo_client.start_session() as mongo_session:
            while True:
                kafka_consumer_record = await self.kafka_consumer.getone()
                event = Event.model_validate_json(kafka_consumer_record.value)
                try:
                    await self.mongo_db[self.mongo_collection_inbox].insert_one(
                        {
                            "_id": event.model_dump(
                                mode="json",
                                include={"topic", "content_schema", "idempotency_key"},
                            ),
                            "handled": False,
                        },
                        session=mongo_session,
                    )
                except pymongo.errors.DuplicateKeyError:
                    pass

                while True:
                    try:
                        async with mongo_session.start_transaction():
                            result = await self.mongo_db[
                                self.mongo_collection_inbox
                            ].update_one(
                                {
                                    "_id": event.model_dump(
                                        mode="json",
                                        include={
                                            "topic",
                                            "content_schema",
                                            "idempotency_key",
                                        },
                                    ),
                                    "handled": False,
                                },
                                {
                                    "$set": {
                                        "handled": True,
                                        "handled_at": datetime.now(tz=UTC),
                                    }
                                },
                                session=mongo_session,
                            )
                            if not result.modified_count:
                                raise _EventAlreadyHandledError

                            handled_kafka_messages.append(kafka_consumer_record)
                            await handler(event, mongo_session)
                    except _EventAlreadyHandledError:
                        pass
                    except Exception as ex:
                        logging.getLogger(__name__).critical(
                            "Failed to handle event %r: %r",
                            event.model_dump(
                                mode="json",
                                include={"topic", "content_schema", "idempotency_key"},
                            ),
                            ex,
                        )
                        continue
                    await self.kafka_consumer.commit()
                    break


class _FailedToResumeChangeStream(IntEnum):
    CHANGE_STREAM_FATAL_ERROR = 280
    CHANGE_STREAM_HISTORY_LOST = 286


class _EventAlreadyHandledError(Exception):
    pass
