import asyncio
import logging
import uuid
from contextlib import (
    AbstractAsyncContextManager,
    asynccontextmanager,
    nullcontext,
    suppress,
)
from datetime import UTC, datetime, timedelta
from enum import IntEnum
from typing import Any, AsyncIterator, Mapping, Protocol, TypeAlias, cast

import pymongo.errors
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from bson import ObjectId, Timestamp
from motor.motor_asyncio import (
    AsyncIOMotorChangeStream,
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

__all__ = ["Event", "EventListener", "EventOutbox", "EventHandler"]

_EventPublishIntent: TypeAlias = Mapping[str, Any]
_ChangeEvent: TypeAlias = Mapping[str, Any]


class Event(BaseModel):
    topic: str
    content_schema: str
    partition_key: int = 0
    idempotency_key: str = Field(default_factory=lambda: uuid.uuid4().hex)
    occurred_at: AwareDatetime = Field(default_factory=lambda: datetime.now(tz=UTC))

    model_config = ConfigDict(extra="allow")


class EventListener:
    def __init__(self, events: list[Event]) -> None:
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
        pass  # pragma: no cover


class EventOutbox:
    def __init__(
        self,
        mongo_client: AsyncIOMotorClient,
        kafka_producer: AIOKafkaProducer,
        kafka_consumer: AIOKafkaConsumer,
        *,
        mongo_db: AsyncIOMotorDatabase | None = None,
        mongo_collection_outbox: str = "transactional-outbox",
        mongo_collection_inbox: str = "transactional-inbox",
        mongo_event_expiration: timedelta = timedelta(days=1),
    ) -> None:
        self._mongo_client = mongo_client
        self._mongo_db = (
            mongo_db
            if mongo_db is not None
            else self._mongo_client.get_default_database()
        )
        self._mongo_outbox = self._mongo_db[mongo_collection_outbox]
        self._mongo_inbox = self._mongo_db[mongo_collection_inbox]
        self._mongo_event_expiration = mongo_event_expiration
        self._kafka_consumer = kafka_consumer
        self._kafka_producer = kafka_producer

    async def create_indexes(self) -> None:
        await self._ensure_outbox_unique_index()
        await self._ensure_outbox_ttl_index()
        await self._ensure_inbox_ttl_index()

    def event_listener(
        self, mongo_session: AsyncIOMotorClientSession
    ) -> AbstractAsyncContextManager[EventListener]:
        async def func() -> AsyncIterator[EventListener]:
            events: list[Event] = []
            listener = EventListener(events)
            async with _ensure_session_in_transaction(mongo_session):
                yield listener
                await self._insert_publish_intents_to_database(mongo_session, events)

        return asynccontextmanager(func)()

    def run_event_handler(
        self, handler: EventHandler
    ) -> AbstractAsyncContextManager[None]:
        async def func() -> AsyncIterator[None]:
            async with self._run_publish_events_task():
                async with self._run_handle_events_task(handler):
                    yield

        return asynccontextmanager(func)()

    async def _ensure_outbox_unique_index(self):
        await self._mongo_outbox.create_index(
            [
                ("payload.topic", 1),
                ("payload.content_schema", 1),
                ("payload.idempotency_key", 1),
            ],
            name="idempotency",
            unique=True,
        )

    async def _ensure_outbox_ttl_index(self):
        while True:
            try:
                await self._mongo_outbox.create_index(
                    "published_at",
                    name="expiration",
                    partialFilterExpression={"published": True},
                    expireAfterSeconds=int(
                        self._mongo_event_expiration.total_seconds()
                    ),
                )
                break
            except pymongo.errors.OperationFailure as ex:
                if ex.code == _OperationFailureCode.IndexOptionsConflict:
                    await self._mongo_outbox.drop_index("expiration")
                else:
                    raise

    async def _ensure_inbox_ttl_index(self):
        while True:
            try:
                await self._mongo_inbox.create_index(
                    "handled_at",
                    name="expiration",
                    partialFilterExpression={"handled": True},
                    expireAfterSeconds=int(
                        self._mongo_event_expiration.total_seconds()
                    ),
                )
                break
            except pymongo.errors.OperationFailure as ex:
                if ex.code == _OperationFailureCode.IndexOptionsConflict:
                    await self._mongo_inbox.drop_index("expiration")
                else:
                    raise

    async def _insert_publish_intents_to_database(
        self, mongo_session: AsyncIOMotorClientSession, events: list[Event]
    ) -> None:
        if events:
            documents = [
                {
                    "payload": event.model_dump(mode="json"),
                    "published": False,
                }
                for event in events
            ]
            events.clear()
            await self._mongo_outbox.insert_many(
                documents,
                session=mongo_session,
            )

    @asynccontextmanager
    async def _run_publish_events_task(self) -> AsyncIterator[None]:
        async with await self._mongo_client.start_session() as mongo_session:
            task = asyncio.create_task(self._publish_events(mongo_session))
            try:
                yield
            finally:
                task.cancel()

    async def _publish_events(self, mongo_session: AsyncIOMotorClientSession) -> None:
        while True:
            event_publisher = _EventPublisher(
                mongo_session,
                self._mongo_outbox,
                self._kafka_consumer,
                self._kafka_producer,
            )
            try:
                await event_publisher.publish_events()
            except _PartitionAssignmentChanged:
                pass
            except Exception:  # noqa, pragma: no cover
                logging.getLogger(__name__).critical(
                    "Unexpected exception occurred "
                    "while publishing events from %r collection",
                    self._mongo_outbox.name,
                    exc_info=True,
                )
                # TODO: Configure delay between retries
                await asyncio.sleep(1)

    @asynccontextmanager
    async def _run_handle_events_task(
        self, handler: EventHandler
    ) -> AsyncIterator[None]:
        async with await self._mongo_client.start_session() as mongo_session:
            task = asyncio.create_task(self._handle_events(mongo_session, handler))
            try:
                yield
            finally:
                task.cancel()

    async def _handle_events(
        self, mongo_session: AsyncIOMotorClientSession, handler: EventHandler
    ) -> None:
        while True:
            event_consumer = _EventConsumer(
                mongo_session,
                self._mongo_inbox,
                self._kafka_consumer,
                handler,
            )
            try:
                await event_consumer.handle_events()
            except Exception:  # noqa, pragma: no cover
                logging.getLogger(__name__).critical(
                    "Unexpected exception occurred "
                    "while handling events from %r collection",
                    self._mongo_outbox.name,
                    exc_info=True,
                )
                # TODO: Configure delay between retries
                await asyncio.sleep(1)


def _ensure_session_in_transaction(
    mongo_session: AsyncIOMotorClientSession,
) -> AbstractAsyncContextManager[Any]:
    if bool(mongo_session.in_transaction):
        return nullcontext()
    return mongo_session.start_transaction()


class _ChangeStreamInvalidated(Exception):
    def __init__(self, resume_token: Mapping[str, Any]) -> None:
        self.resume_token = resume_token


class _PartitionAssignmentChanged(Exception):
    pass


class _EventPublisher:
    def __init__(
        self,
        mongo_session: AsyncIOMotorClientSession,
        mongo_outbox: AsyncIOMotorCollection,
        kafka_consumer: AIOKafkaConsumer,
        kafka_producer: AIOKafkaProducer,
    ) -> None:
        self.mongo_session = mongo_session
        self.mongo_outbox = mongo_outbox
        self.kafka_consumer = kafka_consumer
        self.kafka_producer = kafka_producer
        self.assignment = kafka_consumer.assignment()

    @property
    def _assignment_changed(self) -> bool:
        return self.assignment != self.kafka_consumer.assignment()

    async def publish_events(self) -> None:
        async for intent in self._iterate_intents():
            await self._publish(intent)

    async def _iterate_intents(self) -> AsyncIterator[_EventPublishIntent]:
        latest_handled_object_id: ObjectId | None = None

        async for document in self.mongo_outbox.find(
            {"published": False},
            session=self.mongo_session,
        ):
            if self._assignment_changed:
                raise _PartitionAssignmentChanged

            yield document
            latest_handled_object_id = document["_id"]

        if latest_handled_object_id:
            id_generated_at = latest_handled_object_id.generation_time
            start_at_operation_time = Timestamp(id_generated_at, 0)
        else:
            # TODO: Consider delay between
            cluster_time_document = cast(
                Mapping[str, Any],
                self.mongo_session.cluster_time,
            )
            start_at_operation_time = cluster_time_document["clusterTime"]

        async for document in self._subscribe_to_change_stream(start_at_operation_time):
            yield document

    async def _subscribe_to_change_stream(
        self, start_at_operation_time: Timestamp
    ) -> AsyncIterator[Mapping[str, Any]]:
        pipeline = [{"$match": {"operationType": {"$in": ["insert", "invalidate"]}}}]
        change_stream = self.mongo_outbox.watch(
            pipeline,
            start_at_operation_time=start_at_operation_time,
            session=self.mongo_session,
        )
        while True:
            async with change_stream:
                try:
                    while True:
                        change_event = await self._next_change_event(change_stream)

                        if change_event["operationType"] == "invalidate":
                            raise _ChangeStreamInvalidated(change_event["_id"])

                        yield change_event["fullDocument"]

                except _ChangeStreamInvalidated as ex:
                    change_stream = self.mongo_outbox.watch(
                        pipeline,
                        start_after=ex.resume_token,
                        session=self.mongo_session,
                    )

    async def _next_change_event(
        self, change_stream: AsyncIOMotorChangeStream
    ) -> _ChangeEvent:
        task: asyncio.Task[_ChangeEvent] = asyncio.create_task(anext(change_stream))
        while True:
            with suppress(TimeoutError):
                return await asyncio.wait_for(
                    asyncio.shield(task),
                    timeout=1,  # TODO: Configure timeout
                )
            if self._assignment_changed:
                task.cancel()
                raise _PartitionAssignmentChanged

    async def _publish(self, intent: _EventPublishIntent) -> None:
        event = Event.model_validate(intent["payload"])
        partition = self._partition(event)
        if not self._assigned(event.topic, partition):
            return

        async with self.mongo_session.start_transaction():
            published_at = datetime.now(tz=UTC)
            update_result = await self.mongo_outbox.update_one(
                {"_id": intent["_id"], "published": False},
                {"$set": {"published": True, "published_at": published_at}},
                session=self.mongo_session,
            )
            if update_result.modified_count:
                await self.kafka_producer.send_and_wait(
                    event.topic,
                    event.model_dump_json().encode(),
                    partition=partition,
                    timestamp_ms=int(published_at.timestamp() * 1000),
                )
            else:
                logging.getLogger(__name__).info(
                    "Skipped already published event %r from %r collection",
                    intent["_id"],
                    self.mongo_outbox.name,
                )

    def _partition(self, event: Event) -> int:
        return event.partition_key % len(
            self.kafka_consumer.partitions_for_topic(event.topic)
        )

    def _assigned(self, topic: str, partition: int) -> bool:
        return any(
            topic_partition.partition == partition
            for topic_partition in self.assignment
            if topic_partition.topic == topic
        )


class _EventConsumer:
    def __init__(
        self,
        mongo_session: AsyncIOMotorClientSession,
        mongo_inbox: AsyncIOMotorCollection,
        kafka_consumer: AIOKafkaConsumer,
        event_handler: EventHandler,
    ) -> None:
        self.mongo_session = mongo_session
        self.mongo_inbox = mongo_inbox
        self.kafka_consumer = kafka_consumer
        self.event_handler = event_handler

    async def handle_events(self) -> None:
        while True:
            kafka_consumer_record = await self.kafka_consumer.getone()
            event = Event.model_validate_json(kafka_consumer_record.value)

            document_id = event.model_dump(
                mode="json",
                include={"topic", "content_schema", "idempotency_key"},
            )

            with suppress(pymongo.errors.DuplicateKeyError):
                await self.mongo_inbox.insert_one(
                    {"_id": document_id, "handled": False},
                    session=self.mongo_session,
                )

            while True:
                try:
                    await self._handle_event(document_id, event)
                    await self.kafka_consumer.commit()
                    break
                except Exception:  # noqa
                    logging.getLogger(__name__).critical(
                        "Failed to handle event %r from %r collection",
                        document_id,
                        self.mongo_inbox.name,
                        exc_info=True,
                    )
                    # TODO: Configure delay between retries
                    await asyncio.sleep(1)

    async def _handle_event(self, document_id: Mapping[str, Any], event: Event) -> None:
        async with self.mongo_session.start_transaction():
            result = await self.mongo_inbox.update_one(
                {"_id": document_id, "handled": False},
                {
                    "$set": {
                        "handled": True,
                        "handled_at": datetime.now(tz=UTC),
                    }
                },
                session=self.mongo_session,
            )
            if result.modified_count:
                await self.event_handler(event, self.mongo_session)
            else:
                logging.getLogger(__name__).info(
                    "Skipped already handled event %r from %r collection",
                    document_id,
                    self.mongo_inbox.name,
                )


class _OperationFailureCode(IntEnum):
    IndexOptionsConflict = 85
