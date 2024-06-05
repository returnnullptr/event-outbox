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
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from bson import ObjectId, Timestamp
from motor.motor_asyncio import (
    AsyncIOMotorChangeStream,
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorDatabase,
)
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

__all__ = ["Event", "EventListener", "EventOutbox", "EventHandler"]

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
        self.mongo_client = mongo_client
        self.mongo_db = (
            mongo_db
            if mongo_db is not None
            else self.mongo_client.get_default_database()
        )
        self.mongo_collection_outbox = mongo_collection_outbox
        self.mongo_collection_inbox = mongo_collection_inbox
        self.mongo_event_expiration = mongo_event_expiration
        self.kafka_consumer = kafka_consumer
        self.kafka_producer = kafka_producer

    async def create_indexes(self) -> None:
        await self._outbox.create_index(
            [
                ("payload.topic", 1),
                ("payload.content_schema", 1),
                ("payload.idempotency_key", 1),
            ],
            name="idempotency",
            unique=True,
        )
        await self._ensure_outbox_ttl_index()
        await self._ensure_inbox_ttl_index()

    def event_listener(
        self, mongo_session: AsyncIOMotorClientSession
    ) -> AbstractAsyncContextManager[EventListener]:
        async def event_listener() -> AsyncIterator[EventListener]:
            events: list[Event] = []
            listener = EventListener(events)
            async with _ensure_session_in_transaction(mongo_session):
                yield listener
                if events:
                    documents = [
                        {
                            "payload": event.model_dump(mode="json"),
                            "published": False,
                        }
                        for event in events
                    ]
                    events.clear()
                    await self._outbox.insert_many(
                        documents,
                        session=mongo_session,
                    )

        return asynccontextmanager(event_listener)()

    def run_event_handler(
        self, handler: EventHandler
    ) -> AbstractAsyncContextManager[None]:
        async def run_event_handler() -> AsyncIterator[None]:
            async with (
                await self.mongo_client.start_session() as producer_session,
                await self.mongo_client.start_session() as consumer_session,
            ):
                tasks = [
                    asyncio.create_task(
                        self._produce_messages(producer_session),
                    ),
                    asyncio.create_task(
                        self._consume_messages(consumer_session, handler),
                    ),
                ]
                try:
                    yield
                finally:
                    for task in tasks:
                        task.cancel()

        return asynccontextmanager(run_event_handler)()

    @property
    def _outbox(self):
        return self.mongo_db[self.mongo_collection_outbox]

    @property
    def _inbox(self):
        return self.mongo_db[self.mongo_collection_inbox]

    async def _ensure_inbox_ttl_index(self):
        while True:
            try:
                await self._inbox.create_index(
                    "handled_at",
                    name="expiration",
                    partialFilterExpression={"handled": True},
                    expireAfterSeconds=int(self.mongo_event_expiration.total_seconds()),
                )
                break
            except pymongo.errors.OperationFailure as ex:
                if ex.code == _OperationFailureCode.IndexOptionsConflict:
                    await self._inbox.drop_index("expiration")
                else:
                    raise

    async def _ensure_outbox_ttl_index(self):
        while True:
            try:
                await self._outbox.create_index(
                    "published_at",
                    name="expiration",
                    partialFilterExpression={"published": True},
                    expireAfterSeconds=int(self.mongo_event_expiration.total_seconds()),
                )
                break
            except pymongo.errors.OperationFailure as ex:
                if ex.code == _OperationFailureCode.IndexOptionsConflict:
                    await self._outbox.drop_index("expiration")
                else:
                    raise

    async def _produce_messages(self, mongo_session: AsyncIOMotorClientSession) -> None:
        while True:
            assignment = self.kafka_consumer.assignment()
            try:
                latest_handled_object_id: ObjectId | None = None
                async for document in self._outbox.find(
                    {"published": False},
                    session=mongo_session,
                ):
                    if assignment != self.kafka_consumer.assignment():
                        raise _PartitionAssignmentChanged

                    await self._publish_event(mongo_session, document)
                    latest_handled_object_id = document["_id"]

                if latest_handled_object_id:
                    id_generated_at = latest_handled_object_id.generation_time
                    start_at_operation_time = Timestamp(id_generated_at, 0)
                else:
                    cluster_time_document = cast(
                        Mapping[str, Any], mongo_session.cluster_time
                    )
                    start_at_operation_time = cluster_time_document["clusterTime"]

                pipeline = [
                    {"$match": {"operationType": {"$in": ["insert", "invalidate"]}}}
                ]
                change_stream = self._outbox.watch(
                    pipeline,
                    start_at_operation_time=start_at_operation_time,
                    session=mongo_session,
                )
                while True:
                    async with change_stream:
                        try:
                            while True:
                                change_event = await self._next_change_event(
                                    change_stream, assignment
                                )

                                if change_event["operationType"] == "invalidate":
                                    raise _ChangeStreamInvalidated(change_event["_id"])

                                document = change_event["fullDocument"]
                                await self._publish_event(mongo_session, document)

                        except _ChangeStreamInvalidated as ex:
                            change_stream = self._outbox.watch(
                                pipeline,
                                start_after=ex.resume_token,
                                session=mongo_session,
                            )
            except _PartitionAssignmentChanged:
                pass
            except Exception:  # noqa, pragma: no cover
                logging.getLogger(__name__).critical(
                    "Unexpected exception occurred "
                    "while publishing events from %r collection",
                    self.mongo_collection_outbox,
                    exc_info=True,
                )
                # TODO: Configure delay between retries
                await asyncio.sleep(1)

    async def _next_change_event(
        self,
        change_stream: AsyncIOMotorChangeStream,
        producer_assignment: frozenset[TopicPartition],
    ) -> _ChangeEvent:
        task: asyncio.Task[_ChangeEvent] = asyncio.create_task(anext(change_stream))
        while True:
            with suppress(TimeoutError):
                return await asyncio.wait_for(
                    asyncio.shield(task),
                    timeout=1,  # TODO: Configure timeout
                )
            if producer_assignment != self.kafka_consumer.assignment():
                task.cancel()
                raise _PartitionAssignmentChanged

    async def _publish_event(
        self,
        mongo_session: AsyncIOMotorClientSession,
        document: Mapping[str, Any],
    ) -> None:
        event = Event.model_validate(document["payload"])
        partition = event.partition_key % len(
            self.kafka_consumer.partitions_for_topic(event.topic)
        )
        if not any(
            topic_partition.partition == partition
            for topic_partition in self.kafka_consumer.assignment()
            if topic_partition.topic == event.topic
        ):
            return

        async with mongo_session.start_transaction():
            published_at = datetime.now(tz=UTC)
            update_result = await self._outbox.update_one(
                {"_id": document["_id"], "published": False},
                {"$set": {"published": True, "published_at": published_at}},
                session=mongo_session,
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
                    document["_id"],
                    self.mongo_collection_outbox,
                )

    async def _consume_messages(
        self, mongo_session: AsyncIOMotorClientSession, handler: EventHandler
    ) -> None:
        while True:
            try:
                kafka_consumer_record = await self.kafka_consumer.getone()
                event = Event.model_validate_json(kafka_consumer_record.value)

                document_id = event.model_dump(
                    mode="json",
                    include={"topic", "content_schema", "idempotency_key"},
                )

                with suppress(pymongo.errors.DuplicateKeyError):
                    await self._inbox.insert_one(
                        {"_id": document_id, "handled": False},
                        session=mongo_session,
                    )

                while True:
                    try:
                        await self._handle_event(
                            document_id, event, mongo_session, handler
                        )
                        await self.kafka_consumer.commit()
                        break
                    except Exception:  # noqa
                        logging.getLogger(__name__).critical(
                            "Failed to handle event %r from %r collection",
                            document_id,
                            self.mongo_collection_inbox,
                            exc_info=True,
                        )
                        # TODO: Configure delay between retries
                        await asyncio.sleep(1)
            except Exception:  # noqa, pragma: no cover
                logging.getLogger(__name__).critical(
                    "Unexpected exception occurred "
                    "while handling events from %r collection",
                    self.mongo_collection_outbox,
                    exc_info=True,
                )
                # TODO: Configure delay between retries
                await asyncio.sleep(1)

    async def _handle_event(
        self,
        document_id: Mapping[str, Any],
        event: Event,
        mongo_session: AsyncIOMotorClientSession,
        handler: EventHandler,
    ) -> None:
        async with mongo_session.start_transaction():
            result = await self._inbox.update_one(
                {"_id": document_id, "handled": False},
                {
                    "$set": {
                        "handled": True,
                        "handled_at": datetime.now(tz=UTC),
                    }
                },
                session=mongo_session,
            )
            if result.modified_count:
                await handler(event, mongo_session)
            else:
                logging.getLogger(__name__).info(
                    "Skipped already handled event %r from %r collection",
                    document_id,
                    self.mongo_collection_outbox,
                )


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


class _OperationFailureCode(IntEnum):
    IndexOptionsConflict = 85
