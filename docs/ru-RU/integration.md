# Интеграция

Исходный код ограниченного контекста (bounded context) определяет формальную объектную модель предметной области. Необходимо предоставить доступ к такому контексту, чтобы пользователи могли использовать формальную модель для решения своих задач. Обычно, для этих целей создается служба с открытым протоколом (open-host service). Например, HTTP-сервер, RPC или даже Pub/Sub.

В этом примере используется HTTP-фреймворк [FastAPI] для интеграции с пользовательским интерфейсом (UI) и библиотека [event-outbox] для обеспечения гарантий доставки и идемпотентной обработки событий (domain event) между контекстами (bounded contexts) с помощью [Apache Kafka] и [MongoDB].

## Инициализация

Библиотека [event-outbox] предоставляет [фасад (facade)](https://refactoring.guru/design-patterns/facade) `EventOutbox` - точку входа для работы с библиотекой.

При запуске процесса, для механизма Pub/Sub необходимо запустить 2 процесса:
- Цикл отправки событий из `Transactional Outbox` в топик (topic) [Apache Kafka] с гарантией доставки хотя бы 1 раз (at least once).
- Цикл обработки событий из топиков (topics) [Apache Kafka] через `Transactional Inbox` с гарантией идемпотентности обработки (idempotent consumer).

Эти процессы запускаются контекстным менеджером `EventOutbox.run_event_handler` в `lifespan` приложения на [FastAPI]:

```python
from typing import Annotated, AsyncIterator
from event_outbox import EventOutbox, Event
from motor.motor_asyncio import AsyncIOMotorClientSession, AsyncIOMotorClient
from contextlib import asynccontextmanager, AsyncExitStack
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
# noinspection PyUnresolvedReferences
from fastapi import Depends, FastAPI

def get_event_outbox() -> EventOutbox:
    raise NotImplementedError


EventOutboxDependency = Annotated[EventOutbox, Depends(get_event_outbox)]


# noinspection PyUnusedLocal
async def event_handler(
    event: Event, 
    session: AsyncIOMotorClientSession,
    outbox: EventOutbox,
) -> None:
    ...


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with AsyncExitStack() as stack:
        mongo_connection_string: str = ...
        kafka_bootstrap_servers: str = ...

        mongo_client = AsyncIOMotorClient(
            mongo_connection_string,
            tz_aware=True,
        )
        stack.callback(mongo_client.close)

        kafka_producer = await stack.enter_async_context(
            AIOKafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                enable_idempotence=True,
                acks="all",
            )
        )

        kafka_consumer = await stack.enter_async_context(
            AIOKafkaConsumer(
                *("booking", "rapid_testing", "reporting"),
                bootstrap_servers=kafka_bootstrap_servers,
                group_id="monolith",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
        )

        event_outbox = EventOutbox(
            mongo_client, 
            kafka_producer, 
            kafka_consumer,
        )
        await event_outbox.create_indexes()
        await stack.enter_async_context(
            event_outbox.run_event_handler(
                lambda event, session: event_handler(
                    event,
                    session,
                    event_outbox,
                )
            )
        )
        app.dependency_overrides = {
            get_event_outbox: lambda: event_outbox,
        }
        yield


def app_factory() -> FastAPI:
    return FastAPI(lifespan=lifespan)
```

Также, на инициализации происходит внедрение зависимости `EventOutbox` в обработчики запросов (request handler) и обработчик событий (event handler).

> __NOTE__:\
> Пример инициализации в исходном коде [этого репозитории](https://github.com/returnnullptr/event-outbox-example/blob/main/example/infrastructure/http_server.py).

Для внедрения зависимости `EventOutbox` в обработчики запросов, используется механизм внедрения зависимостей [FastAPI]:

```python
from typing import Annotated
from event_outbox import EventOutbox
from pydantic import BaseModel
# noinspection PyUnresolvedReferences
from fastapi import Depends, FastAPI

def get_event_outbox() -> EventOutbox:
    raise NotImplementedError

EventOutboxDependency = Annotated[EventOutbox, Depends(get_event_outbox)]

app = FastAPI()

@app.post("/")
async def request_handler(
    outbox: EventOutboxDependency,
) -> BaseModel:
    assert isinstance(outbox, EventOutbox)
    return ...
```

Внедрение зависимости `EventOutbox` в обработчики событий не обязательно:

```python
from typing import AsyncIterator
from event_outbox import EventOutbox, Event
from motor.motor_asyncio import AsyncIOMotorClientSession, AsyncIOMotorClient
from contextlib import asynccontextmanager, AsyncExitStack
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
# noinspection PyUnresolvedReferences
from fastapi import FastAPI


# noinspection PyUnusedLocal
async def event_handler(
    event: Event,
    session: AsyncIOMotorClientSession,
    outbox: EventOutbox,
) -> None:
    ...


# noinspection PyUnusedLocal
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with AsyncExitStack() as stack:
        mongo_client: AsyncIOMotorClient = ...
        kafka_producer: AIOKafkaProducer = ...
        kafka_consumer: AIOKafkaConsumer = ...

        event_outbox = EventOutbox(
            mongo_client,
            kafka_producer,
            kafka_consumer,
        )
        await event_outbox.create_indexes()
        await stack.enter_async_context(
            event_outbox.run_event_handler(
                lambda event, session: event_handler(
                    event,
                    session,
                    event_outbox,
                )
            )
        )
        yield


def app_factory() -> FastAPI:
    return FastAPI(lifespan=lifespan)
```

В `outbox_dependency.run_event_handler` [адаптер (adapter)](https://refactoring.guru/design-patterns/adapter) обработчика событий. Интерфейс обработчикм объявлен в библиотеке [event-outbox] как протокол `EventHandler`. Использование `lambda` позволяет передать в функцию экземпляр `outbox: EventOutbox` и другие зависимости, которые появляются на этапе инициализации приложения, а также использовать саму `lambda` как объект, реализующий протокол `EventHandler` и принимающий всего два аргумента.

В примере используется 2 типа _конечных точек_ (endpoints):
- Обработчик запроса (request handler). Разрабатывается с использованием [FastAPI].
- Обработчик события (event handler). Разрабатывается с использованием [event-outbox].

## Request handler

Обработчик запроса на [FastAPI] может выглядеть так:

```python
from typing import Annotated, Literal
from event_outbox import Event, EventListener, EventOutbox
from pydantic import BaseModel
# noinspection PyUnresolvedReferences
from fastapi import APIRouter, Depends
# noinspection PyUnresolvedReferences
import bounded_context


def get_event_outbox() -> EventOutbox:
    raise NotImplementedError


router = APIRouter()


@router.post("/")
async def request_handler(
    outbox: Annotated[EventOutbox, Depends(get_event_outbox)],
) -> BaseModel:
    role = bounded_context.Role("role-1")
    async with await outbox.mongo_client.start_session() as session:
        async with outbox.event_listener(session) as listener:
            aggregate = bounded_context.Aggregate("role-1")
            role.command(
                aggregate,
                bounded_context.Entity("entity-1"),
                bounded_context.ValueObject.name,
                BoundedContextEventListener(listener),
            )
            await outbox.mongo_db["aggregates"].insert_one(
                {
                    "role_id": aggregate.role_id,
                    "entity_id": aggregate.entity.id if aggregate.entity else None,
                    "value": aggregate.value_object,
                },
                session=session,
            )

    return BaseModel()


class BoundedContextEventListener(bounded_context.EventListener):
    def __init__(self, listener: EventListener) -> None:
        self.listener = listener

    def domain_event(self, aggregate: bounded_context.Aggregate) -> None:
        self.listener.event_occurred(
            DomainEvent(
                role_id=aggregate.role_id,
                entity_id=aggregate.entity.id if aggregate.entity else None,
                value_object=aggregate.value_object,
            )
        )


class DomainEvent(Event):
    topic: Literal["bounded-context"] = "bounded-context"
    content_schema: Literal["DomainEvent"] = "DomainEvent"
    role_id: str
    entity_id: str | None
    value_object: bounded_context.ValueObject | None
```

Далее этот код будет рассмотрен по частям.

### Transactional Outbox

```python
from typing import Literal
from event_outbox import Event, EventOutbox

class EventOccurred(Event):
    topic: Literal["bounded_context"] = "bounded_context"
    content_schema: Literal["EventOccurred"] = "EventOccurred"

async def request_handler(outbox: EventOutbox) -> None:
    async with await outbox.mongo_client.start_session() as session:
        async with outbox.event_listener(session) as listener:
            listener.event_occurred(EventOccurred())
```

Обработчик запроса самостоятельно получает сессию базы данных. 

Транзакцию открывает менеджер контекста из библиотеки [event-outbox]. Этот же менеджер контекста предоставляет синхронный интерфейс для оповещения о произошедших событиях.

Все события (domain event), которые произошли во время жизни `listener`, синхронно собираются в памяти при вызове метода `listener.event_occurred`.

При выходе из контекстного менеджера `outbox.event_listener(session)`, транзакция коммитится. Произошедшие события записываются в базу данных вместе с остальными изменениями, в одной транзакции. Библиотека [event-outbox] реализует шаблон `Transactional Outbox`.

> __GUARANTEE__:\
> Если транзакция не зафиксирована (aborted), то произошедшие события гарантированно не будут отправлены.

> __GUARANTEE__:\
> Если транзакция зафиксирована (committed), то произошедшие события гарантированно будут отправлены хотя бы 1 раз.

Такой подход позволяет при конфликте параллельных транзакций, полностью избежать обработки событий, которые, по факту, не произошли. С другой стороны, гарантирует отправку событий, которые произошли, хотя бы один раз.

### Event Listener Adapter

#### event_outbox.EventListener

```python
from pydantic import BaseModel
from contextlib import AbstractAsyncContextManager
from motor.motor_asyncio import AsyncIOMotorClientSession

class Event(BaseModel): ...

class EventListener:
    def event_occurred(self, event: Event) -> None: ...

class EventOutbox:
    def event_listener(
        self, mongo_session: AsyncIOMotorClientSession
    ) -> AbstractAsyncContextManager[EventListener]:
        ...
```

Библиотека [event-outbox] предоставляет реализацию класса `EventListener` с единственным методом `event_occurred`. Экземпляр `EventListener` создается при входе в контекстный менеджер `EventOutbox.event_listener`.

```python
from typing import Literal
from event_outbox import Event, EventOutbox
from motor.motor_asyncio import AsyncIOMotorClientSession

class EventOccurred(Event):
    topic: Literal["bounded_context"] = "bounded_context"
    content_schema: Literal["EventOccurred"] = "EventOccurred"
    
async def handler(outbox: EventOutbox) -> ...:
    session: AsyncIOMotorClientSession = ...

    async with outbox.event_listener(session) as listener:
        listener.event_occurred(EventOccurred())
```

Этот метод синхронно вызывается, когда в рамках жизни контекстного менеджера `outbox.event_listener(session)`, которым создан `listener`, происходит какое-то событие класса `Event`. 

Библиотека [event-outbox] не зависит от содержания событий, но диктует способ описания схемы данных - библиотеку [Pydantic]. Например, `EventOccurred` - это [Pydantic] модель данных, определяющие формат и содержание события.

#### bounded_context.EventListener

```python
from abc import ABC, abstractmethod

class Aggregate: ...

class EventListener(ABC):
    @abstractmethod
    def domain_event(self, aggregate: Aggregate) -> None:
        pass
```

В исходном коде ограниченного контекста удобно определить интерфейс `bounded_context.EventListener`, который знает про все события предметной области, происходящие в этом контексте.

> __NOTE__:\
> Вы можете посмотреть примеры таких интерфейсов в исходном коде контекстов (bounded context) [этого пакета](https://github.com/returnnullptr/event-outbox-example/tree/main/example/domain).

```python
from typing import Literal
from event_outbox import EventListener, Event

# noinspection PyUnresolvedReferences
import bounded_context

class EventOccurred(Event):
    topic: Literal["bounded_context"] = "bounded_context"
    content_schema: Literal["EventOccurred"] = "EventOccurred"
    role_id: str
    value: str

class BoundedContextEventListener(bounded_context.EventListener):
    def __init__(self, listener: EventListener) -> None:
        self.listener = listener

    def domain_event(self, aggregate: bounded_context.Aggregate) -> None:
        self.listener.event_occurred(
            EventOccurred(
                role_id=aggregate.role_id, 
                value=aggregate.value_object,
            )
        )
```

Между `bounded_context.EventListener` и `EventListener` из [event-outbox] можно написать небольшой [адаптер (adapter)](https://refactoring.guru/design-patterns/adapter) `BoundedContextEventListener`, чтобы транслировать события предметной области в структуры данных, которые способны сериализоваться для передачи по сети.

> __TIP__:\
> Модели данных [Pydantic], используемые для передачи событий по сети, являются частью инфраструктуры. Код предметной области не смешивается с моделями [Pydantic] и не зависит от них. Для этого используется инверсия зависимостей через интерфейс `bounded_context.EventListener`.

## Event handler

Когда событие поступает по сети, оно сохраняется в базу данных, инициализируется [оптимистическая блокировка](https://ru.wikipedia.org/wiki/%D0%91%D0%BB%D0%BE%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%BA%D0%B0_(%D0%A1%D0%A3%D0%91%D0%94)), после обрабатывается. Если в двух транзакциях произойдет обработка одного и того же события, то только одно измененеие будет зафиксировано. Библиотека [event-outbox] реализует шаблон `Idempotent Consumer` / `Transactional Inbox`.

```python
from event_outbox import Event, EventOutbox
from motor.motor_asyncio import AsyncIOMotorClientSession, AsyncIOMotorDatabase


# noinspection PyUnusedLocal
async def external_request(idempotency_key: str) -> ...:
    ...


# noinspection PyUnusedLocal
async def event_handler(
    event: Event,
    db: AsyncIOMotorDatabase,
    session: AsyncIOMotorClientSession,
    outbox: EventOutbox
) -> None:
    await external_request(event.idempotency_key)
    async with outbox.event_listener(session) as listener:
        await db["collection"].insert_one({}, session=session)


async def main() -> None:
    db_dependency: AsyncIOMotorDatabase = ...
    outbox_dependency: EventOutbox = ...

    async with outbox_dependency.run_event_handler(
        lambda event, session: event_handler(
            event,
            db_dependency,
            session,
            outbox_dependency,
        )
    ):
        ...
```

Обработчик событий работает подобно обработчику запроса. Однако, для обработчика событий библиотека [event-outbox] сама _открывает_ и _коммитит транзакцию_, в которой происходит обработка события. Это необходимо, чтобы в одну транзакцию попали:
- Оптимистическая блокировка входящего события
- Изменения в базе данных, сделанные обработчиком события
- Новые события, которые произошли во время обработки

Библиотека [event-outbox] дает следующие гарантии:

> __GUARANTEE__:\
> Если событие было обработано в рамках открытой транзакции [MongoDB], при этом обработка события не имеет наблюдаемого влияния на другие системы (side-effects), то гарантируется идемпотентность обработки.

> __GUARANTEE__:\
> Если для обработки события требуется влияние на другую систему (side-effect), то можно гарантировать идемпотентсноть, используя ключ идемпотентности (idempotency key) события.

Для отправки запросов во внешнюю систему рекомендуется использовать ключ идемпотентности `event.idempotency_key`.

[event-outbox]: https://github.com/returnnullptr/event-outbox
[FastAPI]: https://fastapi.tiangolo.com/
[Apache Kafka]: https://kafka.apache.org/
[MongoDB]: https://www.mongodb.com/
[Pydantic]: https://docs.pydantic.dev/latest/ 

## Ссылки

- [Мотивация](https://github.com/returnnullptr/event-outbox/blob/main/docs/ru-RU/motivation.md)
