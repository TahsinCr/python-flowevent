[← Belge Haritası](README.md) · [Önceki: 05 · Performans ve C Çekirdeği](performance-and-c-core.md)

# 06 · FlowEvent API Başvuru Kartı

Bu sayfa `flowevent` (`v2.0.0`) paketinden dışa aktarılan tüm public sınıfları, metot imzalarını, veri tiplerini, enum'ları ve istisna hiyerarşisini özetler.

Kurulu paketin sürümüne `flowevent.__version__` ile erişebilirsin.

---

## Konu Dizini

- [`EventBus` (Senkron)](#eventbus-senkron)
- [`AsyncEventBus` (Asenkron)](#asynceventbus-asenkron)
- [`EventEngine` ve `AsyncEventEngine`](#eventengine-ve-asynceventengine)
- [`AsyncEventQueue` ve Gözlemcisi](#asynceventqueue-ve-gözlemcisi)
- [Geçmiş, Filtreleme, Hook ve Bağlam Türleri](#geçmiş-filtreleme-hook-ve-bağlam-türleri)
- [Abonelik, Scope ve Callback Yardımcıları](#abonelik-scope-ve-callback-yardımcıları)
- [Hata Hiyerarşisi](#hata-hiyerarşisi)

---

## `EventBus` (Senkron)

`BaseEventBus[T]` alt sınıfıdır.

### Kurucu Parametreleri

| Parametre | Tip | Varsayılan | Açıklama |
| --- | --- | --- | --- |
| `history` | `EventHistory[T] \| int \| None` | `None` | Tam sayı verilirse `MemoryEventHistory(history)` kurar. |
| `routing_key` | `Callable[[T], Hashable] \| None` | `None` | Olaylardan indeks anahtarı çıkaran fonksiyon. |
| `error_policy` | `ErrorPolicy` | `ErrorPolicy.ISOLATE` | `"isolate"` veya `"raise"`. |
| `executor` | `ThreadPoolExecutor \| None` | `None` | Callback teslimatı için opsiyonel thread havuzu. |
| `replay_buffer_limit` | `int` | `1000` | Replay sırasında birikecek maksimum canlı olay sayısı. |
| `max_schedules` | `int` | `64` | Aynı anda açık olabilecek maksimum `publish_every` görevi. |
| `shutdown_timeout` | `float` | `5.0` | `close()` sırasında zamanlanmış thread'lerin durma bütçesi (sn). |
| `actions` / `on_*` | `EventBusActions[T] \| Callable` | `None` | `on_before`, `on_after`, `on_error`, `on_timeout` (ikisinden yalnızca biri verilir). |

### Temel Üyeler

- `publish(event: T) -> PublishResult`
- `subscribe(callback, *, event_filter=None, event_type=None, predicate=None, routing_key=None, once=False, times=None, replay=0) -> Subscription`
- `once(callback, *, event_filter=None, event_type=None, predicate=None, routing_key=None, replay=0) -> Subscription`
- `wait_for(*, event_filter=None, event_type=None, predicate=None, routing_key=None, timeout=None) -> T | None`
- `publish_every(event: T, interval: float, *, times: int | None = None, immediately: bool = True) -> Subscription`
- `latest(...) -> T | None`, `query(..., limit=None) -> tuple[T, ...]`
- `clear() -> None`, `close() -> None` (`with` desteği), `closed`, `subscriber_count`, `stats -> EventBusStats`, `history`, `error_policy`

[Yukarı dön](#konu-dizini)

---

## `AsyncEventBus` (Asenkron)

`BaseEventBus[T]` alt sınıfıdır.

### Kurucu Parametreleri

| Parametre | Tip | Varsayılan | Açıklama |
| --- | --- | --- | --- |
| `history` | `EventHistory[T] \| int \| None` | `None` | İsteğe bağlı geçmiş deposu. |
| `error_policy` | `ErrorPolicy` | `ErrorPolicy.ISOLATE` | Hata davranışı. |
| `delivery_mode` | `DeliveryMode` | `DeliveryMode.SEQUENTIAL` | `"sequential"` veya `"concurrent"`. |
| `max_concurrency` | `int \| None` | `None` | `CONCURRENT` modda yayın başına maksimum eşzamanlı coroutine. |
| `replay_buffer_limit` | `int` | `1000` | Replay sırasındaki maksimum canlı olay tamponu. |
| `replay_scan_batch_size` | `int \| None` | `None` | Büyük geçmiş taramalarında event loop'a nefes aldırma aralığı. |
| `max_schedules` | `int` | `64` | Maksimum aktif `publish_every` sayısı. |
| `actions` / `on_*` | `AsyncEventBusActions[T] \| Callable` | `None` | Coroutine hook'lar. |

### Temel Üyeler

- `async publish(event: T) -> PublishResult`
- `publish_threadsafe(event: T) -> concurrent.futures.Future[PublishResult]`
- `async subscribe(callback, *, event_filter=None, event_type=None, predicate=None, once=False, times=None, replay=0) -> AsyncSubscription`
- `async once(...) -> AsyncSubscription`
- `async wait_for(..., timeout=None) -> T | None`
- `async publish_every(event: T, interval: float, *, times: int | None = None, immediately: bool = True) -> AsyncSubscription`
- `latest(...)`, `query(..., limit=None)`, `async clear()`, `async close()` (`async with` desteği), `delivery_mode`, `stats`, `subscriber_count`, `closed`

[Yukarı dön](#konu-dizini)

---

## `EventEngine` ve `AsyncEventEngine`

İsimlendirilmiş kanal kayıt defteri (`running`, `channel_names -> tuple[str, ...]`).

- `EventEngine(*, history=None, error_policy=..., executor=None, actions=None, on_before=..., on_after=..., on_error=..., on_timeout=...)`
- `AsyncEventEngine(*, history=None, error_policy=..., delivery_mode=..., max_concurrency=None, actions=None, on_before=..., on_after=..., on_error=..., on_timeout=...)`
- Ortak metotlar: `channel(name)`, `add(name, bus)`, `remove(name) -> bool`, `subscribe(channel, cb, ...)`, `once(channel, cb, ...)`, `publish(channel, event)`, `wait_for(channel, ...)`, `publish_every(channel, ...)`, `start()`, `stop()` / `close()` (`async` versiyonda `remove`, `subscribe`, `once`, `publish`, `wait_for`, `publish_every`, `start`, `stop`, `close` coroutine'dir).

[Yukarı dön](#konu-dizini)

---

## `AsyncEventQueue` ve Gözlemcisi

- `AsyncEventQueue(bus: EventBus[T], *, event_filter=None, event_type=None, predicate=None, routing_key=None, capacity=128, overflow='error', observer=None)`
- Üyeler: `async receive() -> T`, `async aclose() -> None`, `stats() -> AsyncEventQueueStats`, `closed -> bool`, `async for` ve `async with` desteği.
- `AsyncEventQueueStats`: `size`, `capacity`, `received`, `dropped`, `overflow_errors`, `closed`.
- `EventQueueObserver`: `record(name, *, status='success', dropped=0, rejected=0, queue_depth=None) -> None`.

[Yukarı dön](#konu-dizini)

---

## Geçmiş, Filtreleme, Hook ve Bağlam Türleri

| Tür | Özeti |
| --- | --- |
| `MemoryEventHistory(capacity=1_000, *, block_size=None)` | `EventHistory[T]` implementasyonu (`append`, `latest`, `query`, `clear`, `__len__`, `capacity`). |
| `EventFilter(event_type=None, predicate=None)` | Tip (`isinstance`) ve senkron `bool` yüklem filtresi (`matches(event)`). |
| `DeliveryMode` / `ErrorPolicy` | `StrEnum`: `SEQUENTIAL` / `CONCURRENT`, `ISOLATE` / `RAISE`. |
| `PublishResult` | `matched`, `delivered`, `failed`, `errors: tuple[Exception, ...]`, `successful -> bool`. |
| `EventBusStats` | `published`, `delivered`, `failed`. |
| `EventBusActions` / `AsyncEventBusActions` | `on_before`, `on_after`, `on_error`, `on_timeout`; bağlamlar: `EventErrorContext(event, error)`, `EventTimeoutContext(event_filter, timeout)`. |
| `EventEnvelope(event, id=..., correlation_id=None, source=None, timestamp=..., metadata=...)` | Salt-okunur metadata (`_ReadOnlyMetadata`), UTC zaman damgası ve otomatik korelasyon kimliği. |
| `correlation_context(correlation_id)` / `current_correlation_id()` | Görev ve thread-yerel `ContextVar` yönetimi. |

[Yukarı dön](#konu-dizini)

---

## Abonelik, Scope ve Callback Yardımcıları

| Tür | Özeti |
| --- | --- |
| `Subscription` / `AsyncSubscription` | `id`, `active`, `cancel()` (asenkron sürümde `async def`), `with` / `async with` desteği. |
| `SubscriptionScope` / `AsyncSubscriptionScope` | `add(handle)`, `close()` / `async aclose()`, `closed`; başarısız iptallerde `ExceptionGroup` + retry sahipliği. |
| `CancelHandle` / `AsyncCancelHandle` | `cancel()` protokolleri. |
| `CallbackSubscription(identifier, cancel, callback, *, asynchronous=False, once=False, max_calls=None, frequency_hz=None, timeout=None, predicate=None, enabled=True, on_before=None, on_success=None, on_error=None, on_timeout=None, on_after=None)` | Gelişmiş abonelik sarmalayıcı (`invoke`, `invoke_async`, `enable`, `disable`, `cancel`, dekoratör hook'ları) + `CallbackContext(event, result=None, error=None, elapsed=0.0)`. |

[Yukarı dön](#konu-dizini)

---

## Hata Hiyerarşisi

```text
RuntimeError
└── EventBusError
    ├── EventBusClosedError
    ├── InvalidEventHandlerError      (ayrıca TypeError)
    ├── CallbackTimeoutError          (ayrıca TimeoutError)
    ├── EventShutdownTimeoutError     (ayrıca TimeoutError)
    ├── EventQueueClosedError
    └── QueueOverflowError
```

[Yukarı dön](#konu-dizini)

---

**Önceki:** [05 · Performans ve C Çekirdeği](performance-and-c-core.md) · **Harita:** [Belge Haritası](README.md)
