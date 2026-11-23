[← Belge Haritası](README.md) · [Önceki: 03 · Asenkron EventBus, Engine ve Köprüler](async-bus-engine-and-bridges.md) · [Sonraki: 05 · Performans ve C Çekirdeği →](performance-and-c-core.md)

# 04 · Geçmiş, Filtreleme, Scope ve Context

Bu rehber sınırlı olay geçmişini (`MemoryEventHistory` ve `replay`), filtreleri (`EventFilter`), toplu abonelik sahipliğini (`SubscriptionScope`), gelişmiş callback sarmalayıcısını (`CallbackSubscription`) ve `ContextVar` tabanlı korelasyon yayılımını anlatır.

---

## Konu Dizini

- [Olay Geçmişi (`MemoryEventHistory`) ve Bloklu Depolama](#olay-geçmişi-memoryeventhistory-ve-bloklu-depolama)
- [Replay Tamponlama: Geçmiş ve Canlı Arasında Olay Kaybetmemek](#replay-tamponlama-geçmiş-ve-canlı-arasında-olay-kaybetmemek)
- [`EventFilter` ile Tip ve Yüklem Filtreleme](#eventfilter-ile-tip-ve-yüklem-filtreleme)
- [`SubscriptionScope` ve `AsyncSubscriptionScope`](#subscriptionscope-ve-asyncsubscriptionscope)
- [`CallbackSubscription` ile Hız Sınırı, Timeout ve Hook'lar](#callbacksubscription-ile-hız-sınırı-timeout-ve-hooklar)
- [`EventEnvelope` ve `correlation_context`](#eventenvelope-ve-correlation_context)

---

## Olay Geçmişi (`MemoryEventHistory`) ve Bloklu Depolama

Bir `EventBus` veya `AsyncEventBus` kurarken `history=100` verebilir veya özelleştirilmiş `MemoryEventHistory(capacity=100_000, block_size=256)` ya da kendi `EventHistory` alt sınıfını geçebilirsin:

- **`block_size` ne işe yarar?** Geçmiş değiştikten sonra deque’in tam görüntüsü `O(N)` öğe kopyalayabilir. `block_size` ile tamamlanan bloklar paylaşılır ve filtreli yeniden oynatmada öğe kopyaları azalır. Sık ekleme yapılan iş yüklerinde varsayılan deque uygundur.
- `bus.latest(...)` en yeni eşleşmeyi verir. `bus.query(..., limit=N)` son `N` eşleşmeye kadar sonucu kronolojik sırayla döndürür. Filtreye ve depolama biçimine bağlı olarak geçmişi tarayabilir veya bir görüntüsünü alabilir.

[Yukarı dön](#konu-dizini)

---

## Replay Tamponlama: Geçmiş ve Canlı Arasında Olay Kaybetmemek

`bus.subscribe(cb, replay=5)` çağırdığında kütüphane iki aşamayı yarış durumu olmadan birleştirir:

1. Abone geçici olarak `replaying` modunda kaydedilir ve o anki geçmiş snapshot'ı çekilir.
2. `cb` geçmişteki son 5 eşleşen olayı alırken **aynı anda başka bir thread/task tarafından yayımlanan yeni olaylar** abonenin özel `replay_buffer`'ına (`replay_buffer_limit`, varsayılan `1_000`) sıralı olarak biriktirilir.
3. Doğrudan dağıtım başlamadan önce tamponlanan olaylar işlenir. Tampon dolarsa taşmaya neden olan yayın, hata politikasına göre `BufferError` bildirir ve yeniden oynatılan abonelik iptal edilir.

Not: `AsyncEventBus` tarafında çok büyük replay işlemlerinde event loop'u tek seferde kilitlememek için `replay_scan_batch_size` parametresi ile periyodik `await asyncio.sleep(0)` nefes alma noktaları bırakılabilir.

[Yukarı dön](#konu-dizini)

---

## `EventFilter` ile Tip ve Yüklem Filtreleme

Her abonelik, `latest`, `query` ve `wait_for` çağrısında iki yoldan biriyle filtre verilebilir (ikisi aynı anda verilemez):

- **Doğrudan anahtar kelimelerle:** `event_type=MyEvent` (veya `(EventA, EventB)`), `predicate=lambda e: e.priority > 5`
- **Hazır nesne ile:** `event_filter=EventFilter(event_type=..., predicate=...)`

**Mimari kural:** `predicate` mutlaka **senkron** olmalı ve tam `bool` (`True`/`False`) döndürmelidir (`async def` veya `1`/`"yes"` reddedilir; dönen coroutine varsa uyarı üretmemesi için kapatılıp `TypeError` fırlatılır). Çünkü bir yayın esnasında filtre değerlendirmesi kilit/snapshot adımında deterministik ve hızlı olmalıdır.

[Yukarı dön](#konu-dizini)

---

## `SubscriptionScope` ve `AsyncSubscriptionScope`

Bir bileşen onlarca aboneliğe veya iptal tutamacına (`cancel()` metoduna sahip herhangi bir nesne) sahipse, çıkışta hepsini tek tek takip etmek yerine `SubscriptionScope` veya `AsyncSubscriptionScope` kullanabilirsin:

```python
from flowevent import EventBus, SubscriptionScope

seen: list[int] = []
with EventBus[int]() as bus:
    with SubscriptionScope() as scope:
        scope.add(bus.subscribe(seen.append))
        scope.add(bus.subscribe(lambda x: seen.append(x * 10)))
        bus.publish(2)
    bus.publish(3)  # Scope kapanınca her iki abonelik ters sırada iptal edildi

assert seen == [2, 20]
```

- Kayıt sırasının **tersine (LIFO)** temizler.
- Bir tutamacın `cancel()` çağrısı başarısız olsa bile **kalanların tamamı iptal edilmeye çalışılır**; başarısız olanlar scope içinde tutulur ve yeniden `close()` / `aclose()` denendiğinde yalnızca onlar tekrar denenir (`ExceptionGroup` ile raporlanır).
- Kapanmış bir scope'a yeni tutamaç eklemek `RuntimeError` verir.

[Yukarı dön](#konu-dizini)

---

## `CallbackSubscription` ile Hız Sınırı, Timeout ve Hook'lar

Frekans sınırını, zaman aşımını ve hook’ları uygulamak için `CallbackSubscription.invoke` ya da `await CallbackSubscription.invoke_async` kullan. Sarmalayıcıyı doğrudan çağırmak özgün callback’i çalıştırır ve bu politikaları uygulamaz. Senkron zaman aşımı callback döndükten sonra bildirilir. Asenkron iptal için callback’in işbirliği gerekir.

- `frequency_hz`: Belirtilen frekanstan daha sık gelen olayları sessizce atlar (`throttle`).
- `timeout`: Süre bütçesi takibi yapar; senkron veya async çağrının süresini aşması durumunda `on_timeout` hook'unu tetikler (`CallbackTimeoutError` bütçe kontrolü; Python'daki bloklayan C/senkron kodları zorla kesmek için arka planda yetim thread bırakmaz).
- `enable()` / `disable()`: Aboneliği bus'tan sökmeden geçici olarak susturur.

[Yukarı dön](#konu-dizini)

---

## `EventEnvelope` ve `correlation_context`

Olay nesnelerini değiştirmeden izleme metadatası taşımak istersen **`EventEnvelope`** kullanabilirsin:

- `correlation_context("req-991")` bloğu içinde oluşturulan `EventEnvelope(payload)` otomatik olarak aktif `correlation_id`'yi (veya verilmemişse kendi `id` değerini) alır.
- `metadata` sözlüğü sığ olarak kopyalanır ve salt-okunur (`_ReadOnlyMetadata`) tutulur; yine de `copy.deepcopy` ve `pickle` destekler.
- `timestamp` her zaman UTC zaman damgasına çevrilir (timezone bilgisi olmayan naive `datetime` `ValueError` ile reddedilir).
- `EventBus(..., executor=...)` kullanıldığında aktif `ContextVar` bağlamı işçi thread'e kopyalanır; `asyncio` tarafında `ContextVar` zaten görevler arası otomatik izole taşınır.

```python
from flowevent import EventEnvelope, correlation_context, current_correlation_id

with correlation_context("trace-42"):
    assert current_correlation_id() == "trace-42"
    envelope = EventEnvelope({"ok": True}, source="api-gateway", metadata={"v": 1})

assert envelope.correlation_id == "trace-42"
assert envelope.metadata["v"] == 1
```

[Yukarı dön](#konu-dizini)

---

**Önceki:** [03 · Asenkron EventBus, Engine ve Köprüler](async-bus-engine-and-bridges.md) · **Sonraki:** [05 · Performans ve C Çekirdeği →](performance-and-c-core.md)
