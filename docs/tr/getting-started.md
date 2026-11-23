[← Belge Haritası](README.md) · [Sonraki: 02 · Senkron EventBus ve Yönlendirme →](sync-bus-and-routing.md)

# 01 · Başlangıç ve Mimari Felsefe

Bu rehber `flowevent` kurulumunu, çıplak Python callback listelerinin üretimde neden yetersiz kaldığını, temel mimari tercihleri ve ilk senkron/asenkron örnekleri anlatır.

---

## Konu Dizini

- [Kurulum](#kurulum)
- [Problem: Basit Bir Observer Listesi Neden Yetmez?](#problem-basit-bir-observer-listesi-neden-yetmez)
- [Mimari Tercihler ("Neden Böyle Tasarlandı?")](#mimari-tercihler-neden-böyle-tasarlandı)
  - [Senkron ve `asyncio` Dünyalarını Ayrı Tutmak](#senkron-ve-asyncio-dünyalarını-ayrı-tutmak)
  - [Süreç İçi (`in-process`) Sınırları](#süreç-içi-in-process-sınırları)
  - [Seçici C Çekirdeği ve Saf Python Simetrisi](#seçici-c-çekirdeği-ve-saf-python-simetrisi)
- [İlk Senkron Örnek ve `PublishResult`](#ilk-senkron-örnek-ve-publishresult)
- [İlk Asenkron Örnek](#ilk-asenkron-örnek)

---

## Kurulum

`flowevent` **Python 3.10 veya üzeri** gerektirir ve çalışma zamanı bağımlılığı yoktur. Desteklenen işletim sistemleri ve CPU mimarileri için önceden derlenmiş wheel'ler C çekirdeğini (`flowevent._core`) otomatik içerir; C derleyicisi olmayan ortamlarda ise saf Python olarak sorunsuz çalışır.

```bash
# PyPI üzerinden kurulum
python -m pip install flowevent

# Depo kökünden geliştirme kurulumu
python -m pip install -e ".[dev]"
```

[Yukarı dön](#konu-dizini)

---

## Problem: Basit Bir Observer Listesi Neden Yetmez?

Birçok projede olay dağıtımı şu masum kodla başlar:

```python
listeners = []


def publish(event):
    for fn in listeners:
        fn(event)
```

Gerçek uygulamada büyüdükçe 5 sorun arka arkaya patlar:

1. **Dağıtım sırasında abonelik değişimi:** Bir callback çalışırken kendi aboneliğini (`once`) veya başka bir aboneliği iptal ederse `RuntimeError: list changed during iteration` veya atlanan aboneler oluşur.
2. **Eşzamanlı `once` yarış durumu (race condition):** İki thread aynı anda `publish()` çağırdığında "yalnızca 1 kez çalışması gereken" abone iki kez tetiklenebilir.
3. **Yavaş veya hatalı bir abone herkesi bozar:** Bir işleyici istisna fırlattığında sonraki aboneler hiç çalışmaz ya da hangi hatanın kime ait olduğu kaybolur.
4. **Geç gelen abonenin kaçırdığı olaylar (`replay` boşluğu):** "Son 10 olayı ver, sonra canlıya geç" derken geçmişi okuma ile canlıya abone olma arasındaki milisaniyede gelen yeni olaylar ya kaçar ya da çift işlenir.
5. **Thread'den `asyncio` döngüsüne geçiş maliyeti:** Her olayda `loop.call_soon_threadsafe(asyncio.create_task, ...)` yapmak tüketici yavaşladığında bellekte on binlerce kontrolsüz `Task` biriktirir.

`flowevent`, tüm bu yarış durumlarını kilit/snapshot/tampon tasarımlarıyla çözülmüş şekilde sunar.

[Yukarı dön](#konu-dizini)

---

## Mimari Tercihler ("Neden Böyle Tasarlandı?")

### Senkron ve `asyncio` Dünyalarını Ayrı Tutmak

`flowevent` içinde `EventBus` **tamamen senkrondur**; arka planda gizli bir `asyncio` event loop veya daemon event thread açmaz. `AsyncEventBus` ise doğrudan mevcut `asyncio` döngüsü üzerinde çalışır (thread havuzuna sarılmış sahte bir asenkron sarmalayıcı değildir).

- Bu sayede senkron bir CLI/Flask/Worker uygulaması `asyncio` yükü ödemez.
- İki dünya birbirine bağlanacaksa bu geçiş açık bir sınır üzerinden (`AsyncEventQueue` veya `AsyncEventBus.publish_threadsafe`) yapılır.

### Süreç İçi (`in-process`) Sınırları

- Olaylar ağa serileştirilmez; doğrudan Python nesneleri olarak bellekte teslim edilir (`EventBus` veya `AsyncEventBus`).
- Sunucular arası iletişim, disk tabanlı kalıcılık veya yeniden başlatma sonrası mesaj kurtarma gerekiyorsa Kafka, RabbitMQ, Redis Streams veya NATS kullanılmalıdır.

### Seçici C Çekirdeği ve Saf Python Simetrisi

Tüm kütüphane önce saf Python olarak doğru semantiklerle tasarlanmıştır. Sadece yüksek frekanslı senkron `EventBus` sıcak yolu opsiyonel `flowevent._core` C uzantısıyla hızlandırılır; aynı davranışı ve aynı API'yi saf Python yolu da birebir sağlar.

[Yukarı dön](#konu-dizini)

---

## İlk Senkron Örnek ve `PublishResult`

`publish()` metodu sessizce `None` değil, teslimat özetini tutan immutable bir **`PublishResult`** döndürür; `bus.stats` ise kümülatif `EventBusStats` verir.

```python
from dataclasses import dataclass
from flowevent import EventBus


@dataclass(frozen=True)
class OrderPlaced:
    order_id: str


seen: list[str] = []

with EventBus[OrderPlaced]() as bus:
    with bus.subscribe(lambda e: seen.append(e.order_id)):
        result = bus.publish(OrderPlaced("ord-101"))

assert seen == ["ord-101"]
assert result.matched == 1
assert result.delivered == 1
assert result.failed == 0
assert result.successful is True
assert bus.stats.published == 1
```

[Yukarı dön](#konu-dizini)

---

## İlk Asenkron Örnek

```python
import asyncio
from flowevent import AsyncEventBus


async def main() -> None:
    received: list[str] = []

    async def on_event(msg: str) -> None:
        await asyncio.sleep(0)
        received.append(msg)

    async with AsyncEventBus[str]() as bus:
        sub = await bus.subscribe(on_event)
        async with sub:
            result = await bus.publish("ready")

    assert result.delivered == 1
    assert received == ["ready"]


asyncio.run(main())
```

[Yukarı dön](#konu-dizini)

---

**Önceki:** [Belge Haritası](README.md) · **Sonraki:** [02 · Senkron EventBus ve Yönlendirme →](sync-bus-and-routing.md)
