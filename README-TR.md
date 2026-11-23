[![CI durumu][ci-shield]][ci]
[![PyPI sürümü][pypi-shield]][pypi]
[![Python 3.10+][python-shield]][python]
[![MIT Lisansı][license-shield]][license]
[English][readme-en] | **Türkçe**

# FlowEvent

[Repository][repository] · [Changelog][changelog] · [Latest release][releases] · [MIT License][license]

Python uygulamaları ve kütüphaneleri için süreç içi olay dağıtımı. Tipli olayları senkron veya asenkron bir bus üzerinden yayımlayıp abonelikleri yönetebilirsiniz.

FlowEvent, [Vehicle Autonomy Core][vehicle-autonomy-core] projesinin olay dağıtım bileşeninden doğdu.

## İçindekiler

- [Başlangıç](#getting-started)
- [Temel kavramlar](#core-concepts)
- [Olaylarla çalışma](#working-with-events)
- [Asenkron uygulamalar](#async-applications)
- [Performans ve C uzantısı](#performance)
- [Geliştirme ve lisans](#development)

<a id="getting-started"></a>

## Başlangıç

### Kurulum

Proje kökünde Python 3.10 veya üzeriyle kurun.

~~~sh
python -m pip install flowevent
~~~

### İlk olayınızı yayımlayın

Yayıncı sıradan bir Python nesnesini bus’a verir. Senkron yayın dönmeden önce abone olayı alır. İçteki context manager’dan çıkınca abonelik iptal edilir.

~~~python
from dataclasses import dataclass

from flowevent import EventBus


@dataclass(frozen=True)
class OrderPlaced:
    order_id: str


def send_confirmation(event: OrderPlaced) -> None:
    print(f"Order placed: {event.order_id}")


with EventBus[OrderPlaced]() as bus:
    with bus.subscribe(send_confirmation):
        result = bus.publish(OrderPlaced("order-42"))
        assert result.delivered == 1
~~~

<a id="core-concepts"></a>

## Temel kavramlar

Senkron işleyiciler için `EventBus`, coroutine işleyicileri için `AsyncEventBus` kullanın. Olay grupları farklı bileşenlere aitse isimlendirilmiş kanallar açabilirsiniz. `AsyncEventQueue`, senkron yayıncıyı asenkron tüketiciye bağlar. Olaylar aynı süreç içinde teslim edilir. Tip açıklamaları beklenen olay tipini editöre ve tip denetleyicisine bildirir. Çalışma zamanında olayları seçmek için filtre kullanın.

<a id="working-with-events"></a>

## Olaylarla çalışma

### Olayları filtreleyin ve geçmişten alın

Bus’a sınırlı bir geçmiş ekleyip yönlendirme anahtarıyla abone olun. Örnekte aynı abonelik önce eşleşen son olayı geçmişten alır, ardından yeni eşleşmeleri alır.

~~~python
from flowevent import EventBus

received = []
with EventBus[tuple[str, str]](history=3, routing_key=lambda event: event[0]) as bus:
    bus.publish(("orders", "created"))
    bus.publish(("billing", "invoice_sent"))
    with bus.subscribe(received.append, routing_key="orders", replay=1):
        bus.publish(("orders", "packed"))

assert received == [("orders", "created"), ("orders", "packed")]
~~~

<a id="async-applications"></a>

## Asenkron uygulamalar

### Asenkron işleyicilere olay yayımlayın

İşleyiciler asenkron çalışıyorsa `AsyncEventBus` kullanın. Bus’ı ve aboneliklerini aynı asyncio event loop’unda kullanın.

`DeliveryMode.CONCURRENT` seçtiğinizde her yayının aynı anda kaç işleyici çalıştıracağını `max_concurrency` ile sınırlayın. Bu sınır verilmezse bir yayın, eşleşen her işleyici için ayrı bir görev oluşturabilir.

~~~python
import asyncio

from flowevent import AsyncEventBus


async def main() -> None:
    received = []

    async def collect(event: str) -> None:
        received.append(event)

    async with AsyncEventBus[str]() as bus:
        subscription = await bus.subscribe(collect)
        async with subscription:
            await bus.publish("order.shipped")

    assert received == ["order.shipped"]


asyncio.run(main())
~~~

<a id="performance"></a>

## Performans ve C uzantısı

İsteğe bağlı C çekirdeği, desteklenen senkron `EventBus` yollarında dağıtım yükünü azaltır. Bu özellikle sık yayın yapıldığında veya kısa işleyicilerin bağlı olduğu çok sayıda aboneye olay yönlendirildiğinde önem kazanır. Saf Python uygulaması aynı API’yi sunar. Yayımlanan sürümlerin senaryoları, ortamları ve ölçümleri [benchmark arşivinde][benchmarks] yer alır.

<a id="development"></a>

## Geliştirme ve lisans

~~~sh
python -m pip install -e ".[dev]"
python run_tests.py
~~~

C uzantısını yeniden derlemek ve testlerde zorunlu tutmak için `python run_tests.py --native` çalıştırın.

Proje [MIT Lisansı][license] ile yayımlanır.

## Diğer projeler

[FlowCoord][repo-flowcoord] · [FlowPipeline][repo-flowpipeline]

[ci-shield]: https://img.shields.io/github/actions/workflow/status/TahsinCr/python-flowevent/ci.yml?branch=main&event=push&label=CI&style=for-the-badge&logo=github&logoColor=white
[pypi-shield]: https://img.shields.io/pypi/v/flowevent?label=PyPI&style=for-the-badge&logo=pypi&logoColor=white
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge
[ci]: https://github.com/TahsinCr/python-flowevent/actions/workflows/ci.yml?query=branch%3Amain+event%3Apush
[pypi]: https://pypi.org/project/flowevent/
[python]: https://www.python.org/downloads/
[readme-en]: https://github.com/TahsinCr/python-flowevent/blob/main/README.md
[repository]: https://github.com/TahsinCr/python-flowevent
[changelog]: https://github.com/TahsinCr/python-flowevent/blob/main/CHANGELOG.md
[license]: https://github.com/TahsinCr/python-flowevent/blob/main/LICENSE
[repo-flowcoord]: https://github.com/TahsinCr/python-flowcoord
[repo-flowpipeline]: https://github.com/TahsinCr/python-flowpipeline
[releases]: https://github.com/TahsinCr/python-flowevent/releases/latest
[benchmarks]: https://github.com/TahsinCr/python-flowevent/blob/main/benchmarks/results/README.md
[vehicle-autonomy-core]: https://github.com/TahsinCr/vehicle-autonomy-core
