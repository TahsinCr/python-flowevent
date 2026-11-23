[← Belge Haritası](README.md) · [Önceki: 04 · Geçmiş, Filtreleme ve Scope](history-filtering-scopes-and-context.md) · [Sonraki: 06 · API Başvuru Kartı →](api-reference.md)

# 05 · Performans, C Çekirdeği ve Free-Threaded Python

Bu rehber `flowevent._core` C uzantısının neden ve nereye eklendiğini, saf Python ile davranış özdeşliğini, ortam değişkenlerini ve free-threaded CPython uyumluluğunu açıklar.

---

## Konu Dizini

- [Neden Tüm Kütüphane Değil de Yalnızca Sıcak Yollar C'de?](#neden-tüm-kütüphane-değil-de-yalnızca-sıcak-yollar-cde)
- [Derleme ve Çalışma Zamanı Kontrolleri](#derleme-ve-çalışma-zamanı-kontrolleri)
- [Free-Threaded Python (`3.13t–3.15t`) ve GIL Koruması](#free-threaded-python-313t315t-ve-gil-koruması)
- [Benchmark Suite (`pyperf`) ile Yerel Ölçüm](#benchmark-suite-pyperf-ile-yerel-ölçüm)

---

## Neden Tüm Kütüphane Değil de Yalnızca Sıcak Yollar C'de?

`flowevent._core` (`flowevent/_core.c`), yüksek frekanslı **senkron `EventBus` dağıtım** yolunu (`BusCore`, `Subscriber`, atomik `Flag`) hızlandırır:

- Küçük ve hızlı callback'lerin çok sayıda veya yüksek sıklıkta tetiklendiği durumlarda Python nesne tahsisi, kilit/bayrak kontrolü ve yönlendirme snapshot aramalarındaki yorumlayıcı yükünü ciddi ölçüde düşürür.
- `AsyncEventBus`, `AsyncEventQueue`, `EventEngine`, `SubscriptionScope` ve `MemoryEventHistory` gibi koordinasyon veya Python nesne/coroutine ağırlıklı bileşenler bakım kolaylığı ve geliştirici dostluğu için saf Python olarak tutulur.
- `EventBus` sınıfının `_subscriber_snapshot` veya `subscribe` gibi metodları kullanıcı tarafından alt sınıfta override edilmişse, davranış tutarlılığını korumak için C hızlı-snapshot yolu otomatik devreden çıkar.

[Yukarı dön](#konu-dizini)

---

## Derleme ve Çalışma Zamanı Kontrolleri

`flowevent` iki katmanlı emniyetle dağıtılır:

1. **Kurulum zamanı (`setup.py`):** `flowevent._core` varsayılan olarak `optional=True` ile derlenir; C derleyicisi olmayan ortamlarda paket kırılmaz, saf Python olarak kurulur.
2. **Çalışma zamanı (`flowevent._native`):** Uzantı yüklendiyse `CORE` modülü bağlanır; yoksa sessizce saf Python uygulamasına düşer.

| Ortam Değişkeni | Aşama | Etkisi |
| --- | --- | --- |
| `EVENTFLOW_NO_EXTENSIONS=1` | Çalışma zamanı | C uzantısı derlenmiş olsa bile yüklenmez; saf Python yolunu kullanmaya zorlar. |
| `EVENTFLOW_PURE_PYTHON=1` | Paket derleme / kurulum | `setup.py` C uzantısını hiç derlemez (`py3-none-any` wheel üretimi vb.). |
| `EVENTFLOW_REQUIRE_NATIVE=1` | Paket derleme / kurulum | `optional=False` yapar; C derleyicisi yoksa kurulumu hata ile durdurur. |

Geliştirme ve test sırasında uzantıyı zorunlu kılmak için:

```bash
python run_tests.py --native
```

[Yukarı dön](#konu-dizini)

---

## Free-Threaded Python (`3.13t–3.15t`) ve GIL Koruması

`flowevent._core` çok aşamalı başlatma (`Py_mod_gil = Py_MOD_GIL_NOT_USED`) ve platforma özgü atomik bellek işlemleri (Windows için `<intrin.h>`, GCC/Clang için atomikler) kullanır:

- `flowevent._native._load_core()`, free-threaded (`PYTHON_GIL=0`) yorumlayıcıda `_core` yüklenmeden önce ve yüklendikten sonra `sys._is_gil_enabled()` değerini kontrol eder.
- Eğer yanlış derlenmiş bir uzantı ithali çalışma zamanında GIL'i sessizce yeniden açarsa `RuntimeError("importing flowevent._core enabled the free-threaded GIL")` fırlatarak **fail-closed** durur.

[Yukarı dön](#konu-dizini)

---

## Benchmark Suite (`pyperf`) ile Yerel Ölçüm

Depodaki tüm benchmark'lar `pyperf` motorunu ve kaynak parmak izini kullanır. C uzantılı ve saf Python modlarını aynı makinede karşılaştırmak için:

```bash
python -m pip install -e ".[dev]"
EVENTFLOW_NO_EXTENSIONS=1 python -m benchmarks.benchmark_publish --fast --output publish-python.json
python -m benchmarks.benchmark_publish --fast --output publish-core.json
python -m pyperf compare_to publish-python.json publish-core.json --table
```

[Yukarı dön](#konu-dizini)

---

**Önceki:** [04 · Geçmiş, Filtreleme, Scope ve Context](history-filtering-scopes-and-context.md) · **Sonraki:** [06 · API Başvuru Kartı →](api-reference.md)
