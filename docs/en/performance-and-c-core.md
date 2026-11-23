[← Documentation Map](README.md) · [Previous: 04 · History, Filtering, Scopes, and Context](history-filtering-scopes-and-context.md) · [Next: 06 · API Reference →](api-reference.md)

# 05 · Performance, C Core, and Free-Threaded Python

This guide explains what `flowevent._core` accelerates, how pure-Python parity is preserved, environment toggles, and free-threaded CPython safeguards.

---

## Table of Contents

- [Why Accelerate Only the Hot Path in C?](#why-accelerate-only-the-hot-path-in-c)
- [Build and Runtime Controls](#build-and-runtime-controls)
- [Free-Threaded Python (`3.13t–3.15t`) and Fail-Closed GIL Checks](#free-threaded-python-313t315t-and-fail-closed-gil-checks)
- [Measuring Locally with `pyperf`](#measuring-locally-with-pyperf)

---

## Why Accelerate Only the Hot Path in C?

`flowevent._core` (`flowevent/_core.c`) optimizes the synchronous **`EventBus` publish and routing path** (`BusCore`, `Subscriber`, atomic `Flag`):

- Under high-frequency publishing or large routed subscriber sets with small handlers, moving object checks, lock flags, and route snapshot dispatch into C cuts interpreter overhead dramatically.
- Orchestration-heavy paths (`AsyncEventBus`, `AsyncEventQueue`, `EventEngine`, `SubscriptionScope`, `MemoryEventHistory`) remain clean pure Python for readability and contributor ergonomics.
- If a subclass overrides `EventBus._subscriber_snapshot` or `EventBus.subscribe`, the C fast-snapshot path steps aside automatically so subclass semantics always win.

[Back to top](#table-of-contents)

---

## Build and Runtime Controls

`flowevent` ships with two layers of portability:

1. **Build time (`setup.py`):** `flowevent._core` compiles with `optional=True` by default, so source installs on machines without a C compiler succeed cleanly as pure Python.
2. **Import time (`flowevent._native`):** Uses `CORE` when available and falls back transparently to pure Python if missing or disabled.

| Environment Variable | Stage | Effect |
| --- | --- | --- |
| `EVENTFLOW_NO_EXTENSIONS=1` | Runtime | Forces pure-Python execution even when `_core` is installed. |
| `EVENTFLOW_PURE_PYTHON=1` | Build / install | Skips compiling `_core` (used for `py3-none-any` wheels). |
| `EVENTFLOW_REQUIRE_NATIVE=1` | Build / install | Sets `optional=False`; fails build if `_core` cannot compile. |

During local development, require and test the compiled extension with:

```bash
python run_tests.py --native
```

[Back to top](#table-of-contents)

---

## Free-Threaded Python (`3.13t–3.15t`) and Fail-Closed GIL Checks

`flowevent._core` declares `Py_MOD_GIL_NOT_USED` via multi-phase module initialization and uses platform-native atomics (MSVC `<intrin.h>` on Windows AMD64/ARM64, GCC/Clang atomics elsewhere):

- On free-threaded interpreters (`PYTHON_GIL=0`), `flowevent._native._load_core()` checks `sys._is_gil_enabled()` before and after importing `_core`.
- If importing `_core` ever re-enables the GIL unexpectedly, it fails closed with `RuntimeError("importing flowevent._core enabled the free-threaded GIL")`.

[Back to top](#table-of-contents)

---

## Measuring Locally with `pyperf`

All repository benchmarks run via `pyperf` with source fingerprinting. Compare pure Python vs compiled `_core` side-by-side:

```bash
python -m pip install -e ".[dev]"
EVENTFLOW_NO_EXTENSIONS=1 python -m benchmarks.benchmark_publish --fast --output publish-python.json
python -m benchmarks.benchmark_publish --fast --output publish-core.json
python -m pyperf compare_to publish-python.json publish-core.json --table
```

[Back to top](#table-of-contents)

---

**Previous:** [04 · History, Filtering, Scopes, and Context](history-filtering-scopes-and-context.md) · **Next:** [06 · API Reference →](api-reference.md)
