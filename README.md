# QMemo News Radar

Самостоятельный сервис: каждые 30 минут читает свежие публикации в X через официальный API, убирает шум и дубли, оценивает маркетинговые поводы для QMemo через LLM и присылает лучшие карточки в Telegram. По кнопке «Использовать» готовит точную цитату и тексты, после одобрения сохраняет один `PublicationPackage` в `publication_outbox`.

Radar изолирован от Brain, Dulty и сайта Quote Memorial: свой бот, своя SQLite, свои зависимости и контейнер.

**Публикация в Quote Memorial и X не реализована и выключена.** Одобренный пакет только ждёт в outbox.

**Внешние расходы: цель $0 в месяц, жёсткий предел $10.** X и LLM — платные функции и по умолчанию выключены; без флагов ни один платный API не вызывается. Бесплатные источники GDELT Global Quotation Graph и RSS/Atom работают без ключей и без бюджета; подробности и замеры потока — [docs/MULTI_SOURCE.md](docs/MULTI_SOURCE.md).

## Что готово

| Этап | Что делает |
| --- | --- |
| X read-only | recent search по аккаунтам и запросам, lookup ручных ссылок, `since_id`, пагинация, повторы и 429 |
| Фильтрация | возраст до 60 минут, блок-лист авторов и слов, пустые публикации, дубли по ID, URL и тексту |
| LLM-ранжирование | пакеты до 10 событий, структурированный JSON, одна попытка исправления, итог 0-100 считает код |
| Telegram | подборки до 5 карточек 2-3 раза в день, срочные карточки от 80, не больше 10 карточек в сутки |
| Черновик | точная цитата из источника, автор из источника, тексты для QMemo и X, одна переделка, ручная проверка фактов |
| Outbox | атомарное одобрение, один пакет на публикацию, без внешних запросов |
| Runtime | планировщик, `/run`, `/status`, пауза, JSON-журналы, healthcheck, корректная остановка |
| Multi-source основа | реестр источников, пакетная загрузка, метрики по источникам, отчёт retention |
| Бесплатные источники | GDELT Global Quotation Graph (цитата = объект, курсор по минутам, пропуски 404) и RSS/Atom (ETag/Last-Modified, защита от частных адресов), команда `sample` |
| Бюджет | `BudgetGuard` и `cost_ledger`: платный вызов блокируется до запроса при выключенном флаге, неизвестной цене или месячном пределе |
| Отбор (M4) | фильтр до записи, точные копии как упоминания, near dedup и истории (MinHash-LSH), объяснимая преселекция по числу независимых статей, одна цитата на статью |
| Бесплатное ранжирование | без платного LLM истории оцениваются кодом и попадают в Telegram за $0; LLM — необязательное улучшение |
| Аудит и хранение | `funnel`, `clusters`, `cluster <id>`, `rejected`, `gaps`; `prune` — только отчёт, удаление с `--apply` |

Путь `X → фильтр → ранжирование → SQLite → Telegram → черновик → одобрение → outbox` проверен сквозными тестами с поддельными X, LLM и Telegram и офлайн-командой `dry-run`. С настоящими ключами X, LLM и Telegram сервис в этом репозитории не запускался — это первый шаг пилота.

## Первый запуск в Docker

Нужны Docker с Compose v2 и четыре внешних доступа.

1. **Telegram-бот.** В [@BotFather](https://t.me/BotFather) выполните `/newbot` и сохраните токен. Это должен быть новый бот только для Radar.
2. **Ваш Telegram ID.** Узнайте числовой ID, например, у [@userinfobot](https://t.me/userinfobot). Если ID указан неверно, бот ответит «Нет доступа» и покажет ID, с которого пришло сообщение.
3. **X API (платно, необязательно).** В [console.x.com](https://console.x.com) создайте приложение, пополните кредиты (оплата за каждое прочитанное сообщение) и скопируйте **Bearer Token**. Нужны только операции чтения. Включается флагами `RADAR_PAID_SOURCES_ENABLED=true` и `RADAR_X_PAID_SEARCH_ENABLED=true`.
4. **LLM (платно, необязательно).** Любой OpenAI-совместимый endpoint: base URL, ключ, название модели и оценка стоимости одного запроса. Включается `RADAR_PAID_LLM_ENABLED=true`. Без LLM события собираются и фильтруются, но не оцениваются, поэтому карточек нет.

Затем в корне репозитория:

```bash
cp .env.example .env
cp sources.example.yaml sources.yaml
```

Заполните в `.env` обязательные переменные, в `sources.yaml` — аккаунты и запросы. Проверьте конфигурацию без сетевых запросов:

```bash
docker compose run --rm radar qmemo-radar check-config
```

Проверьте весь путь офлайн на отдельной базе (боевая база не затрагивается):

```bash
docker compose run --rm radar qmemo-radar dry-run --db /app/data/dry-run.db
```

Запустите сервис:

```bash
docker compose up -d --build
```

Откройте бота в Telegram, отправьте `/status`, затем `/run` для первого сбора.

## Переменные окружения

Обязательные для `qmemo-radar run`:

| Переменная | Назначение |
| --- | --- |
| `RADAR_TELEGRAM_BOT_TOKEN` | токен отдельного бота Radar |
| `RADAR_ALLOWED_TELEGRAM_ID` | единственный Telegram ID, которому бот отвечает |

Расходы и платные функции:

| Переменная | По умолчанию | Назначение |
| --- | --- | --- |
| `RADAR_COST_TARGET_USD_MONTHLY` | `0` | цель; превышение пишет предупреждение в журнал |
| `RADAR_COST_HARD_LIMIT_USD_MONTHLY` | `10` | предел, не больше 10; платный вызов сверх него не отправляется |
| `RADAR_PAID_SOURCES_ENABLED` | `false` | платные источники и lookup ручных ссылок X |
| `RADAR_X_PAID_SEARCH_ENABLED` | `false` | X recent search (вместе с предыдущим) |
| `RADAR_PAID_LLM_ENABLED` | `false` | LLM-ранжирование и черновики |

Нужны только при включённых платных функциях:

| Переменная | Назначение |
| --- | --- |
| `RADAR_X_BEARER_TOKEN` | Bearer Token приложения X (только чтение) |
| `RADAR_LLM_BASE_URL` | OpenAI-совместимый base URL, например `https://api.openai.com/v1` или `https://api.anthropic.com/v1/` |
| `RADAR_LLM_API_KEY` | ключ LLM-провайдера |
| `RADAR_LLM_MODEL` | модель, например `claude-sonnet-5` |
| `RADAR_LLM_COST_PER_CALL_USD` | верхняя оценка стоимости одного запроса; без неё платный LLM-вызов блокируется |

Бесплатные источники:

| Переменная | По умолчанию | Назначение |
| --- | --- | --- |
| `RADAR_GDELT_ENABLED` | `false` | читать GDELT Global Quotation Graph |
| `RADAR_GDELT_SAFETY_LAG_MINUTES` | `10` | минуты новее «сейчас минус задержка» не запрашиваются |
| `RADAR_GDELT_MAX_MINUTES_PER_RUN` | `60` | минутных файлов за сбор; ограничивает догон после простоя |
| `RADAR_GDELT_LANGUAGES` | `English` | языки через запятую, `*` — все |
| `RADAR_GDELT_ALLOW_UNKNOWN_LANGUAGE` | `false` | принимать статьи без языка |
| `RADAR_RSS_MAX_RESPONSE_BYTES` | `5000000` | предел ответа одной ленты, не больше 5 МБ |

Необязательные:

| Переменная | По умолчанию | Назначение |
| --- | --- | --- |
| `RADAR_TIMEZONE` | `Asia/Ho_Chi_Minh` | часовой пояс подборок и отчётов |
| `RADAR_DIGEST_TIMES` | `10:00,15:00,20:00` | два или три времени подборок |
| `RADAR_COLLECT_INTERVAL_MINUTES` | `30` | интервал сбора |
| `RADAR_MAX_EVENT_AGE_MINUTES` | `60` | максимальный возраст публикации (кроме ручных ссылок) |
| `RADAR_DIGEST_CARD_LIMIT` | `5` | карточек в одной подборке |
| `RADAR_DAILY_CARD_LIMIT` | `10` | карточек в сутки, включая срочные |
| `RADAR_URGENT_THRESHOLD` | `80` | порог срочной карточки |
| `RADAR_DIGEST_THRESHOLD` | `65` | порог попадания в подборку |
| `RADAR_ARCHIVE_THRESHOLD` | `50` | нижняя граница, должна быть не выше порога подборки |
| `RADAR_EVENT_TTL_HOURS` | `48` | через сколько часов нерешённое событие устаревает |
| `RADAR_RAW_RETENTION_DAYS` | `14` | через сколько дней шум считается удаляемым (пока только отчёт в `status`) |
| `RADAR_LLM_TEMPERATURE` | `0` | температура модели |
| `RADAR_LOG_LEVEL` | `INFO` | уровень журналов |
| `RADAR_DB_PATH` | `data/radar.db` | путь к SQLite (в Docker `/app/data/radar.db`) |
| `RADAR_SOURCES_PATH` | `sources.yaml` | путь к источникам (в Docker `/app/sources.yaml`) |
| `RADAR_QMEMO_PUBLISHING_ENABLED` | `false` | должна оставаться `false`, иначе `run` не стартует |
| `RADAR_X_PUBLISHING_ENABLED` | `false` | должна оставаться `false`, иначе `run` не стартует |

Секретов публикации нет и не требуется.

## sources.yaml

```yaml
x:
  accounts:
    - handle: example_founder
      enabled: true
  queries:
    - name: crypto_predictions
      query: '(crypto OR Solana) (predicts OR says OR promises) lang:en -is:retweet'
      enabled: true
  max_pages_per_query: 1
blocked_authors: []
blocked_terms: [giveaway, airdrop]
```

```yaml
rss:
  feeds:
    - name: bbc_world
      url: https://feeds.bbci.co.uk/news/world/rss.xml
      enabled: true
```

Каждая лента — источник `rss:<name>` со своим checkpoint; ответ `304` — успешный сбор без новых записей. Каждый аккаунт и каждый запрос X — отдельный источник со своим checkpoint (`account:<handle>`, `query:<name>`). Первый сбор читает последние 60 минут, дальше только новые публикации через `since_id`. Ошибка одного источника не останавливает остальные и видна в `/status`.

Отбор (`selection`) тоже задаётся в `sources.yaml`: пороги фильтра до записи, заблокированные домены и слова, темы, пороги склейки историй и преселекции. Пример со всеми полями и значениями по умолчанию — в `sources.example.yaml`. Без секции работают значения по умолчанию.

```yaml
selection:
  gate: {min_words: 5, blocked_domains: [spam.example]}
  topics:
    - {name: ai, terms: [artificial intelligence, openai], weight: 5}
  preselection: {min_score: 55}
```

После изменения файла: `docker compose restart radar`.

## Telegram

Команды:

| Команда | Что делает |
| --- | --- |
| `/status` | работает ли Radar, пауза, последний сбор, состояние X и LLM, счётчики за сегодня, outbox |
| `/run` | сбор сейчас через тот же конвейер и ту же блокировку, что и расписание |
| `/today` | карточки за сегодня, очередь и остаток дневного лимита |
| `/saved` | отложенные события и одобренные пакеты в outbox |
| `/pause`, `/resume` | остановить и продолжить автоматический сбор и отправку; `/run` работает и на паузе |

Карточка: заголовок, пересказ, автор, время, ссылка, балл, связь с QMemo, формат, целевое действие, риск. Кнопки: **Использовать**, **Пропустить**, **Позже** (вернётся в следующей подборке), **Почему такой балл?**.

Черновик: цитата, автор, язык, контекст, текст для QMemo, основной и короткий текст для X с `{qmemo_url}`, угол, призыв к действию, статус проверки фактов. Кнопки: **Принято**, **Переделать короче**, **Другой угол**, **Отказаться**, **Проверено вручную** (если есть непроверенные факты). Пока открыт первый черновик, обычное сообщение считается инструкцией для единственной переделки.

Ручная ссылка: отправьте боту `https://x.com/<handle>/status/<id>`. Публикация загрузится через API, будет оценена при следующем сборе (или сразу после `/run`) и придёт карточкой независимо от возраста и порога (в пределах дневного лимита). Это работает и для публикации, которую Radar уже собрал и отправил в архив. Текст, похожий на ссылку, никогда не считается инструкцией для переделки черновика.

## Команды и диагностика

| Команда | Что делает |
| --- | --- |
| `qmemo-radar run` | боевой режим: миграции, Telegram poller, планировщик |
| `qmemo-radar check-config` | проверка переменных и `sources.yaml` без сети, код 78 при ошибке |
| `qmemo-radar dry-run --db <path>` | офлайн-прогон полного пути до outbox |
| `qmemo-radar status` | события, outbox, поток по источникам за 24 часа, расходы за месяц, retention — в JSON |
| `qmemo-radar healthcheck` | код 0, если планировщик отмечался последние 3 минуты |
| `qmemo-radar init-db` | создать базу и применить миграции |
| `qmemo-radar sample --source gdelt\|rss\|rss:<name> [--limit 20] [--random]` | последние (или случайные) сохранённые объекты источника в JSON; только чтение, без сети |
| `qmemo-radar funnel [--hours 24]` | воронка: получено → отброшено по причинам → копии → истории → преселекция → Telegram; только чтение |
| `qmemo-radar clusters [--state preselected] [--order score\|mentions\|recent] [--limit 20]` | истории с числом статей, сайтов и оценкой |
| `qmemo-radar cluster <id>` | одна история: варианты текста, где встречалась, почему прошла или нет |
| `qmemo-radar rejected [--reason too_few_words] [--limit 20]` | случайные примеры отброшенного фильтром до записи |
| `qmemo-radar gaps [--skip <минута>\|all]` | заблокированные минуты GDELT; `--skip` отказывается от них с записью |
| `qmemo-radar prune [--apply]` | что можно удалить по retention; без `--apply` ничего не удаляет |

```bash
docker compose ps                                  # статус и health контейнера
docker compose logs -f radar                       # JSON-журналы
docker compose exec radar qmemo-radar status       # события и outbox
docker compose exec radar qmemo-radar healthcheck  # проверка планировщика
docker compose restart radar                       # после изменения .env или sources.yaml
```

Журналы — одна JSON-строка на запись с полями `run_id`, `event_id`, `module`, `operation`, `result`, `error_code`. Ключи, токены, полный текст публикаций и промпты в журналы не пишутся.

Контейнер работает не от root, хранит базу в томе `radar_data`, применяет миграции до старта и останавливается по SIGTERM. Если обязательная переменная не задана, `run` завершается с кодом 78 и списком проблем в журнале.

## Локальная разработка

Требуется Python 3.12+.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,telegram]"
make check
qmemo-radar dry-run --db data/dry-run.db
```

`make check` запускает `ruff check .`, `mypy src` и `pytest`. Тесты не ходят в сеть: сетевые соединения в них заблокированы. Проверка реальной модели на fixtures — по желанию:

```bash
RADAR_LIVE_LLM_EVAL=1 pytest -m live
```

Нагрузочный тест на 200 000 объектов входит в обычный `pytest`; на 1 000 000 — по желанию:

```bash
RADAR_BENCHMARK_1M=1 pytest -s tests/test_throughput.py
```

## Документы

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — устройство и контракты.
- [docs/MULTI_SOURCE.md](docs/MULTI_SOURCE.md) — открытые источники, бюджет, метрики, retention, план M3.
- [docs/PILOT.md](docs/PILOT.md) — недельный пилот.
- [SECURITY.md](SECURITY.md) — секреты и защита от публикации.
