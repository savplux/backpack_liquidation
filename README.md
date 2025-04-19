📖 Описание

Бот одновременно открывает две противоположные позиции (лонг и шорт) на выбранном инструменте с одинаковым депо и заданным плечом. Как только одна из позиций ликвидируется, оставшаяся позиция автоматически закрывается, а остаток средств возвращается на основной аккаунт.

⚙️ Основные возможности

Парные позиции: одновременно открывает лонг и шорт.

Автоматизация полного цикла: депозит → открытие → мониторинг → закрытие → свип средств.

Конфигурируемые задержки: случайные паузы между запросами для обхода защиты.

Ретрай и устойчивость: логика повторов при ошибках, экспоненциальный бэкофф.

Подробный лог: цветное консольное логирование и файловые логи.

🚀 Установка

Клонируйте репозиторий:

git clone https://github.com/ваш_логин/backpack-liquidation-bot.git
cd backpack-liquidation-bot

Создайте и активируйте виртуальное окружение:

python3 -m venv venv
source venv/bin/activate   # Linux/MacOS
venv\\Scripts\\activate  # Windows

Установите зависимости:

pip install -r requirements.txt

🔧 Конфигурация (config.yaml)

Пример config.yaml:

main_account:
  address: "ВАШ_MAIN_ADDRESS"

api:
  key: "ВАШ_MAIN_API_KEY"
  secret: "ВАШ_MAIN_API_SECRET"

symbol: "SOL_USDC_PERP"
initial_deposit: "10"
check_interval: 60
action_delay:
  min: 10
  max: 20
pair_start_delay_max: 40
leverage: 50

pairs:
  - short_account:
      name: "ShortBot1"
      address: "ADDRESS_SHORT_1"
      api_key: "SHORT1_KEY"
      api_secret: "SHORT1_SECRET"
    long_account:
      name: "LongBot1"
      address: "ADDRESS_LONG_1"
      api_key: "LONG1_KEY"
      api_secret: "LONG1_SECRET"

main_account.address: адрес вашего основного кошелька для сбора средств.

api.key/api.secret: ключи основного аккаунта для свипа и депозита.

symbol: точное имя перпетуального контракта.

initial_deposit: сумма в USDC на каждый суб‑аккаунт за цикл.

check_interval: интервал проверки позиций в секундах.

action_delay.min/max: диапазон задержек между запросами.

pair_start_delay_max: максимальная начальная задержка для потоков.

leverage: кредитное плечо (например, 50).

📦 Запуск

python backpack_liquidation_bot.py

При запуске бот создаст папку logs/ и начнёт работу по конфигу.

📈 Логирование

Консоль: цветные логи с помощью colorlog.

Файл: логи сохраняются в logs/backpack_liquidation_YYYYMMDD_HHMMSS.log.
