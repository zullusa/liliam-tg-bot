#!/usr/bin/env bash
# Гейт покрытия claudetg: все тесты (юнит-сквозные + проверка CLI на настоящем
# бинарнике) и сводка покрытия с порогом. Пороги поднимают по мере появления
# тестов; опускают только вместе с причиной в комментарии.
#
#   ./scripts/coverage.sh          # тесты + сводка, порог по строкам
#   MIN_LINES=98 ./scripts/coverage.sh
set -euo pipefail
cd "$(dirname "$0")/.."

export PATH="$HOME/.cargo/bin:$HOME/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin:$PATH"

# Пороги сняты по фактическому прогону: 98,88 % строк целиком, худший файл —
# 91,20 % (opencode.rs: там же тело живого теста под `#[ignore]`, его запускать
# в CI нельзя). В производственном коде непокрыто 6 строк, все три ветки
# недостижимы из теста: конец бесконечного цикла поллинга, отказ внутренней
# задачи tokio и ветка-гарант `unreachable!()`.
MIN_LINES="${MIN_LINES:-98}"
MIN_FILE_LINES="${MIN_FILE_LINES:-91}"

# Скрипты подставного агента пишутся в /tmp; застарелые каталоги тестов
# съедают место и мешают следующим прогонам.
find /tmp -maxdepth 1 -name 'claudetg-*' -mmin +20 -exec rm -rf {} + 2>/dev/null || true

# вывод не глушим: иначе конвейер спрячет неудачу тестов, и гейт «пройдёт»
# при падающих проверках
cargo llvm-cov --summary-only \
    --fail-under-lines "$MIN_LINES" \
    --fail-under-file-lines "$MIN_FILE_LINES"
