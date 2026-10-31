# claudetg

Telegram-бот, который передаёт сообщения в Claude Code (`claude -p`) и присылает ответы в чат.
Получает обновления через long polling (`getUpdates`), ходит в Telegram API через HTTP-прокси.

## Как это работает
- Каждое сообщение запускает `claude -p --output-format stream-json --resume <session>`
  в рабочей директории чата; текст запроса передаётся через stdin.
- `session_id` сохраняется в `~/.local/state/claudetg/state.json`, поэтому разговор
  продолжается и после перезапуска. Если Claude задаёт вопрос, просто ответь сообщением:
  ответ уйдёт в ту же сессию.
- Пока Claude работает, бот показывает «печатает…» и (при `SHOW_TOOLS=true`) какие инструменты запускаются.
- Сообщения, пришедшие пока Claude работает, встают в очередь и после завершения уходят одним запросом.
- Сообщения от пользователей не из `ALLOWED_USER_IDS` игнорируются.

Команды: `/new`, `/cancel`, `/cwd [путь]`, `/status`, `/help`. Остальные команды со слешем передаются Claude как есть.

## Установка как пользовательской службы
```sh
cargo build --release
install -Dm755 target/release/claudetg ~/.local/bin/claudetg
install -Dm600 deploy/claudetg.env.example ~/.config/claudetg/claudetg.env   # затем отредактируй
install -Dm644 deploy/claudetg.service ~/.config/systemd/user/claudetg.service
systemctl --user daemon-reload
systemctl --user enable --now claudetg
loginctl enable-linger $USER   # чтобы служба работала без активного входа в систему
journalctl --user -u claudetg -f
```

## Права доступа
`claude -p` не может спросить разрешение интерактивно, поэтому режим задаётся `CLAUDE_PERMISSION_MODE`:
`auto` (по умолчанию) — классификатор сам решает, что безопасно; `bypassPermissions` — без
ограничений (только если доверяешь всем, у кого есть доступ к боту); `acceptEdits` — только правки
файлов, команды shell будут отклонены.
