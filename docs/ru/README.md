# Документация tuitab

Быстрый обозреватель табличных данных в терминале с управлением с клавиатуры —
**CSV · JSON · JSONL · YAML · TOML · Parquet · Arrow · Excel · Markdown ·
SQLite · DuckDB**.

> 🇬🇧 This documentation is also available in [English](../en/README.md).

## Содержание

| Руководство | О чём |
|-------------|-------|
| [Начало работы](getting-started.md) | Установка, открытие файлов, просмотр каталогов и нескольких файлов, режим stdin/pipe |
| [Горячие клавиши](keybindings.md) | Полный справочник команд по режимам |
| [Выражения](expressions.md) | Вычисляемые столбцы и фильтры-выражения: операторы, функции, даты |
| [Графики](charts.md) | Гистограмма, частоты, линейный и сгруппированный столбчатый график; закрепление и детализация |
| [JOIN](join.md) | Пошаговый мастер JOIN, типы соединений и DIFF двух таблиц |
| [Базы данных](database.md) | Правка таблицы SQLite/DuckDB и запись обратно, сборка базы с нуля |
| [Сводные таблицы](pivot.md) | Синтаксис сводных таблиц, агрегации и примеры |
| [MCP-сервер](mcp.md) | Как дать ИИ-ассистенту считать движком tuitab: инструменты, операции, подключение |
| [Рецепты](recipes.md) | Практические приёмы: дедупликация, доля от итога, буфер обмена, экспорт и другое |

## Кратко

```sh
tuitab data.csv                   # открыть файл
tuitab config.toml                # JSON / YAML / TOML открываются деревом
tuitab orders.csv customers.csv   # просмотреть несколько файлов списком
tuitab ./reports/                 # просмотреть каталог
tuitab inventory.sqlite           # файла ещё нет — откроется пустой лист
cat data.csv | tuitab -t csv      # прочитать из конвейера
```

Когда таблица открыта: перемещение — `hjkl`, сортировка — `[` / `]` (`z[` / `z]`
для второго ключа), график — `V`, статистика — `I`, оконный столбец — `zw`,
встроенная справка — `?`. Подробности в
[Горячих клавишах](keybindings.md).

## Смотрите также

- [Основной README проекта](https://github.com/denisotree/tuitab/blob/master/README.md)
- [Руководство для контрибьюторов](https://github.com/denisotree/tuitab/blob/master/CONTRIBUTING.md)
- [История изменений](https://github.com/denisotree/tuitab/blob/master/CHANGELOG.md)
