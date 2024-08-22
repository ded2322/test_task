# DatabaseSynchronizer

Класс для синхронизации данных между тестовой и боевой базами данных с использованием SQLAlchemy.

## Основные методы

- `__init__(sample_db_url: str, target_db_url: str)`: Инициализация синхронизатора.
- `synchronize()`: Запуск процесса синхронизации.

## Вспомогательные методы

- `_sync_table(sample_session, target_session, table_name: str)`: Синхронизация отдельной таблицы.
- `_process_data_differences(target_session, target_table: Table, sample_dict: Dict, target_dict: Dict)`: Обработка различий в данных.
- `_update_row(session, table: Table, sample_row: Any, target_row: Any)`: Обновление существующей записи.
- `_insert_row(session, table: Table, sample_row: Any)`: Вставка новой записи.

## Статические методы

- `_create_data_dict(table: Table, data: list) -> Dict[Tuple, Any]`: Создание словаря данных.
- `_get_primary_key(table: Table, row: Any) -> Tuple`: Получение первичного ключа.
- `_get_update_data(table: Table, sample_row: Any, target_row: Any) -> Dict`: Получение данных для обновления.

## Примечания

Класс предполагает, что структура таблиц в тестовой базе данных является эталонной.
Синхронизация выполняется только для таблиц, существующих в обеих базах данных.
Для таблиц, отсутствующих в целевой базе данных, выводится предупреждение.
Синхронизатор обновляет существующие записи и добавляет новые, но не удаляет записи из целевой базы данных.