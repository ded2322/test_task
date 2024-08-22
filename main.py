from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from typing import Dict, Any, Tuple


class DatabaseSynchronizer:
    def __init__(self, sample_db_url: str, target_db_url: str):
        """
        Инициализация класса синхронизации баз данных.

        Args:
            sample_db_url (str): URL подключения к тестовой базе данных (образцу).
            target_db_url (str): URL подключения к боевой базе данных (целевой).
        """
        self.sample_engine = create_engine(sample_db_url)
        self.target_engine = create_engine(target_db_url)

        self.sample_metadata = MetaData(bind=self.sample_engine)
        self.target_metadata = MetaData(bind=self.target_engine)

        self.sample_metadata.reflect()
        self.target_metadata.reflect()

        self.SampleSession = sessionmaker(bind=self.sample_engine)
        self.TargetSession = sessionmaker(bind=self.target_engine)

    def synchronize(self):
        """Синхронизирует данные между базами данных."""
        with self.SampleSession() as sample_session, self.TargetSession() as target_session:
            for table_name in self.sample_metadata.tables.keys():
                if table_name in self.target_metadata.tables:
                    self._sync_table(sample_session, target_session, table_name)
                else:
                    print(f"Таблица {table_name} отсутствует в боевой базе данных.")

    def _sync_table(self, sample_session, target_session, table_name: str):
        """
        Синхронизирует данные для указанной таблицы.

        Args:
            sample_session: Сессия для тестовой базы данных.
            target_session: Сессия для боевой базы данных.
            table_name (str): Имя таблицы для синхронизации.
        """
        sample_table = Table(table_name, self.sample_metadata, autoload_with=self.sample_engine)
        target_table = Table(table_name, self.target_metadata, autoload_with=self.target_engine)

        sample_data = sample_session.query(sample_table).all()
        target_data = target_session.query(target_table).all()

        sample_dict = self._create_data_dict(sample_table, sample_data)
        target_dict = self._create_data_dict(target_table, target_data)

        self._process_data_differences(target_session, target_table, sample_dict, target_dict)

        target_session.commit()

    @staticmethod
    def _create_data_dict(table: Table, data: list) -> Dict[Tuple, Any]:
        """
        Создает словарь данных, где ключом является первичный ключ записи.

        Args:
            table (Table): Объект таблицы SQLAlchemy.
            data (list): Список записей.

        Returns:
            Dict[Tuple, Any]: Словарь данных.
        """
        return {DatabaseSynchronizer._get_primary_key(table, row): row for row in data}

    @staticmethod
    def _get_primary_key(table: Table, row: Any) -> Tuple:
        """
        Возвращает значение первичного ключа для указанной строки таблицы.

        Args:
            table (Table): Объект таблицы SQLAlchemy.
            row (Any): Объект строки SQLAlchemy.

        Returns:
            Tuple: Кортеж с значениями первичного ключа.
        """
        primary_keys = [key.name for key in table.primary_key]
        return tuple(getattr(row, key) for key in primary_keys)

    def _process_data_differences(self, target_session, target_table: Table, sample_dict: Dict, target_dict: Dict):
        """
        Обрабатывает различия между данными в тестовой и боевой базах.

        Args:
            target_session: Сессия боевой базы данных.
            target_table (Table): Объект таблицы боевой базы данных.
            sample_dict (Dict): Словарь данных тестовой базы.
            target_dict (Dict): Словарь данных боевой базы.
        """
        for primary_key, sample_row in sample_dict.items():
            if primary_key in target_dict:
                self._update_row(target_session, target_table, sample_row, target_dict[primary_key])
            else:
                self._insert_row(target_session, target_table, sample_row)

    def _update_row(self, session, table: Table, sample_row: Any, target_row: Any):
        """
        Обновляет данные существующей записи в боевой базе данных.

        Args:
            session: Сессия боевой базы данных.
            table (Table): Объект таблицы SQLAlchemy.
            sample_row (Any): Объект строки из тестовой базы данных.
            target_row (Any): Объект строки из боевой базы данных.
        """
        update_data = self._get_update_data(table, sample_row, target_row)
        if update_data:
            primary_key_filter = {col.name: getattr(target_row, col.name) for col in table.primary_key}
            session.query(table).filter_by(**primary_key_filter).update(update_data)

    @staticmethod
    def _get_update_data(table: Table, sample_row: Any, target_row: Any) -> Dict:
        """
        Получает данные для обновления.

        Args:
            table (Table): Объект таблицы SQLAlchemy.
            sample_row (Any): Объект строки из тестовой базы данных.
            target_row (Any): Объект строки из боевой базы данных.

        Returns:
            Dict: Словарь с данными для обновления.
        """
        return {
            column.name: getattr(sample_row, column.name)
            for column in table.columns
            if getattr(sample_row, column.name) != getattr(target_row, column.name)
        }

    @staticmethod
    def _insert_row(session, table: Table, sample_row: Any):
        """
        Вставляет новую запись в боевую базу данных.

        Args:
            session: Сессия боевой базы данных.
            table (Table): Объект таблицы SQLAlchemy.
            sample_row (Any): Объект строки из тестовой базы данных.
        """
        insert_data = {column.name: getattr(sample_row, column.name) for column in table.columns}
        session.execute(table.insert().values(insert_data))


def main():
    sample_db_url = 'postgresql://user:password@localhost/test_db'
    target_db_url = 'postgresql://user:password@localhost/prod_db'

    synchronizer = DatabaseSynchronizer(sample_db_url, target_db_url)
    synchronizer.synchronize()


if __name__ == "__main__":
    main()