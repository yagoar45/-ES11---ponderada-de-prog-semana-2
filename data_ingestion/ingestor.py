import logging
from datetime import datetime
from .supabase_connection import SupabaseConnection

class DataIngestor:
    def __init__(self, supabase_url, supabase_key):
        self.db_connection = SupabaseConnection(supabase_url, supabase_key)
        logging.basicConfig(level=logging.INFO)

    def transform_data(self, data):
        return {k: (v if v is not None else "indefinido") for k, v in data.items()}

    def transform_dates_in_column(self, data_list, column_name):
        for data in data_list:
            if column_name in data and data[column_name]:
                try:
                    data[column_name] = datetime.strptime(data[column_name], "%Y-%m-%d-%H.%M.%S.%f").strftime("%Y-%m-%d %H:%M:%S")
                except ValueError as e:
                    logging.error(f"Erro ao transformar data: {e}")
                    raise
        return data_list

    def ingest_data(self, table, data):
        try:
            transformed_data = self.transform_data(data)
            self.db_connection.insert(table, transformed_data)
            logging.info("Dados inseridos com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao inserir dados: {e}")
            raise 