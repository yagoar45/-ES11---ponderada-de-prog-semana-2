import logging

class DataIngestor:
    def __init__(self, db_connection):
        self.db_connection = db_connection
        logging.basicConfig(level=logging.INFO)

    def ingest_data(self, data):
        try:
            # Simulação de inserção de dados
            self.db_connection.insert(data)
            logging.info("Dados inseridos com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao inserir dados: {e}")
            raise 