import pytest
from data_ingestion.ingestor import DataIngestor

class MockDBConnection:
    def insert(self, data):
        if not data:
            raise ValueError("Dados vazios não podem ser inseridos.")

def test_ingest_data_success():
    db = MockDBConnection()
    ingestor = DataIngestor(db)
    data = {"key": "value"}
    ingestor.ingest_data(data)

def test_ingest_data_failure():
    db = MockDBConnection()
    ingestor = DataIngestor(db)
    with pytest.raises(ValueError):
        ingestor.ingest_data(None) 