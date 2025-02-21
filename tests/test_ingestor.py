import pytest
import os
from dotenv import load_dotenv
from data_ingestion.ingestor import DataIngestor

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

class MockSupabaseConnection:
    def insert(self, table, data):
        if not data:
            raise ValueError("Dados vazios não podem ser inseridos.")
        if table != "InteliFalhas":
            raise ValueError("Nome da tabela incorreto.")

def test_ingest_data_success(monkeypatch):
    def mock_init(self, url, key):
        self.client = MockSupabaseConnection()

    monkeypatch.setattr("data_ingestion.supabase_connection.SupabaseConnection.__init__", mock_init)

    ingestor = DataIngestor(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    data = {"key": "value"}
    ingestor.ingest_data("InteliFalhas", data)

def test_ingest_data_failure(monkeypatch):
    def mock_init(self, url, key):
        self.client = MockSupabaseConnection()

    monkeypatch.setattr("data_ingestion.supabase_connection.SupabaseConnection.__init__", mock_init)

    ingestor = DataIngestor(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    with pytest.raises(ValueError):
        ingestor.ingest_data("InteliFalhas", None)

def test_transform_data():
    ingestor = DataIngestor(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    data = {"name": None, "age": 25, "city": None}
    expected = {"name": "indefinido", "age": 25, "city": "indefinido"}
    assert ingestor.transform_data(data) == expected

def test_transform_date():
    ingestor = DataIngestor(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    date_str = "2010-01-29-02.25.25.000000"
    expected = "2010-01-29 02:25:25"
    assert ingestor.transform_date(date_str) == expected

def test_transform_dates_in_column(monkeypatch):
    def mock_init(self, url, key):
        self.client = MockSupabaseConnection()

    monkeypatch.setattr("data_ingestion.supabase_connection.SupabaseConnection.__init__", mock_init)

    ingestor = DataIngestor(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    data_list = [
        {"DATA DETECCAO": "2010-01-29-02.25.25.000000", "other_field": "value1"},
        {"DATA DETECCAO": "2011-02-30-03.15.45.000000", "other_field": "value2"}
    ]
    expected = [
        {"DATA DETECCAO": "2010-01-29 02:25:25", "other_field": "value1"},
        {"DATA DETECCAO": "2011-02-30 03:15:45", "other_field": "value2"}
    ]
    transformed_data = ingestor.transform_dates_in_column(data_list, "DATA DETECCAO")
    assert transformed_data == expected
    for data in transformed_data:
        ingestor.ingest_data("InteliFalhas", data)
