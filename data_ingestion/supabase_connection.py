from supabase import create_client, Client

class SupabaseConnection:
    def __init__(self, url: str, key: str):
        self.client: Client = create_client(url, key)

    def insert(self, table: str, data: dict):
        response = self.client.table(table).insert(data).execute()
        if response.error:
            raise Exception(f"Erro ao inserir dados: {response.error}")
        return response.data 