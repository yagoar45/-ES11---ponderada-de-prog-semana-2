# Projeto de Ingestão de Dados com Supabase

Este projeto é um pacote Python que realiza a ingestão de dados em um banco de dados Supabase. Ele inclui funcionalidades para transformar dados antes de inseri-los, como preencher campos nulos e transformar formatos de data.

## Estrutura do Projeto

- `data_ingestion/`
  - `__init__.py`: Torna a pasta um pacote Python.
  - `ingestor.py`: Contém a lógica principal para ingestão e transformação de dados.
  - `supabase_connection.py`: Gerencia a conexão com o Supabase.
- `tests/`
  - `test_ingestor.py`: Contém testes unitários para verificar a funcionalidade do pacote.
- `.env`: Armazena variáveis de ambiente sensíveis, como URL e chave do Supabase.

## Funcionalidades

- **Ingestão de Dados:** Insere dados em uma tabela específica no Supabase.
- **Transformação de Dados:**
  - Preenche campos nulos com "indefinido".
  - Transforma datas no formato `2010-01-29-02.25.25.000000` para `YYYY-MM-DD HH:MM:SS`.
- **Testes Unitários:** Usa Pytest para garantir que todas as funcionalidades funcionem conforme esperado.
