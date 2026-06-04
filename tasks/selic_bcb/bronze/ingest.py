import os
import requests
import logging
import argparse
import pandas as pd
import great_expectations as gx
from utils.dq_checks import validate_dataframe

# Configura o logger padrão do módulo
logger = logging.getLogger(__name__)

# URL da API do Banco Central (BCB) contendo a série da taxa SELIC diária
API_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=01/01/2020&dataFinal=31/12/2024"

def ingest_data(base_dir: str) -> None:
    """
    Consome a API da taxa SELIC, realiza o Quality Check (Bronze) na memória,
    e se passar, salva os dados brutos em Parquet na camada Bronze sob 'selic_raw'.
    """
    logger.info("Iniciando a task de ingestão de dados (Bronze) com DQ integrado.")
    
    try:
        # Requisita dados da API do BCB
        logger.info(f"Consumindo API do Banco Central: {API_URL}")
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"API consumida com sucesso. Total de registros brutos recebidos: {len(data)}")
        
        if not data:
            raise ValueError("A API retornou uma lista vazia de dados.")
            
        df = pd.DataFrame(data)
        
        # --- DATA QUALITY CHECK (Bronze) ---
        # Valida que as colunas obrigatórias existem, o dataset não está vazio e não há nulos
        logger.info("Executando checagem de qualidade (DQ) na memória antes de salvar na Bronze...")
        expectations = [
            gx.expectations.ExpectColumnToExist(column="data"),
            gx.expectations.ExpectColumnToExist(column="valor"),
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="data"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="valor"),
        ]
        validate_dataframe(df, expectations, "dq_bronze")
        
        # Extrai o ano da data (formato dd/MM/aaaa) para particionamento
        df["ano"] = df["data"].apply(lambda x: x.split("/")[-1] if isinstance(x, str) and "/" in x else "unknown")

        # Define o caminho de destino da Bronze
        bronze_dir = os.path.join(base_dir, "bronze", "selic_raw")
        os.makedirs(bronze_dir, exist_ok=True)
        
        # Limpeza preventiva para manter idempotência (deleta arquivos antigos e partições)
        import shutil
        for item in os.listdir(bronze_dir):
            item_path = os.path.join(bronze_dir, item)
            if os.path.isdir(item_path) and item.startswith("ano="):
                shutil.rmtree(item_path)
            elif os.path.isfile(item_path) and item.endswith(".parquet"):
                os.remove(item_path)
        
        logger.info(f"Gravando dados brutos particionados por ano na Bronze em: {bronze_dir}")
        
        # Grava os dados na camada Bronze com particionamento por ano
        df.to_parquet(
            bronze_dir,
            partition_cols=["ano"],
            index=False,
            engine="pyarrow"
        )
        
        logger.info(f"Ingestão (Bronze) concluída com sucesso. Total de registros processados: {len(df)}")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de comunicação HTTP ao consumir a API do Banco Central: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Falha durante a ingestão/DQ dos dados para a Bronze: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    # Configura o logging básico para execução manual no terminal
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    # Define o parseador de argumentos de linha de comando usando argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-dir", type=str, required=True)

    args = parser.parse_args()
    ingest_data(base_dir=args.base_dir)
