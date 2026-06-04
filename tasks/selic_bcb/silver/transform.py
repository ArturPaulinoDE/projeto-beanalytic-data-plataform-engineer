import os
import logging
import argparse
import json
import pandas as pd
import great_expectations as gx
from datetime import datetime
from utils.dq_checks import validate_dataframe

# Configura o logger padrão do módulo
logger = logging.getLogger(__name__)

def transform_data(base_dir: str) -> None:
    """
    Lê os dados brutos da Bronze (sob selic_raw), aplica regras de qualidade de dados,
    registrando e rejeitando os registros inválidos em log estruturado.
    Caso contrário, padroniza as colunas e executa as validações do Great Expectations (Silver DQ),
    salvando os dados na camada Silver.
    """
    logger.info("Iniciando a task de transformação e validação de qualidade (Silver) com DQ integrado.")
    
    bronze_dir = os.path.join(base_dir, "bronze", "selic_raw")
    silver_dir = os.path.join(base_dir, "silver")
    
    os.makedirs(silver_dir, exist_ok=True)
    
    # Valida a presença do diretório de entrada
    if not os.path.exists(bronze_dir) or not os.listdir(bronze_dir):
        logger.error(f"Diretório da camada Bronze não encontrado ou vazio: {bronze_dir}")
        raise FileNotFoundError(f"Dados da camada Bronze não encontrados em {bronze_dir}")
        
    try:
        logger.info(f"Lendo dados brutos da Bronze a partir de: {bronze_dir}")
        df_bronze = pd.read_parquet(bronze_dir, engine="pyarrow")
        logger.info(f"Total de registros lidos da Bronze: {len(df_bronze)}")
        
        # --- VALIDAÇÕES E FILTRAGENS VETORIZADAS COM PANDAS ---
        # 1. Identifica valores nulos em qualquer coluna obrigatória
        has_null = df_bronze["data"].isna() | df_bronze["valor"].isna()
        
        # 2. Valida o formato da string de data usando Expressão Regular (Regex) no padrão dd/MM/aaaa
        data_cleaned = df_bronze["data"].astype(str).str.strip()
        matches_date_regex = data_cleaned.str.match(r"^\d{2}/\d{2}/\d{4}$")
        
        # 3. Valida se é uma data real no calendário (ex: converte para Datetime; valores irreais como 30/02 viram NaT)
        parsed_dates = pd.to_datetime(data_cleaned, format="%d/%m/%Y", errors="coerce")
        is_valid_calendar = parsed_dates.notna()
        
        # 4. Valida se a data está no intervalo de negócio delimitado de 2020 a 2024
        in_date_range = (parsed_dates >= "2020-01-01") & (parsed_dates <= "2024-12-31")
        
        # 5. Valida se o campo 'valor' é numérico (converte para float; valores corrompidos viram NaN)
        valor_cleaned = df_bronze["valor"].astype(str).str.strip()
        parsed_values = pd.to_numeric(valor_cleaned, errors="coerce")
        is_numeric_value = parsed_values.notna()
        
        # 6. Valida que o valor da taxa diária não é negativo
        is_non_negative = parsed_values >= 0.0
        
        # Concatenação das condições de integridade para isolar registros válidos
        is_record_valid = (
            (~has_null) &
            matches_date_regex &
            is_valid_calendar &
            in_date_range &
            is_numeric_value &
            is_non_negative
        )
        
        df_valid = df_bronze[is_record_valid].copy()
        df_invalid = df_bronze[~is_record_valid].copy()
        
        # Se houver registros inválidos, rejeita e registra em log estruturado
        if not df_invalid.empty:
            invalid_logs = []
            for idx, row in df_invalid.iterrows():
                val_data = row.get("data")
                val_valor = row.get("valor")
                reasons = []
                
                if pd.isna(val_data) or pd.isna(val_valor):
                    reasons.append(f"Campos obrigatórios ausentes. Chaves encontradas: {list(row.index)}")
                else:
                    v_data_str = str(val_data).strip()
                    if not v_data_str or not matches_date_regex.loc[idx]:
                        reasons.append(f"Formato de data inválido: '{val_data}'. Esperado 'dd/MM/aaaa'.")
                    elif not is_valid_calendar.loc[idx]:
                        reasons.append(f"Data com valores calendários inválidos: '{val_data}'.")
                    elif not in_date_range.loc[idx]:
                        reasons.append(f"Data fora do intervalo permitido (2020-2024): '{val_data}'.")
                        
                    if not is_numeric_value.loc[idx]:
                        reasons.append(f"Valor não é numérico: '{val_valor}'.")
                    elif not is_non_negative.loc[idx]:
                        reasons.append(f"Valor da taxa SELIC diária não pode ser negativo: {parsed_values.loc[idx]}.")
                
                reason_str = " | ".join(reasons) if reasons else "Erro indeterminado de integridade."
                log_entry = {
                    "linha_index": int(idx),
                    "data_original": str(val_data) if not pd.isna(val_data) else None,
                    "valor_original": str(val_valor) if not pd.isna(val_valor) else None,
                    "motivo_rejeicao": reason_str,
                    "rejeitado_em": datetime.now().isoformat()
                }
                invalid_logs.append(log_entry)
                # Log estruturado no console
                logger.warning(f"REGISTRO REJEITADO: {json.dumps(log_entry)}")
            
            # Grava o log estruturado em um arquivo JSON de auditoria
            logs_dir = os.path.join(base_dir, "silver", "logs")
            os.makedirs(logs_dir, exist_ok=True)
            log_file = os.path.join(logs_dir, "rejected_records.json")
            
            with open(log_file, "w", encoding="utf-8") as lf:
                json.dump(invalid_logs, lf, indent=4, ensure_ascii=False)
                
            logger.info(f"Total de {len(df_invalid)} registros inválidos rejeitados e salvos em {log_file}")
            
        # Criação do DataFrame estruturado para a Silver com os dados tipados (apenas registros válidos)
        df_silver = pd.DataFrame()
        df_silver["data"] = parsed_dates.loc[df_valid.index].dt.date
        df_silver["valor"] = parsed_values.loc[df_valid.index].astype(float)
        df_silver["ano"] = parsed_dates.loc[df_valid.index].dt.year
        df_silver["mes"] = parsed_dates.loc[df_valid.index].dt.month
        
        if df_silver.empty:
            raise ValueError("Falha na transformação: nenhum registro válido restou após a filtragem.")
        
        # Eliminação preventiva de registros duplicados na mesma data
        len_antes = len(df_silver)
        df_silver = df_silver.drop_duplicates(subset=["data"])
        len_depois = len(df_silver)
        
        if len_antes != len_depois:
            logger.info(f"Removidos {len_antes - len_depois} registros duplicados na Silver.")
            
        df_silver = df_silver.sort_values(by="data").reset_index(drop=True)
        
        # --- DATA QUALITY CHECK (Silver) ---
        # Checagem de qualidade formal usando Great Expectations antes de gravar em disco
        logger.info("Executando checagem de qualidade (DQ) na memória antes de salvar na Silver...")
        expectations = [
            # Colunas esperadas
            gx.expectations.ExpectTableColumnsToMatchSet(column_set=["data", "valor", "ano", "mes"]),
            # Nulos
            gx.expectations.ExpectColumnValuesToNotBeNull(column="data"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="valor"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="ano"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="mes"),
            # Tipos
            gx.expectations.ExpectColumnValuesToBeInTypeList(column="valor", type_list=["float", "float64", "double"]),
            # Valores não negativos de taxa
            gx.expectations.ExpectColumnValuesToBeBetween(column="valor", min_value=0.0),
            # Limites cronológicos [2020-01-01, 2024-12-31]
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="data",
                min_value=datetime.strptime("2020-01-01", "%Y-%m-%d").date(),
                max_value=datetime.strptime("2024-12-31", "%Y-%m-%d").date()
            )
        ]
        validate_dataframe(df_silver, expectations, "dq_silver")
        
        # --- GRAVAÇÃO DOS DADOS NA SILVER ---
        silver_output_dir = os.path.join(silver_dir, "selic_clean")
        os.makedirs(silver_output_dir, exist_ok=True)
        
        # Limpeza preventiva (idempotência)
        for file in os.listdir(silver_output_dir):
            if file.endswith(".parquet"):
                os.remove(os.path.join(silver_output_dir, file))
                
        output_file = os.path.join(silver_output_dir, "clean_data.parquet")
        logger.info(f"Salvando dados limpos na Silver em: {output_file}")
        
        df_silver.to_parquet(
            output_file,
            index=False,
            engine="pyarrow"
        )
        
        logger.info(f"Transformação (Silver) concluída com sucesso. Válidos: {len(df_silver)}")
        
    except Exception as e:
        logger.error(f"Erro inesperado durante o processamento da camada Silver: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    # Configura o logging básico para execução manual no terminal
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    # Define o parseador de argumentos de linha de comando usando argparse
    parser = argparse.ArgumentParser(description="Task de transformação de dados na camada Silver do SELIC BCB.")
    parser.add_argument(
        "--base-dir",
        type=str,
        required=True,
        help="Caminho absoluto para o diretório de dados base."
    )
    
    args = parser.parse_args()
    transform_data(base_dir=args.base_dir)
