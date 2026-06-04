import os
import logging
import argparse
import pandas as pd
import great_expectations as gx
from utils.dq_checks import validate_dataframe

# Configura o logger padrão do módulo
logger = logging.getLogger(__name__)

def aggregate_data(base_dir: str) -> None:
    """
    Lê os dados da Silver, gera as visões da camada Gold, realiza os Quality Checks na memória,
    e se passarem, grava as tabelas na camada Gold.
    """
    logger.info("Iniciando a task de agregação de dados (Gold) com DQ integrado.")
    
    silver_file = os.path.join(base_dir, "silver", "selic_clean", "clean_data.parquet")
    gold_dir = os.path.join(base_dir, "gold")
    
    # Valida a presença do arquivo de entrada da Silver
    if not os.path.exists(silver_file):
        logger.error(f"Arquivo da camada Silver não encontrado: {silver_file}")
        raise FileNotFoundError(f"Dados da Silver não encontrados em {silver_file}")
        
    try:
        logger.info(f"Lendo dados da Silver a partir de: {silver_file}")
        df = pd.read_parquet(silver_file, engine="pyarrow")
        logger.info(f"Total de registros lidos da Silver: {len(df)}")
        
        # Cria o fator diário (1 + taxa_diaria / 100) para viabilizar cálculos de juros compostos
        df["fator_diario"] = 1.0 + (df["valor"] / 100.0)
        
        # --- TABELA 1: MÉTRICAS MENSAIS ---
        logger.info("Calculando as métricas mensais (média e variação acumulada).")
        # 1.1. Média simples da taxa SELIC diária por ano e mês
        df_media_mensal = df.groupby(["ano", "mes"])["valor"].mean().reset_index()
        df_media_mensal = df_media_mensal.rename(columns={"valor": "media_mensal"})
        
        # 1.2. Produtório acumulado dos fatores diários por ano e mês
        df_fator_mensal = df.groupby(["ano", "mes"])["fator_diario"].prod().reset_index()
        # Variação acumulada mensal (%) = (Fator Acumulado - 1) * 100
        df_fator_mensal["variacao_mensal"] = (df_fator_mensal["fator_diario"] - 1.0) * 100.0
        
        # 1.3. Consolida as métricas mensais
        df_metricas_mensais = pd.merge(df_media_mensal, df_fator_mensal[["ano", "mes", "variacao_mensal"]], on=["ano", "mes"])
        df_metricas_mensais = df_metricas_mensais.sort_values(by=["ano", "mes"]).reset_index(drop=True)
        
        # --- TABELA 2: MÉTRICAS ANUAIS ---
        logger.info("Calculando as métricas anuais (taxa acumulada anual).")
        # 2.1. Produtório acumulado dos fatores diários por ano para obter a taxa acumulada anual
        df_fator_anual = df.groupby("ano")["fator_diario"].prod().reset_index()
        # Variação acumulada anual (%) = (Fator Acumulado - 1) * 100
        df_fator_anual["taxa_acumulada_anual"] = (df_fator_anual["fator_diario"] - 1.0) * 100.0
        
        # 2.2. Consolida as métricas anuais
        df_metricas_anuais = df_fator_anual[["ano", "taxa_acumulada_anual"]].sort_values(by="ano").reset_index(drop=True)
        
        # --- DATA QUALITY CHECKS (Gold) ---
        # Executa as validações na memória para as tabelas finais da Gold antes de gravar
        logger.info("Executando checagem de qualidade (DQ) na memória para Gold...")
        
        # Validação da tabela de métricas mensais (exige exatamente 60 registros no range 2020-2024 e sem nulos)
        expectations_mensal = [
            gx.expectations.ExpectTableColumnsToMatchSet(column_set=["ano", "mes", "media_mensal", "variacao_mensal"]),
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=60, max_value=60),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="media_mensal"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="variacao_mensal"),
        ]
        validate_dataframe(df_metricas_mensais, expectations_mensal, "dq_gold_mensal")
        
        # Validação da tabela de métricas anuais (exige exatamente 5 registros, um por ano, e sem nulos)
        expectations_anual = [
            gx.expectations.ExpectTableColumnsToMatchSet(column_set=["ano", "taxa_acumulada_anual"]),
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=5, max_value=5),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="taxa_acumulada_anual"),
        ]
        validate_dataframe(df_metricas_anuais, expectations_anual, "dq_gold_anual")
        
        # --- GRAVAÇÃO DOS DADOS NA GOLD ---
        # 1. Grava as Métricas Mensais
        mensal_dir = os.path.join(gold_dir, "metricas_mensais")
        os.makedirs(mensal_dir, exist_ok=True)
        for file in os.listdir(mensal_dir):
            if file.endswith(".parquet"):
                os.remove(os.path.join(mensal_dir, file))
                 
        mensal_file = os.path.join(mensal_dir, "metricas_mensais.parquet")
        logger.info(f"Salvando tabela Gold Métricas Mensais em: {mensal_file}")
        df_metricas_mensais.to_parquet(mensal_file, index=False, engine="pyarrow")
        
        # 2. Grava as Métricas Anuais
        anual_dir = os.path.join(gold_dir, "metricas_anuais")
        os.makedirs(anual_dir, exist_ok=True)
        for file in os.listdir(anual_dir):
            if file.endswith(".parquet"):
                os.remove(os.path.join(anual_dir, file))
                 
        anual_file = os.path.join(anual_dir, "metricas_anuais.parquet")
        logger.info(f"Salvando tabela Gold Métricas Anuais em: {anual_file}")
        df_metricas_anuais.to_parquet(anual_file, index=False, engine="pyarrow")
        
        logger.info(f"Agregação (Gold) concluída com sucesso. Meses calculados: {len(df_metricas_mensais)}, Anos calculados: {len(df_metricas_anuais)}")
        
    except Exception as e:
        logger.error(f"Erro inesperado durante o cálculo/DQ das agregações da Gold: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    # Configura o logging básico para execução manual no terminal
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    # Define o parseador de argumentos de linha de comando usando argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-dir", type=str, required=True)

    args = parser.parse_args()
    aggregate_data(base_dir=args.base_dir)
