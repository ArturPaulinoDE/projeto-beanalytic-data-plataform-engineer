import os
import logging
import argparse
import pandas as pd

# Configura o logger padrão do módulo
logger = logging.getLogger(__name__)

def validate_gold_data(base_dir: str) -> None:
    """
    Valida a integridade das tabelas Gold (métricas mensais e anuais) geradas.
    Verifica a presença dos arquivos, número de registros, tipos e ausência de nulos.
    """
    logger.info("Iniciando a task de validação final (Gold Integrity Check).")
    
    mensal_file = os.path.join(base_dir, "gold", "metricas_mensais", "metricas_mensais.parquet")
    anual_file = os.path.join(base_dir, "gold", "metricas_anuais", "metricas_anuais.parquet")
    
    # 1. Verifica presença dos arquivos
    if not os.path.exists(mensal_file):
        raise FileNotFoundError(f"Tabela Gold Mensal não encontrada: {mensal_file}")
    if not os.path.exists(anual_file):
        raise FileNotFoundError(f"Tabela Gold Anual não encontrada: {anual_file}")
        
    try:
        # 2. Carrega as tabelas
        df_mensal = pd.read_parquet(mensal_file, engine="pyarrow")
        df_anual = pd.read_parquet(anual_file, engine="pyarrow")
        
        logger.info(f"Tabela Gold Mensal carregada com {len(df_mensal)} registros.")
        logger.info(f"Tabela Gold Anual carregada com {len(df_anual)} registros.")
        
        # 3. Validação de Schema da Tabela Mensal
        cols_mensal_esperadas = {"ano", "mes", "media_mensal", "variacao_mensal"}
        if not cols_mensal_esperadas.issubset(df_mensal.columns):
            missing = cols_mensal_esperadas - set(df_mensal.columns)
            raise ValueError(f"Colunas ausentes na tabela mensal: {missing}")
            
        # 4. Validação de Schema da Tabela Anual
        cols_anual_esperadas = {"ano", "taxa_acumulada_anual"}
        if not cols_anual_esperadas.issubset(df_anual.columns):
            missing = cols_anual_esperadas - set(df_anual.columns)
            raise ValueError(f"Colunas ausentes na tabela anual: {missing}")
            
        # 5. Validação de Ausência de Valores Nulos
        if df_mensal.isnull().any().any():
            null_cols = df_mensal.columns[df_mensal.isnull().any()].tolist()
            raise ValueError(f"Tabela mensal possui valores nulos nas colunas: {null_cols}")
            
        if df_anual.isnull().any().any():
            null_cols = df_anual.columns[df_anual.isnull().any()].tolist()
            raise ValueError(f"Tabela anual possui valores nulos nas colunas: {null_cols}")
            
        # 6. Validação de Quantidade de Registros (2020 a 2024 = 5 anos completos)
        # Mensal: 5 anos * 12 meses = 60 registros
        if len(df_mensal) != 60:
            raise ValueError(f"Tabela mensal possui quantidade incorreta de meses: {len(df_mensal)} (esperado: 60)")
            
        # Anual: 5 anos = 5 registros
        if len(df_anual) != 5:
            raise ValueError(f"Tabela anual possui quantidade incorreta de anos: {len(df_anual)} (esperado: 5)")
            
        # 7. Validação de Limites de Valores e Intervalos Coerentes
        # Os anos devem estar entre 2020 e 2024
        if not df_mensal["ano"].between(2020, 2024).all():
            raise ValueError("Tabela mensal possui anos fora do intervalo 2020-2024.")
        if not df_anual["ano"].between(2020, 2024).all():
            raise ValueError("Tabela anual possui anos fora do intervalo 2020-2024.")
            
        # Os meses devem estar entre 1 e 12
        if not df_mensal["mes"].between(1, 12).all():
            raise ValueError("Tabela mensal possui meses fora do intervalo 1-12.")
            
        # A média e a variação acumulada não podem ser taxas extraordinariamente irreais (ex: > 100% ao dia/mês em tempos normais)
        if not df_mensal["media_mensal"].between(0.0, 5.0).all():
            raise ValueError("Tabela mensal possui médias diárias mensais fora da margem de segurança (0% a 5% ao dia).")
            
        logger.info("Validação final (Gold Integrity Check) concluída com 100% de sucesso!")
        
    except Exception as e:
        logger.error(f"Falha na validação de integridade da Gold: {str(e)}")
        raise

if __name__ == "__main__":
    # Configura o logging básico para execução manual no terminal
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    # Define o parseador de argumentos de linha de comando usando argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-dir", type=str, required=True)

    args = parser.parse_args()
    validate_gold_data(base_dir=args.base_dir)
