import os
import json
import pytest
import pandas as pd
from datetime import date
from tasks.selic_bcb.silver.transform import transform_data
from tasks.selic_bcb.gold.aggregate import aggregate_data
from tasks.selic_bcb.gold.validate import validate_gold_data

def test_silver_transformation_rejection_and_logging(tmp_path):
    """
    Testa se a transformação da Silver filtra registros válidos,
    grava os registros inválidos no JSON de rejeição e prossegue com sucesso.
    """
    base_dir = str(tmp_path)
    
    # 1. Cria dados mockados na Bronze
    # 3 registros válidos, 4 registros inválidos
    mock_bronze_data = [
        {"data": "02/01/2020", "valor": "0.017"},     # Válido
        {"data": "03/01/2020", "valor": "0.018"},     # Válido
        {"data": "30/02/2020", "valor": "0.015"},     # Inválido: data inexistente (fevereiro não tem dia 30)
        {"data": "05/01/2020", "valor": "-0.01"},     # Inválido: taxa negativa
        {"data": "06/01/2020", "valor": "abc"},       # Inválido: valor não numérico
        {"data": "01/01/2019", "valor": "0.02"},      # Inválido: ano fora do intervalo (2020-2024)
        {"data": "07/01/2020", "valor": "0.019"}      # Válido
    ]
    
    bronze_dir = os.path.join(base_dir, "bronze", "selic_raw")
    os.makedirs(bronze_dir, exist_ok=True)
    df_bronze = pd.DataFrame(mock_bronze_data)
    df_bronze["ano"] = df_bronze["data"].apply(lambda x: x.split("/")[-1])
    
    # Grava no formato particionado (Bronze)
    df_bronze.to_parquet(bronze_dir, partition_cols=["ano"], index=False, engine="pyarrow")
    
    # 2. Executa a transformação
    transform_data(base_dir=base_dir)
    
    # 3. Asserções
    silver_file = os.path.join(base_dir, "silver", "selic_clean", "clean_data.parquet")
    assert os.path.exists(silver_file)
    
    df_silver = pd.read_parquet(silver_file)
    # Devem restar exatamente 3 registros válidos
    assert len(df_silver) == 3
    assert set(df_silver["ano"]) == {2020}
    assert (df_silver["valor"] >= 0).all()
    
    # Verifica se os logs estruturados foram gravados
    log_file = os.path.join(base_dir, "silver", "logs", "rejected_records.json")
    assert os.path.exists(log_file)
    
    with open(log_file, "r", encoding="utf-8") as lf:
        rejected_list = json.load(lf)
    
    # Devem haver exatamente 4 registros rejeitados
    assert len(rejected_list) == 4
    # Valida presença das chaves de log estruturado
    first_rejected = rejected_list[0]
    assert "linha_index" in first_rejected
    assert "data_original" in first_rejected
    assert "valor_original" in first_rejected
    assert "motivo_rejeicao" in first_rejected
    assert "rejeitado_em" in first_rejected

def test_silver_transformation_all_invalid(tmp_path):
    """
    Testa se a transformação falha com ValueError caso todos os registros sejam inválidos.
    """
    base_dir = str(tmp_path)
    
    mock_bronze_data = [
        {"data": "30/02/2020", "valor": "0.015"},
        {"data": "05/01/2020", "valor": "-0.01"}
    ]
    
    bronze_dir = os.path.join(base_dir, "bronze", "selic_raw")
    os.makedirs(bronze_dir, exist_ok=True)
    df_bronze = pd.DataFrame(mock_bronze_data)
    df_bronze["ano"] = df_bronze["data"].apply(lambda x: x.split("/")[-1])
    df_bronze.to_parquet(bronze_dir, partition_cols=["ano"], index=False, engine="pyarrow")
    
    with pytest.raises(ValueError, match="nenhum registro válido restou"):
        transform_data(base_dir=base_dir)

from unittest.mock import patch

@patch("tasks.selic_bcb.gold.aggregate.validate_dataframe")
def test_gold_aggregation_and_validation(mock_validate, tmp_path):
    """
    Testa o pipeline na camada Gold: calcula agregações e valida os cálculos.
    """
    base_dir = str(tmp_path)
    
    # Cria dados limpos mockados na Silver abrangendo 2 meses para verificação do produtório
    # Mês 1: 2 dias úteis com taxa de 1% ao dia (fatores: 1.01 e 1.01) -> variação acumulada = 2.01%
    # Mês 2: 1 dia útil com taxa de 2% ao dia (fator: 1.02)
    mock_silver_data = [
        {"data": date(2020, 1, 2), "valor": 1.0, "ano": 2020, "mes": 1},
        {"data": date(2020, 1, 3), "valor": 1.0, "ano": 2020, "mes": 1},
        {"data": date(2020, 2, 3), "valor": 2.0, "ano": 2020, "mes": 2}
    ]
    
    silver_dir = os.path.join(base_dir, "silver", "selic_clean")
    os.makedirs(silver_dir, exist_ok=True)
    df_silver = pd.DataFrame(mock_silver_data)
    df_silver.to_parquet(os.path.join(silver_dir, "clean_data.parquet"), index=False, engine="pyarrow")
    
    # Executa a agregação (com validação de DF mockada para ignorar a contagem exata de 60 linhas de produção)
    aggregate_data(base_dir=base_dir)
    
    # Verifica arquivos gerados
    mensal_file = os.path.join(base_dir, "gold", "metricas_mensais", "metricas_mensais.parquet")
    anual_file = os.path.join(base_dir, "gold", "metricas_anuais", "metricas_anuais.parquet")
    assert os.path.exists(mensal_file)
    assert os.path.exists(anual_file)
    
    df_mensal = pd.read_parquet(mensal_file)
    df_anual = pd.read_parquet(anual_file)
    
    # Asserções de agregação
    # Mês 1: média = 1.0, acumulado = (1.01 * 1.01 - 1) * 100 = 2.01%
    row_jan = df_mensal[df_mensal["mes"] == 1].iloc[0]
    assert row_jan["media_mensal"] == pytest.approx(1.0)
    assert row_jan["variacao_mensal"] == pytest.approx(2.01)
    
    # Mês 2: média = 2.0, acumulado = 2.0%
    row_feb = df_mensal[df_mensal["mes"] == 2].iloc[0]
    assert row_feb["media_mensal"] == pytest.approx(2.0)
    assert row_feb["variacao_mensal"] == pytest.approx(2.0)
    
    # Ano 2020 acumulado total: (1.01 * 1.01 * 1.02 - 1) * 100 = (1.0201 * 1.02 - 1) * 100 = 4.0502%
    row_year = df_anual[df_anual["ano"] == 2020].iloc[0]
    assert row_year["taxa_acumulada_anual"] == pytest.approx(4.0502)
