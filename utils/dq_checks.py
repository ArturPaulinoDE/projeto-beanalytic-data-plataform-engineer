import uuid
import logging
import pandas as pd
import great_expectations as gx

# Configura o logger do módulo
logger = logging.getLogger(__name__)

def validate_dataframe(df: pd.DataFrame, expectations: list, suite_name: str) -> None:
    """
    Função auxiliar que valida um DataFrame usando Great Expectations v1.0+.
    Caso as expectativas falhem, gera um erro detalhado impedindo a continuação do pipeline.
    """
    context = gx.get_context()
    uid = uuid.uuid4().hex[:8]
    
    # 1. Registra a fonte de dados Pandas de forma fluida
    batch = (
        context.data_sources.add_pandas(f"ds_{uid}")
        .add_dataframe_asset("df")
        .add_batch_definition_whole_dataframe("batch")
        .get_batch(batch_parameters={"dataframe": df})
    )
    
    # 2. Cria a Expectation Suite e executa a validação
    suite = context.suites.add(gx.core.expectation_suite.ExpectationSuite(f"suite_{uid}", expectations=expectations))
    results = batch.validate(suite)
    
    # 3. Trata os erros de validação
    if not results.success:
        failures = [
            f"- {res.expectation_config.type} na coluna '{res.expectation_config.kwargs.get('column', 'N/A')}'"
            for res in results.results if not res.success
        ]
        error_msg = f"Quality Check falhou para a suíte '{suite_name}':\n" + "\n".join(failures)
        logger.error(error_msg)
        raise ValueError(error_msg)
    logger.info(f"Suíte de qualidade '{suite_name}' validada com sucesso via Great Expectations!")
