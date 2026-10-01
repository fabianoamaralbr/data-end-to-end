"""Camada bronze: dados como recebidos + metadados de ingestao + chave de ingestao."""
from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Separadores que nao aparecem em CSV/JSON de origem: evitam colisoes do tipo
# ("ab", "c") vs ("a", "bc") e distinguem NULL de string vazia.
_FIELD_SEP = "\u001f"
_NULL_MARK = "\u0000"


def ingestion_key(columns: Sequence[str]) -> Column:
    """Hash deterministico (arquivo de origem + conteudo do registro).

    Reingerir o mesmo arquivo gera as mesmas chaves, entao o MERGE insert-only da
    bronze descarta o que ja foi carregado: reexecutar a ingestao nao duplica dados.
    Um arquivo reescrito com valores corrigidos gera chaves novas (nova versao do
    registro), que a silver resolve pela ordem de ingestao.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit(_NULL_MARK)) for c in ["_source_file", *columns]]
    return F.sha2(F.concat_ws(_FIELD_SEP, *parts), 256)


def add_ingestion_metadata(df: DataFrame, *, business_columns: Sequence[str], run_id: str) -> DataFrame:
    """Adiciona ``_ingested_at``, ``_run_id`` e ``_ingestion_key``.

    Espera ``_source_file``/``_source_modified_at`` (de ``_metadata`` do Auto Loader)
    e ``_rescued_data`` ja presentes no DataFrame.
    """
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingestion_key", ingestion_key([*business_columns, "_rescued_data"]))
    )
