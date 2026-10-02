"""Escritas idempotentes em Delta Lake.

Todas as escritas do pipeline passam por aqui e sao seguras para reexecucao:
reprocessar o mesmo micro-batch (retry do job, checkpoint perdido) produz o mesmo
estado final da tabela.
"""
from __future__ import annotations

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame


def key_condition(keys: Sequence[str]) -> str:
    """Condicao de juncao do MERGE (``<=>`` para que chaves nulas tambem casem)."""
    return " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)


def newer_than_condition(order_cols: Sequence[str]) -> str:
    """Comparacao lexicografica ``(s.c1, s.c2, ...) > (t.c1, t.c2, ...)``.

    Usada no MERGE para so substituir a versao da tabela por uma versao mais nova,
    com o mesmo criterio de desempate da deduplicacao (``silver.deduplicate_latest``).
    """
    clauses = []
    for i, column in enumerate(order_cols):
        equal_prefix = [f"s.`{c}` = t.`{c}`" for c in order_cols[:i]]
        clauses.append("(" + " AND ".join([*equal_prefix, f"s.`{column}` > t.`{column}`"]) + ")")
    return " OR ".join(clauses)


def merge_insert_new(target: DeltaTable, source: DataFrame, keys: Sequence[str]) -> None:
    """Insere apenas registros cuja chave ainda nao existe (append idempotente).

    Linhas com a mesma chave no proprio batch sao identicas por construcao
    (a chave e um hash do conteudo), entao manter qualquer uma delas e equivalente.
    """
    (
        target.alias("t")
        .merge(source.dropDuplicates(list(keys)).alias("s"), key_condition(keys))
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_upsert_latest(
    target: DeltaTable,
    source: DataFrame,
    keys: Sequence[str],
    order_cols: Sequence[str],
) -> None:
    """Upsert por chave de negocio mantendo sempre a versao mais recente.

    ``source`` precisa estar deduplicado por ``keys`` (um registro por chave).
    """
    (
        target.alias("t")
        .merge(source.alias("s"), key_condition(keys))
        .whenMatchedUpdateAll(condition=newer_than_condition(order_cols))
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_replace_slice(
    target: DeltaTable,
    source: DataFrame,
    keys: Sequence[str],
    slice_column: str,
    slice_values: Sequence[str],
) -> None:
    """Substitui atomicamente uma fatia da tabela (ex.: todos os registros de alguns simbolos).

    Linhas da fatia presentes no ``source`` sao atualizadas/inseridas; linhas da fatia
    que nao existem mais no ``source`` sao removidas. Fora da fatia nada e tocado.
    """
    in_list = ", ".join("'" + v.replace("'", "''") + "'" for v in slice_values)
    (
        target.alias("t")
        .merge(source.alias("s"), key_condition(keys))
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(condition=f"t.`{slice_column}` IN ({in_list})")
        .execute()
    )
