"""Carga BigQuery e publicação atômica da candidata validada."""

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"

# Dicionário único: as mesmas definições documentam as tabelas atuais e as próximas cargas.
COLUMN_DESCRIPTIONS = {
    "tabela_id": "Código da tabela SIDRA de origem; 8888 identifica a PIM-PF Regional.",
    "nivel_territorial": "Nível IBGE: 1 = Brasil, 2 = grande região, 3 = unidade da federação.",
    "localidade_id": "Código original IBGE da localidade; STRING preserva sua representação.",
    "localidade_nome": "Nome da localidade retornado pelo IBGE.",
    "periodo": "Mês de referência no formato AAAAMM, conforme a fonte SIDRA.",
    "variavel_id": "Código do indicador SIDRA; 12606 é o número-índice selecionado para a Gold.",
    "variavel_nome": "Descrição do indicador conforme o catálogo do IBGE.",
    "classificacao_id": "Código da classificação SIDRA; 544 representa seções e atividades CNAE 2.0.",
    "categoria_id": "Código SIDRA da seção, atividade ou agregado industrial na classificação 544.",
    "categoria_nome": "Nome da seção, atividade ou agregado industrial conforme o IBGE.",
    "unidade_codigo": "Código da unidade de medida na fonte; pode estar vazio para dados indisponíveis.",
    "unidade_nome": "Unidade do indicador conforme o IBGE; pode estar vazia para dados indisponíveis.",
    "valor_original": "Texto recebido do IBGE, incluindo marcadores; preservado para auditoria.",
    "status_valor": "Disponibilidade: valido, sem_indice, nao_aplicavel, indisponivel, sigilo ou ausente.",
    "run_id": "Identificador da coleta que originou a linha; vincula dados, manifestos e recibos GCS.",
    "source_uri": "URI do JSON original comprimido na camada Bronze do GCS.",
    "source_sha256": "SHA-256 dos bytes JSON originais, antes da compressão gzip.",
    "rules_version": "Versão das regras de transformação aplicadas à Silver.",
    "mes_referencia": "Primeiro dia do mês ao qual o indicador se refere; não é a data da carga.",
    "valor": "Valor do indicador em NUMERIC(38,9), na unidade da fonte; marcadores tornam-se NULL.",
    "ingested_at": "Data e hora UTC da coleta original na Bronze.",
    "uf_codigo": "Código IBGE da unidade da federação, armazenado como STRING.",
    "uf_nome": "Nome da unidade da federação conforme o IBGE.",
    "atividade_codigo": "Código SIDRA da atividade de transformação na classificação CNAE 2.0 (544).",
    "atividade_nome": "Nome da atividade industrial de transformação conforme o IBGE.",
    "indice_atividade": "Número-índice da variável SIDRA 12606, base média 2022 = 100; NULL se não divulgado.",
    "media_indices_uf_mes": "Média aritmética dos índices numéricos das atividades na UF/mês; ignora NULL e não é o agregado oficial ponderado.",
    "quantidade_atividades_validas": "Quantidade de índices numéricos usados como denominador da média na UF/mês.",
    "diferenca": "Índice da atividade menos a média da UF/mês, em pontos de índice; pode ser negativa e é NULL para índice ausente.",
}
TABLE_DESCRIPTIONS = {
    "staging": "Carga auxiliar dos dados Silver da SIDRA 8888 referentes a 2025, com todos os indicadores e níveis territoriais. Após publicação, preservada somente para a execução atual e a anterior; uso interno do pipeline.",
    "validacao": "Candidata da análise PIM 2025: uma linha por UF, mês e atividade de transformação. Validada antes da publicação; após publicação, preservada somente para a execução atual e a anterior. Não é a tabela de consumo.",
    "gold": "Análise PIM-PF Regional 2025 (IBGE/SIDRA 8888, variável 12606). Uma linha por UF/mês/atividade de transformação, com índice, média simples dos índices disponíveis e diferença em pontos de índice. Chave: uf_codigo, mes_referencia, atividade_codigo. Dados públicos; sem expiração temporária.",
}


def described_schema(schema):
    """Preserva tipos e demais propriedades; exige definição para cada coluna recebida."""
    unknown = {field.name for field in schema} - COLUMN_DESCRIPTIONS.keys()
    if unknown:
        raise ValueError(f"Colunas sem descrição no dicionário: {sorted(unknown)}")
    return [bigquery.SchemaField.from_api_repr({**field.to_api_repr(),
            "description": COLUMN_DESCRIPTIONS[field.name]}) for field in schema]


def table_labels(layer):
    return {"fonte": "ibge_sidra", "camada": layer, "ano_referencia": "2025", "classificacao": "publico"}


def table_id(project, dataset, table):
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,61}[a-z0-9]", project):
        raise ValueError("Projeto GCP inválido")
    if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,200}", item) for item in (dataset, table)):
        raise ValueError("Dataset/tabela inválido")
    return f"{project}.{dataset}.{table}"


class GoldLoader:
    def __init__(self, project, dataset, location, client=None, max_bytes=1_000_000_000):
        self.project, self.dataset, self.location = project, dataset, location
        self.gold = table_id(project, dataset, "pim_transformacao_2025")
        self.client = client or bigquery.Client(project=project, location=location)
        self.max_bytes = max_bytes

    def query(self, filename, **identifiers):
        # Os nomes de tabelas já passaram por table_id; não recebemos SQL livre.
        return (SQL_DIR / filename).read_text(encoding="utf-8").format(**identifiers)

    def describe(self, identifier, layer, run_id=None):
        """Reaplica os metadados após cargas que podem substituir o schema da tabela."""
        table = self.client.get_table(identifier)
        if not run_id and table.description:
            marker = " Coleta de origem (run_id): "
            if marker in table.description:
                run_id = table.description.split(marker, 1)[1].removesuffix(".")
        table.schema = described_schema(table.schema)
        table.description = TABLE_DESCRIPTIONS[layer]
        if run_id:
            table.description += f" Coleta de origem (run_id): {run_id}."
        table.labels = {**table.labels, **table_labels(layer)}
        return self.client.update_table(table, ["schema", "description", "labels"])

    def load(self, manifest):
        """Carrega apenas 2025 em uma tabela temporária, sem alterar a Gold atual."""
        if manifest["scope"] != "full" or manifest["status"] != "complete":
            raise ValueError("Publicação exige snapshot completo; aplicar baseline para backfill parcial")
        expected = {f"2025{m:02}" for m in range(1, 13)}
        if not expected <= set(manifest["periods"]):
            raise ValueError("Snapshot sem os 12 meses de 2025")
        from .ingestion import sha256
        suffix = sha256(manifest["run_id"].encode())[:16]
        stage = table_id(self.project, self.dataset, f"stg_pim_sidra_2025_{suffix}")
        candidate = table_id(self.project, self.dataset, f"val_pim_transformacao_2025_{suffix}")
        batches = [b for b in manifest["batches"] if any(p.startswith("2025") for p in b["periods"])]
        uris = [b["uri"] for b in batches]
        if not uris or not all(uri.startswith("gs://") for uri in uris):
            raise ValueError("BigQuery exige URIs Silver GCS explícitas")
        for identifier in (stage, candidate):
            table = bigquery.Table(identifier)
            table.expires = datetime.now(timezone.utc) + timedelta(days=2)
            self.client.create_table(table, exists_ok=True)
        config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,
                                       write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        job = self.client.load_table_from_uri(uris, stage, job_config=config, location=self.location)
        job.result(timeout=600)
        if job.output_rows != sum(b["rows"] for b in batches):
            raise ValueError("Carga staging com contagem divergente da Silver")
        self.describe(stage, "staging", manifest["run_id"])
        return {"stage": stage, "candidate": candidate, "load_job_id": job.job_id,
                "expected_rows": sum(b["rows"] for b in batches), "run_id": manifest["run_id"]}

    def build_and_validate(self, state):
        """Calcula a candidata e confere seus dados antes de permitir a publicação."""
        query = self.query("create_gold.sql", silver_table=state["stage"])
        config = bigquery.QueryJobConfig(destination=state["candidate"],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, maximum_bytes_billed=self.max_bytes)
        job = self.client.query(query, job_config=config, location=self.location)
        job.result(timeout=600)
        validation = self.validate(state["candidate"])
        # O ranking também precisa funcionar antes de substituir a tabela final.
        top = self.top3(state["candidate"])
        self.describe(state["candidate"], "validacao", state.get("run_id"))
        return {**state, "build_job_id": job.job_id, "validation": validation, "top3_rows": len(top)}

    def validate(self, table):
        # O recorte do case contém 17 UFs × 12 meses × 24 atividades = 4.896 linhas.
        config = bigquery.QueryJobConfig(maximum_bytes_billed=self.max_bytes)
        rows = list(self.client.query(self.query("validate_gold.sql", gold_table=table),
                                     job_config=config, location=self.location).result(timeout=300))
        if len(rows) != 1 or rows[0]["invalid_rows"] != 0 or rows[0]["rows_count"] != 4896:
            raise ValueError("Candidata Gold inválida: cálculos, chaves ou cobertura")
        return dict(rows[0])

    def publish(self, state):
        # A troca é atômica: a Gold anterior só sai quando a nova tabela está pronta.
        # Auxiliares ainda não publicadas expiram em dois dias, caso a execução falhe.
        # As descrições entram no próprio DDL: a Gold já nasce documentada na troca.
        schema = described_schema(self.client.get_table(state["candidate"]).schema)
        current = state["candidate"].rsplit("_", 1)[-1]
        if not re.fullmatch(r"[0-9a-f]{16}", current):
            raise ValueError("Identificador da candidata inválido")
        try:
            previous_labels = self.client.get_table(self.gold).labels
        except NotFound:
            previous_labels = {}
        # Repetir a publicação da mesma coleta não descarta a execução anterior.
        previous = previous_labels.get("execucao_atual")
        if previous == current:
            previous = previous_labels.get("execucao_anterior")
        publication_labels = {**table_labels("gold"), "execucao_atual": current}
        if previous:
            publication_labels["execucao_anterior"] = previous
        columns = ", ".join(f"`{f.name}` {f.field_type} OPTIONS(description={json.dumps(f.description, ensure_ascii=False)})"
                            for f in schema)
        labels = ", ".join(f"({json.dumps(k)}, {json.dumps(v)})" for k, v in publication_labels.items())
        sql = (f"CREATE OR REPLACE TABLE `{self.gold}` ({columns}) "
               f"OPTIONS(expiration_timestamp=NULL, description={json.dumps(TABLE_DESCRIPTIONS['gold'], ensure_ascii=False)}, labels=[{labels}]) "
               f"AS SELECT * FROM `{state['candidate']}`")
        job = self.client.query(sql, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=self.max_bytes),
                                location=self.location)
        job.result(timeout=600)
        removed = self.retain_auxiliaries(current, previous)
        return {**state, "gold_table": self.gold, "publish_job_id": job.job_id,
                "removed_auxiliary_tables": removed}

    def retain_auxiliaries(self, current, previous=None):
        """Só após publicar: mantém os dois pares e remove auxiliares mais antigas."""
        keep = {suffix for suffix in (current, previous) if suffix}
        if not keep or any(not re.fullmatch(r"[0-9a-f]{16}", suffix) for suffix in keep):
            raise ValueError("Identificador de retenção inválido")
        # Confere e preserva os pares antes de excluir qualquer tabela antiga.
        for suffix in sorted(keep):
            for prefix, layer in (("stg_pim_sidra_2025_", "staging"),
                                  ("val_pim_transformacao_2025_", "validacao")):
                identifier = table_id(self.project, self.dataset, prefix + suffix)
                table = self.describe(identifier, layer)
                table.expires = None
                self.client.update_table(table, ["expires"])
        removed = []
        for table in self.client.list_tables(f"{self.project}.{self.dataset}"):
            match = re.fullmatch(r"(?:stg_pim_sidra_2025_|val_pim_transformacao_2025_)([0-9a-f]{16})", table.table_id)
            if match and match[1] not in keep:
                self.client.delete_table(table.reference, not_found_ok=True)
                removed.append(table.table_id)
        return sorted(removed)

    def top3(self, table=None):
        config = bigquery.QueryJobConfig(maximum_bytes_billed=self.max_bytes)
        return [dict(row) for row in self.client.query(
            self.query("top3_atividades.sql", gold_table=table or self.gold),
            job_config=config, location=self.location).result(timeout=300)]
