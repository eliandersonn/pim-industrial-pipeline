"""HTTP e contrato da API. Nenhuma conversão dos valores originais na Bronze."""

import json
import logging
import random
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from itertools import product

import requests

LOG = logging.getLogger(__name__)
METADATA_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/8888"
VALUES_URL = "https://apisidra.ibge.gov.br/values/t/8888"


class SidraError(ValueError):
    """Resposta inválida, incompleta ou incompatível com a consulta."""


def decode_json(raw):
    try:
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise SidraError("Resposta não contém JSON válido") from exc


class SidraClient:
    def __init__(self, session=None, sleep=time.sleep, attempts=4):
        if not 1 <= attempts <= 4:
            raise ValueError("attempts deve estar entre 1 e 4")
        self.session = session or requests.Session()
        self.sleep = sleep
        self.attempts = attempts

    def get(self, url):
        """Preserva os bytes do corpo; até quatro tentativas para erros transitórios."""
        for attempt in range(1, self.attempts + 1):
            response = None
            try:
                response = self.session.get(url, timeout=(10, 120))
                if response.status_code != 429 and response.status_code < 500:
                    response.raise_for_status()
                    decode_json(response.content)
                    return response.content
                response.raise_for_status()
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                transient = response is None or response.status_code == 429 or response.status_code >= 500
                if not transient or attempt == self.attempts:
                    raise
                delay = 2 ** (attempt - 1) + random.uniform(0, 0.5)
                retry_after = response.headers.get("Retry-After") if response is not None else None
                if retry_after:
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        try:
                            wait = (parsedate_to_datetime(retry_after) - datetime.now(timezone.utc)).total_seconds()
                        except (ValueError, TypeError, OverflowError):
                            wait = 0
                    delay = max(delay, wait)
                # Se a API pedir uma espera longa, deixamos o Airflow tentar mais tarde.
                if delay > 120:
                    raise SidraError("Retry-After excede 120 s; reagendar a execução") from exc
                LOG.warning(json.dumps({"event": "http_retry", "attempt": attempt,
                                        "status": response.status_code if response is not None else None,
                                        "delay_seconds": round(delay, 3)}))
                self.sleep(delay)
            finally:
                if response is not None:
                    response.close()

    def catalog(self):
        """Descobre os meses, locais, indicadores e atividades disponíveis na fonte."""
        meta = decode_json(self.get(f"{METADATA_URL}/metadados"))
        periods = decode_json(self.get(f"{METADATA_URL}/periodos"))
        levels = meta["nivelTerritorial"]["Administrativo"]
        locations = decode_json(self.get(f"{METADATA_URL}/localidades/{'|'.join(levels)}"))
        catalog = {"metadata": meta, "periods": periods, "locations": locations}
        validate_catalog(catalog)
        return catalog


def validate_catalog(catalog):
    """Interrompe a coleta se a estrutura da tabela mudar ou vier incompleta."""
    try:
        meta = catalog["metadata"]
        if (meta["id"] != 8888 or len(meta["classificacoes"]) != 1
                or meta["classificacoes"][0]["id"] != 544
                or meta["nivelTerritorial"]["Especial"] or meta["nivelTerritorial"]["IBGE"]):
            raise ValueError("Tabela ou classificação não suportada")
        for values in (catalog["periods"], catalog["locations"], meta["variaveis"],
                       meta["classificacoes"][0]["categorias"]):
            if not values or len(values) != len({str(x["id"]) for x in values}):
                raise ValueError("Dimensão vazia ou duplicada")
        for period in catalog["periods"]:
            datetime.strptime(str(period["id"]), "%Y%m")
        if (not all(x["nivel"]["id"] in {"N1", "N2", "N3"} for x in catalog["locations"])
                or set(meta["nivelTerritorial"]["Administrativo"]) != {x["nivel"]["id"] for x in catalog["locations"]}):
            raise ValueError("Níveis territoriais incompatíveis")
    except (KeyError, TypeError, ValueError) as exc:
        raise SidraError("Catálogo vazio, duplicado ou com estrutura não suportada") from exc


def plan_batches(catalog, start=None, end=None, max_values=40000):
    """Lotes de até um ano, abaixo do limite observado de 50.000 células."""
    if not 1 <= max_values <= 40000:
        raise ValueError("Teto operacional deve estar entre 1 e 40000")
    for bound in (start, end):
        if bound is not None and not re.fullmatch(r"\d{6}", bound):
            raise ValueError("Período deve usar YYYYMM")
        if bound:
            datetime.strptime(bound, "%Y%m")
    if start and end and start > end:
        raise ValueError("Início posterior ao fim")
    periods = sorted(str(p["id"]) for p in catalog["periods"])
    periods = [p for p in periods if (not start or p >= start) and (not end or p <= end)]
    if not periods:
        raise ValueError("Nenhum período disponível no intervalo")
    cells = (len(catalog["locations"]) * len(catalog["metadata"]["variaveis"])
             * len(catalog["metadata"]["classificacoes"][0]["categorias"]))
    size = min(12, max_values // cells)
    if size == 0:
        raise SidraError("Um mês excede o teto operacional; revisar divisão por dimensões")
    batches = []
    for year in sorted({p[:4] for p in periods}):
        annual = [p for p in periods if p.startswith(year)]
        batches.extend(annual[i:i + size] for i in range(0, len(annual), size))
    return batches


def values_url(catalog, periods):
    """Monta uma consulta com todas as dimensões e somente os meses deste lote."""
    groups = {}
    for location in catalog["locations"]:
        groups.setdefault(location["nivel"]["id"].lower(), []).append(str(location["id"]))
    territories = "/".join(f"{level}/{','.join(sorted(ids))}" for level, ids in sorted(groups.items()))
    variables = ",".join(str(v["id"]) for v in catalog["metadata"]["variaveis"])
    categories = ",".join(str(c["id"]) for c in catalog["metadata"]["classificacoes"][0]["categorias"])
    return f"{VALUES_URL}/{territories}/v/{variables}/p/{','.join(periods)}/c544/{categories}"


def validate_coverage(raw, catalog, periods):
    """Confere cada combinação esperada; contar linhas apenas não detecta trocas."""
    rows = decode_json(raw)
    required = {"NC", "D1C", "D2C", "D3C", "D4C", "V", "MC", "MN"}
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        raise SidraError("Esperada lista SIDRA com cabeçalho")
    header = rows[0]
    if (not required <= header.keys() or header["D2C"] != "Variável (Código)"
            or header["D3C"] != "Mês (Código)"
            or header["D4C"] != "Seções e atividades industriais (CNAE 2.0) (Código)"):
        raise SidraError("Cabeçalho SIDRA incompatível")
    expected = set(product(
        ((x["nivel"]["id"][1:], str(x["id"])) for x in catalog["locations"]),
        (str(x["id"]) for x in catalog["metadata"]["variaveis"]), periods,
        (str(x["id"]) for x in catalog["metadata"]["classificacoes"][0]["categorias"]),
    ))
    observed = set()
    markers = {}
    for row in rows[1:]:
        if not isinstance(row, dict) or not required <= row.keys() or not all(isinstance(row[k], str) for k in required):
            raise SidraError("Observação com campos ausentes ou tipos inválidos")
        key = ((row["NC"], row["D1C"]), row["D2C"], row["D3C"], row["D4C"])
        if key in observed:
            raise SidraError("Chave duplicada na resposta SIDRA")
        observed.add(key)
        if not re.fullmatch(r"-?\d+(?:\.\d+)?", row["V"]):
            markers[row["V"]] = markers.get(row["V"], 0) + 1
    missing, unexpected = expected - observed, observed - expected
    if missing or unexpected:
        raise SidraError(f"Cobertura inválida: {len(missing)} ausentes, {len(unexpected)} inesperadas")
    return {"rows": len(observed), "expected_rows": len(expected), "markers": markers}
