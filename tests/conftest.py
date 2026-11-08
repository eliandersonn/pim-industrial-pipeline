import copy
import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def catalog():
    def read(name):
        return json.loads((FIXTURES / name).read_text(encoding="utf-8-sig"))
    return {"metadata": read("metadados_8888.json"), "periods": read("periodos_8888.json"),
            "locations": read("localidades_8888.json")}


@pytest.fixture
def sample():
    return (FIXTURES / "amostra_202501.json").read_bytes()


@pytest.fixture
def tiny_catalog(catalog):
    result = copy.deepcopy(catalog)
    result["periods"] = [{"id": "202501"}, {"id": "202502"}]
    result["locations"] = [result["locations"][0]]
    result["metadata"]["nivelTerritorial"]["Administrativo"] = ["N1"]
    result["metadata"]["variaveis"] = result["metadata"]["variaveis"][:1]
    result["metadata"]["classificacoes"][0]["categorias"] = result["metadata"]["classificacoes"][0]["categorias"][:1]
    return result
