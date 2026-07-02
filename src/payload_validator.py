"""Validação do payload contra o contrato (contracts/payload.schema.json).

Roda ANTES de publicar. Se o payload violar o contrato, o pipeline falha aqui,
em vez de quebrar silenciosamente o pipeline downstream (EDA + previsão + dashboard).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List

try:
    from jsonschema import Draft202012Validator
except ImportError:  # jsonschema é opcional; sem ele, a validação é ignorada com aviso
    Draft202012Validator = None


class ContractValidationError(Exception):
    pass


def load_schema(schema_path: str | Path) -> dict:
    p = Path(schema_path)
    if not p.exists():
        raise FileNotFoundError(f"Contrato não encontrado: {p}")
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_payload(payload: dict, schema_path: str | Path) -> None:
    """Valida `payload` contra o schema. Lança ContractValidationError se inválido."""
    if Draft202012Validator is None:
        import warnings
        warnings.warn("jsonschema não instalado — validação de contrato ignorada.")
        return

    schema = load_schema(schema_path)
    validator = Draft202012Validator(schema)
    erros: List[str] = []
    for err in sorted(validator.iter_errors(payload), key=lambda e: list(e.path)):
        caminho = "/".join(str(p) for p in err.path) or "(raiz)"
        erros.append(f"  - {caminho}: {err.message}")

    if erros:
        raise ContractValidationError(
            "Payload não conforme ao contrato:\n" + "\n".join(erros)
        )