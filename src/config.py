"""Configuração do pipeline.

As regras de negócio (janela, chaves, caminhos das zonas) ficam fora do código,
em `config/settings.yaml`. Se o ficheiro não existir, usam-se os defaults abaixo,
de modo que o pipeline roda sem dependência obrigatória de YAML.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

try:
    import yaml
except ImportError:  # pyyaml é opcional; sem ele, usa apenas os defaults
    yaml = None


DEFAULTS = {
    # Zonas de dados
    "raw_dir": "data/01_raw",
    "trusted_dir": "data/02_trusted",
    "refined_dir": "data/03_refined",
    "trusted_filename": "core_processos.parquet",
    "serie_filename": "serie_tx_cong.parquet",
    "payload_filename": "payload_tx_mensal.json",
    # Leitura dos CSVs brutos
    "csv_sep": "#",
    "csv_encoding": "utf-8-sig",
    "raw_glob_pattern": "*.csv",       # ex.: "analise_txcg_*.csv" -- fica em config, não no código
    # Arquivamento: move CSVs já incorporados à trusted para fora do raw_dir,
    # para que a próxima execução não precise relê-los. Seguro porque a trusted
    # já persiste tudo de forma cumulativa (ver src/pipeline.py).
    "auto_archive": True,
    "processed_subdir": "processados",  # subpasta dentro de raw_dir (fora do glob)
    # Faixa plausível de anos, usada SÓ na auto-detecção do intervalo de
    # competências (--desde/--ate automáticos). Datas fora daqui (erro de
    # digitação, campo corrompido no extrato) são ignoradas para essa
    # heurística, mas permanecem intactas na trusted e no cálculo normal.
    "ano_minimo_plausivel": 1990,
    "ano_maximo_plausivel": 2100,
    # Piso FIXO para o "desde" -- só entra em ação na primeiríssima execução
    # (quando ainda não existe série). Nas execuções seguintes, o pipeline já
    # continua sozinho de onde parou (proxima_competencia()), então este piso
    # deixa de ter efeito. Use para dizer "não quero nada antes de X",
    # independente de qual seja a distribuição mais antiga real nos dados.
    "desde_fixo": None,
    # Modelo de dados
    "keys": ["comarca", "serventia"],          # chaves de agregação (nomes)
    "id_columns": [],                          # ex.: ["comarca_id","serventia_id"] se existirem no bruto
    "date_columns": ["data_distribuicao", "data_baixa"],
    "required_columns": ["processo_id", "comarca", "serventia",
                         "data_distribuicao", "data_baixa"],
    # Regra da métrica
    "rolling_window_months": 12,
    # Contrato / payload
    "schema_version": "1.0.0",
    "contract_path": "contracts/payload.schema.json",
    "validate_payload": True,
    # Competências FIXAS a calcular, se você quiser manter uma lista hardcoded.
    # Deixe vazia (padrão) para o pipeline detectar automaticamente o intervalo
    # que falta processar (ver --desde/--ate/auto-detecção em src/main.py).
    "competencias": [],
}


@dataclass
class Config:
    raw_dir: str
    trusted_dir: str
    refined_dir: str
    trusted_filename: str
    serie_filename: str
    payload_filename: str
    csv_sep: str
    csv_encoding: str
    raw_glob_pattern: str
    auto_archive: bool
    processed_subdir: str
    ano_minimo_plausivel: int
    ano_maximo_plausivel: int
    desde_fixo: str | None
    keys: List[str]
    id_columns: List[str]
    date_columns: List[str]
    required_columns: List[str]
    rolling_window_months: int
    schema_version: str
    contract_path: str
    validate_payload: bool
    competencias: List[str] = field(default_factory=list)

    # -- caminhos derivados -------------------------------------------------
    @property
    def trusted_path(self) -> Path:
        return Path(self.trusted_dir) / self.trusted_filename

    @property
    def serie_path(self) -> Path:
        return Path(self.refined_dir) / self.serie_filename

    @property
    def payload_path(self) -> Path:
        return Path(self.refined_dir) / self.payload_filename

    @property
    def processed_dir(self) -> Path:
        return Path(self.raw_dir) / self.processed_subdir

    @property
    def group_cols(self) -> List[str]:
        """Colunas de agrupamento efetivas (nomes + IDs estáveis, se houver)."""
        return list(self.keys) + [c for c in self.id_columns if c not in self.keys]

    @property
    def usecols(self) -> List[str]:
        """Colunas mínimas a ler do CSV (evita carregar as 60+ colunas do bruto)."""
        cols = list(dict.fromkeys(self.required_columns + self.id_columns))
        return cols

    # -- carga --------------------------------------------------------------
    @classmethod
    def load(cls, path: Optional[str] = "config/settings.yaml") -> "Config":
        data = dict(DEFAULTS)
        if path and Path(path).exists() and yaml is not None:
            with open(path, "r", encoding="utf-8") as f:
                user = yaml.safe_load(f) or {}
            data.update({k: v for k, v in user.items() if v is not None})
        # ignora chaves desconhecidas para não quebrar em evoluções do YAML
        known = {k: data[k] for k in DEFAULTS}
        return cls(**known)