"""Ponto de entrada do pipeline produtor.

Modos de uso:

  # AUTO (recomendado): detecta sozinho o intervalo que falta processar.
  # desde = competência seguinte à última já calculada (ou o mês mais antigo
  #         dos dados brutos, na primeiríssima execução);
  # até   = o mês mais recente com atividade real nos dados (nunca "inventa"
  #         competências futuras sem dado).
  python -m src.main

  # Fixa só o início; o fim continua automático.
  python -m src.main --desde 2024-01

  # Intervalo fixo dos dois lados.
  python -m src.main --desde 2020-01 --ate 2025-12

  # Lista explícita/esparsa (para recomputar meses específicos).
  python -m src.main --competencias 2026-01 2026-03

Ao final de uma execução bem-sucedida, os CSVs brutos já incorporados à
trusted são movidos para data/01_raw/processados/<timestamp>/, para que a
próxima execução não precise relê-los (ver --no-archive para desligar).
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime

import pandas as pd

try:  # funciona tanto como `python -m src.main` quanto dentro de src/
    from .config import Config
    from .pipeline import JurimetriaPipeline
    from .payload_validator import validate_payload, ContractValidationError
except ImportError:
    from config import Config
    from pipeline import JurimetriaPipeline
    from payload_validator import validate_payload, ContractValidationError


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Pipeline da Taxa de Congestionamento Mensal")
    p.add_argument("--config", default="config/settings.yaml",
                   help="caminho do settings.yaml (opcional)")
    p.add_argument("--competencias", nargs="*", default=None,
                   help="lista explícita de competências YYYY-MM (recompute pontual)")
    p.add_argument("--desde", default=None,
                   help="competência inicial YYYY-MM (default: auto-detectada)")
    p.add_argument("--ate", default=None,
                   help="competência final YYYY-MM (default: auto-detectada)")
    p.add_argument("--raw-dir", default=None, help="sobrescreve a zona RAW")
    p.add_argument("--no-archive", action="store_true",
                   help="não move os CSVs processados para processados/")
    return p.parse_args(argv)


def gerar_competencias(desde: str, ate: str) -> list[str]:
    """Lista de competências YYYY-MM entre desde e ate, inclusive."""
    periodo = pd.period_range(start=desde, end=ate, freq="M")
    return [str(p) for p in periodo]


def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    args = parse_args(argv)

    cfg = Config.load(args.config)
    if args.raw_dir:
        cfg.raw_dir = args.raw_dir
    if args.no_archive:
        cfg.auto_archive = False

    pipeline = JurimetriaPipeline(cfg)
    pipeline.ingest_and_upsert_trusted()          # RAW -> TRUSTED

    # --- Decide QUAIS competências calcular -------------------------------
    if args.competencias:
        competencias = args.competencias
        logging.info("Competências explícitas: %s .. %s (%d meses)",
                     competencias[0], competencias[-1], len(competencias))
    elif cfg.competencias:  # compatibilidade: lista fixa via settings.yaml
        competencias = cfg.competencias
        logging.info("Competências do settings.yaml: %s .. %s (%d meses)",
                     competencias[0], competencias[-1], len(competencias))
    else:
        proxima = pipeline.proxima_competencia()
        if proxima:
            desde = args.desde or proxima
        else:
            # Primeiríssima execução: sem série ainda. Usa o mínimo real da
            # trusted, mas nunca antes do piso fixo (se configurado).
            desde = args.desde or pipeline.competencia_minima_trusted()
            if cfg.desde_fixo and pd.Period(cfg.desde_fixo, freq="M") > pd.Period(desde, freq="M"):
                logging.info("desde_fixo (%s) é posterior ao mínimo real dos dados (%s); "
                            "usando o piso fixo.", cfg.desde_fixo, desde)
                desde = cfg.desde_fixo

        ate = args.ate or pipeline.competencia_maxima_trusted()
        # nunca ultrapassa o mês corrente, mesmo que os dados fossem mais recentes
        ate = min(ate, datetime.now().strftime("%Y-%m"))

        if pd.Period(desde, freq="M") > pd.Period(ate, freq="M"):
            logging.info("Nada novo para processar (já calculado até %s; sem "
                        "dados novos além disso).", pipeline.proxima_competencia())
            return 0

        competencias = gerar_competencias(desde, ate)
        logging.info("Competências auto-detectadas: %s .. %s (%d meses)",
                     desde, ate, len(competencias))

    pipeline.calcular_competencias(competencias)  # TRUSTED -> REFINED (incremental)

    payload = pipeline.build_payload()            # REFINED -> payload (série completa)

    if cfg.validate_payload:
        try:
            validate_payload(payload, cfg.contract_path)
            logging.info("Contrato: payload válido.")
        except (ContractValidationError, FileNotFoundError) as e:
            logging.error("Falha na validação de contrato — payload NÃO publicado.\n%s", e)
            return 1

    pipeline.save_payload(payload)                # publica o JSON

    arquivados = pipeline.archive_ingested_files()
    if arquivados:
        logging.info("%d arquivo(s) bruto(s) arquivado(s) em %s",
                     len(arquivados), cfg.processed_dir)

    logging.info("Concluído. JSON pronto para o pipeline downstream.")
    return 0


if __name__ == "__main__":
    sys.exit(main())