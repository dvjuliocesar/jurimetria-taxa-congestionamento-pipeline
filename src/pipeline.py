"""Núcleo do pipeline: limpeza, tratamento, cálculo da taxa e geração do payload.

Invariantes de negócio preservadas:
  * Upsert no grão `processo_id` (a versão com baixa preenchida vence a nula).
  * `data_baixa` nula = estoque pendente (processo não concluído).
  * Estoque no fim do mês calculado de forma ABSOLUTA por mês (point-in-time),
    o que torna cada competência imutável e idempotente.
  * Denominador com janela deslizante de 12 meses de baixados.

Correção importante: as datas são normalizadas (hora zerada) na ingestão, para
que comparações contra o fim do mês (00:00 do último dia) não excluam baixas
ocorridas no último dia do mês.
"""

from __future__ import annotations

import glob
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class SchemaError(Exception):
    """CSV bruto sem as colunas exigidas."""


class JurimetriaPipeline:
    def __init__(self, cfg):
        self.cfg = cfg
        self.df_trusted: pd.DataFrame | None = None
        self._arquivos_ingeridos: List[Path] = []

    # ================================================================== #
    # RAW -> TRUSTED
    # ================================================================== #
    def ingest_and_upsert_trusted(self) -> pd.DataFrame:
        """Lê os CSVs novos da zona RAW, limpa, e faz upsert na tabela core.

        A tabela core é persistida em Parquet (zona TRUSTED), tornando o upsert
        durável entre execuções: um lote novo atualiza o estado sem reprocessar
        os CSVs anteriores.
        """
        cfg = self.cfg
        padrao = str(Path(cfg.raw_dir) / cfg.raw_glob_pattern)
        arquivos = sorted(glob.glob(padrao))

        if not arquivos:
            if cfg.trusted_path.exists():
                # Normal em execuções agendadas: nenhum extrato novo hoje.
                # A trusted já persiste tudo que foi ingerido antes -- só carrega.
                logger.info("Ingestão: nenhum CSV novo em %s. Usando trusted existente.",
                           cfg.raw_dir)
                self.df_trusted = pd.read_parquet(cfg.trusted_path)
                self._arquivos_ingeridos = []
                return self.df_trusted
            # Primeiríssima execução: sem CSV e sem trusted, não há de onde partir.
            raise ValueError(
                f"Nenhum CSV encontrado em: {cfg.raw_dir} (padrão: {cfg.raw_glob_pattern}) "
                "e ainda não existe trusted persistida."
            )

        logger.info("Ingestão: %d ficheiro(s) em %s", len(arquivos), cfg.raw_dir)
        novos = []
        for arq in arquivos:
            logger.info("  lendo %s", arq)
            df_tmp = pd.read_csv(
                arq, sep=cfg.csv_sep, encoding=cfg.csv_encoding,
                usecols=lambda c, cols=set(cfg.usecols): c in cols,  # só o essencial
                low_memory=False,
            )
            self._validar_schema(df_tmp, arq)
            novos.append(df_tmp)

        df_novo = pd.concat(novos, ignore_index=True)
        logger.info("Ingestão: %d registos brutos lidos", len(df_novo))

        df_novo = self._limpar(df_novo)

        # Combina com o estado anterior (se existir) e faz o upsert.
        if cfg.trusted_path.exists():
            df_ant = pd.read_parquet(cfg.trusted_path)
            df_all = pd.concat([df_ant, df_novo], ignore_index=True)
        else:
            df_all = df_novo

        self.df_trusted = self._upsert_processos(df_all)

        cfg.trusted_path.parent.mkdir(parents=True, exist_ok=True)
        self.df_trusted.to_parquet(cfg.trusted_path, index=False)
        logger.info("Trusted: %d processos únicos -> %s",
                    len(self.df_trusted), cfg.trusted_path)

        # Guardado para archive_ingested_files(): só arquivamos DEPOIS que todo
        # o resto do pipeline (cálculo + validação + payload) tiver funcionado.
        self._arquivos_ingeridos = [Path(a) for a in arquivos]
        return self.df_trusted

    def _validar_schema(self, df: pd.DataFrame, origem: str) -> None:
        faltando = set(self.cfg.usecols) - set(df.columns)
        if faltando:
            raise SchemaError(f"{origem}: colunas em falta {sorted(faltando)}")

    def _limpar(self, df: pd.DataFrame) -> pd.DataFrame:
        """Tipagem de datas + normalização + descarte de distribuição inválida."""
        for col in self.cfg.date_columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.normalize()
        antes = len(df)
        df = df.dropna(subset=["data_distribuicao"]).copy()
        removidos = antes - len(df)
        if removidos:
            logger.info("Limpeza: %d registos sem data_distribuicao removidos", removidos)
        return df

    def archive_ingested_files(self) -> List[Path]:
        """Move os CSVs já incorporados à trusted para uma subpasta de arquivo,
        para que a próxima execução não precise relê-los.

        Seguro por construção: a trusted (core_processos.parquet) já persiste
        cumulativamente tudo o que foi ingerido até agora (ver
        ingest_and_upsert_trusted, que sempre lê a trusted existente + o que
        há de novo). Mover os CSVs brutos não apaga nenhum dado -- eles já
        estão salvos noutro formato, mais compacto e mais rápido de ler.

        Só deve ser chamado DEPOIS que o restante do pipeline (cálculo +
        validação de contrato + gravação do payload) tiver concluído com
        sucesso, para que uma falha no meio do caminho deixe os CSVs no
        lugar, prontos para nova tentativa.
        """
        if not self.cfg.auto_archive or not self._arquivos_ingeridos:
            return []

        destino_base = self.cfg.processed_dir / datetime.now().strftime("%Y%m%dT%H%M%S")
        destino_base.mkdir(parents=True, exist_ok=True)

        movidos = []
        for origem in self._arquivos_ingeridos:
            destino = destino_base / origem.name
            origem.rename(destino)
            movidos.append(destino)
            logger.info("Arquivado: %s -> %s", origem, destino)

        self._arquivos_ingeridos = []
        return movidos

    def _filtrar_plausiveis(self, serie_datas: pd.Series, rotulo: str) -> pd.Series:
        """Descarta, SÓ para fins de auto-detecção do intervalo, datas fora da
        faixa plausível (config: ano_minimo_plausivel/ano_maximo_plausivel).
        Não afeta a trusted nem o cálculo normal -- essas linhas continuam lá,
        intactas, e entram no cálculo se uma competência específica cobrir a
        data delas."""
        minimo = pd.Timestamp(year=self.cfg.ano_minimo_plausivel, month=1, day=1)
        maximo = pd.Timestamp(year=self.cfg.ano_maximo_plausivel, month=12, day=31)
        validas = serie_datas.dropna()
        plausiveis = validas[(validas >= minimo) & (validas <= maximo)]
        descartadas = len(validas) - len(plausiveis)
        if descartadas:
            logger.warning(
                "%d data(s) em '%s' fora da faixa plausível [%d-%d] foram "
                "ignoradas SÓ para a auto-detecção do intervalo de competências "
                "(continuam intactas na trusted). Verifique a qualidade dos "
                "dados brutos -- provável erro de digitação na origem.",
                descartadas, rotulo, self.cfg.ano_minimo_plausivel, self.cfg.ano_maximo_plausivel,
            )
        if plausiveis.empty:
            raise RuntimeError(
                f"Nenhuma data plausível em '{rotulo}' dentro de "
                f"[{self.cfg.ano_minimo_plausivel}-{self.cfg.ano_maximo_plausivel}]. "
                "Ajuste ano_minimo_plausivel/ano_maximo_plausivel no settings.yaml "
                "ou passe --desde/--ate explicitamente."
            )
        return plausiveis

    def proxima_competencia(self) -> str | None:
        """Competência seguinte à última já calculada na série refined.
        Retorna None se ainda não existir série (primeira execução)."""
        if not self.cfg.serie_path.exists():
            return None
        serie = pd.read_parquet(self.cfg.serie_path)
        if serie.empty:
            return None
        maxima = pd.Period(serie["competencia"].max(), freq="M")
        return str(maxima + 1)

    def competencia_minima_trusted(self) -> str:
        """Mês da distribuição plausível mais antiga na trusted. Usado só
        quando ainda não há série (primeira execução), para descobrir onde
        começar automaticamente."""
        if self.df_trusted is None or self.df_trusted.empty:
            raise RuntimeError("Trusted vazia. Rode ingest_and_upsert_trusted() primeiro.")
        plausiveis = self._filtrar_plausiveis(self.df_trusted["data_distribuicao"], "data_distribuicao")
        return plausiveis.min().strftime("%Y-%m")

    def competencia_maxima_trusted(self) -> str:
        """Teto natural para o intervalo automático: o mês plausível mais
        recente com atividade REAL na trusted (distribuição OU baixa, o que
        for maior). Evita gerar competências 'do futuro' sem nenhum dado --
        ver o caso de baixas em 2026 sem distribuições novas em 2026: usar só
        a distribuição deixaria essas baixas de fora do intervalo automático."""
        if self.df_trusted is None or self.df_trusted.empty:
            raise RuntimeError("Trusted vazia. Rode ingest_and_upsert_trusted() primeiro.")
        candidatos = pd.concat([
            self._filtrar_plausiveis(self.df_trusted["data_distribuicao"], "data_distribuicao"),
            self._filtrar_plausiveis(self.df_trusted["data_baixa"], "data_baixa"),
        ])
        return candidatos.max().strftime("%Y-%m")

    def _upsert_processos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Dedup no grão processo_id mantendo a versão mais atualizada.

        Ordena com nulos primeiro; keep='last' faz uma baixa preenchida vencer
        a nula. Em caso de duas baixas preenchidas, mantém a mais recente.
        """
        df = df.sort_values(["processo_id", "data_baixa"], na_position="first")
        return df.drop_duplicates(subset=["processo_id"], keep="last").reset_index(drop=True)

    # ================================================================== #
    # TRUSTED -> REFINED
    # ================================================================== #
    def point_in_time_snapshot(self, competencia: str) -> pd.DataFrame:
        """Métricas de UMA competência (YYYY-MM), calculadas de forma absoluta."""
        if self.df_trusted is None:
            raise RuntimeError("Trusted não carregada. Rode ingest_and_upsert_trusted().")

        g = self.cfg.group_cols
        w = self.cfg.rolling_window_months

        m = pd.to_datetime(competencia)
        inicio_mes = m.replace(day=1)
        fim_mes = inicio_mes + pd.offsets.MonthEnd(1)
        inicio_janela = inicio_mes - pd.DateOffset(months=w - 1)

        df = self.df_trusted

        dist = (df[(df.data_distribuicao >= inicio_mes) & (df.data_distribuicao <= fim_mes)]
                .groupby(g).size().rename("processos_distribuidos"))

        baix = (df[(df.data_baixa >= inicio_mes) & (df.data_baixa <= fim_mes)]
                .groupby(g).size().rename("processos_baixados"))

        # Estoque no fim do mês: distribuído até fim_mes e ainda não baixado
        # aos olhos deste mês (baixa nula OU baixa em mês futuro).
        pend = (df[(df.data_distribuicao <= fim_mes) &
                   (df.data_baixa.isna() | (df.data_baixa > fim_mes))]
                .groupby(g).size().rename("estoque_pendente"))

        baix12 = (df[(df.data_baixa >= inicio_janela) & (df.data_baixa <= fim_mes)]
                  .groupby(g).size().rename("baixados_ultimos_12_meses"))

        met = pd.concat([dist, baix, pend, baix12], axis=1).fillna(0).astype(int)

        denom = met["estoque_pendente"] + met["baixados_ultimos_12_meses"]
        met["taxa_congestionamento_pct"] = np.where(
            denom > 0, (met["estoque_pendente"] / denom) * 100, 0.0
        ).round(2)

        met = met.reset_index()
        met.insert(0, "competencia", competencia)
        return met

    def calcular_competencias(self, competencias: List[str]) -> pd.DataFrame:
        """Calcula APENAS as competências pedidas (incremental) e faz upsert
        na série completa do refined. As competências anteriores permanecem
        imutáveis."""
        if not competencias:
            raise ValueError("Lista de competências vazia.")
        novos = pd.concat(
            [self.point_in_time_snapshot(c) for c in competencias],
            ignore_index=True,
        )
        for c in competencias:
            logger.info("Refined: métricas calculadas para %s", c)
        return self._upsert_refined(novos)

    def _upsert_refined(self, novos: pd.DataFrame) -> pd.DataFrame:
        cfg = self.cfg
        chave = cfg.group_cols + ["competencia"]
        cfg.serie_path.parent.mkdir(parents=True, exist_ok=True)

        if cfg.serie_path.exists():
            hist = pd.read_parquet(cfg.serie_path)
            idx_novos = pd.MultiIndex.from_frame(novos[chave])
            idx_hist = pd.MultiIndex.from_frame(hist[chave])
            hist = hist[~idx_hist.isin(idx_novos)]          # remove competências recalculadas
            serie = pd.concat([hist, novos], ignore_index=True)
        else:
            serie = novos

        serie = serie.sort_values(cfg.group_cols + ["competencia"]).reset_index(drop=True)
        serie.to_parquet(cfg.serie_path, index=False)
        logger.info("Refined: série completa com %d linhas -> %s",
                    len(serie), cfg.serie_path)
        return serie

    # ================================================================== #
    # REFINED -> PAYLOAD (série COMPLETA, para o pipeline downstream)
    # ================================================================== #
    def build_payload(self) -> dict:
        """Monta o payload (envelope + série histórica completa).

        O downstream (EDA + previsão) precisa de toda a história, não só das
        competências recém-calculadas — por isso o payload é a série completa.
        """
        cfg = self.cfg
        if not cfg.serie_path.exists():
            raise RuntimeError("Série refined inexistente. Rode calcular_competencias().")

        serie = pd.read_parquet(cfg.serie_path).sort_values(
            cfg.group_cols + ["competencia"]
        )

        # to_json converte tipos numpy corretamente; recarregamos como objeto Python.
        registos = json.loads(serie.to_json(orient="records", force_ascii=False))

        corpo = json.dumps(registos, ensure_ascii=False, sort_keys=True)
        checksum = hashlib.sha256(corpo.encode("utf-8")).hexdigest()

        payload = {
            "schema_version": cfg.schema_version,
            "metadata": {
                "gerado_em": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "metrica": "taxa_congestionamento_mensal",
                "janela_baixados_meses": cfg.rolling_window_months,
                "grao": cfg.group_cols + ["competencia"],
                "competencia_min": serie["competencia"].min(),
                "competencia_max": serie["competencia"].max(),
                "record_count": int(len(serie)),
                "checksum_sha256": checksum,
            },
            "data": registos,
        }
        return payload

    def save_payload(self, payload: dict) -> Path:
        cfg = self.cfg
        cfg.payload_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cfg.payload_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        logger.info("Payload gravado -> %s (%d registos)",
                    cfg.payload_path, payload["metadata"]["record_count"])
        return cfg.payload_path