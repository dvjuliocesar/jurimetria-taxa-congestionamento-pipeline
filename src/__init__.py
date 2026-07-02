"""Pipeline produtor da Taxa de Congestionamento Mensal (Jurimetria).

Fluxo: RAW (CSVs) -> TRUSTED (upsert por processo) -> REFINED (série + payload JSON).
Este pacote cobre apenas a limpeza, o tratamento, o cálculo e a geração do JSON.
Adaptadores opcionais (base de dados, broker) não fazem parte deste núcleo.
"""

__version__ = "1.0.0"