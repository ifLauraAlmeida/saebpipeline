#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Exemplo de extensão do tratamento para outros diretórios
(DIRETOR, DOCENTES, ESCOLAS) e múltiplos anos
"""

from src.tratamento import extrair_zip, tratar_txt_para_csv
from src.logger import setup_logger

logger = setup_logger(__name__)


def processar_ano_completo(ano, bronze_dir="data/bronze", silver_dir="data/silver"):
    """Processa todos os diretórios de um ano."""

    logger.info(f"\n{'='*60}")
    logger.info(f"PROCESSANDO ANO {ano} COMPLETO")
    logger.info(f"{'='*60}\n")

    # Extrai zip uma única vez
    extrair_zip(ano, bronze_dir)

    # Lista de diretórios a processar
    subdirs = ["ALUNOS", "DIRETOR", "DOCENTES", "ESCOLAS"]

    for subdir in subdirs:
        logger.info(f"\n{'─'*50}")
        logger.info(f"Processando {subdir}")
        logger.info(f"{'─'*50}")
        try:
            tratar_txt_para_csv(ano, bronze_dir, silver_dir, subdir=subdir)
        except Exception as e:
            logger.error(f"Erro ao processar {subdir}: {str(e)}")
            continue

    logger.info(f"\n{'='*60}")
    logger.info(f"Processamento do ano {ano} concluído!")
    logger.info(f"{'='*60}\n")


def processar_multiplos_anos(anos, bronze_dir="data/bronze", silver_dir="data/silver"):
    """Processa múltiplos anos."""
    for ano in anos:
        try:
            processar_ano_completo(ano, bronze_dir, silver_dir)
        except Exception as e:
            logger.error(f"Erro ao processar ano {ano}: {str(e)}")
            continue


if __name__ == "__main__":
    # Exemplo 1: Processar ano 1995 completo
    # processar_ano_completo(1995)

    # Exemplo 2: Processar apenas um diretório (DIRETOR)
    # tratar_txt_para_csv(1995, "data/bronze", "data/silver", subdir="DIRETOR")

    # Exemplo 3: Processar múltiplos anos
    # anos_disponiveis = [1995, 1997, 1999, 2001, 2003, 2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021, 2023]
    # processar_multiplos_anos(anos_disponiveis)

    # Por padrão, processa apenas 1995 ALUNOS (veja src/tratamento.py main())
    print("Use as funções acima para estender o processamento:")
    print("- processar_ano_completo(ano): Processa todos os diretórios de um ano")
    print("- processar_multiplos_anos(anos): Processa múltiplos anos")
    print(
        "- tratar_txt_para_csv(ano, ..., subdir='DIRETOR'): Processa um diretório específico"
    )
