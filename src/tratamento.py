import os
import zipfile
import pandas as pd
import re
from pathlib import Path
from logger import setup_logger

# Setup logger
logger = setup_logger(__name__)

# Anos com TXT (fixed-width)
ANOS_TXT = [1995, 1997, 2001, 2003, 2005]
# Anos com CSV (já têm cabeçalho)
ANOS_CSV = [2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021, 2023]


def extrair_zip(ano, bronze_dir):
    """Extrai todos os arquivos zip do diretório bronze/ano, se ainda não foram extraídos."""
    ano_dir = Path(bronze_dir) / str(ano)

    # Verifica se os arquivos já foram extraídos
    dados_dir = ano_dir / "DADOS"
    inputs_dir = ano_dir / "INPUTS_SAS_SPSS"

    if dados_dir.exists() and inputs_dir.exists():
        logger.info(f"Ano {ano}: Arquivos já extraídos. Pulando descompactação.")
        return

    logger.info(f"Ano {ano}: Iniciando descompactação...")
    for file in os.listdir(ano_dir):
        if file.endswith(".zip"):
            zip_path = ano_dir / file
            try:
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(ano_dir)
                logger.info(f"  ✓ Descompactado: {file}")
            except Exception as e:
                logger.error(f"  ✗ Erro ao descompactar {file}: {str(e)}")


def ler_estrutura_sas(sas_path):
    """Lê um arquivo .sas e retorna nomes e colspecs das colunas."""
    nomes = []
    posicoes = []

    try:
        with open(sas_path, encoding="latin1") as f:
            for line in f:
                match = re.match(r"@([0-9]+)\s+([A-Z0-9_]+)\s+([\$A-Za-z0-9\.]+)", line)
                if match:
                    pos = int(match.group(1))
                    nome = match.group(2)
                    posicoes.append((pos, nome))
    except Exception as e:
        logger.error(f"Erro ao ler {sas_path}: {str(e)}")
        return [], []

    if not posicoes:
        logger.error(f"Nenhuma coluna encontrada em {sas_path}")
        return [], []

    # Ordena por posição
    posicoes.sort(key=lambda x: x[0])

    # Constrói colspecs (converte de 1-based para 0-based)
    colspecs = []
    for i, (pos, nome) in enumerate(posicoes):
        start = pos - 1
        if i < len(posicoes) - 1:
            end = posicoes[i + 1][0] - 1
        else:
            end = None
        colspecs.append((start, end))
        nomes.append(nome)

    logger.debug(f"Estrutura extraída: {len(nomes)} colunas")
    return nomes, colspecs


def encontrar_arquivo_sas(ano, ano_dir, base_nome, subdir):
    """Encontra o arquivo .sas correspondente ao arquivo de dados."""
    sas_dir = ano_dir / "INPUTS_SAS_SPSS" / subdir

    if not sas_dir.exists():
        return None

    # Procura arquivo .sas que contenha o base_nome
    for f in os.listdir(sas_dir):
        if base_nome in f and f.endswith(".sas"):
            return sas_dir / f

    # Se não encontrar exato, tenta nome genérico
    if subdir == "DIRETOR":
        for f in os.listdir(sas_dir):
            if "DIRETOR" in f and f.endswith(".sas"):
                return sas_dir / f
    elif subdir == "DOCENTES":
        for f in os.listdir(sas_dir):
            if "DOCENTE" in f and f.endswith(".sas"):
                return sas_dir / f
    elif subdir == "ESCOLAS":
        for f in os.listdir(sas_dir):
            if "ESCOLA" in f and f.endswith(".sas"):
                return sas_dir / f

    return None


def processar_txt(txt_path, ano, ano_dir, silver_ano_dir, subdir):
    """Processa um arquivo TXT usando .sas como estrutura."""
    base_nome = txt_path.stem
    logger.info(f"  → Processando: {txt_path.name}")

    sas_file = encontrar_arquivo_sas(ano, ano_dir, base_nome, subdir)

    if sas_file is None:
        logger.error(f"    ✗ Arquivo .sas não encontrado para {base_nome}")
        return False

    nomes, colspecs = ler_estrutura_sas(sas_file)

    if not nomes:
        logger.error(f"    ✗ Não foi possível extrair estrutura de {sas_file.name}")
        return False

    try:
        df = pd.read_fwf(txt_path, colspecs=colspecs, names=nomes, dtype=str)
        logger.info(
            f"    ✓ Dataframe criado: {len(df)} linhas × {len(df.columns)} colunas"
        )

        # Tratamento: remover espaços, padronizar nulos
        df = df.apply(
            lambda col: col.apply(lambda x: x.strip() if isinstance(x, str) else x)
        )
        df = df.replace({"": None, "NA": None, "nan": None})

        csv_path = silver_ano_dir / txt_path.name.replace(".TXT", ".csv").replace(
            ".txt", ".csv"
        )
        df.to_csv(csv_path, index=False)
        logger.info(f"    ✓ Salvo em: {csv_path.name}")
        return True
    except Exception as e:
        logger.error(f"    ✗ Erro ao processar {base_nome}: {str(e)}")
        return False


def processar_csv(csv_path, ano, silver_ano_dir):
    """Processa um arquivo CSV que já possui cabeçalho."""
    logger.info(f"  → Processando: {csv_path.name}")

    try:
        df = pd.read_csv(csv_path, dtype=str)
        logger.info(
            f"    ✓ Dataframe criado: {len(df)} linhas × {len(df.columns)} colunas"
        )

        # Tratamento: remover espaços, padronizar nulos
        df = df.apply(
            lambda col: col.apply(lambda x: x.strip() if isinstance(x, str) else x)
        )
        df = df.replace({"": None, "NA": None, "nan": None})

        # Salvar em silver
        csv_saida = silver_ano_dir / csv_path.name
        df.to_csv(csv_saida, index=False)
        logger.info(f"    ✓ Salvo em: {csv_saida.name}")
        return True
    except Exception as e:
        logger.error(f"    ✗ Erro ao processar {csv_path.name}: {str(e)}")
        return False


def tratar_ano(ano, bronze_dir, silver_dir, subdirs=None):
    """Processa todos os arquivos de dados de um ano específico.

    Args:
        ano: Ano a processar
        bronze_dir: Diretório bronze
        silver_dir: Diretório silver
        subdirs: Lista de subdiretórios a processar
                Se None, processa ALUNOS apenas
    """
    if subdirs is None:
        subdirs = ["ALUNOS"]

    ano_dir = Path(bronze_dir) / str(ano)
    silver_ano_dir = Path(silver_dir) / str(ano)
    silver_ano_dir.mkdir(parents=True, exist_ok=True)

    # Extrai zip se necessário
    extrair_zip(ano, bronze_dir)

    logger.info(f"\nAno {ano}: Processando subdirectórios...")

    arquivos_processados = 0

    for subdir in subdirs:
        dados_subdir = ano_dir / "DADOS" / subdir

        if not dados_subdir.exists():
            logger.warning(f"  {subdir}: Diretório não encontrado")
            continue

        logger.info(f"  {subdir}:")

        # Detectar se é TXT ou CSV
        txt_files = list(dados_subdir.glob("*.TXT")) + list(dados_subdir.glob("*.txt"))
        csv_files = list(dados_subdir.glob("*.csv")) + list(dados_subdir.glob("*.CSV"))

        if txt_files:
            logger.debug(f"    Detectado: TXT ({len(txt_files)} arquivos)")
            for txt_path in txt_files:
                if processar_txt(txt_path, ano, ano_dir, silver_ano_dir, subdir):
                    arquivos_processados += 1

        elif csv_files:
            logger.debug(f"    Detectado: CSV ({len(csv_files)} arquivos)")
            for csv_path in csv_files:
                if processar_csv(csv_path, ano, silver_ano_dir):
                    arquivos_processados += 1

        else:
            logger.warning(f"    Nenhum arquivo encontrado")

    return arquivos_processados


def main():
    bronze_dir = "data/bronze"
    silver_dir = "data/silver"

    # Lista de anos a processar
    anos_para_processar = ANOS_TXT + ANOS_CSV

    # Subdirectórios a processar
    subdirs = ["ALUNOS", "DIRETOR", "DOCENTES", "ESCOLAS"]

    logger.info("\n" + "=" * 70)
    logger.info(f"INICIANDO PIPELINE COMPLETO DE TRATAMENTO SAEB")
    logger.info(f"Anos: {anos_para_processar}")
    logger.info(f"Subdirectórios: {subdirs}")
    logger.info("=" * 70)

    total_arquivos = 0
    for ano in anos_para_processar:
        try:
            arquivos = tratar_ano(ano, bronze_dir, silver_dir, subdirs=subdirs)
            total_arquivos += arquivos
        except Exception as e:
            logger.error(f"Erro ao processar ano {ano}: {str(e)}")
            continue

    logger.info("\n" + "=" * 70)
    logger.info(f"PIPELINE CONCLUÍDO!")
    logger.info(f"Total de arquivos processados: {total_arquivos}")
    logger.info("=" * 70 + "\n")


if __name__ == "__main__":
    main()
