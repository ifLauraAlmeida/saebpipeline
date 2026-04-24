# Script de Tratamento SAEB - Camada Silver

## Objetivo
Converter arquivos TXT fixed-width da camada Bronze para CSV na camada Silver, usando as definições de coluna dos arquivos `.sas` (INPUT_SAS).

## Características
✓ Extração automática de ZIPs (com verificação de já extraído)
✓ Parsing automático de estrutura de colunas a partir de arquivos .sas
✓ Conversão fixed-width → CSV com colspecs
✓ Tratamento de dados (remoção de espaços, padronização de nulos)
✓ Logging detalhado com logger.py
✓ Suporte para múltiplos subdiretórios (ALUNOS, DIRETOR, DOCENTES, ESCOLAS)

## Uso Básico

### Processar todos os ALUNOS de um ano
```bash
python src/tratamento.py
```

### Processar um arquivo específico
```python
from tratamento import extrair_zip, tratar_txt_para_csv

# Processar apenas MATEMATICA_03ANO
tratar_txt_para_csv(1995, "data/bronze", "data/silver", 
                   arquivo_alvo="MATEMATICA_03ANO", subdir="ALUNOS")
```

### Processar outros diretórios
```python
# DIRETOR
tratar_txt_para_csv(1995, "data/bronze", "data/silver", subdir="DIRETOR")

# DOCENTES
tratar_txt_para_csv(1995, "data/bronze", "data/silver", subdir="DOCENTES")

# ESCOLAS
tratar_txt_para_csv(1995, "data/bronze", "data/silver", subdir="ESCOLAS")
```

## Estrutura de Dados

### Entrada (Bronze)
```
data/bronze/1995/
├── micro_saeb1995.zip
├── DADOS/
│   ├── ALUNOS/
│   │   ├── MATEMATICA_03ANO.TXT
│   │   ├── PORTUGUES_03ANO.TXT
│   │   └── ...
│   ├── DIRETOR/
│   ├── DOCENTES/
│   └── ESCOLAS/
└── INPUTS_SAS_SPSS/
    ├── ALUNOS/
    │   ├── INPUT_SAS_MATEMATICA_03ANO.sas
    │   └── ...
    ├── DIRETOR/
    ├── DOCENTES/
    └── ESCOLAS/
```

### Saída (Silver)
```
data/silver/1995/
├── MATEMATICA_03ANO.csv (9049 linhas × 47 colunas)
├── MATEMATICA_04SERIE.csv (11886 linhas × 40 colunas)
├── MATEMATICA_08SERIE.csv (14609 linhas × 47 colunas)
├── PORTUGUES_03ANO.csv (9171 linhas × 47 colunas)
├── PORTUGUES_04SERIE.csv (12033 linhas × 40 colunas)
└── PORTUGUES_08SERIE.csv (14705 linhas × 47 colunas)
```

## Processamento Realizado (1995)

| Arquivo | Linhas | Colunas |
|---------|--------|---------|
| MATEMATICA_03ANO | 9.049 | 47 |
| MATEMATICA_04SERIE | 11.886 | 40 |
| MATEMATICA_08SERIE | 14.609 | 47 |
| PORTUGUES_03ANO | 9.171 | 47 |
| PORTUGUES_04SERIE | 12.033 | 40 |
| PORTUGUES_08SERIE | 14.705 | 47 |

**Total: 70.748 registros | 13 MB de dados**

## Próximos Passos
1. Processar DIRETOR, DOCENTES e ESCOLAS
2. Estender para outros anos (1997, 1999, 2001, etc.)
3. Implementar tratamento de tipos de dados específicos
4. Adicionar validações de qualidade

## Log de Execução
O script gera logs detalhados:
- INFO: Operações principais
- DEBUG: Estrutura extraída (número de colunas)
- ERROR: Problemas durante processamento
- WARNING: Avisos sobre arquivos não processados
