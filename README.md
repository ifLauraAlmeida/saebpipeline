# SAEB Data Pipeline 🚀

Este projeto automatiza o ciclo completo de vida dos microdados do SAEB (Sistema de Avaliação da Educação Básica), eliminando a necessidade de interação manual com o portal do INEP.

### 📌 Visão Geral

O objetivo é transformar arquivos brutos de largura fixa (Fixed Width Files - TXT), que possuem estruturas complexas e pesadas, em um conjunto de dados limpo, padronizado e pronto para análise.

### 🛠️ Arquitetura do Projeto

O pipeline foi desenhado seguindo os princípios de engenharia de dados moderna:

    Ingestão (Scraping): Coleta automatizada via BeautifulSoup e requests, identificando links de download e organizando-os por ano.

    Camada Bronze (data/bronze/{ano}): Armazenamento dos arquivos originais (.zip ou .txt) conforme baixados do site oficial.

    Processamento (Wrangling): Utilização da função pd.read_fwf() do Pandas para interpretar as posições exatas das colunas nos arquivos TXT antigos.

    Padronização: Conversão de tipos (dtype=str para códigos geográficos) e tratamento de valores nulos (missings).

### 🚀 Tecnologias Utilizadas

    Python 3.x

    Pandas: Orquestração e tratamento de dados.

    BeautifulSoup4: Web scraping do portal de dados abertos.

    Re (Regex): Extração inteligente de metadados e anos dos arquivos.

### 📂 Estrutura de Pastas

├── data/
│   └── bronze/          # Dados brutos organizados por ano [cite: 42]
│       ├── 2019/
│       └── 2021/
├── scripts/
│   ├── scraper.py       # Lógica de coleta automatizada [cite: 31]
│   └── transform.py     # Tratamento de TXT para Parquet/CSV [cite: 26]
└── README.md

🚧 Como Executar (Em breve)

(Espaço reservado para instruções de instalação e execução do script principal)
