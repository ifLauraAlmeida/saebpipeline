import os
import requests
from bs4 import BeautifulSoup
import re
from logger import logger, log_progress

def organizar_downloads_saeb():
    url_principal = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/saeb"
    
    logger.info("Iniciando processo de scraping no portal do INEP.")
    
    try:
        # Timeout para evitar que o script fique travado se o site do INEP estiver lento
        response = requests.get(url_principal, verify=False, timeout=30)
        response.raise_for_status()
        logger.debug("Conexão com a página principal estabelecida com sucesso.")
    except Exception as e:
        logger.error(f"Falha crítica ao acessar a URL: {e}")
        return

    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', class_='external-link')
    logger.info(f"Encontrados {len(links)} links potenciais para análise.")

    for link in links:
        href = link.get('href')
        texto_link = link.get_text(strip=True)
        
        if not href or '.zip' not in href.lower():
            continue
            
        # Regex melhorada para pegar o ano
        match_ano = re.search(r'\b(19|20)\d{2}\b', texto_link) or re.search(r'\b(19|20)\d{2}\b', href)
        ano = match_ano.group(0) if match_ano else "sem_ano"
        
        # Caminho absoluto para evitar problemas de diretório
        # O Python criará as pastas a partir de onde o script é executado
        pasta_destino = os.path.join("data", "bronze", ano)
        os.makedirs(pasta_destino, exist_ok=True)
        
        nome_arquivo = href.split('/')[-1]
        caminho_completo = os.path.join(pasta_destino, nome_arquivo)
        
        if os.path.exists(caminho_completo):
            logger.warning(f"[{ano}] O arquivo {nome_arquivo} já existe. Pulando...")
            continue
            
        logger.info(f"[{ano}] Iniciando download: {nome_arquivo}")
        
        try:
            r = requests.get(href, stream=True, verify=False, timeout=60)
            r.raise_for_status()
            
            total_size = int(r.headers.get('content-length', 0))
            downloaded_size = 0
            
            with open(caminho_completo, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        if total_size > 0:
                            log_progress(downloaded_size, total_size, nome_arquivo)
            
            print("") # Pula linha após a barra de progresso
            logger.info(f"[{ano}] Download concluído com sucesso.")
            
        except Exception as e:
            logger.error(f"[{ano}] Erro no download de {nome_arquivo}: {e}")

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    organizar_downloads_saeb()