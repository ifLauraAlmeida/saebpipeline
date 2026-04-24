import os
import zipfile
import pandas as pd
from logger import logger

def explorar_e_catalogar_bronze():
    base_path = "data/bronze"
    export_path = "data/explorarzip"
    
    logger.info("--- Iniciando Catalogação da Camada Bronze ---")
    
    if not os.path.exists(base_path):
        logger.error(f"Pasta {base_path} não encontrada.")
        return

    # Garante que a pasta de destino do relatório existe
    os.makedirs(export_path, exist_ok=True)
    
    dados_catalogo = []
    anos = sorted([d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))])

    for ano in anos:
        caminho_ano = os.path.join(base_path, ano)
        zips = [f for f in os.listdir(caminho_ano) if f.endswith('.zip')]
        
        for zip_nome in zips:
            caminho_zip = os.path.join(caminho_ano, zip_nome)
            logger.info(f"[{ano}] Mapeando estruturas de {zip_nome}")
            
            try:
                with zipfile.ZipFile(caminho_zip, 'r') as z:
                    for item in z.infolist():
                        # Coleta metadados de cada arquivo dentro do ZIP
                        info_arquivo = {
                            "ano": ano,
                            "arquivo_zip": zip_nome,
                            "caminho_interno": item.filename,
                            "extensao": os.path.splitext(item.filename)[1].lower(),
                            "tamanho_mb": round(item.file_size / (1024 * 1024), 2),
                            "eh_diretorio": item.filename.endswith('/')
                        }
                        dados_catalogo.append(info_arquivo)
                        
            except zipfile.BadZipFile:
                logger.error(f"[{ano}] Arquivo corrompido: {zip_nome}")

    # Criando o DataFrame
    if dados_catalogo:
        df_catalogo = pd.DataFrame(dados_catalogo)
        
        # Salvando em CSV e Excel (opcional) para fácil leitura
        csv_file = os.path.join(export_path, "catalogo_estruturas_saeb.csv")
        df_catalogo.to_csv(csv_file, index=False, encoding='utf-8-sig')
        
        logger.info(f"Catálogo gerado com sucesso em: {csv_file}")
        print(f"\n✅ Catálogo exportado! Total de arquivos mapeados: {len(df_catalogo)}")
        print(df_catalogo.head()) # Exibe as primeiras linhas no console
        
        return df_catalogo
    else:
        logger.warning("Nenhum dado foi encontrado para catalogar.")
        return None

if __name__ == "__main__":
    df = explorar_e_catalogar_bronze()