import logging
import sys
import os
from datetime import datetime

def setup_logger(name="SAEB_Pipeline"):
    log_format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    # Garante que a pasta 'logs' exista
    os.makedirs("logs", exist_ok=True)
    
    logger_obj = logging.getLogger(name) # Nomeamos internamente para não confundir
    logger_obj.setLevel(logging.DEBUG)

    # Console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    logger_obj.addHandler(console_handler)

    # Arquivo dentro da pasta logs
    log_path = os.path.join("logs", "pipeline_execution.log")
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    logger_obj.addHandler(file_handler)

    return logger_obj

# ESSA LINHA É A MAIS IMPORTANTE: ela cria a variável que o scraper importa
logger = setup_logger()

def log_progress(current, total, filename):
    percent = (current / total) * 100
    # Usando sys.stdout.write para a barra de progresso não sujar o arquivo de log
    sys.stdout.write(f"\r[DOWNLOAD] {filename}: {percent:.2f}%")
    sys.stdout.flush()