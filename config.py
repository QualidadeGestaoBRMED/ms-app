# config.py
import configparser
import logging
import os
from typing import Dict, Any
from dotenv import load_dotenv

def setup_logging():
    """Configura o sistema de logging para salvar em arquivo e mostrar no console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.FileHandler("multisistema.log", mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def get_config(config_path: str = 'config.ini') -> Dict[str, Any]:
    """
    Lê o .env e o config.ini, valida, e retorna um dicionário de configuração unificado.
    """
    load_dotenv()  # Carrega as variáveis do arquivo .env para o ambiente

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")

    config_parser = configparser.ConfigParser()
    config_parser.read(config_path, encoding='utf-8')

    config = {
        "google_sheets": {
            "spreadsheet_id": os.getenv("SPREADSHEET_ID"),
            "aba_carimbo": config_parser.get('GOOGLE_SHEETS', 'aba_carimbo'),
            "scopes": ["https://www.googleapis.com/auth/spreadsheets"]
        },
        "brnet": {
            "creds": {
                "usuario": os.getenv("BRNET_USER"),
                "senha": os.getenv("BRNET_PASSWORD"),
                "email_relatorio": config_parser.get('BRNET_CREDENCIALS', 'email_relatorio')
            },
            "grupos_para_baixar": [
                "GRUPO TRIGO", "ICTSI RIO", "CONCREMAT", "CONSTELLATION - EXAMES OCUPACIONAIS", "VLT RIO",
                "V.TAL - REDE NEUTRA DE TELECOMUNICACOES S.A.", "IKM", "BAKER HUGHES", "RIP ES", "RIP MACAÉ"
            ]
        },
        "paths": {
            "databases": config_parser.get('CAMINHOS', 'pasta_databases')
        },
        "mapping": {
            "arquivos_abas": dict(config_parser.items('MAPEAMENTO_ARQUIVOS_ABAS'))
        },
        "schema": {
            "colunas_rpa": [
                'ID_Unico', 'Paciente', 'CPF/Passaporte', 'Função', 'Setor', 'Empresa', 'Grupo',
                'Local do Atendimento', 'Atendido Em', 'Previsto Para', 'Liberado Em',
                'Status Expedição - BR MED', 'Exame Alterado', 'Tipo de Pedido'
            ]
        }
    }

    # Validações críticas
    if not config['google_sheets']['spreadsheet_id']:
        raise ValueError("SPREADSHEET_ID não encontrado no .env ou variáveis de ambiente.")
    if not config['brnet']['creds']['usuario'] or not config['brnet']['creds']['senha']:
        raise ValueError("BRNET_USER e BRNET_PASSWORD não encontrados no .env ou variáveis de ambiente.")
    
    db_path = config['paths']['databases']
    if not os.path.exists(db_path):
        os.makedirs(db_path)
        logging.info(f"Pasta de databases criada em: {db_path}")

    logging.info("Configurações carregadas e validadas com sucesso.")
    return config