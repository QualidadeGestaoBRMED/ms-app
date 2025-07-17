# main.py
import logging
from config import setup_logging, get_config
from utils.file_handler import limpar_pasta_downloads
from rpa.brnet_downloader import baixar_todos_relatorios
from data_processing.sync_manager import sincronizar_todas_as_abas
from google_api.sheets_service import SheetsService

def main():
    setup_logging()
    logging.info(">>> INICIANDO PROCESSO DE SINCRONIZAÇÃO BRNET <<<")

    try:
        # 1. Carregar e validar configurações de forma centralizada
        config = get_config()
        
        # 2. Inicializar serviços essenciais
        sheets_service = SheetsService(config['google_sheets'])

        # 3. Limpar ambiente de execuções anteriores
        limpar_pasta_downloads(config['paths']['databases'])

        # 4. Fase de Extração (RPA)
        baixar_todos_relatorios(config)

        # 5. Fase de Processamento e Sincronização
        sincronizar_todas_as_abas(config, sheets_service)
        
        # 6. Atualizar carimbo de sucesso na planilha
        sheets_service.update_timestamp(config['google_sheets']['aba_carimbo'])

        logging.info(">>> PROCESSO FINALIZADO COM SUCESSO <<<")

    except Exception as e:
        logging.critical(f"ERRO IRRECUPERÁVEL NO FLUXO PRINCIPAL: {e}", exc_info=True)

if __name__ == "__main__":
    main()