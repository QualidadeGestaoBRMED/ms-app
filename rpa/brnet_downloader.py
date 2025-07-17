# rpa/brnet_downloader.py
import logging
import os
import time
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright

from utils.helpers import retry_with_backoff

def baixar_todos_relatorios(config: dict):
    """Orquestra o download de relatórios para todos os grupos listados na configuração."""
    logging.info("--- INICIANDO FASE DE DOWNLOAD DOS RELATÓRIOS ---")
    grupos_pendentes = list(config['brnet']['grupos_para_baixar'])
    
    for tentativa in range(1, 4):
        if not grupos_pendentes: break
        logging.info(f"===> TENTATIVA DE DOWNLOAD {tentativa}/3 <===")
        falhas_nesta_rodada = [grupo for grupo in grupos_pendentes if not _baixar_relatorio_individual_com_retry(config, grupo)]
        
        grupos_pendentes = falhas_nesta_rodada
        if grupos_pendentes:
            logging.warning(f"Grupos que falharam na tentativa {tentativa}: {grupos_pendentes}")
            if tentativa < 3: time.sleep(10)
    
    if grupos_pendentes: logging.error(f"DOWNLOAD FALHOU DEFINITIVAMENTE para os grupos: {grupos_pendentes}")
    else: logging.info("--- FASE DE DOWNLOAD CONCLUÍDA COM SUCESSO ---")

def _baixar_relatorio_individual_com_retry(config: dict, grupo: str) -> bool:
    """Tenta baixar um único relatório, usando o helper de retry."""
    try:
        retry_with_backoff(lambda: _baixar_relatorio_individual(config, grupo), max_retries=2, base_delay=3)
        return True
    except Exception as e:
        logging.error(f"Falha ao baixar relatório para o grupo '{grupo}': {e}")
        return False

def _baixar_relatorio_individual(config: dict, grupo: str):
    """Lógica principal do Playwright para baixar um único relatório."""
    logging.info(f"Iniciando download para o grupo: {grupo}")
    creds = config['brnet']['creds']
    pasta_destino = config['paths']['databases']
    hoje, ontem, data_inicio = datetime.now(), datetime.now() - timedelta(days=1), datetime.now() - timedelta(days=60)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(accept_downloads=True)
        try:
            page.goto("https://operacoes.grupobrmed.com.br/relatorios/expedicao/monitoramento-prazos/", timeout=60000)
            page.locator("#username").fill(creds['usuario'])
            page.get_by_placeholder("Digite sua senha").fill(creds['senha'])
            page.get_by_role("button", name="Entrar").click()
            page.locator("#id_company_group").wait_for(timeout=30000)
            page.locator("#id_company_group").select_option(label=grupo)
            page.locator("#id_start").fill(data_inicio.strftime("%d/%m/%Y"))
            page.locator("#id_end").fill(ontem.strftime("%d/%m/%Y"))
            page.locator("#id_email").fill(creds['email_relatorio'])
            page.locator("#submit").click()
            page.locator("#reload_table").wait_for(timeout=45000)
            page.locator("#reload_table").click()
            
            with page.expect_download(timeout=90000) as download_info:
                page.get_by_role("link", name="Download").first.wait_for(timeout=45000)
                page.get_by_role("link", name="Download").first.click()
            
            download = download_info.value
            safe_grupo_name = "".join(c for c in grupo if c.isalnum()).lower()
            nome_arquivo = f"grupo_{safe_grupo_name}_{ontem.strftime('%Y%m%d')}.xls"
            caminho_salvo = os.path.join(pasta_destino, nome_arquivo)
            download.save_as(caminho_salvo)
            
            if not os.path.exists(caminho_salvo) or os.path.getsize(caminho_salvo) < 100:
                raise Exception("Arquivo baixado não foi salvo ou está vazio.")
            logging.info(f"Download para '{grupo}' concluído com sucesso: {nome_arquivo}")
        finally:
            browser.close()