import logging
import os
from typing import List, Optional

def limpar_pasta_downloads(pasta: str):
    """Remove todos os arquivos da pasta de destino para garantir uma execução limpa."""
    logging.info(f"Limpando a pasta de downloads: {pasta}")
    try:
        if not os.path.exists(pasta):
            logging.warning(f"A pasta de downloads '{pasta}' não existe. Nada a limpar.")
            return

        for filename in os.listdir(pasta):
            file_path = os.path.join(pasta, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
        logging.info(f"Pasta de downloads '{pasta}' limpa.")
    except Exception as e:
        logging.error(f"Erro ao limpar a pasta de downloads: {e}")
        raise

def encontrar_arquivo_por_palavra_chave(pasta: str, palavra_chave: str, nome_aba: str) -> Optional[str]:
    """Encontra um arquivo na pasta de downloads usando uma chave ou palavras do nome da aba."""
    logging.info(f"Procurando arquivo para '{palavra_chave}' (aba: {nome_aba})...")
    
    arquivos_na_pasta = os.listdir(pasta)
    if not arquivos_na_pasta:
        logging.warning(f"A pasta de downloads '{pasta}' está vazia.")
        return None

    
    arquivos_exatos = [f for f in arquivos_na_pasta if palavra_chave.lower() in f.lower()]
    if arquivos_exatos:
        caminho = os.path.join(pasta, arquivos_exatos[0])
        logging.info(f"Arquivo encontrado por chave exata: {caminho}")
        return caminho

    palavras_chave_aba = _extrair_palavras_chave_da_aba(nome_aba)
    for arquivo in arquivos_na_pasta:
        if all(palavra.lower() in arquivo.lower() for palavra in palavras_chave_aba):
            caminho = os.path.join(pasta, arquivo)
            logging.info(f"Arquivo encontrado por palavras-chave: {caminho}")
            return caminho
            
    logging.warning(f"Nenhum arquivo correspondente encontrado para a aba '{nome_aba}'.")
    return None

def _extrair_palavras_chave_da_aba(nome_aba: str) -> List[str]:
    """Função auxiliar para extrair palavras significativas do nome da aba."""
    palavras_ignorar = {'grupo', 'da', 'de', 'do', 'e', 's.a.', 'ltda', 'exames', 'ocupacionais'}
    nome_limpo = nome_aba.lower().replace('.', ' ').replace('-', ' ').replace('_', ' ')
    palavras = [p for p in nome_limpo.split() if len(p) >= 3 and p not in palavras_ignorar]
    return palavras if palavras else [nome_aba.split()[0].lower()]