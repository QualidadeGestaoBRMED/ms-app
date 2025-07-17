import logging
import random
import time
import unicodedata

def retry_with_backoff(func, max_retries=3, base_delay=2):
    """
    Executa uma função com sistema de retentativas (retry) e espera exponencial (backoff).
    """
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"FALHA DEFINITIVA após {max_retries} tentativas: {e}")
                raise
            
            wait_time = (base_delay * (2 ** attempt)) + random.uniform(0, 1)
            logging.warning(f"Tentativa {attempt + 1}/{max_retries} falhou: {e}. Nova tentativa em {wait_time:.2f}s...")
            time.sleep(wait_time)

def sanitizar_nome_coluna(nome_coluna: str) -> str:
    """
    Remove acentos e caracteres especiais de uma string para usá-la como nome de coluna.
    """
    if not isinstance(nome_coluna, str):
        return "coluna_invalida"
        
    # Normaliza para decompor acentos
    s = ''.join(c for c in unicodedata.normalize('NFD', nome_coluna)
                if unicodedata.category(c) != 'Mn')
    # Substitui caracteres e espaços
    return s.replace(' ', '_').replace('-', '_').replace('.', '_').replace('/', '_')