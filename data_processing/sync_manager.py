import logging
import os
import pandas as pd
from typing import Dict, List, Tuple
from google_api.sheets_service import SheetsService
from utils.file_handler import encontrar_arquivo_por_palavra_chave
from utils.helpers import sanitizar_nome_coluna

def sincronizar_todas_as_abas(config: Dict, sheets_service: SheetsService):
    """Orquestra o processo de sincronização para todas as abas mapeadas no config."""
    logging.info("--- INICIANDO FASE DE PROCESSAMENTO E SINCRONIZAÇÃO ---")
    for chave, nome_aba in config['mapping']['arquivos_abas'].items():
        try:
            _sincronizar_aba(config, sheets_service, chave, nome_aba)
        except Exception as e:
            logging.error(f"Erro crítico ao processar a aba '{nome_aba}': {e}", exc_info=True)
    logging.info("--- FASE DE PROCESSAMENTO E SINCRONIZAÇÃO CONCLUÍDA ---")

def _sincronizar_aba(config: Dict, sheets_service: SheetsService, chave_arquivo: str, nome_aba: str):
    """Processa e sincroniza os dados de um único arquivo para uma única aba."""
    logging.info(f"--- Processando aba: {nome_aba} ---")
    caminho_arquivo = encontrar_arquivo_por_palavra_chave(config['paths']['databases'], chave_arquivo, nome_aba)
    if not caminho_arquivo: return

    df_arquivo = _ler_e_preparar_arquivo_local(caminho_arquivo, config['schema']['colunas_rpa'])
    if df_arquivo.empty:
        logging.warning(f"Arquivo '{os.path.basename(caminho_arquivo)}' resultou em zero dados válidos. Pulando.")
        return

    _, df_planilha, id_para_indice = sheets_service.get_data(nome_aba)
    novos, atualizacoes = _comparar_dados(df_arquivo, df_planilha, id_para_indice, config['schema']['colunas_rpa'])
    
    if novos: sheets_service.append_data(nome_aba, novos)
    if atualizacoes: sheets_service.batch_update_data(nome_aba, atualizacoes, len(config['schema']['colunas_rpa']))
    if not novos and not atualizacoes: logging.info(f"Nenhuma alteração necessária para a aba '{nome_aba}'.")

def _ler_e_preparar_arquivo_local(caminho: str, colunas_rpa: List[str]) -> pd.DataFrame:
    try:
        df = pd.read_html(caminho, encoding='utf-8')[0]
        header_idx = next((i for i, row in df.head(10).iterrows() if 'Paciente' in str(row.values)), -1)
        if header_idx == -1: return pd.DataFrame()
        
        df.columns = [sanitizar_nome_coluna(col) for col in df.iloc[header_idx]]
        df = df.iloc[header_idx + 1:].reset_index(drop=True)
        
        mapa_nomes = {'CPF_Passaporte': 'CPF/Passaporte', 'Local_do_Atendimento': 'Local do Atendimento', 'Atendido_Em': 'Atendido Em', 'Previsto_Para': 'Previsto Para', 'Liberado_Em': 'Liberado Em', 'Status_Expedicao_BR_MED': 'Status Expedição - BR MED', 'Exame_Alterado': 'Exame Alterado', 'Tipo_de_Pedido': 'Tipo de Pedido', 'Funcao': 'Função', 'Paciente': 'Paciente', 'Setor': 'Setor', 'Empresa': 'Empresa', 'Grupo': 'Grupo'}
        df.rename(columns=mapa_nomes, inplace=True)
        
        colunas_essenciais = ['CPF/Passaporte', 'Previsto Para', 'Tipo de Pedido']
        if not all(col in df.columns for col in colunas_essenciais): return pd.DataFrame()
        df.dropna(subset=colunas_essenciais, how='any', inplace=True)
        if df.empty: return pd.DataFrame()

        df['Previsto Para'] = pd.to_datetime(df['Previsto Para'], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y')
        df['ID_Unico'] = df['CPF/Passaporte'].astype(str).str.strip() + "-" + df['Previsto Para'].astype(str).str.strip() + "-" + df['Tipo de Pedido'].astype(str).str.strip().str[:3]
        df.drop_duplicates(subset=['ID_Unico'], keep='first', inplace=True)
        
        
        for col in colunas_rpa:
            if col not in df.columns:
                df[col] = ''
        return df[colunas_rpa]
    except Exception as e:
        logging.error(f"Falha ao ler ou processar o arquivo '{os.path.basename(caminho)}': {e}")
        return pd.DataFrame()

def _comparar_dados(df_arquivo: pd.DataFrame, df_planilha: pd.DataFrame, id_para_indice: Dict, colunas_rpa: List) -> Tuple[List, List]:
    novos, atualizacoes = [], []
    for _, linha_arquivo in df_arquivo.iterrows():
        id_unico, dados_formatados = linha_arquivo['ID_Unico'], linha_arquivo.fillna('').astype(str).tolist()
        if id_unico not in id_para_indice:
            novos.append(dados_formatados)
        else:
            indice, linha_planilha = id_para_indice[id_unico], df_planilha.iloc[id_para_indice[id_unico] - 2]
            if any(str(linha_arquivo.get(c, '')).strip() != str(linha_planilha.get(c, '')).strip() for c in colunas_rpa):
                atualizacoes.append((indice, dados_formatados))
    logging.info(f"Comparação finalizada: {len(novos)} novos, {len(atualizacoes)} atualizações.")
    return novos, atualizacoes