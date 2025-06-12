import pandas as pd
import os
import os.path
import configparser
import logging
import sqlite3
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime

# --- Lendo as Configurações do Arquivo .INI ---
config = configparser.ConfigParser()
config.read('config.ini')

# --- Configurações Globais ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SAMPLE_SPREADSHEET_ID = config['GOOGLE_SHEETS']['spreadsheet_id']
ABA_CARIMBO = config['GOOGLE_SHEETS']['aba_carimbo']
PASTA_DATABASES = config['CAMINHOS']['pasta_databases']
NOME_BANCO_DE_DADOS = os.path.join(PASTA_DATABASES, 'database.sqlite')

def autenticar_google_sheets():
    """Autentica com a API do Google Sheets e retorna as credenciais."""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def atualizar_CARIMBO():
    """Escreve a data e hora atuais para registrar a execução bem-sucedida."""
    data_hora_termino = datetime.now().strftime("%d/%m/%y %H:%M")
    logging.info(f"Atualizando carimbo de execução com a data e hora: {data_hora_termino}")
    try:
        creds = autenticar_google_sheets()
        service = build('sheets', 'v4', credentials=creds)
        body = {"values": [[data_hora_termino]]}
        request = service.spreadsheets().values().update(
            spreadsheetId=SAMPLE_SPREADSHEET_ID, 
            range=ABA_CARIMBO, 
            valueInputOption="USER_ENTERED", 
            body=body
        ).execute()
        logging.info(f"Carimbo atualizado com sucesso: {request.get('updatedCells')} célula atualizada.")
    except HttpError as err:
        logging.error(f"Ocorreu um erro ao atualizar o carimbo: {err}")

def escrever_dados_planilha(dados_para_adicionar, nome_da_aba):
    """Descobre a primeira linha vazia e usa 'update' para colar os dados."""
    try:
        creds = autenticar_google_sheets()
        service = build('sheets', 'v4', credentials=creds)
        
        result = service.spreadsheets().values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=nome_da_aba).execute()
        ultima_linha = len(result.get('values', []))
        
        range_para_escrever = f"{nome_da_aba}!A{ultima_linha + 1}"
        
        request = service.spreadsheets().values().update(
            spreadsheetId=SAMPLE_SPREADSHEET_ID,
            range=range_para_escrever,
            valueInputOption="USER_ENTERED",
            body={"values": dados_para_adicionar}
        ).execute()
        logging.info(f"-> {request.get('updatedCells')} células adicionadas com sucesso na aba '{nome_da_aba}'.")

    except HttpError as err:
        logging.error(f"Ocorreu um erro ao escrever dados no Google Sheets: {err}")

def inicializar_banco_de_dados():
    """Cria o arquivo do banco de dados e a tabela 'atendimentos' se não existirem."""
    conn = sqlite3.connect(NOME_BANCO_DE_DADOS)
    cursor = conn.cursor()
    # Nomes de colunas sanitizados para serem seguros em SQL
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS atendimentos (
            ID_Unico TEXT PRIMARY KEY, Paciente TEXT, CPF_Passaporte TEXT, Funcao TEXT, Setor TEXT,
            Empresa TEXT, Grupo TEXT, Local_do_Atendimento TEXT, Atendido_Em TEXT, Previsto_Para TEXT,
            Liberado_Em TEXT, Status_Expedicao___BR_MED TEXT, Exame_Alterado TEXT, Tipo_de_Pedido TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logging.info(f"Banco de dados '{NOME_BANCO_DE_DADOS}' inicializado com sucesso.")

def atualizar_banco_com_arquivos_locais(mapa_arquivos_abas):
    """Lê os arquivos HTML e atualiza o banco de dados SQLite usando uma transação."""
    logging.info("Iniciando atualização transacional do banco de dados local...")
    conn = sqlite3.connect(NOME_BANCO_DE_DADOS)
    
    try:
        for chave_arquivo, nome_aba in mapa_arquivos_abas.items():
            arquivos_encontrados = [f for f in os.listdir(PASTA_DATABASES) if f.startswith(chave_arquivo)]
            if not arquivos_encontrados:
                continue
            
            caminho_completo = os.path.join(PASTA_DATABASES, arquivos_encontrados[0])
            logging.info(f"Processando arquivo para o banco: {arquivos_encontrados[0]}")
            
            tabelas = pd.read_html(caminho_completo, encoding='utf-8')
            df_arquivo = tabelas[0]
            
            header_row_index = next((i for i, row in df_arquivo.head(10).iterrows() if 'Paciente' in str(row.values)), -1)
            if header_row_index == -1:
                logging.warning(f"Cabeçalho 'Paciente' não encontrado no arquivo {chave_arquivo}. Pulando.")
                continue

            df_arquivo.columns = df_arquivo.iloc[header_row_index]
            df_arquivo = df_arquivo.iloc[header_row_index + 1:].reset_index(drop=True)
            
            # Sanitiza os nomes das colunas para serem seguros em SQL
            novos_nomes = {col: str(col).replace(' ', '_').replace('-', '_').replace('.', '_').replace('/', '_') for col in df_arquivo.columns}
            df_arquivo.rename(columns=novos_nomes, inplace=True)
            
            # Validação de dados essenciais
            colunas_essenciais = ['CPF_Passaporte', 'Previsto_Para', 'Tipo_de_Pedido']
            df_arquivo.dropna(subset=colunas_essenciais, inplace=True)
            if df_arquivo.empty:
                logging.warning(f"Nenhuma linha válida no arquivo {chave_arquivo} após validação.")
                continue

            # Mapeamento para as colunas do banco de dados
            colunas_db = [
                'Paciente', 'CPF_Passaporte', 'Funcao', 'Setor', 'Empresa', 'Grupo', 
                'Local_do_Atendimento', 'Atendido_Em', 'Previsto_Para', 'Liberado_Em', 
                'Status_Expedicao___BR_MED', 'Exame_Alterado', 'Tipo_de_Pedido'
            ]
            df_para_db = pd.DataFrame()
            for col in colunas_db:
                if col in df_arquivo.columns:
                    df_para_db[col] = df_arquivo[col]
                else:
                    df_para_db[col] = ''
            
            # Formatação e criação do ID_Unico
            df_para_db['Previsto_Para'] = pd.to_datetime(df_para_db['Previsto_Para'], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y').fillna('')
            df_para_db['ID_Unico'] = (df_para_db['CPF_Passaporte'].astype(str).str.strip() + "-" + 
                                     df_para_db['Previsto_Para'].astype(str).str.strip() + "-" + 
                                     df_para_db['Tipo_de_Pedido'].astype(str).str.strip().str[:3])
            
            df_para_db.drop_duplicates(subset=['ID_Unico'], keep='first', inplace=True)
            
            # Inserção Robusta no Banco de Dados usando tabela temporária
            df_para_db.to_sql('atendimentos_temp', conn, if_exists='replace', index=False)
            cursor = conn.cursor()
            colunas_str = ', '.join(f'"{c}"' for c in df_para_db.columns)
            cursor.execute(f'INSERT OR REPLACE INTO atendimentos ({colunas_str}) SELECT {colunas_str} FROM atendimentos_temp')
            
    except Exception as e:
        conn.rollback()
        logging.exception(f"Erro durante a atualização do banco. Todas as alterações foram desfeitas (rollback).")
    else:
        conn.commit()
        logging.info("Todos os arquivos processados com sucesso. Alterações salvas no banco de dados.")
    finally:
        conn.close()

def sincronizar_sheets_com_banco(nome_aba):
    """Lê dados do SQLite e insere apenas os registros novos no Google Sheets."""
    logging.info(f"--- Sincronizando aba: {nome_aba} ---")
    conn = sqlite3.connect(NOME_BANCO_DE_DADOS)
    try:
        creds = autenticar_google_sheets()
        service = build("sheets", "v4", credentials=creds)
        result = service.spreadsheets().values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=nome_aba).execute()
        
        valores_existentes = result.get("values", [])
        if not valores_existentes:
            logging.warning(f"Aba '{nome_aba}' está vazia. Pulando sincronização.")
            conn.close()
            return
            
        cabecalho_planilha = valores_existentes[0]
        ids_existentes = []
        if len(valores_existentes) > 1:
            dados_existentes = valores_existentes[1:]
            df_sheet = pd.DataFrame(dados_existentes)
            num_cols_para_renomear = min(len(cabecalho_planilha), df_sheet.shape[1])
            df_sheet.columns = cabecalho_planilha[:num_cols_para_renomear]
            if 'ID_Unico' in df_sheet.columns:
                ids_existentes = df_sheet['ID_Unico'].dropna().tolist()

        query = f"SELECT * FROM atendimentos WHERE Grupo = '{nome_aba}'"
        df_sqlite = pd.read_sql_query(query, conn)
        
        if df_sqlite.empty:
            logging.info(f"Nenhum dado no banco para o grupo '{nome_aba}'.")
            conn.close()
            return

        mapa_nomes_invertido = {
            'CPF_Passaporte': 'CPF/Passaporte', 'Local_do_Atendimento': 'Local do Atendimento',
            'Atendido_Em': 'Atendido Em', 'Previsto_Para': 'Previsto Para', 'Liberado_Em': 'Liberado Em',
            'Status_Expedicao___BR_MED': 'Status Expedição - BR MED', 'Exame_Alterado': 'Exame Alterado',
            'Tipo_de_Pedido': 'Tipo de Pedido', 'Funcao': 'Função'
        }
        df_sqlite.rename(columns=mapa_nomes_invertido, inplace=True)
        
        df_final = df_sqlite.reindex(columns=cabecalho_planilha, fill_value='')
        df_novos_registros = df_final[~df_final['ID_Unico'].isin(ids_existentes)]
    
        if df_novos_registros.empty:
            logging.info("Nenhum registro novo para sincronizar com o Google Sheets.")
        else:
            logging.info(f"Encontrados {len(df_novos_registros)} novos registros para adicionar ao Google Sheets...")
            df_sanitizado = df_novos_registros.fillna('')
            dados_para_adicionar = df_sanitizado.values.tolist()
            escrever_dados_planilha(dados_para_adicionar, nome_aba)

    except Exception as e:
        logging.exception(f"Ocorreu um erro durante a sincronização da aba {nome_aba}: {e}")
    finally:
        conn.close()

def main(): 
    """Função principal que orquestra todo o processo."""
    mapa_arquivos_abas = dict(config['MAPEAMENTO_ARQUIVOS_ABAS'])

    if not os.path.isdir(PASTA_DATABASES):
        logging.critical(f"A pasta '{PASTA_DATABASES}' não foi encontrada (config.ini). A execução não pode continuar.")
        return

    logging.info("="*50)
    logging.info(f"Iniciando processo de atualização...")
    logging.info("="*50)

    inicializar_banco_de_dados()
    atualizar_banco_com_arquivos_locais(mapa_arquivos_abas)
    
    logging.info("="*50)
    logging.info("Iniciando sincronização do banco de dados local com o Google Sheets...")
    logging.info("="*50)
    for nome_aba in mapa_arquivos_abas.values():
        sincronizar_sheets_com_banco(nome_aba)
    
    logging.info("Processo principal finalizado.")
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler("multisistema.log", mode='w'), logging.StreamHandler()])
    main()
    atualizar_CARIMBO()