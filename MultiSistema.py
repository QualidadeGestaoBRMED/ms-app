import pandas as pd
import os
import os.path
import configparser
import logging
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime

# --- Lendo as Configurações do Arquivo .INI ---
config = configparser.ConfigParser()
config.read('config.ini')

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SAMPLE_SPREADSHEET_ID = config['GOOGLE_SHEETS']['spreadsheet_id']
ABA_CARIMBO = config['GOOGLE_SHEETS']['aba_carimbo']

# Autenticação com Google Sheets
def autenticar_google_sheets():
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
    """Escreve a data e hora atuais na aba 'Feriados' para registrar a execução."""
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
        )
        response = request.execute()
        logging.info(f"Carimbo atualizado com sucesso: {response.get('updatedCells')} célula atualizada.")
    except HttpError as err:
        logging.error(f"Ocorreu um erro ao atualizar o carimbo: {err}")

def escrever_dados_planilha(dados_para_adicionar, nome_da_aba):
    """Descobre a primeira linha vazia e usa 'update' para colar os dados."""
    try:
        creds = autenticar_google_sheets()
        service = build('sheets', 'v4', credentials=creds)
        
        result = service.spreadsheets().values().get(
            spreadsheetId=SAMPLE_SPREADSHEET_ID,
            range=nome_da_aba
        ).execute()
        ultima_linha = len(result.get('values', []))
        
        range_para_escrever = f"{nome_da_aba}!A{ultima_linha + 1}"
        
        request = service.spreadsheets().values().update(
            spreadsheetId=SAMPLE_SPREADSHEET_ID,
            range=range_para_escrever,
            valueInputOption="USER_ENTERED",
            body={"values": dados_para_adicionar}
        )
        response = request.execute()
        logging.info(f"-> {response.get('updatedCells')} células adicionadas com sucesso na aba '{nome_da_aba}'.")

    except HttpError as err:
        logging.error(f"Ocorreu um erro ao escrever dados: {err}")

def processar_planilha_empresa(caminho_arquivo, nome_aba):
    """Função central que processa um arquivo e atualiza a aba correspondente."""
    logging.info(f"--- Processando para a aba: {nome_aba} ---")
    
    ids_existentes = []
    try:
        creds = autenticar_google_sheets()
        service = build("sheets", "v4", credentials=creds)
        result = service.spreadsheets().values().get(
            spreadsheetId=SAMPLE_SPREADSHEET_ID, 
            range=nome_aba
        ).execute()
        
        valores_existentes = result.get("values", [])
        if not valores_existentes:
            logging.warning(f"Aba '{nome_aba}' está vazia ou sem cabeçalho. Pulando.")
            return
            
        cabecalho_planilha = valores_existentes[0]
        if len(valores_existentes) > 1:
            dados_existentes = valores_existentes[1:]
            df_sheet = pd.DataFrame(dados_existentes)
            num_cols_para_renomear = min(len(cabecalho_planilha), df_sheet.shape[1])
            df_sheet.columns = cabecalho_planilha[:num_cols_para_renomear]
            if 'ID_Unico' in df_sheet.columns:
                ids_existentes = df_sheet['ID_Unico'].dropna().tolist()
            else:
                logging.warning(f"A coluna 'ID_Unico' não existe no cabeçalho da aba '{nome_aba}'.")
        
    except HttpError as err:
        logging.error(f"Erro ao ler a planilha: {err}")
        return

    try:
        tabelas = pd.read_html(caminho_arquivo, encoding='utf-8')
        df_arquivo = tabelas[0]
        header_row_index = next((i for i, row in df_arquivo.head(10).iterrows() if 'Paciente' in str(row.values)), -1)
        if header_row_index == -1:
            logging.warning(f"Não foi possível encontrar o cabeçalho ('Paciente') no arquivo {os.path.basename(caminho_arquivo)}. Pulando.")
            return
        df_arquivo.columns = df_arquivo.iloc[header_row_index]
        df_arquivo = df_arquivo.iloc[header_row_index + 1:].reset_index(drop=True)
    except Exception:
        logging.exception(f"Falha crítica ao ler ou limpar o arquivo {os.path.basename(caminho_arquivo)}.")
        return

    try:
        for col in ['CPF/Passaporte', 'Previsto Para', 'Tipo de Pedido']:
            if col not in df_arquivo.columns:
                logging.error(f"A coluna essencial '{col}' não foi encontrada no arquivo de origem. Pulando arquivo.")
                return
            df_arquivo[col] = df_arquivo[col].astype(str).fillna('')
        df_arquivo['Previsto Para'] = pd.to_datetime(df_arquivo['Previsto Para'], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y').fillna('')
        df_arquivo['ID_Unico'] = (df_arquivo['CPF/Passaporte'].str.strip() + "-" + 
                                 df_arquivo['Previsto Para'].str.strip() + "-" + 
                                 df_arquivo['Tipo de Pedido'].str.strip().str[:3])
        df_final = df_arquivo.reindex(columns=cabecalho_planilha, fill_value='')
    except KeyError as e:
        logging.error(f"A coluna essencial {e} não foi encontrada durante a criação do ID_Unico.")
        return

    df_novos_registros = df_final[~df_final['ID_Unico'].isin(ids_existentes)]
    
    if df_novos_registros.empty:
        logging.info("Nenhum registro novo encontrado neste arquivo.")
    else:
        logging.info(f"Encontrados {len(df_novos_registros)} novos registros para adicionar...")
        df_sanitizado = df_novos_registros.fillna('')
        dados_para_adicionar = df_sanitizado.values.tolist()
        escrever_dados_planilha(dados_para_adicionar, nome_aba)

def main(): 
    """Função principal que orquestra todo o processo."""
    pasta_databases = config['CAMINHOS']['pasta_databases']
    mapa_arquivos_abas = dict(config['MAPEAMENTO_ARQUIVOS_ABAS'])

    if not os.path.isdir(pasta_databases):
        logging.critical(f"A pasta '{pasta_databases}' não foi encontrada (config.ini). A execução não pode continuar.")
        return

    logging.info("="*50)
    logging.info(f"Iniciando processo de atualização das abas...")
    logging.info("="*50)

    for chave_arquivo, nome_aba in mapa_arquivos_abas.items():
        arquivos_encontrados = [f for f in os.listdir(pasta_databases) if f.startswith(chave_arquivo)]
        if arquivos_encontrados:
            caminho_completo = os.path.join(pasta_databases, arquivos_encontrados[0])
            processar_planilha_empresa(caminho_completo, nome_aba)
        else:
            logging.warning(f"Nenhum arquivo encontrado para a chave '{chave_arquivo}' na pasta DATABASES.")
    
    logging.info("Processo principal finalizado.")
    
if __name__ == "__main__":
    # Configura o logger para registrar mensagens em um arquivo e também no terminal.
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("multisistema.log", mode='w'), # Salva em um arquivo
            logging.StreamHandler() # Mostra no terminal
        ]
    )
    
    main()
    atualizar_CARIMBO()