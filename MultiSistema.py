import pandas as pd
import os
import os.path
import configparser
import logging
import sqlite3
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
from dateutil.relativedelta import relativedelta
import unicodedata

# --- Lendo as Configurações do Arquivo .INI ---
config = configparser.ConfigParser()
config.read('config.ini')

# --- Configurações Globais ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SAMPLE_SPREADSHEET_ID = config['GOOGLE_SHEETS']['spreadsheet_id']
ABA_CARIMBO = config['GOOGLE_SHEETS']['aba_carimbo']
PASTA_DATABASES = config['CAMINHOS']['pasta_databases']
NOME_BANCO_DE_DADOS = os.path.join(PASTA_DATABASES, 'database.sqlite')
BRNET_CREDS = config['BRNET_CREDENCIALS']

def excluir_arquivos_antigos():
    """
    Remove todos os arquivos da pasta de databases, exceto arquivos .sqlite.
    Garante que a execução sempre comece com a pasta limpa.
    """
    logging.info(f"Limpando a pasta de destino: {PASTA_DATABASES}")
    try:
        for filename in os.listdir(PASTA_DATABASES):
            if filename.endswith('.sqlite'):
                continue
            file_path = os.path.join(PASTA_DATABASES, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
        logging.info("Pasta limpa com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao limpar a pasta de databases: {e}")

def baixar_relatorios_playwright(grupo):
    """
    Realiza o login no sistema BRNET via Playwright, preenche o formulário de relatório,
    executa o fluxo de 3 cliques para solicitar, atualizar e baixar o relatório do grupo informado.
    O arquivo baixado é salvo com nome padronizado na pasta de databases.
    """
    logging.info(f"Iniciando download para o grupo: {grupo}")
    link = "https://operacoes.grupobrmed.com.br/relatorios/expedicao/monitoramento-prazos/"
    usuario = BRNET_CREDS['usuario']
    senha = BRNET_CREDS['senha']
    email = BRNET_CREDS['email_relatorio']

    hoje = datetime.now()
    ontem = hoje - relativedelta(days=1)
    dois_meses_atras = hoje - relativedelta(months=2)
    start_date = dois_meses_atras.strftime("%d/%m/%Y")
    end_date = ontem.strftime("%d/%m/%Y")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(link, timeout=60000)
            page.locator("#username").fill(usuario)
            page.get_by_placeholder("Digite sua senha").fill(senha)
            page.get_by_role("button", name="Entrar").click()
            page.locator("#id_company_group").wait_for(timeout=20000)
            page.locator("#id_company_group").select_option(label=grupo)
            page.locator("#id_start").fill(start_date)
            page.locator("#id_end").fill(end_date)
            page.locator("#id_email").fill(email)
            page.locator("#submit").click()
            logging.info("Relatório solicitado. Aguardando a tabela de resultados...")
            page.locator("#reload_table").wait_for(timeout=30000)
            page.locator("#reload_table").click()
            logging.info("Tabela de resultados atualizada. Aguardando link de download...")
            with page.expect_download(timeout=60000) as download_info:
                page.get_by_role("link", name="Download").first.click()
            download = download_info.value
            safe_grupo_name = "".join(c for c in grupo if c.isalnum()).lower()
            nome_seguro_arquivo = f"grupo_{safe_grupo_name}_{ontem.strftime('%Y%m%d')}.xls"
            caminho_salvo = os.path.join(PASTA_DATABASES, nome_seguro_arquivo)
            download.save_as(caminho_salvo)
            logging.info(f"Download para '{grupo}' concluído com sucesso: {caminho_salvo}")
            browser.close()
    except Exception as e:
        logging.exception(f"Falha crítica no download para o grupo {grupo}.")

def autenticar_google_sheets():
    """
    Autentica na API do Google Sheets utilizando OAuth2.
    Retorna as credenciais válidas para uso nas operações com a API.
    """
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def atualizar_CARIMBO():
    """
    Atualiza a aba de carimbo no Google Sheets com a data e hora atuais,
    indicando o momento da última execução bem-sucedida do script.
    """
    data_hora_termino = datetime.now().strftime("%d/%m/%y %H:%M")
    logging.info(
        f"Atualizando carimbo de execução com a data e hora: {data_hora_termino}")
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
        logging.info(
            f"Carimbo atualizado com sucesso: {request.get('updatedCells')} célula atualizada.")
    except HttpError as err:
        logging.error(f"Ocorreu um erro ao atualizar o carimbo: {err}")

def escrever_dados_planilha(dados_para_adicionar, nome_da_aba):
    """
    Escreve os dados fornecidos na primeira linha vazia da aba especificada do Google Sheets.
    """
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
        ).execute()
        logging.info(
            f"-> {request.get('updatedCells')} células adicionadas com sucesso na aba '{nome_da_aba}'.")
    except HttpError as err:
        logging.error(
            f"Ocorreu um erro ao escrever dados no Google Sheets: {err}")

def inicializar_banco_de_dados():
    """
    Cria o arquivo do banco de dados SQLite e a tabela 'atendimentos' caso não existam.
    """
    conn = sqlite3.connect(NOME_BANCO_DE_DADOS)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS atendimentos (
            ID_Unico TEXT PRIMARY KEY, Paciente TEXT, CPF_Passaporte TEXT, Funcao TEXT, Setor TEXT,
            Empresa TEXT, Grupo TEXT, Local_do_Atendimento TEXT, Atendido_Em TEXT, Previsto_Para TEXT,
            Liberado_Em TEXT, Status_Expedicao_BR_MED TEXT, Exame_Alterado TEXT, Tipo_de_Pedido TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logging.info(
        f"Banco de dados '{NOME_BANCO_DE_DADOS}' inicializado com sucesso.")

def sanitizar_nome_coluna(nome_coluna):
    """
    Remove acentos e substitui caracteres problemáticos por underscore nos nomes de colunas.
    Garante compatibilidade dos nomes de colunas com o SQLite.
    """
    s = ''.join(c for c in unicodedata.normalize('NFD', str(nome_coluna))
                if unicodedata.category(c) != 'Mn')
    return s.replace(' ', '_').replace('-', '_').replace('.', '_').replace('/', '_')

def atualizar_banco_com_arquivos_locais(mapa_arquivos_abas):
    """
    Lê os arquivos baixados, processa os dados e atualiza o banco de dados SQLite.
    Utiliza transação para garantir atomicidade das operações.
    """
    logging.info("Iniciando atualização transacional do banco de dados local...")
    conn = sqlite3.connect(NOME_BANCO_DE_DADOS)
    try:
        for chave_arquivo, nome_aba in mapa_arquivos_abas.items():
            arquivos_encontrados = [
                f for f in os.listdir(PASTA_DATABASES) if f.startswith(chave_arquivo)]
            if not arquivos_encontrados:
                continue
            caminho_completo = os.path.join(
                PASTA_DATABASES, arquivos_encontrados[0])
            logging.info(
                f"Processando arquivo para o banco: {arquivos_encontrados[0]}")
            tabelas = pd.read_html(caminho_completo, encoding='utf-8')
            df_arquivo = tabelas[0]
            header_row_index = next((i for i, row in df_arquivo.head(
                10).iterrows() if 'Paciente' in str(row.values)), -1)
            if header_row_index == -1:
                logging.warning(
                    f"Cabeçalho 'Paciente' não encontrado no arquivo {chave_arquivo}. Pulando.")
                continue
            df_arquivo.columns = df_arquivo.iloc[header_row_index]
            df_arquivo = df_arquivo.iloc[header_row_index + 1:].reset_index(drop=True)
            df_arquivo.columns = [sanitizar_nome_coluna(
                col) for col in df_arquivo.columns]
            colunas_essenciais = [
                'CPF_Passaporte', 'Previsto_Para', 'Tipo_de_Pedido']
            df_arquivo.dropna(subset=colunas_essenciais, inplace=True)
            if df_arquivo.empty:
                logging.warning(
                    f"Nenhuma linha válida no arquivo {chave_arquivo} após validação.")
                continue
            colunas_db = {
                'ID_Unico': 'ID_Unico', 'Paciente': 'Paciente', 'CPF_Passaporte': 'CPF_Passaporte',
                'Funcao': 'Funcao', 'Setor': 'Setor', 'Empresa': 'Empresa', 'Grupo': 'Grupo',
                'Local_do_Atendimento': 'Local_do_Atendimento', 'Atendido_Em': 'Atendido_Em',
                'Previsto_Para': 'Previsto_Para', 'Liberado_Em': 'Liberado_Em',
                'Status_Expedicao_BR_MED': 'Status_Expedicao_BR_MED', 'Exame_Alterado': 'Exame_Alterado',
                'Tipo_de_Pedido': 'Tipo_de_Pedido'
            }
            df_para_db = pd.DataFrame()
            for db_col, arquivo_col in colunas_db.items():
                if arquivo_col in df_arquivo.columns:
                    df_para_db[db_col] = df_arquivo[arquivo_col]
                else:
                    df_para_db[db_col] = ''
            df_para_db['Previsto_Para'] = pd.to_datetime(
                df_para_db['Previsto_Para'], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y').fillna('')
            df_para_db['ID_Unico'] = (df_para_db['CPF_Passaporte'].astype(str).str.strip() + "-" +
                                     df_para_db['Previsto_Para'].astype(
                                         str).str.strip() + "-" +
                                     df_para_db['Tipo_de_Pedido'].astype(str).str.strip().str[:3])
            df_para_db.drop_duplicates(subset=['ID_Unico'], keep='first', inplace=True)
            df_para_db.to_sql('atendimentos_temp', conn,
                              if_exists='replace', index=False)
            cursor = conn.cursor()
            colunas_str = ', '.join(f'"{c}"' for c in df_para_db.columns)
            cursor.execute(
                f'INSERT OR REPLACE INTO atendimentos ({colunas_str}) SELECT {colunas_str} FROM atendimentos_temp')
    except Exception as e:
        conn.rollback()
        logging.exception(
            f"Erro durante a atualização do banco. Todas as alterações foram desfeitas (rollback).")
    else:
        conn.commit()
        logging.info(
            "Todos os arquivos processados com sucesso. Alterações salvas no banco de dados.")
    finally:
        conn.close()

def sincronizar_sheets_com_banco(nome_aba):
    """
    Sincroniza os dados do banco SQLite com a aba correspondente do Google Sheets.
    Apenas registros novos (não presentes na planilha) são enviados.
    """
    logging.info(f"--- Sincronizando aba: {nome_aba} ---")
    conn = sqlite3.connect(NOME_BANCO_DE_DADOS)
    try:
        creds = autenticar_google_sheets()
        service = build("sheets", "v4", credentials=creds)
        result = service.spreadsheets().values().get(
            spreadsheetId=SAMPLE_SPREADSHEET_ID, range=nome_aba).execute()
        valores_existentes = result.get("values", [])
        if not valores_existentes:
            logging.warning(
                f"Aba '{nome_aba}' está vazia. Pulando sincronização.")
            conn.close()
            return
        cabecalho_planilha = valores_existentes[0]
        ids_existentes = []
        if len(valores_existentes) > 1:
            dados_existentes = valores_existentes[1:]
            df_sheet = pd.DataFrame(dados_existentes)
            num_cols_para_renomear = min(
                len(cabecalho_planilha), df_sheet.shape[1])
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
            'Status_Expedicao_BR_MED': 'Status Expedição - BR MED', 'Exame_Alterado': 'Exame Alterado',
            'Tipo_de_Pedido': 'Tipo de Pedido', 'Funcao': 'Função'
        }
        df_sqlite.rename(columns=mapa_nomes_invertido, inplace=True)
        df_final = df_sqlite.reindex(
            columns=cabecalho_planilha, fill_value='')
        df_novos_registros = df_final[~df_final['ID_Unico'].isin(
            ids_existentes)]
        if df_novos_registros.empty:
            logging.info(
                "Nenhum registro novo para sincronizar com o Google Sheets.")
        else:
            logging.info(
                f"Encontrados {len(df_novos_registros)} novos registros para adicionar ao Google Sheets...")
            df_sanitizado = df_novos_registros.fillna('')
            dados_para_adicionar = df_sanitizado.values.tolist()
            escrever_dados_planilha(dados_para_adicionar, nome_aba)
    except Exception as e:
        logging.exception(
            f"Ocorreu um erro durante a sincronização da aba {nome_aba}: {e}")
    finally:
        conn.close()

def main():
    """
    Função principal que orquestra todo o fluxo:
    1. Limpa a pasta de databases.
    2. Baixa os relatórios dos grupos definidos.
    3. Atualiza o banco de dados local com os arquivos baixados.
    4. Sincroniza os dados do banco com o Google Sheets.
    5. Atualiza o carimbo de execução.
    """
    mapa_arquivos_abas = dict(config['MAPEAMENTO_ARQUIVOS_ABAS'])
    excluir_arquivos_antigos()
    logging.info("="*50)
    logging.info("Iniciando fase de download dos relatórios do BRNET...")
    logging.info("="*50)
    grupos_para_baixar = [
        "GRUPO TRIGO", "ICTSI RIO", "CONCREMAT", "CONSTELLATION - EXAMES OCUPACIONAIS", "VLT RIO",
        "V.TAL - REDE NEUTRA DE TELECOMUNICACOES S.A.", "IKM", "BAKER HUGHES", "RIP ES", "RIP MACAÉ"
    ]
    for grupo in grupos_para_baixar:
        baixar_relatorios_playwright(grupo)
    logging.info("="*50)
    logging.info(f"Iniciando processo de atualização do banco de dados...")
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
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("multisistema.log", mode='w'),
            logging.StreamHandler()
        ]
    )
    main()
    atualizar_CARIMBO()
