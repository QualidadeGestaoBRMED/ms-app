import pandas as pd
import os
import os.path
import configparser
import logging
import time
import random
from typing import Dict, List, Tuple, Optional
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
config.read('config.ini', encoding='utf-8')

# --- Configurações Globais ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SAMPLE_SPREADSHEET_ID = config['GOOGLE_SHEETS']['spreadsheet_id']
ABA_CARIMBO = config['GOOGLE_SHEETS']['aba_carimbo']
PASTA_DATABASES = config['CAMINHOS']['pasta_databases']
BRNET_CREDS = config['BRNET_CREDENCIALS']

# Colunas esperadas nos arquivos
COLUNAS_ESPERADAS = [
    'Paciente', 'CPF_Passaporte', 'Funcao', 'Setor', 'Empresa', 'Grupo',
    'Local_do_Atendimento', 'Atendido_Em', 'Previsto_Para', 'Liberado_Em',
    'Status_Expedicao_BR_MED', 'Exame_Alterado', 'Tipo_de_Pedido'
]

def validar_configuracoes():
    """
    Valida se todas as configurações necessárias estão presentes.
    Levanta ValueError se alguma configuração estiver faltando.
    """
    required_keys = ['usuario', 'senha', 'email_relatorio']
    for key in required_keys:
        if not BRNET_CREDS.get(key):
            raise ValueError(f"Credencial '{key}' não encontrada no config.ini")
    
    if not SAMPLE_SPREADSHEET_ID:
        raise ValueError("ID da planilha não encontrado no config.ini")
    
    if not os.path.exists(PASTA_DATABASES):
        os.makedirs(PASTA_DATABASES)
        logging.info(f"Pasta de databases criada: {PASTA_DATABASES}")
    
    logging.info("Configurações validadas com sucesso.")

def obter_colunas_rpa():
    """
    Retorna a lista das colunas que vêm do RPA (até coluna N).
    Colunas da coluna O em diante são do AppSheet e não devem ser sobrescritas.
    """
    return [
        'ID_Unico', 'Paciente', 'CPF/Passaporte', 'Função', 'Setor', 'Empresa', 'Grupo',
        'Local do Atendimento', 'Atendido Em', 'Previsto Para', 'Liberado Em',
        'Status Expedição - BR MED', 'Exame Alterado', 'Tipo de Pedido'
    ]

def retry_with_backoff(func, max_retries=3, base_delay=1):
    """
    Executa uma função com retry e backoff exponencial.
    """
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"Falha definitiva após {max_retries} tentativas: {e}")
                raise
            
            wait_time = (base_delay * (2 ** attempt)) + random.uniform(0, 1)
            logging.warning(f"Tentativa {attempt + 1} falhou: {e}. Aguardando {wait_time:.2f}s...")
            time.sleep(wait_time)

def excluir_arquivos_antigos():
    """
    Remove todos os arquivos da pasta de databases.
    Garante que a execução sempre comece com a pasta limpa.
    """
    logging.info(f"Limpando a pasta de destino: {PASTA_DATABASES}")
    try:
        arquivos_removidos = 0
        for filename in os.listdir(PASTA_DATABASES):
            file_path = os.path.join(PASTA_DATABASES, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                arquivos_removidos += 1
        logging.info(f"Pasta limpa com sucesso. {arquivos_removidos} arquivos removidos.")
    except Exception as e:
        logging.error(f"Erro ao limpar a pasta de databases: {e}")
        raise

def baixar_relatorios_playwright(grupo: str) -> bool:
    """
    Realiza o login no sistema BRNET via Playwright, preenche o formulário de relatório,
    executa o fluxo de 3 cliques para solicitar, atualizar e baixar o relatório do grupo informado.
    O arquivo baixado é salvo com nome padronizado na pasta de databases.
    Retorna True se o download foi bem-sucedido, False caso contrário.
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

    def download_processo():
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                
                page.set_default_timeout(30000)
                page.set_default_navigation_timeout(60000)
                
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
                
                if os.path.exists(caminho_salvo) and os.path.getsize(caminho_salvo) > 0:
                    logging.info(f"Download para '{grupo}' concluído com sucesso: {caminho_salvo}")
                    return True
                else:
                    raise Exception("Arquivo não foi salvo corretamente ou está vazio")
                    
            finally:
                browser.close()
    
    try:
        return retry_with_backoff(download_processo, max_retries=2, base_delay=2)
    except Exception as e:
        logging.error(f"Falha no download para o grupo {grupo}: {e}")
        return False

def baixar_relatorios_com_retry(grupos_para_baixar: List[str]) -> List[str]:
    """
    Realiza o download dos relatórios com sistema de retry melhorado.
    Retorna lista de grupos que falharam definitivamente.
    """
    tentativa = 1
    grupos_pendentes = grupos_para_baixar.copy()
    grupos_falharam_definitivamente = []
    
    while tentativa <= 3 and grupos_pendentes:
        logging.info(f"=== TENTATIVA {tentativa} de 3 ===")
        grupos_falharam_nesta_tentativa = []
        
        for i, grupo in enumerate(grupos_pendentes, 1):
            logging.info(f"Processando grupo {i}/{len(grupos_pendentes)}: {grupo}")
            
            try:
                sucesso = baixar_relatorios_playwright(grupo)
                if not sucesso:
                    grupos_falharam_nesta_tentativa.append(grupo)
                else:
                    logging.info(f"Sucesso no download: {grupo}")
            except Exception as e:
                logging.error(f"✗ Erro inesperado para {grupo}: {e}")
                grupos_falharam_nesta_tentativa.append(grupo)
            
            if i < len(grupos_pendentes):
                time.sleep(2)
        
        if grupos_falharam_nesta_tentativa:
            logging.warning(f"Grupos que falharam na tentativa {tentativa}: {grupos_falharam_nesta_tentativa}")
            grupos_pendentes = grupos_falharam_nesta_tentativa
            tentativa += 1
            
            if tentativa <= 3:
                pausa = 5 * tentativa
                logging.info(f"Aguardando {pausa}s antes da próxima tentativa...")
                time.sleep(pausa)
        else:
            logging.info("Todos os downloads foram concluídos com sucesso!")
            grupos_pendentes = []
            break
    
    if grupos_pendentes:
        grupos_falharam_definitivamente = grupos_pendentes
        logging.error("=== GRUPOS QUE FALHARAM DEFINITIVAMENTE ===")
        for grupo in grupos_falharam_definitivamente:
            logging.error(f"- {grupo}")
        
        print("\n" + "="*60)
        print("⚠️  ATENÇÃO: OS SEGUINTES GRUPOS FALHARAM NO DOWNLOAD:")
        for grupo in grupos_falharam_definitivamente:
            print(f"- {grupo}")
        print("="*60 + "\n")
    
    sucesso_count = len(grupos_para_baixar) - len(grupos_falharam_definitivamente)
    logging.info(f"Relatório de downloads: {sucesso_count}/{len(grupos_para_baixar)} grupos baixados com sucesso")
    
    return grupos_falharam_definitivamente

def autenticar_google_sheets():
    """
    Autentica na API do Google Sheets utilizando OAuth2 com tratamento de erro melhorado.
    Retorna as credenciais válidas para uso nas operações com a API.
    """
    creds = None
    try:
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logging.info("Renovando token de acesso...")
                creds.refresh(Request())
            else:
                logging.info("Realizando nova autenticação...")
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
                logging.info("Token salvo com sucesso.")
        
        return creds
    except Exception as e:
        logging.error(f"Erro na autenticação do Google Sheets: {e}")
        raise

def atualizar_CARIMBO():
    """
    Atualiza a aba de carimbo no Google Sheets com a data e hora atuais,
    indicando o momento da última execução bem-sucedida do script.
    """
    data_hora_termino = datetime.now().strftime("%d/%m/%y %H:%M")
    logging.info(f"Atualizando carimbo de execução com a data e hora: {data_hora_termino}")
    
    def atualizar_carimbo_processo():
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
    
    try:
        retry_with_backoff(atualizar_carimbo_processo, max_retries=3)
    except Exception as e:
        logging.error(f"Erro definitivo ao atualizar o carimbo: {e}")

def escrever_dados_planilha(dados_para_adicionar: List[List], nome_da_aba: str):
    """
    Adiciona novos dados à planilha usando o método append, que não interfere
    nas colunas adjacentes com fórmulas de array. Escreve apenas nas colunas
    gerenciadas pelo RPA (A até N).
    """
    if not dados_para_adicionar:
        return

    def append_dados_processo():
        creds = autenticar_google_sheets()
        service = build('sheets', 'v4', credentials=creds)
        
        body = {"values": dados_para_adicionar}
        
        result = service.spreadsheets().values().append(
            spreadsheetId=SAMPLE_SPREADSHEET_ID,
            range=f"{nome_da_aba}!A1", 
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body
        ).execute()
        
        logging.info(f"-> {result.get('updates').get('updatedCells')} células adicionadas com sucesso na aba '{nome_da_aba}' via append.")

    try:
        retry_with_backoff(append_dados_processo, max_retries=3)
    except Exception as e:
        logging.error(f"Erro definitivo ao adicionar dados no Google Sheets para aba {nome_da_aba}: {e}")

# ### NOVA FUNÇÃO ADICIONADA ###
def executar_updates_em_lote_colunas_rpa(updates_data: List[Tuple[int, List]], nome_da_aba: str, colunas_rpa: List[str]):
    """
    Executa updates em lote APENAS nas colunas gerenciadas pelo RPA (A-N).
    Preserva as colunas do AppSheet/ARRAYFORMULA de serem sobrescritas.
    """
    if not updates_data:
        return

    def executar_batch_update():
        creds = autenticar_google_sheets()
        service = build('sheets', 'v4', credentials=creds)
        
        ultima_coluna_letra = chr(ord('A') + len(colunas_rpa) - 1)
        
        batch_data = []
        for row_index, row_data in updates_data:
            range_to_update = f"{nome_da_aba}!A{row_index}:{ultima_coluna_letra}{row_index}"
            batch_data.append({
                'range': range_to_update,
                'values': [row_data]
            })
            
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': batch_data
        }
        
        result = service.spreadsheets().values().batchUpdate(
            spreadsheetId=SAMPLE_SPREADSHEET_ID,
            body=body
        ).execute()
        
        total_updated = result.get('totalUpdatedCells', 0)
        logging.info(f"-> {total_updated} células atualizadas em lote (colunas A-{ultima_coluna_letra}) na aba '{nome_da_aba}'.")

    try:
        retry_with_backoff(executar_batch_update, max_retries=3)
    except Exception as e:
        logging.error(f"Erro definitivo ao executar updates em lote (colunas RPA) na aba {nome_da_aba}: {e}")

def sanitizar_nome_coluna(nome_coluna: str) -> str:
    """
    Remove acentos e substitui caracteres problemáticos por underscore nos nomes de colunas.
    Garante compatibilidade dos nomes de colunas.
    """
    if pd.isna(nome_coluna):
        return "coluna_vazia"
    
    s = ''.join(c for c in unicodedata.normalize('NFD', str(nome_coluna))
                if unicodedata.category(c) != 'Mn')
    return s.replace(' ', '_').replace('-', '_').replace('.', '_').replace('/', '_')

def encontrar_cabecalho(df: pd.DataFrame, palavras_chave: List[str] = None) -> int:
    """
    Encontra a linha do cabeçalho no DataFrame baseado em palavras-chave.
    Retorna o índice da linha do cabeçalho ou -1 se não encontrar.
    """
    if palavras_chave is None:
        palavras_chave = ['Paciente', 'CPF', 'Nome', 'Previsto']
    
    for i in range(min(15, len(df))):
        row_str = ' '.join(str(val) for val in df.iloc[i].values if pd.notna(val))
        if any(palavra in row_str for palavra in palavras_chave):
            logging.info(f"Cabeçalho encontrado na linha {i}: {row_str[:100]}...")
            return i
    
    logging.warning(f"Cabeçalho não encontrado com palavras-chave: {palavras_chave}")
    return -1

def verificar_schema_arquivo(df: pd.DataFrame, nome_arquivo: str) -> Dict[str, any]:
    """
    Verifica o schema do arquivo e detecta possíveis problemas.
    Retorna um dicionário com informações sobre o schema.
    """
    colunas_atuais = set(df.columns)
    colunas_esperadas_set = set(COLUNAS_ESPERADAS)
    
    info_schema = {
        'colunas_faltando': colunas_esperadas_set - colunas_atuais,
        'colunas_extras': colunas_atuais - colunas_esperadas_set,
        'total_colunas': len(df.columns),
        'total_linhas': len(df),
        'schema_ok': True
    }
    
    if info_schema['colunas_faltando']:
        logging.warning(f"[{nome_arquivo}] Colunas faltando: {info_schema['colunas_faltando']}")
        info_schema['schema_ok'] = False
    
    if info_schema['colunas_extras']:
        logging.info(f"[{nome_arquivo}] Colunas extras encontradas: {info_schema['colunas_extras']}")
    
    if info_schema['total_linhas'] == 0:
        logging.warning(f"[{nome_arquivo}] Arquivo está vazio!")
        info_schema['schema_ok'] = False
    
    logging.info(f"[{nome_arquivo}] Schema: {info_schema['total_linhas']} linhas, {info_schema['total_colunas']} colunas")
    
    return info_schema

def obter_dados_existentes_planilha_completos(nome_aba: str) -> Tuple[List[str], pd.DataFrame, Dict[str, int]]:
    """
    Obtém todos os dados existentes da aba do Google Sheets com retry.
    Retorna o cabeçalho, DataFrame com todos os dados e um dicionário mapeando ID_Unico -> índice da linha.
    """
    def obter_dados_processo():
        creds = autenticar_google_sheets()
        service = build("sheets", "v4", credentials=creds)
        result = service.spreadsheets().values().get(
            spreadsheetId=SAMPLE_SPREADSHEET_ID, range=nome_aba).execute()
        return result.get("values", [])
    
    try:
        valores_existentes = retry_with_backoff(obter_dados_processo, max_retries=3)
        
        if not valores_existentes:
            logging.warning(f"Aba '{nome_aba}' está vazia.")
            return [], pd.DataFrame(), {}
        
        cabecalho_planilha = valores_existentes[0]
        
        # ### PROTEÇÃO CONTRA CABEÇALHOS DUPLICADOS ###
        if len(cabecalho_planilha) != len(set(cabecalho_planilha)):
            logging.error(f"ERRO CRÍTICO NA ABA '{nome_aba}': Nomes de colunas duplicados encontrados no cabeçalho. Corrija a planilha.")
            # Opcional: poderia levantar uma exceção para parar o script
            # raise ValueError(f"Cabeçalho duplicado na aba {nome_aba}")
            return [], pd.DataFrame(), {}

        if len(valores_existentes) <= 1:
            return cabecalho_planilha, pd.DataFrame(), {}
        
        dados_existentes = valores_existentes[1:]
        df_sheet = pd.DataFrame(dados_existentes)
        
        if df_sheet.shape[1] > len(cabecalho_planilha):
            df_sheet = df_sheet.iloc[:, :len(cabecalho_planilha)]

        while df_sheet.shape[1] < len(cabecalho_planilha):
            df_sheet[f'temp_col_{df_sheet.shape[1]}'] = ''
        
        df_sheet.columns = cabecalho_planilha
        
        id_para_indice = {}
        if 'ID_Unico' in df_sheet.columns:
            for idx, id_unico in enumerate(df_sheet['ID_Unico']):
                if pd.notna(id_unico) and str(id_unico).strip():
                    id_para_indice[str(id_unico).strip()] = idx + 2 # +2 porque sheets é base 1 e a linha 1 é o cabeçalho
        
        logging.info(f"Dados obtidos da aba {nome_aba}: {len(df_sheet)} registros, {len(id_para_indice)} com ID único")
        return cabecalho_planilha, df_sheet, id_para_indice
        
    except Exception as e:
        logging.error(f"Erro definitivo ao obter dados existentes da aba {nome_aba}: {e}")
        return [], pd.DataFrame(), {}

def comparar_linhas(linha_arquivo: pd.Series, linha_planilha: pd.Series, colunas_comparar: List[str]) -> bool:
    """
    Compara duas linhas (Series do pandas) para verificar se há diferenças.
    Retorna True se as linhas são diferentes, False se são iguais.
    APENAS compara colunas que vêm do arquivo RPA (até coluna N).
    """
    for coluna in colunas_comparar:
        if coluna in linha_arquivo.index and coluna in linha_planilha.index:
            valor_arquivo = str(linha_arquivo.get(coluna, '')).strip()
            valor_planilha = str(linha_planilha.get(coluna, '')).strip()
            
            if valor_arquivo != valor_planilha:
                logging.debug(f"Diferença em '{coluna}': Arquivo='{valor_arquivo}', Planilha='{valor_planilha}'")
                return True
    
    return False

def encontrar_arquivo_por_palavra_chave(palavra_chave: str, nome_aba: str) -> Optional[str]:
    """
    Encontra um arquivo na pasta DATABASES usando palavras-chave com busca melhorada.
    Retorna o caminho do arquivo encontrado ou None se não encontrar.
    """
    if not os.path.exists(PASTA_DATABASES):
        logging.error(f"Pasta de databases não existe: {PASTA_DATABASES}")
        return None
    
    arquivos_na_pasta = os.listdir(PASTA_DATABASES)
    if not arquivos_na_pasta:
        logging.warning(f"Pasta de databases está vazia: {PASTA_DATABASES}")
        return None
    
    logging.info(f"Procurando arquivo para '{palavra_chave}' (aba: {nome_aba}) entre {len(arquivos_na_pasta)} arquivos")
    
    # 1. Tenta por correspondência exata da chave do config (ex: 'grupo_grupotrigo')
    arquivos_exatos = [f for f in arquivos_na_pasta if palavra_chave.lower() in f.lower()]
    if arquivos_exatos:
        arquivo_encontrado = arquivos_exatos[0]
        logging.info(f"Arquivo encontrado por correspondência de chave exata: {arquivo_encontrado}")
        return os.path.join(PASTA_DATABASES, arquivo_encontrado)

    # 2. Se não encontrar, busca por palavras-chave extraídas do nome da ABA
    palavras_chave_aba = extrair_palavras_chave(nome_aba)
    
    # Busca por TODAS as palavras-chave
    for arquivo in arquivos_na_pasta:
        nome_arquivo_lower = arquivo.lower()
        if all(palavra.lower() in nome_arquivo_lower for palavra in palavras_chave_aba):
            logging.info(f"Arquivo encontrado por palavras-chave (ALL): '{palavras_chave_aba}': {arquivo}")
            return os.path.join(PASTA_DATABASES, arquivo)

    # 3. Busca flexível - QUALQUER uma das palavras-chave
    for arquivo in arquivos_na_pasta:
        nome_arquivo_lower = arquivo.lower()
        if any(palavra.lower() in nome_arquivo_lower for palavra in palavras_chave_aba):
            logging.info(f"Arquivo encontrado por busca flexível (ANY): '{palavras_chave_aba}': {arquivo}")
            return os.path.join(PASTA_DATABASES, arquivo)

    logging.warning(f"✗ Nenhum arquivo encontrado para '{palavra_chave}' ou palavras-chave {palavras_chave_aba}")
    return None

def extrair_palavras_chave(nome_aba: str) -> List[str]:
    """
    Extrai palavras-chave do nome da aba para busca de arquivos com lógica melhorada.
    Remove palavras comuns e mantém apenas as mais significativas.
    """
    palavras_ignorar = {
        'grupo', 'da', 'de', 'do', 'dos', 'das', 'e', 'em', 'para', 'com', 
        's.a.', 'sa', 'ltda', 'ltd', 'inc', 'corp', 'empresa', 'companhia',
        'exames', 'ocupacionais'
    }
    
    nome_limpo = nome_aba.lower().replace('.', ' ').replace('-', ' ').replace('/', ' ').replace('_', ' ')
    palavras = nome_limpo.split()
    
    palavras_significativas = [
        palavra for palavra in palavras 
        if len(palavra) >= 3 and palavra not in palavras_ignorar
    ]
    
    if not palavras_significativas:
        primeira_palavra = nome_aba.split()[0].lower() if nome_aba.split() else nome_aba.lower()
        palavras_significativas = [primeira_palavra]
    
    logging.info(f"Palavras-chave extraídas de '{nome_aba}': {palavras_significativas}")
    return palavras_significativas

# ### FUNÇÃO PRINCIPAL DE PROCESSAMENTO - TOTALMENTE REFEITA ###
def processar_e_sincronizar_arquivo_com_update(chave_arquivo, nome_aba):
    """
    Processa um arquivo e sincroniza com o Google Sheets de forma robusta e segura.
    - Adiciona novos registros escrevendo apenas nas colunas A-N.
    - Atualiza registros existentes modificando apenas as colunas A-N.
    - PRESERVA as colunas com ARRAYFORMULA e dados do AppSheet.
    """
    logging.info(f"--- Processando e sincronizando: {nome_aba} ---")
    
    caminho_completo = encontrar_arquivo_por_palavra_chave(chave_arquivo, nome_aba)
    if not caminho_completo:
        logging.warning(f"Nenhum arquivo encontrado para {chave_arquivo} (aba: {nome_aba})")
        return

    nome_arquivo = os.path.basename(caminho_completo)
    logging.info(f"Processando arquivo: {nome_arquivo}")
    
    try:
        # 1. LEITURA E LIMPEZA DO ARQUIVO
        tabelas = pd.read_html(caminho_completo, encoding='utf-8')
        df_arquivo = tabelas[0]
        
        header_row_index = next((i for i, row in df_arquivo.head(10).iterrows() if 'Paciente' in str(row.values)), -1)
        if header_row_index == -1:
            logging.warning(f"Cabeçalho 'Paciente' não encontrado no arquivo {nome_arquivo}. Pulando.")
            return
            
        df_arquivo.columns = df_arquivo.iloc[header_row_index]
        df_arquivo = df_arquivo.iloc[header_row_index + 1:].reset_index(drop=True)
        df_arquivo.columns = [sanitizar_nome_coluna(col) for col in df_arquivo.columns]
        
        colunas_essenciais = ['CPF_Passaporte', 'Previsto_Para', 'Tipo_de_Pedido']
        df_arquivo.dropna(subset=colunas_essenciais, how='any', inplace=True)
        if df_arquivo.empty:
            logging.warning(f"Nenhuma linha válida no arquivo {nome_arquivo} após validação.")
            return
            
        # 2. PROCESSAMENTO E CRIAÇÃO DO DATAFRAME FINAL
        mapa_nomes_invertido = {
            'CPF_Passaporte': 'CPF/Passaporte', 'Local_do_Atendimento': 'Local do Atendimento',
            'Atendido_Em': 'Atendido Em', 'Previsto_Para': 'Previsto Para', 'Liberado_Em': 'Liberado Em',
            'Status_Expedicao_BR_MED': 'Status Expedição - BR MED', 'Exame_Alterado': 'Exame Alterado',
            'Tipo_de_Pedido': 'Tipo de Pedido', 'Funcao': 'Função'
        }
        colunas_arquivo_renomeadas = {v: k for k, v in mapa_nomes_invertido.items()}

        df_processado = pd.DataFrame()
        for col_planilha in obter_colunas_rpa():
            col_sanitizada = colunas_arquivo_renomeadas.get(col_planilha, col_planilha)
            col_sanitizada = sanitizar_nome_coluna(col_sanitizada)
            
            if col_sanitizada in df_arquivo.columns:
                df_processado[col_planilha] = df_arquivo[col_sanitizada]
        
        df_processado['Previsto Para'] = pd.to_datetime(
            df_processado['Previsto Para'], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y').fillna('')
        
        df_processado['ID_Unico'] = (
            df_processado['CPF/Passaporte'].astype(str).str.strip() + "-" +
            df_processado['Previsto Para'].astype(str).str.strip() + "-" +
            df_processado['Tipo de Pedido'].astype(str).str.strip().str[:3]
        )
        
        df_processado.drop_duplicates(subset=['ID_Unico'], keep='first', inplace=True)
        
        # 3. OBTENÇÃO DOS DADOS DA PLANILHA E COMPARAÇÃO
        cabecalho_planilha, df_planilha_existente, id_para_indice = obter_dados_existentes_planilha_completos(nome_aba)
        if not cabecalho_planilha:
            return # Erro já foi logado na função
            
        colunas_rpa = obter_colunas_rpa()
        registros_novos = []
        registros_para_atualizar = []

        for _, linha_arquivo in df_processado.iterrows():
            id_unico = str(linha_arquivo['ID_Unico']).strip()

            if id_unico in id_para_indice:
                indice_planilha = id_para_indice[id_unico]
                linha_planilha = df_planilha_existente.iloc[indice_planilha - 2]
                
                if comparar_linhas(linha_arquivo, linha_planilha, colunas_rpa):
                    logging.info(f"Detectada atualização para ID: {id_unico}")
                    dados_para_atualizar = linha_arquivo[colunas_rpa].fillna('').tolist()
                    registros_para_atualizar.append((indice_planilha, dados_para_atualizar))
            else:
                logging.info(f"Detectado novo registro: ID {id_unico}")
                dados_para_adicionar = linha_arquivo[colunas_rpa].fillna('').tolist()
                registros_novos.append(dados_para_adicionar)

        # 4. EXECUÇÃO DAS OPERAÇÕES NA PLANILHA
        if registros_novos:
            logging.info(f"Adicionando {len(registros_novos)} novos registros (apenas colunas A-N)...")
            escrever_dados_planilha(registros_novos, nome_aba)
        
        if registros_para_atualizar:
            logging.info(f"Atualizando {len(registros_para_atualizar)} registros (apenas colunas A-N)...")
            executar_updates_em_lote_colunas_rpa(registros_para_atualizar, nome_aba, colunas_rpa)
        
        if not registros_novos and not registros_para_atualizar:
            logging.info(f"Nenhuma alteração necessária na aba {nome_aba}. Dados já estão sincronizados.")
        else:
            total = len(registros_novos) + len(registros_para_atualizar)
            logging.info(f"Sincronização concluída para {nome_aba}: {len(registros_novos)} novos, {len(registros_para_atualizar)} atualizados = {total} operações.")
            
    except Exception as e:
        logging.exception(f"Erro CRÍTICO ao processar arquivo para aba {nome_aba}: {e}")


def main():
    """
    Função principal que orquestra todo o fluxo.
    """
    validar_configuracoes()
    mapa_arquivos_abas = dict(config['MAPEAMENTO_ARQUIVOS_ABAS'])
    excluir_arquivos_antigos()
    
    logging.info("="*50)
    logging.info("Iniciando fase de download dos relatórios do BRNET...")
    logging.info("="*50)
    
    # ### ALTERAÇÃO REVERTIDA ###
    # Voltamos a usar a lista fixa para garantir que os nomes corretos sejam usados no download.
    grupos_para_baixar = [
        "GRUPO TRIGO", "ICTSI RIO", "CONCREMAT", "CONSTELLATION - EXAMES OCUPACIONAIS", "VLT RIO",
        "V.TAL - REDE NEUTRA DE TELECOMUNICACOES S.A.", "IKM", "BAKER HUGHES", "RIP ES", "RIP MACAÉ"
    ]
    
    baixar_relatorios_com_retry(grupos_para_baixar)
    
    logging.info("="*50)
    logging.info("Iniciando processamento e sincronização com UPDATE em lote...")
    logging.info("="*50)
    
    for chave_arquivo, nome_aba in mapa_arquivos_abas.items():
        processar_e_sincronizar_arquivo_com_update(chave_arquivo, nome_aba)
    
    logging.info("Processo principal finalizado com funcionalidade de UPDATE.")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("multisistema.log", mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    main()
    atualizar_CARIMBO()