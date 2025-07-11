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
                
                # Configurações de timeout mais robustas
                page.set_default_timeout(30000)
                page.set_default_navigation_timeout(60000)
                
                # Login e preenchimento do formulário
                page.goto(link, timeout=60000)
                page.locator("#username").fill(usuario)
                page.get_by_placeholder("Digite sua senha").fill(senha)
                page.get_by_role("button", name="Entrar").click()
                
                # Aguarda e preenche o formulário
                page.locator("#id_company_group").wait_for(timeout=20000)
                page.locator("#id_company_group").select_option(label=grupo)
                page.locator("#id_start").fill(start_date)
                page.locator("#id_end").fill(end_date)
                page.locator("#id_email").fill(email)
                
                # Solicita o relatório
                page.locator("#submit").click()
                logging.info("Relatório solicitado. Aguardando a tabela de resultados...")
                
                # Aguarda e atualiza a tabela
                page.locator("#reload_table").wait_for(timeout=30000)
                page.locator("#reload_table").click()
                logging.info("Tabela de resultados atualizada. Aguardando link de download...")
                
                # Realiza o download
                with page.expect_download(timeout=60000) as download_info:
                    page.get_by_role("link", name="Download").first.click()
                
                download = download_info.value
                safe_grupo_name = "".join(c for c in grupo if c.isalnum()).lower()
                nome_seguro_arquivo = f"grupo_{safe_grupo_name}_{ontem.strftime('%Y%m%d')}.xls"
                caminho_salvo = os.path.join(PASTA_DATABASES, nome_seguro_arquivo)
                download.save_as(caminho_salvo)
                
                # Verifica se o arquivo foi salvo corretamente
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
            
            # Pequena pausa entre downloads para evitar sobrecarga
            if i < len(grupos_pendentes):
                time.sleep(2)
        
        if grupos_falharam_nesta_tentativa:
            logging.warning(f"Grupos que falharam na tentativa {tentativa}: {grupos_falharam_nesta_tentativa}")
            grupos_pendentes = grupos_falharam_nesta_tentativa
            tentativa += 1
            
            # Pausa maior entre tentativas
            if tentativa <= 3:
                pausa = 5 * tentativa
                logging.info(f"Aguardando {pausa}s antes da próxima tentativa...")
                time.sleep(pausa)
        else:
            logging.info("Todos os downloads foram concluídos com sucesso!")
            grupos_pendentes = []
            break
    
    # Grupos que falharam definitivamente
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
    
    # Relatório final
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
    Escreve os dados fornecidos na primeira linha vazia da aba especificada do Google Sheets.
    """
    def escrever_dados_processo():
        creds = autenticar_google_sheets()
        service = build('sheets', 'v4', credentials=creds)
        
        # Obtém o número de linhas existentes
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
        
        logging.info(f"-> {request.get('updatedCells')} células adicionadas com sucesso na aba '{nome_da_aba}'.")
    
    try:
        retry_with_backoff(escrever_dados_processo, max_retries=3)
    except Exception as e:
        logging.error(f"Erro definitivo ao escrever dados no Google Sheets para aba {nome_da_aba}: {e}")

def executar_updates_em_lote(updates_data: List[Tuple[int, List]], nome_da_aba: str):
    """
    Executa múltiplas atualizações em lote usando batchUpdate do Google Sheets.
    Muito mais eficiente que fazer updates individuais.
    """
    if not updates_data:
        return
    
    def executar_batch_update():
        creds = autenticar_google_sheets()
        service = build('sheets', 'v4', credentials=creds)
        
        # Prepara as requisições em lote
        batch_data = []
        for row_index, row_data in updates_data:
            batch_data.append({
                'range': f"{nome_da_aba}!A{row_index + 1}",  # +1 porque Sheets usa índice 1
                'values': [row_data]
            })
        
        # Executa o batch update
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': batch_data
        }
        
        result = service.spreadsheets().values().batchUpdate(
            spreadsheetId=SAMPLE_SPREADSHEET_ID,
            body=body
        ).execute()
        
        total_updated = result.get('totalUpdatedCells', 0)
        logging.info(f"-> {total_updated} células atualizadas em lote na aba '{nome_da_aba}'.")
    
    try:
        retry_with_backoff(executar_batch_update, max_retries=3)
    except Exception as e:
        logging.error(f"Erro definitivo ao executar updates em lote na aba {nome_da_aba}: {e}")

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
    
    # Procura nas primeiras 15 linhas
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
        
        if len(valores_existentes) <= 1:
            # Apenas cabeçalho, sem dados
            return cabecalho_planilha, pd.DataFrame(), {}
        
        dados_existentes = valores_existentes[1:]
        
        # Criar DataFrame com os dados existentes
        df_sheet = pd.DataFrame(dados_existentes)
        
        # Garantir que temos o número correto de colunas
        num_cols_para_renomear = min(len(cabecalho_planilha), df_sheet.shape[1])
        
        # Adicionar colunas vazias se necessário
        while df_sheet.shape[1] < len(cabecalho_planilha):
            df_sheet[f'temp_col_{df_sheet.shape[1]}'] = ''
        
        # Renomear colunas
        df_sheet.columns = cabecalho_planilha[:df_sheet.shape[1]]
        
        # Criar mapeamento ID_Unico -> índice da linha (baseado em 0, mas será convertido para base 1 no update)
        id_para_indice = {}
        if 'ID_Unico' in df_sheet.columns:
            for idx, id_unico in enumerate(df_sheet['ID_Unico']):
                if pd.notna(id_unico) and str(id_unico).strip():
                    id_para_indice[str(id_unico).strip()] = idx + 1  # +1 porque a linha 0 é o cabeçalho
        
        logging.info(f"Dados obtidos da aba {nome_aba}: {len(df_sheet)} registros, {len(id_para_indice)} com ID único")
        return cabecalho_planilha, df_sheet, id_para_indice
        
    except Exception as e:
        logging.error(f"Erro definitivo ao obter dados existentes da aba {nome_aba}: {e}")
        return [], pd.DataFrame(), {}

def comparar_linhas(linha_arquivo: pd.Series, linha_planilha: pd.Series, colunas_comparar: List[str]) -> bool:
    """
    Compara duas linhas (Series do pandas) para verificar se há diferenças.
    Retorna True se as linhas são diferentes, False se são iguais.
    """
    diferencas_encontradas = []
    
    for coluna in colunas_comparar:
        if coluna in linha_arquivo.index and coluna in linha_planilha.index:
            valor_arquivo = str(linha_arquivo[coluna]).strip() if pd.notna(linha_arquivo[coluna]) else ''
            valor_planilha = str(linha_planilha[coluna]).strip() if pd.notna(linha_planilha[coluna]) else ''
            
            if valor_arquivo != valor_planilha:
                diferencas_encontradas.append(coluna)
    
    if diferencas_encontradas:
        logging.debug(f"Diferenças encontradas nas colunas: {diferencas_encontradas}")
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
    
    logging.info(f"Procurando arquivo para '{palavra_chave}' entre {len(arquivos_na_pasta)} arquivos")
    
    # Primeiro tenta encontrar por correspondência exata da chave original
    arquivos_exatos = [f for f in arquivos_na_pasta if f.startswith(palavra_chave)]
    if arquivos_exatos:
        arquivo_encontrado = arquivos_exatos[0]
        logging.info(f"Arquivo encontrado por correspondência exata: {arquivo_encontrado}")
        return os.path.join(PASTA_DATABASES, arquivo_encontrado)
    
    # Se não encontrar, procura por palavras-chave baseadas no nome da aba
    palavras_chave_aba = extrair_palavras_chave(nome_aba)
    
    for arquivo in arquivos_na_pasta:
        nome_arquivo_lower = arquivo.lower()
        # Verifica se todas as palavras-chave estão presentes no nome do arquivo
        if all(palavra.lower() in nome_arquivo_lower for palavra in palavras_chave_aba):
            logging.info(f"Arquivo encontrado por palavras-chave '{palavras_chave_aba}': {arquivo}")
            return os.path.join(PASTA_DATABASES, arquivo)
    
    # Busca mais flexível - pelo menos uma palavra-chave
    for arquivo in arquivos_na_pasta:
        nome_arquivo_lower = arquivo.lower()
        if any(palavra.lower() in nome_arquivo_lower for palavra in palavras_chave_aba):
            logging.info(f"✓ Arquivo encontrado por busca flexível: {arquivo}")
            return os.path.join(PASTA_DATABASES, arquivo)
    
    logging.warning(f"✗ Nenhum arquivo encontrado para '{palavra_chave}' ou palavras-chave {palavras_chave_aba}")
    logging.info(f"Arquivos disponíveis: {arquivos_na_pasta}")
    return None

def extrair_palavras_chave(nome_aba: str) -> List[str]:
    """
    Extrai palavras-chave do nome da aba para busca de arquivos com lógica melhorada.
    Remove palavras comuns e mantém apenas as mais significativas.
    """
    # Palavras que devem ser ignoradas na busca
    palavras_ignorar = {
        'grupo', 'da', 'de', 'do', 'dos', 'das', 'e', 'em', 'para', 'com', 
        's.a.', 'sa', 'ltda', 'ltd', 'inc', 'corp', 'empresa', 'companhia'
    }
    
    # Divide o nome da aba em palavras e remove pontuações
    nome_limpo = nome_aba.lower().replace('.', ' ').replace('-', ' ').replace('/', ' ').replace('_', ' ')
    palavras = nome_limpo.split()
    
    # Filtra palavras significativas (remove palavras muito curtas e palavras a ignorar)
    palavras_significativas = [
        palavra for palavra in palavras 
        if len(palavra) >= 3 and palavra not in palavras_ignorar
    ]
    
    # Se não sobrar nenhuma palavra significativa, usa a primeira palavra original
    if not palavras_significativas:
        primeira_palavra = nome_aba.split()[0].lower() if nome_aba.split() else nome_aba.lower()
        palavras_significativas = [primeira_palavra]
    
    logging.info(f"Palavras-chave extraídas de '{nome_aba}': {palavras_significativas}")
    return palavras_significativas

def processar_e_sincronizar_arquivo_com_update(chave_arquivo, nome_aba):
    """
    Processa um arquivo específico e sincroniza com o Google Sheets.
    Detecta e atualiza registros modificados além de adicionar novos.
    Usa operações em lote para máxima eficiência.
    """
    logging.info(f"--- Processando e sincronizando com UPDATE: {nome_aba} ---")
    
    # Busca o arquivo usando a função inteligente
    caminho_completo = encontrar_arquivo_por_palavra_chave(chave_arquivo, nome_aba)
    
    if not caminho_completo:
        logging.warning(f"Nenhum arquivo encontrado para {chave_arquivo} (aba: {nome_aba})")
        return
    
    nome_arquivo = os.path.basename(caminho_completo)
    logging.info(f"Processando arquivo: {nome_arquivo}")
    
    try:
        # Lê o arquivo
        tabelas = pd.read_html(caminho_completo, encoding='utf-8')
        df_arquivo = tabelas[0]
        
        # Encontra o cabeçalho
        header_row_index = next((i for i, row in df_arquivo.head(10).iterrows() 
                               if 'Paciente' in str(row.values)), -1)
        
        if header_row_index == -1:
            logging.warning(f"Cabeçalho 'Paciente' não encontrado no arquivo {nome_arquivo}. Pulando.")
            return
        
        # Configura o DataFrame
        df_arquivo.columns = df_arquivo.iloc[header_row_index]
        df_arquivo = df_arquivo.iloc[header_row_index + 1:].reset_index(drop=True)
        df_arquivo.columns = [sanitizar_nome_coluna(col) for col in df_arquivo.columns]
        
        # Valida colunas essenciais
        colunas_essenciais = ['CPF_Passaporte', 'Previsto_Para', 'Tipo_de_Pedido']
        df_arquivo.dropna(subset=colunas_essenciais, inplace=True)
        
        if df_arquivo.empty:
            logging.warning(f"Nenhuma linha válida no arquivo {nome_arquivo} após validação.")
            return
        
        # Mapeia as colunas para o formato final
        colunas_db = {
            'ID_Unico': 'ID_Unico', 'Paciente': 'Paciente', 'CPF_Passaporte': 'CPF_Passaporte',
            'Funcao': 'Funcao', 'Setor': 'Setor', 'Empresa': 'Empresa', 'Grupo': 'Grupo',
            'Local_do_Atendimento': 'Local_do_Atendimento', 'Atendido_Em': 'Atendido_Em',
            'Previsto_Para': 'Previsto_Para', 'Liberado_Em': 'Liberado_Em',
            'Status_Expedicao_BR_MED': 'Status_Expedicao_BR_MED', 'Exame_Alterado': 'Exame_Alterado',
            'Tipo_de_Pedido': 'Tipo_de_Pedido'
        }
        
        df_processado = pd.DataFrame()
        for db_col, arquivo_col in colunas_db.items():
            if arquivo_col in df_arquivo.columns:
                df_processado[db_col] = df_arquivo[arquivo_col]
            else:
                df_processado[db_col] = ''
        
        # Formata a data e cria o ID único
        df_processado['Previsto_Para'] = pd.to_datetime(
            df_processado['Previsto_Para'], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y').fillna('')
        
        df_processado['ID_Unico'] = (
            df_processado['CPF_Passaporte'].astype(str).str.strip() + "-" +
            df_processado['Previsto_Para'].astype(str).str.strip() + "-" +
            df_processado['Tipo_de_Pedido'].astype(str).str.strip().str[:3]
        )
        
        # Remove duplicatas dentro do próprio arquivo
        df_processado.drop_duplicates(subset=['ID_Unico'], keep='first', inplace=True)
        
        # Obtém dados existentes da planilha
        cabecalho_planilha, df_planilha_existente, id_para_indice = obter_dados_existentes_planilha_completos(nome_aba)
        
        if not cabecalho_planilha:
            logging.warning(f"Não foi possível obter o cabeçalho da aba {nome_aba}")
            return
        
        # Renomeia as colunas para o formato da planilha
        mapa_nomes_invertido = {
            'CPF_Passaporte': 'CPF/Passaporte', 
            'Local_do_Atendimento': 'Local do Atendimento',
            'Atendido_Em': 'Atendido Em', 
            'Previsto_Para': 'Previsto Para', 
            'Liberado_Em': 'Liberado Em',
            'Status_Expedicao_BR_MED': 'Status Expedição - BR MED', 
            'Exame_Alterado': 'Exame Alterado',
            'Tipo_de_Pedido': 'Tipo de Pedido', 
            'Funcao': 'Função'
        }
        
        df_processado.rename(columns=mapa_nomes_invertido, inplace=True)
        
        # Reordena as colunas conforme o cabeçalho da planilha
        df_final = df_processado.reindex(columns=cabecalho_planilha, fill_value='')
        
        # Separa registros novos e atualizações
        registros_novos = []
        registros_para_atualizar = []
        
        # Colunas que devem ser comparadas para detectar mudanças (excluindo ID_Unico)
        colunas_para_comparar = [col for col in cabecalho_planilha if col != 'ID_Unico']
        
        for idx, linha_arquivo in df_final.iterrows():
            id_unico = str(linha_arquivo['ID_Unico']).strip()
            
            if id_unico in id_para_indice:
                # Registro já existe - verificar se precisa ser atualizado
                indice_planilha = id_para_indice[id_unico]
                linha_planilha = df_planilha_existente.iloc[indice_planilha - 1]  # -1 porque id_para_indice já considera o cabeçalho
                
                # Comparar se há diferenças
                if comparar_linhas(linha_arquivo, linha_planilha, colunas_para_comparar):
                    # Há diferenças - adicionar à lista de atualizações
                    dados_linha = linha_arquivo.fillna('').tolist()
                    registros_para_atualizar.append((indice_planilha, dados_linha))
            else:
                # Registro novo - adicionar à lista de novos
                dados_linha = linha_arquivo.fillna('').tolist()
                registros_novos.append(dados_linha)
        
        # Executar as operações
        total_operacoes = 0
        
        # 1. ADICIONAR registros novos
        if registros_novos:
            logging.info(f"Adicionando {len(registros_novos)} novos registros na aba {nome_aba}...")
            escrever_dados_planilha(registros_novos, nome_aba)
            total_operacoes += len(registros_novos)
        
        # 2. ATUALIZAR registros modificados (EM LOTE)
        if registros_para_atualizar:
            logging.info(f"Atualizando {len(registros_para_atualizar)} registros modificados na aba {nome_aba}...")
            executar_updates_em_lote(registros_para_atualizar, nome_aba)
            total_operacoes += len(registros_para_atualizar)
        
        # Relatório final
        if total_operacoes == 0:
            logging.info(f"Nenhuma alteração necessária na aba {nome_aba}. Dados já estão sincronizados.")
        else:
            logging.info(f"Sincronização concluída para {nome_aba}: {len(registros_novos)} novos + {len(registros_para_atualizar)} atualizados = {total_operacoes} operações.")
            
    except Exception as e:
        logging.exception(f"Erro ao processar arquivo para aba {nome_aba}: {e}")

def main():
    """
    Função principal que orquestra todo o fluxo:
    1. Limpa a pasta de databases.
    2. Baixa os relatórios dos grupos definidos com sistema de retry.
    3. Processa e sincroniza cada arquivo com Google Sheets (INCLUINDO UPDATES).
    4. Atualiza o carimbo de execução.
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
    
    # Executa o download com sistema de retry
    grupos_falharam = baixar_relatorios_com_retry(grupos_para_baixar)
    
    logging.info("="*50)
    logging.info("Iniciando processamento e sincronização com UPDATE em lote...")
    logging.info("="*50)
    
    # Processa e sincroniza cada arquivo
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