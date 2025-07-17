import logging
import os.path
from datetime import datetime
from typing import List, Dict, Tuple
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pandas as pd

from utils.helpers import retry_with_backoff

class SheetsService:
    """Encapsula todas as interações com a API do Google Sheets."""

    def __init__(self, google_config: Dict):
        self.spreadsheet_id = google_config['spreadsheet_id']
        self.scopes = google_config['scopes']
        self.creds = self._authenticate()
        self.service = build('sheets', 'v4', credentials=self.creds)

    def _authenticate(self) -> Credentials:
        creds = None
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', self.scopes)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', self.scopes)
                creds = flow.run_local_server(port=0)
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        return creds

    def get_data(self, range_name: str) -> Tuple[List[str], pd.DataFrame, Dict[str, int]]:
        def processo():
            result = self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=range_name).execute()
            return result.get("values", [])
        
        try:
            valores = retry_with_backoff(processo)
            if not valores: return [], pd.DataFrame(), {}
            
            cabecalho, dados = valores[0], valores[1:] if len(valores) > 1 else []
            df = pd.DataFrame(dados)
            if df.empty: return cabecalho, df, {}

            # Garante que o número de colunas do DF corresponda ao cabeçalho
            if df.shape[1] > len(cabecalho): df = df.iloc[:, :len(cabecalho)]
            df.columns = cabecalho
            
            id_para_indice = {str(id_unico).strip(): idx + 2 for idx, id_unico in enumerate(df.get('ID_Unico', [])) if pd.notna(id_unico) and str(id_unico).strip()}
            logging.info(f"Lidos {len(df)} registros da aba '{range_name}'.")
            return cabecalho, df, id_para_indice
        except Exception as e:
            logging.error(f"Erro definitivo ao obter dados da aba {range_name}: {e}")
            return [], pd.DataFrame(), {}
    
    def append_data(self, range_name: str, values: List[List]):
        if not values: return
        retry_with_backoff(lambda: self.service.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id, range=f"{range_name}!A1", valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS", body={"values": values}).execute())
        logging.info(f"Adicionadas {len(values)} novas linhas na aba '{range_name}'.")

    def batch_update_data(self, range_name: str, updates: List[Tuple[int, List]], num_cols_to_update: int):
        if not updates: return
        ultima_coluna = chr(ord('A') + num_cols_to_update - 1)
        data = [{'range': f"{range_name}!A{idx}:{ultima_coluna}{idx}", 'values': [data]} for idx, data in updates]
        retry_with_backoff(lambda: self.service.spreadsheets().values().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body={'valueInputOption': 'USER_ENTERED', 'data': data}).execute())
        logging.info(f"Atualizadas {len(updates)} linhas em lote na aba '{range_name}'.")

    def update_timestamp(self, range_name: str):
        timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        logging.info(f"Atualizando carimbo de execução em '{range_name}' para {timestamp}.")
        retry_with_backoff(lambda: self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id, range=range_name, valueInputOption="USER_ENTERED",
            body={"values": [[timestamp]]}).execute())