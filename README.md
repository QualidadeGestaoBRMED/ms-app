# **BR MED - Sistema de Automação do Multisistemas**

O objetivo principal deste projeto é automatizar o processo de download de relatórios dos grupos configurados no BRNET, sincronização com planilhas do Google Sheets, garantindo a atualização contínua dos dados de atendimentos médicos dos diferentes grupos empresariais no App Sheet.

## **Tecnologias Utilizadas**

Este projeto foi desenvolvido com as seguintes tecnologias:

* Python 3.8+
* Playwright (automação web)
* Google Sheets API
* SQLite (banco de dados local)
* Pandas (processamento de dados)
* App Sheet (visual do aplicativo)

## **Pré-requisitos**

* Git
* Python 3.8 ou superior
* Editor de código (VS Code recomendado)
* Acesso às credenciais do Google Sheets API
* Credenciais de acesso ao sistema BRNET

## **Clonando o Repositório**

```bash
git clone https://github.com/grupobrmed/multisistemas-app.git
cd multisistemas-app

```
## **Instalação das Dependências**
```bash
pip install -r requirements.txt

```

## **Instalação do Playwright**
Após instalar as dependências Python, é necessário instalar os navegadores do Playwright:

```bash
playwright install chromium
```

## **Configuração de Ambiente**
### **Arquivo de Configuração (config.ini)**
Crie um arquivo config.ini na raiz do projeto com o seguinte conteúdo:

### Google Sheets
```bash
spreadsheet_id = SEU_SPREADSHEET_ID_AQUI
aba_carimbo = NOME_DA_ABA_CARIMBO
```

### Caminhos
```bash
pasta_databases = ./databases
```

### BRNET CREDENCIAIS
```bash
usuario = seu_usuario_brnet
senha = sua_senha_brnet
email_relatorio = seu_email@empresa.com
```
### MAPEAMENTO_ARQUIVO_ABAS
```bash
grupo_grupotrigo = GRUPO TRIGO
grupo_ictsirío = ICTSI RIO
grupo_concremat = CONCREMAT
grupo_constellation = CONSTELLATION - EXAMES OCUPACIONAIS
grupo_vltrio = VLT RIO
grupo_vtal = V.TAL - REDE NEUTRA DE TELECOMUNICACOES S.A.
grupo_ikm = IKM
grupo_bakerhughes = BAKER HUGHES
grupo_ripes = RIP ES
grupo_ripmacaé = RIP MACAÉ
```

### Credenciais do Google Sheets
O sistema utiliza a API do Google Sheets para sincronização dos dados. É necessário:

- Arquivo credentials.json: Baixe as credenciais OAuth2 do Google Cloud Console.

- Primeiro acesso: Na primeira execução, o sistema abrirá o navegador para autorização.

- Arquivo token.json: Será criado automaticamente após a primeira autenticação.

### Estrutura de Pastas
Certifique-se de que existe a pasta databases na raiz do projeto:
```bash
mkdir databases
```

### Logs de Execução
O sistema gera logs detalhados no arquivo multisistema.log e também exibe no console.

### **Banco de Dados Local (SQLite)**
Tabela atendimentos com as seguintes colunas:
```bash
ID_Unico (chave primária)
Paciente
CPF_Passaporte
Funcao
Setor
Empresa
Grupo
Local_do_Atendimento
Atendido_Em
Previsto_Para
Liberado_Em
Status_Expedicao_BR_MED
Exame_Alterado
Tipo_de_Pedido
```
### Google Sheets
O sistema sincroniza com abas nomeadas conforme os grupos, mantendo a mesma estrutura de dados e será utlizado como fonte de dados para o App Sheet.

## **Execução do Projeto**
### Execução Completa
```bash
python main.py
```



# **BR MED - Sistema de Automação do Multisistemas**

O objetivo principal deste projeto é automatizar o processo de download de relatórios dos grupos configurados no BRNET, sincronização com planilhas do Google Sheets, garantindo a atualização contínua dos dados de atendimentos médicos dos diferentes grupos empresariais no App Sheet.

## **Tecnologias Utilizadas**

Este projeto foi desenvolvido com as seguintes tecnologias:

* Python 3.8+
* Playwright (automação web)
* Google Sheets API
* SQLite (banco de dados local)
* Pandas (processamento de dados)
* App Sheet (visual do aplicativo)

## **Pré-requisitos**

* Git
* Python 3.8 ou superior
* Editor de código (VS Code recomendado)
* Acesso às credenciais do Google Sheets API
* Credenciais de acesso ao sistema BRNET

## **Clonando o Repositório**

```bash
git clone https://github.com/grupobrmed/multisistemas-app.git
cd multisistemas-app

```
## **Instalação das Dependências**
```bash
pip install -r requirements.txt

```

## **Instalação do Playwright**
Após instalar as dependências Python, é necessário instalar os navegadores do Playwright:

```bash
playwright install chromium
```

## **Configuração de Ambiente**
### **Arquivo de Configuração (config.ini)**
Crie um arquivo config.ini na raiz do projeto com o seguinte conteúdo:

### Google Sheets
```bash
spreadsheet_id = SEU_SPREADSHEET_ID_AQUI
aba_carimbo = NOME_DA_ABA_CARIMBO
```

### Caminhos
```bash
pasta_databases = ./databases
```

### BRNET CREDENCIAIS
```bash
usuario = seu_usuario_brnet
senha = sua_senha_brnet
email_relatorio = seu_email@empresa.com
```
### MAPEAMENTO_ARQUIVO_ABAS
```bash
grupo_grupotrigo = GRUPO TRIGO
grupo_ictsirío = ICTSI RIO
grupo_concremat = CONCREMAT
grupo_constellation = CONSTELLATION - EXAMES OCUPACIONAIS
grupo_vltrio = VLT RIO
grupo_vtal = V.TAL - REDE NEUTRA DE TELECOMUNICACOES S.A.
grupo_ikm = IKM
grupo_bakerhughes = BAKER HUGHES
grupo_ripes = RIP ES
grupo_ripmacaé = RIP MACAÉ
```

### Credenciais do Google Sheets
O sistema utiliza a API do Google Sheets para sincronização dos dados. É necessário:

- Arquivo credentials.json: Baixe as credenciais OAuth2 do Google Cloud Console.

- Primeiro acesso: Na primeira execução, o sistema abrirá o navegador para autorização.

- Arquivo token.json: Será criado automaticamente após a primeira autenticação.

### Estrutura de Pastas
Certifique-se de que existe a pasta databases na raiz do projeto:
```bash
mkdir databases
```

### Logs de Execução
O sistema gera logs detalhados no arquivo multisistema.log e também exibe no console.

### **Banco de Dados Local (SQLite)**
Tabela atendimentos com as seguintes colunas:
```bash
ID_Unico (chave primária)
Paciente
CPF_Passaporte
Funcao
Setor
Empresa
Grupo
Local_do_Atendimento
Atendido_Em
Previsto_Para
Liberado_Em
Status_Expedicao_BR_MED
Exame_Alterado
Tipo_de_Pedido
```
### Google Sheets
O sistema sincroniza com abas nomeadas conforme os grupos, mantendo a mesma estrutura de dados e será utlizado como fonte de dados para o App Sheet.

## **Execução do Projeto**
### Execução Completa
```bash
python main.py
```





