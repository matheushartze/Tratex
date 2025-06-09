import os

# Configuração de caminhos
PASTA_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Caminhos dos dados (ajustar conforme necessário)
PASTA_FIPE = os.path.join(PASTA_BASE, "dados")

# Se os dados estão no Dropbox, descomente e ajuste:
# PASTA_FIPE = r"C:\Users\MATHEUS\BRL Parcerias Dropbox\00. Projetos\PGE-SP. Processos - TRATEX\7. Tratex (Projeto Antigo)\FIPE"

CAMINHO_FLUXO_INICIO = os.path.join(PASTA_FIPE, "fluxo inicio1.dta")
CAMINHO_TAXA_PERITO = os.path.join(PASTA_FIPE, "taxaperito.dta")
CAMINHO_CORRECAO_TJSP = os.path.join(PASTA_FIPE, "CorrecaoMonetariaTJSP.dta")
CAMINHO_TAXA_POUPANCA = os.path.join(PASTA_FIPE, "taxapoupanca.dta")