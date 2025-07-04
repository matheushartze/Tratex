# Tratex - Sistema de Cálculo de Dívida Judicial

Sistema para cálculo de dívida em ações judiciais baseado na metodologia FIPE.
Implementado em Python e R com resultados equivalentes.

## 🎯 Objetivo

Calcular valores devidos em processos judiciais considerando:
- Medições diárias de serviços prestados
- Pagamentos realizados pelo Estado
- Correção monetária para pagamentos após 30 dias
- Juros de mora conforme legislação

## 🔧 Implementações Disponíveis

### 🐍 Python
**Localização:** `python/main.py`

**Instalação:**
```bash
cd python
pip install -r requirements.txt

## 🗂️ Configuração dos Dados

### Opção 1: Dados locais (para desenvolvimento)
```bash
# Criar pasta de dados
mkdir dados
# Colocar arquivos .dta na pasta dados/