import pandas as pd
import numpy as np
import os
from datetime import datetime

# --- CONFIGURAÇÕES E CAMINHOS ---
PASTA_FIPE = r"C:\Users\MATHEUS\BRL Parcerias Dropbox\00. Projetos\PGE-SP. Processos - TRATEX\7. Tratex (Projeto Antigo)\FIPE"
CAMINHO_FLUXO_INICIO = os.path.join(PASTA_FIPE, "fluxo inicio1.dta")
CAMINHO_TAXA_PERITO = os.path.join(PASTA_FIPE, "taxaperito.dta")
CAMINHO_CORRECAO_TJSP = os.path.join(PASTA_FIPE, "CorrecaoMonetariaTJSP.dta")
CAMINHO_TAXA_POUPANCA = os.path.join(PASTA_FIPE, "taxapoupanca.dta")

ORIGEM_STATA_DATE = pd.to_datetime('1960-01-01')
DATA_AVALIACAO_FIPE_PRINCIPAL = pd.to_datetime('1999-03-31')
DATA_CITACAO_DT = ORIGEM_STATA_DATE + pd.Timedelta(days=12858)
DATA_PLANO_REAL_CORTE_DT = ORIGEM_STATA_DATE + pd.Timedelta(days=12600)


def verificar_arquivos_necessarios():
    """Verifica se todos os arquivos necessários existem."""
    arquivos = {
        "Fluxo Início": CAMINHO_FLUXO_INICIO,
        "Taxa Perito": CAMINHO_TAXA_PERITO,
        "Correção TJSP": CAMINHO_CORRECAO_TJSP,
        "Taxa Poupança": CAMINHO_TAXA_POUPANCA,
    }
    todos_existem = True
    print("🔍 Verificando arquivos necessários:")
    for nome, caminho in arquivos.items():
        if os.path.exists(caminho):
            print(f"  ✅ {nome}: Encontrado em {caminho}")
        else:
            print(f"  ❌ {nome}: NÃO ENCONTRADO em {caminho}")
            todos_existem = False
    if not todos_existem:
        print("\n⚠️ ALERTA: Um ou mais arquivos não foram encontrados.")
    print("-" * 60)
    return todos_existem


def carregar_e_pre_processar_dados_iniciais(contrato_especifico=None):
    """Carrega e pré-processa os dados iniciais do arquivo FIPE."""
    print(
        f"🛠️  Iniciando carregamento e pré-processamento (Contrato: {contrato_especifico if contrato_especifico else 'Todos'})...")

    try:
        df_original = pd.read_stata(CAMINHO_FLUXO_INICIO)
    except Exception as e:
        print(f"  ❌ ERRO ao carregar '{os.path.basename(CAMINHO_FLUXO_INICIO)}': {e}")
        return None

    print(f"  📂 '{os.path.basename(CAMINHO_FLUXO_INICIO)}' carregado: {df_original.shape}")

    if 'contrato' in df_original.columns and not contrato_especifico:
        print(f"  🏷️ Contratos únicos encontrados: {df_original['contrato'].astype(str).unique()}")

    df = df_original.copy()

    # Filtrar por contrato específico se solicitado
    if contrato_especifico:
        df_contrato_str = df['contrato'].astype(str)
        contrato_especifico_str = str(contrato_especifico)
        df = df[df_contrato_str == contrato_especifico_str].copy()
        print(f"  🔍 Filtrado para contrato {contrato_especifico}: {df.shape[0]} registros")
        if df.empty:
            print(f"  ⚠️ Nenhum registro encontrado para o contrato {contrato_especifico}.")
            return None

    # Converter colunas de data para numérico
    for col_date_part in ['mes_m', 'dia_m', 'ano_m']:
        df[col_date_part] = pd.to_numeric(df[col_date_part], errors='coerce').astype('Int64')

    df.dropna(subset=['mes_m', 'dia_m', 'ano_m'], inplace=True)

    # Criar coluna de data
    df['date_str'] = df['ano_m'].astype(str) + '-' + \
                     df['mes_m'].astype(str).str.zfill(2) + '-' + \
                     df['dia_m'].astype(str).str.zfill(2)
    df['date'] = pd.to_datetime(df['date_str'], errors='coerce')
    df.dropna(subset=['date'], inplace=True)

    df.sort_values(by=['matching', 'ano_m', 'mes_m', 'dia_m'], inplace=True)

    # Conversão de valores para Reais
    print(f"  💰 Convertendo valores para Reais (antes de {DATA_PLANO_REAL_CORTE_DT.strftime('%d/%m/%Y')})...")
    df['valor_m_numeric'] = pd.to_numeric(df['valor_m'], errors='coerce')

    df['valorreal_calc'] = df['valor_m_numeric']
    mask_pre_real = df['date'] < DATA_PLANO_REAL_CORTE_DT
    df.loc[mask_pre_real, 'valorreal_calc'] = df.loc[mask_pre_real, 'valor_m_numeric'] / (1000 * 2.75)

    # Separar fluxos principal e correção
    df['fluxo_principal_convertido'] = 0.0
    df['fluxo_correcao_convertido'] = 0.0

    df.loc[df['div_rec'] != "cor", 'fluxo_principal_convertido'] = df.loc[df['div_rec'] != "cor", 'valorreal_calc']
    df.loc[df['div_rec'] == "cor", 'fluxo_correcao_convertido'] = df.loc[df['div_rec'] == "cor", 'valorreal_calc']

    df['fluxo_principal_convertido'] = df['fluxo_principal_convertido'].fillna(0)
    df['fluxo_correcao_convertido'] = df['fluxo_correcao_convertido'].fillna(0)

    # Arredondar valores
    df['fluxo_principal_convertido'] = (df['fluxo_principal_convertido'] * 1000000).round() / 1000000
    df['fluxo_correcao_convertido'] = (df['fluxo_correcao_convertido'] * 1000000).round() / 1000000

    # Ajustar datas de medições
    print("  🗓️ Ajustando 'date' de medições (div_rec=='div') para refletir vencimento (+30 dias)...")
    df.loc[df['div_rec'] == "div", 'date'] = df.loc[df['div_rec'] == "div", 'date'] + pd.Timedelta(days=30)

    # Criar colunas finais
    df['somavalorreal1_fipe'] = df['fluxo_principal_convertido']
    df.loc[df['matching'].isin([4.0, 5.0]), 'somavalorreal1_fipe'] = 0.0
    df['somavalorreal1_fipe'] = df['somavalorreal1_fipe'].fillna(0)

    df = df.rename(columns={'fluxo_correcao_convertido': 'somavalorrealcor_fipe'})

    # Verificar colunas necessárias
    for col_check in ['dia_m', 'mes_m', 'ano_m', 'contrato']:
        if col_check not in df.columns:
            if col_check == 'contrato' and contrato_especifico:
                df[col_check] = contrato_especifico
            elif col_check == 'contrato':
                df[col_check] = "Consolidado"
            elif col_check == 'dia_m':
                df[col_check] = df['date'].dt.day
            elif col_check == 'mes_m':
                df[col_check] = df['date'].dt.month
            elif col_check == 'ano_m':
                df[col_check] = df['date'].dt.year

    colunas_para_proxima_etapa = ['date', 'contrato', 'somavalorreal1_fipe', 'somavalorrealcor_fipe',
                                  'dia_m', 'mes_m', 'ano_m']

    print("✅ Pré-processamento inicial FIPE concluído.")
    return df[colunas_para_proxima_etapa]


def consolidar_fluxos_diariamente(df_pre_processado):
    """Consolida os fluxos por data."""
    print("📊 Consolidando fluxos diariamente...")
    df = df_pre_processado.copy()
    df.sort_values(by='date', inplace=True)
    df_consolidado = df.groupby('date', as_index=False).agg(
        somavalorreal1=('somavalorreal1_fipe', 'sum'),
        somavalorrealcor=('somavalorrealcor_fipe', 'sum'),
        dia_m_orig=('dia_m', 'first'),
        mes_m_orig=('mes_m', 'first'),
        ano_m_orig=('ano_m', 'first'),
        contrato=('contrato', 'first')
    )
    print("✅ Consolidação diária concluída.")
    return df_consolidado


def preencher_serie_temporal_diaria(df_consolidado):
    """Preenche a série temporal diária (tsfill)."""
    print("⏳ Preenchendo série temporal diária (tsfill)...")
    df = df_consolidado.copy()
    data_final_tsfill_fipe = pd.to_datetime('2014-09-01')

    if df.empty:
        print("    ⚠️ DataFrame consolidado vazio antes do tsfill.")
        return pd.DataFrame(columns=['date', 'somavalorreal1', 'somavalorrealcor',
                                     'dia_m', 'mes_m', 'ano_m', 'diasmes', 'contrato'])

    data_min_analise = df['date'].min()
    if df['date'].max() < data_final_tsfill_fipe:
        last_row_data = {
            'date': data_final_tsfill_fipe, 'somavalorreal1': 0.0, 'somavalorrealcor': 0.0,
            'dia_m_orig': data_final_tsfill_fipe.day,
            'mes_m_orig': data_final_tsfill_fipe.month,
            'ano_m_orig': data_final_tsfill_fipe.year,
            'contrato': df['contrato'].iloc[-1] if 'contrato' in df.columns and not df.empty else "Consolidado"
        }
        df_final_row = pd.DataFrame([last_row_data])
        df = pd.concat([df, df_final_row], ignore_index=True)
        df.drop_duplicates(subset=['date'], keep='last', inplace=True)

    date_range_completo = pd.date_range(start=data_min_analise, end=data_final_tsfill_fipe, freq='D')
    df_serie_completa = pd.DataFrame({'date': date_range_completo})
    df = pd.merge(df_serie_completa, df, on='date', how='left')

    print("  📝 Preenchendo campos vazios após tsfill...")
    df['dia_m'] = df['date'].dt.day
    df['mes_m'] = df['date'].dt.month
    df['ano_m'] = df['date'].dt.year

    def calcular_dias_mes_fipe(row):
        mes, ano = row['mes_m'], row['ano_m']
        if mes in [1, 3, 5, 7, 8, 10, 12]:
            return 31
        if mes in [4, 6, 9, 11]:
            return 30
        if mes == 2:
            return 29 if ano in [1992, 1996, 2000, 2004, 2008, 2012] else 28
        return 30

    df['diasmes'] = df.apply(calcular_dias_mes_fipe, axis=1)

    df['somavalorreal1'] = df['somavalorreal1'].fillna(0)
    df['somavalorrealcor'] = df['somavalorrealcor'].fillna(0)

    if 'contrato' in df.columns:
        df['contrato'] = df['contrato'].ffill()
        df['contrato'] = df['contrato'].bfill()

    print("✅ Preenchimento da série temporal diária concluído.")
    return df


def adicionar_taxas_financeiras(df_serie_completa):
    """Adiciona as taxas financeiras (Perito, TJSP, Poupança)."""
    print("📈 Adicionando taxas financeiras (Perito, TJSP, Poupança)...")
    df = df_serie_completa.copy()
    df_renamed = df.rename(columns={'ano_m': 'ano', 'mes_m': 'mes'})

    # Taxa do Perito
    try:
        df_taxa_perito = pd.read_stata(CAMINHO_TAXA_PERITO)
        df_merged = pd.merge(df_renamed, df_taxa_perito[['ano', 'mes', 'taxa_usada']], on=['ano', 'mes'], how='left')
        df_merged = df_merged.rename(columns={'taxa_usada': 'taxaperito_mensal'})
        df_merged['taxaperito_mensal'] = df_merged['taxaperito_mensal'] / 100.0
        df_merged['taxaperito_mensal'] = df_merged['taxaperito_mensal'].ffill()
        df_merged['taxaperito_mensal'] = df_merged['taxaperito_mensal'].fillna(0)
        df_merged['taxaperitodia'] = (1 + df_merged['taxaperito_mensal']) ** (1 / df_merged['diasmes']) - 1
        df_merged['taxaperitodia'] = df_merged['taxaperitodia'].fillna(0)
    except Exception as e:
        print(f"  ❌ ERRO ao carregar/processar Taxa Perito: {e}. Usando taxa 0.")
        df_merged = df_renamed.copy()
        df_merged['taxaperitodia'] = 0.0

    # Inflação TJSP
    try:
        df_tjsp = pd.read_stata(CAMINHO_CORRECAO_TJSP)
        df_merged = pd.merge(df_merged, df_tjsp[['ano', 'mes', 'inflacao']], on=['ano', 'mes'], how='left')
        df_merged = df_merged.rename(columns={'inflacao': 'itjsp_mensal'})
        df_merged['itjsp_mensal'] = df_merged['itjsp_mensal'] / 100.0
        df_merged['itjsp_mensal'] = df_merged['itjsp_mensal'].ffill()
        df_merged['itjsp_mensal'] = df_merged['itjsp_mensal'].fillna(0)
        df_merged['itjspdia'] = (1 + df_merged['itjsp_mensal']) ** (1 / df_merged['diasmes']) - 1
        df_merged['itjspdia'] = df_merged['itjspdia'].fillna(0)
    except Exception as e:
        print(f"  ❌ ERRO ao carregar/processar Inflação TJSP: {e}. Usando inflação 0.")
        if 'itjspdia' not in df_merged.columns:
            df_merged['itjspdia'] = 0.0

    # Taxa Poupança
    try:
        df_poupanca = pd.read_stata(CAMINHO_TAXA_POUPANCA)

        # Ajustar nomes das colunas se necessário
        if 'ano_m' in df_poupanca.columns and 'mes_m' in df_poupanca.columns:
            df_poupanca = df_poupanca.rename(columns={'ano_m': 'ano', 'mes_m': 'mes'})

        if 'ano' in df_poupanca.columns and 'mes' in df_poupanca.columns and 'taxapoupanca' in df_poupanca.columns:
            df_merged = pd.merge(df_merged, df_poupanca[['ano', 'mes', 'taxapoupanca']], on=['ano', 'mes'], how='left')
            df_merged['taxapoupanca_mensal'] = df_merged.pop('taxapoupanca') / 100.0
            df_merged['taxapoupanca_mensal'] = df_merged['taxapoupanca_mensal'].ffill()
            df_merged['taxapoupanca_mensal'] = df_merged['taxapoupanca_mensal'].fillna(0)
        else:
            raise ValueError("Colunas necessárias não encontradas em taxapoupanca.dta")

    except Exception as e:
        print(f"  ❌ ERRO ao carregar/processar Taxa Poupança: {e}. Usando poupança 0.")
        if 'taxapoupanca_mensal' not in df_merged.columns:
            df_merged['taxapoupanca_mensal'] = 0.0

    # Configurar juros base
    df_merged['juros_base_mensal'] = 0.005
    data_15715 = ORIGEM_STATA_DATE + pd.Timedelta(days=15715)
    data_18058 = ORIGEM_STATA_DATE + pd.Timedelta(days=18058)

    df_merged.loc[(df_merged['date'] >= data_15715) & (df_merged['date'] <= data_18058), 'juros_base_mensal'] = 0.01

    if 'taxapoupanca_mensal' in df_merged.columns:
        df_merged.loc[df_merged['date'] > data_18058, 'juros_base_mensal'] = df_merged['taxapoupanca_mensal']
    else:
        df_merged.loc[df_merged['date'] > data_18058, 'juros_base_mensal'] = 0.0

    df_merged['juros_dia_periodo2'] = df_merged['juros_base_mensal'] / df_merged['diasmes']
    df_merged['juros_dia_periodo2'] = df_merged['juros_dia_periodo2'].fillna(0)

    df_final = df_merged.rename(columns={'ano': 'ano_m', 'mes': 'mes_m'})
    df_final.sort_values(by='date', inplace=True)
    print("✅ Adição de taxas financeiras concluída.")
    return df_final


def calcular_divida_final_fipe(df_com_taxas):
    """Executa o cálculo da dívida final usando a metodologia FIPE."""
    print("🧮 Executando cálculo da dívida final (metodologia FIPE)...")
    df = df_com_taxas.copy()

    # Período 1: até a data de citação
    print(f"  🗓️  Calculando Período 1 (até {DATA_CITACAO_DT.strftime('%d/%m/%Y')}) usando Taxa do Perito...")
    df['fluxo_net_diario_p1'] = df['somavalorreal1'] + df['somavalorrealcor']
    df['divida_periodo1'] = 0.0

    df_p1_idx = df[df['date'] <= DATA_CITACAO_DT].index
    if not df_p1_idx.empty:
        df.loc[df_p1_idx[0], 'divida_periodo1'] = df.loc[df_p1_idx[0], 'fluxo_net_diario_p1']
        for i in range(1, len(df_p1_idx)):
            idx_atual = df_p1_idx[i]
            idx_anterior = df_p1_idx[i - 1]
            saldo_anterior = df.loc[idx_anterior, 'divida_periodo1']
            taxa_dia_atual = df.loc[idx_atual, 'taxaperitodia']
            fluxo_dia_atual = df.loc[idx_atual, 'fluxo_net_diario_p1']
            df.loc[idx_atual, 'divida_periodo1'] = saldo_anterior * (1 + taxa_dia_atual) + fluxo_dia_atual

    saldo_na_data_citacao = 0.0
    if not df[df['date'] == DATA_CITACAO_DT].empty:
        saldo_na_data_citacao = df.loc[df['date'] == DATA_CITACAO_DT, 'divida_periodo1'].iloc[0]

    print(
        f"    💰 Saldo ao final do Período 1 ({DATA_CITACAO_DT.strftime('%d/%m/%Y')}): R$ {saldo_na_data_citacao:,.2f}")

    # Período 2: após a data de citação
    print(f"  🗓️  Calculando Período 2 (após {DATA_CITACAO_DT.strftime('%d/%m/%Y')})...")
    df['principal_corrigido_p2'] = 0.0
    df['juros_acumulados_corrigidos_p2'] = 0.0
    df['divida_final_calculada'] = 0.0

    idx_citacao_list = df[df['date'] == DATA_CITACAO_DT].index
    if not idx_citacao_list.empty:
        idx_citacao = idx_citacao_list[0]
        df.loc[idx_citacao, 'principal_corrigido_p2'] = saldo_na_data_citacao
        df.loc[idx_citacao, 'juros_acumulados_corrigidos_p2'] = 0.0
        df.loc[idx_citacao, 'divida_final_calculada'] = saldo_na_data_citacao

    df_p2_idx = df[df['date'] > DATA_CITACAO_DT].index

    prev_principal_corrigido = saldo_na_data_citacao
    prev_juros_acum_corrigidos = 0.0

    if not df_p2_idx.empty:
        for idx_atual in df_p2_idx:
            itjsp_dia_atual = df.loc[idx_atual, 'itjspdia']
            fluxo_principal_dia_atual = df.loc[idx_atual, 'somavalorreal1']
            fluxo_correcao_dia_atual = df.loc[idx_atual, 'somavalorrealcor']
            juros_simples_diario_p2 = df.loc[idx_atual, 'juros_dia_periodo2']

            current_principal_corrigido = prev_principal_corrigido * (1 + itjsp_dia_atual) + \
                                          fluxo_principal_dia_atual + fluxo_correcao_dia_atual
            df.loc[idx_atual, 'principal_corrigido_p2'] = current_principal_corrigido

            juros_do_dia = juros_simples_diario_p2 * current_principal_corrigido
            current_juros_acum_corrigidos = (prev_juros_acum_corrigidos * (1 + itjsp_dia_atual)) + juros_do_dia
            df.loc[idx_atual, 'juros_acumulados_corrigidos_p2'] = current_juros_acum_corrigidos

            df.loc[idx_atual, 'divida_final_calculada'] = current_principal_corrigido + \
                                                          (prev_juros_acum_corrigidos * (1 + itjsp_dia_atual))

            prev_principal_corrigido = current_principal_corrigido
            prev_juros_acum_corrigidos = current_juros_acum_corrigidos

    # Obter dívida na data de avaliação FIPE
    divida_na_data_fipe_report = 0.0
    data_fipe_principal_idx = df[df['date'] == DATA_AVALIACAO_FIPE_PRINCIPAL].index
    if not data_fipe_principal_idx.empty:
        divida_na_data_fipe_report = df.loc[data_fipe_principal_idx[0], 'divida_final_calculada']
    elif not df_p2_idx.empty:
        divida_na_data_fipe_report = df.loc[df_p2_idx[-1], 'divida_final_calculada']
    elif not idx_citacao_list.empty:
        divida_na_data_fipe_report = df.loc[idx_citacao_list[0], 'divida_final_calculada']

    print(
        f"    💰 Dívida Final Calculada em {DATA_AVALIACAO_FIPE_PRINCIPAL.strftime('%d/%m/%Y')}: R$ {divida_na_data_fipe_report:,.2f}")
    print("✅ Cálculo da dívida final FIPE concluído.")

    return divida_na_data_fipe_report, saldo_na_data_citacao, df


def salvar_resultados_excel(caminho_arquivo, divida_final_total, vf_periodo1_total, df_calculo_detalhado,
                            resultados_contratos, dfs_contratos_detalhados):
    """Salva todos os resultados em um arquivo Excel com múltiplas abas."""
    print(f"💾 Salvando resultados no Excel: {caminho_arquivo}")

    try:
        with pd.ExcelWriter(caminho_arquivo, engine='openpyxl') as writer:

            # Aba 1: Resumo Principal
            resumo_data = {
                'Métrica': [
                    'Data Processamento',
                    'Data Avaliação FIPE',
                    'Data Citação',
                    'VF Período 1 (Total)',
                    'Dívida Final Total Consolidado',
                    'Soma Contratos Individuais'
                ],
                'Valor': [
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    DATA_AVALIACAO_FIPE_PRINCIPAL.strftime('%Y-%m-%d'),
                    DATA_CITACAO_DT.strftime('%Y-%m-%d'),
                    f"R$ {vf_periodo1_total:,.2f}",
                    f"R$ {divida_final_total:,.2f}",
                    f"R$ {sum(resultados_contratos.values()):,.2f}"
                ]
            }
            df_resumo = pd.DataFrame(resumo_data)
            df_resumo.to_excel(writer, sheet_name='Resumo_Principal', index=False)
            print("  📝 Aba 'Resumo_Principal' salva.")

            # Aba 2: Dados Detalhados do Total Consolidado
            if df_calculo_detalhado is not None and not df_calculo_detalhado.empty:
                df_calculo_detalhado.to_excel(writer, sheet_name='Total_Consolidado_Detalhes', index=False)
                print("  📝 Dados do Total Consolidado salvos na aba 'Total_Consolidado_Detalhes'.")

            # Aba 3: Resultados por Contrato
            contratos_data = []
            for contrato, valor in resultados_contratos.items():
                contratos_data.append({
                    'Contrato': contrato,
                    'Dívida Final (31/03/1999)': valor
                })

            if contratos_data:
                df_contratos = pd.DataFrame(contratos_data)
                df_contratos.to_excel(writer, sheet_name='Resultados_por_Contrato', index=False)
                print("  📝 Resultados por contrato salvos na aba 'Resultados_por_Contrato'.")

            # Abas 4+: Detalhes de cada contrato
            for nome_contrato, df_detalhe in dfs_contratos_detalhados.items():
                if df_detalhe is not None and not df_detalhe.empty:
                    sheet_name = f'Contrato_{nome_contrato.replace("-", "_")}_Detalhes'
                    df_detalhe.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"  📝 Dados do Contrato {nome_contrato} salvos na aba '{sheet_name}'.")

            # Aba Final: Comparação com FIPE
            valores_fipe_tabela2 = {
                "7665-3": 99565172.00,
                "7538-3": 5670133.00,
                "8099-8": 1560444.00
            }

            comparacao_data = []
            soma_calculada = 0

            for contrato in ["7665-3", "7538-3", "8099-8"]:
                calc_val = resultados_contratos.get(contrato, 0.0)
                fipe_val = valores_fipe_tabela2.get(contrato, 0)
                diff = calc_val - fipe_val
                pct_diff = (diff / fipe_val * 100) if fipe_val != 0 else 0
                soma_calculada += calc_val

                comparacao_data.append({
                    'Item': f'Contrato {contrato}',
                    'Calculado_Python': calc_val,
                    'FIPE_Tabela2': fipe_val,
                    'Diferenca_Absoluta': diff,
                    'Diferenca_Percentual': pct_diff
                })

            # Adicionar totais
            total_fipe = 106795749.00
            diff_total_soma = soma_calculada - total_fipe
            pct_diff_total_soma = (diff_total_soma / total_fipe * 100) if total_fipe != 0 else 0

            diff_total_consolidado = divida_final_total - total_fipe
            pct_diff_total_consolidado = (diff_total_consolidado / total_fipe * 100) if total_fipe != 0 else 0

            comparacao_data.extend([
                {
                    'Item': 'Soma Contratos Individuais',
                    'Calculado_Python': soma_calculada,
                    'FIPE_Tabela2': total_fipe,
                    'Diferenca_Absoluta': diff_total_soma,
                    'Diferenca_Percentual': pct_diff_total_soma
                },
                {
                    'Item': 'Total Consolidado (Script)',
                    'Calculado_Python': divida_final_total,
                    'FIPE_Tabela2': total_fipe,
                    'Diferenca_Absoluta': diff_total_consolidado,
                    'Diferenca_Percentual': pct_diff_total_consolidado
                }
            ])

            if comparacao_data:
                df_comparacao = pd.DataFrame(comparacao_data)
                df_comparacao.to_excel(writer, sheet_name='Comparacao_FIPE_Tabela2', index=False)
                print("  📝 Comparação com FIPE salva na aba 'Comparacao_FIPE_Tabela2'.")

            # Aba de Parâmetros
            parametros_data = {
                'Parametro': [
                    'Data_Avaliacao_FIPE',
                    'Data_Citacao',
                    'Data_Plano_Real_Corte',
                    'Origem_Stata_Date',
                    'Caminho_Fluxo_Inicio',
                    'Caminho_Taxa_Perito',
                    'Caminho_Correcao_TJSP',
                    'Caminho_Taxa_Poupanca'
                ],
                'Valor': [
                    DATA_AVALIACAO_FIPE_PRINCIPAL.strftime('%Y-%m-%d'),
                    DATA_CITACAO_DT.strftime('%Y-%m-%d'),
                    DATA_PLANO_REAL_CORTE_DT.strftime('%Y-%m-%d'),
                    ORIGEM_STATA_DATE.strftime('%Y-%m-%d'),
                    CAMINHO_FLUXO_INICIO,
                    CAMINHO_TAXA_PERITO,
                    CAMINHO_CORRECAO_TJSP,
                    CAMINHO_TAXA_POUPANCA
                ]
            }

            df_parametros = pd.DataFrame(parametros_data)
            df_parametros.to_excel(writer, sheet_name='Parametros_Configuracao', index=False)
            print("  📝 Parâmetros salvos na aba 'Parametros_Configuracao'.")

        print(f"✅ Arquivo Excel '{caminho_arquivo}' salvo com sucesso!")
        return True

    except Exception as e:
        print(f"❌ ERRO ao salvar arquivo Excel: {e}")
        print("   Verifique se a biblioteca 'openpyxl' está instalada (pip install openpyxl)")
        print("   Verifique também as permissões de escrita no diretório.")
        return False


def executar_replicacao_fipe_completa():
    """Função principal que executa toda a replicação FIPE."""
    print("🚀 INICIANDO REPLICAÇÃO COMPLETA DO RELATÓRIO FIPE (CONSOLIDADO) 🚀")
    print("=" * 70)

    # Verificar arquivos
    if not verificar_arquivos_necessarios():
        print("❌ Abortando devido à ausência de arquivos de dados.")
        return

    # Definir caminho do arquivo Excel
    try:
        caminho_script = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        caminho_script = os.getcwd()

    nome_arquivo_excel = f"Relatorio_FIPE_Completo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    caminho_excel = os.path.join(caminho_script, nome_arquivo_excel)
    print(f"ℹ️  Arquivo Excel será salvo em: {caminho_excel}")

    # Variáveis para armazenar resultados
    divida_final_total_1999 = 0.0
    vf_periodo1_total = 0.0
    df_calculo_total_detalhado = None
    resultados_por_contrato = {}
    dfs_contratos_detalhados = {}

    try:
        # === CÁLCULO TOTAL CONSOLIDADO ===
        print("\n--- CALCULANDO TOTAL CONSOLIDADO ---")
        df_inicial_total = carregar_e_pre_processar_dados_iniciais(contrato_especifico=None)
        if df_inicial_total is None or df_inicial_total.empty:
            print("❌ Falha no carregamento dos dados totais. Abortando.")
            return

        print("-" * 60)
        df_consolidado_total = consolidar_fluxos_diariamente(df_inicial_total)
        print(f"  Shape após consolidação: {df_consolidado_total.shape}")

        print("-" * 60)
        df_serie_total = preencher_serie_temporal_diaria(df_consolidado_total)
        print(f"  Shape após preenchimento da série: {df_serie_total.shape}")

        print("-" * 60)
        df_com_taxas_total = adicionar_taxas_financeiras(df_serie_total)
        print(f"  Shape após adicionar taxas (Total): {df_com_taxas_total.shape}")

        print("-" * 60)
        divida_final_total_1999, vf_periodo1_total, df_calculo_total_detalhado = calcular_divida_final_fipe(
            df_com_taxas_total)

        print("=" * 70)
        print("🎉 REPLICAÇÃO FIPE (TOTAL CONSOLIDADO) CONCLUÍDA 🎉")
        print(f"  Valor do Saldo em {DATA_CITACAO_DT.strftime('%d/%m/%Y')} (VF Período 1): R$ {vf_periodo1_total:,.2f}")
        print(
            f"  DÍVIDA FINAL TOTAL CALCULADA em {DATA_AVALIACAO_FIPE_PRINCIPAL.strftime('%d/%m/%Y')}: R$ {divida_final_total_1999:,.2f}")
        print("=" * 70)

        # === CÁLCULO POR CONTRATO ===
        print("\n--- CALCULANDO POR CONTRATO (para comparação com Tabela 2 FIPE) ---")
        contratos_fipe_tabela = ["7665-3", "7538-3", "8099-8"]

        for nome_contrato in contratos_fipe_tabela:
            print(f"\n>>> Processando Contrato Específico: {nome_contrato} <<<")

            df_inicial_contrato = carregar_e_pre_processar_dados_iniciais(contrato_especifico=nome_contrato)
            if df_inicial_contrato is None or df_inicial_contrato.empty:
                print(f"  Sem dados para o contrato {nome_contrato}.")
                resultados_por_contrato[nome_contrato] = 0.0
                dfs_contratos_detalhados[nome_contrato] = pd.DataFrame()
                continue

            df_consolidado_contrato = consolidar_fluxos_diariamente(df_inicial_contrato)
            if df_consolidado_contrato.empty:
                print(f"  Sem dados após consolidação para o contrato {nome_contrato}.")
                resultados_por_contrato[nome_contrato] = 0.0
                dfs_contratos_detalhados[nome_contrato] = pd.DataFrame()
                continue

            df_serie_contrato = preencher_serie_temporal_diaria(df_consolidado_contrato)
            df_taxas_contrato = adicionar_taxas_financeiras(df_serie_contrato)

            divida_contrato_1999, vf_p1_contrato, df_calculo_contrato = calcular_divida_final_fipe(df_taxas_contrato)

            resultados_por_contrato[nome_contrato] = divida_contrato_1999
            dfs_contratos_detalhados[nome_contrato] = df_calculo_contrato

            print(
                f"  Resultado Contrato {nome_contrato} (Dívida em {DATA_AVALIACAO_FIPE_PRINCIPAL.strftime('%d/%m/%Y')}): R$ {divida_contrato_1999:,.2f}")
            print("-" * 50)

        # === SALVAMENTO NO EXCEL ===
        sucesso_excel = salvar_resultados_excel(
            caminho_excel,
            divida_final_total_1999,
            vf_periodo1_total,
            df_calculo_total_detalhado,
            resultados_por_contrato,
            dfs_contratos_detalhados
        )

        # === COMPARAÇÃO FINAL ===
        print("\n🔎 Comparação com Valores da Tabela 2 do Relatório FIPE (p. 56):")
        valores_fipe_tabela2 = {
            "7665-3": 99565172.00,
            "7538-3": 5670133.00,
            "8099-8": 1560444.00,
            "Total": 106795749.00
        }

        soma_calculada_contratos_individuais = sum(resultados_por_contrato.values())

        for contrato_id in contratos_fipe_tabela:
            calc_val = resultados_por_contrato.get(contrato_id, 0.0)
            fipe_val = valores_fipe_tabela2.get(contrato_id, 0)
            diff = calc_val - fipe_val
            pct_diff = (diff / fipe_val * 100) if fipe_val != 0 else float('inf')
            print(
                f"  Contrato {contrato_id}: Calculado R$ {calc_val:,.2f} | FIPE R$ {fipe_val:,.2f} | Diferença R$ {diff:,.2f} ({pct_diff:.4f}%)")

        fipe_total_tab2 = valores_fipe_tabela2["Total"]
        diff_soma_total = soma_calculada_contratos_individuais - fipe_total_tab2
        pct_diff_soma_total = (diff_soma_total / fipe_total_tab2 * 100) if fipe_total_tab2 != 0 else float('inf')

        print(f"\n  Soma dos Contratos (calculados individualmente): R$ {soma_calculada_contratos_individuais:,.2f}")
        print(
            f"  Comparado ao Total FIPE Tabela 2 (R$ {fipe_total_tab2:,.2f}): Diferença R$ {diff_soma_total:,.2f} ({pct_diff_soma_total:.4f}%)")

        diff_total_consolidado_vs_fipe = divida_final_total_1999 - fipe_total_tab2
        pct_diff_total_consolidado_vs_fipe = (
                    diff_total_consolidado_vs_fipe / fipe_total_tab2 * 100) if fipe_total_tab2 != 0 else float('inf')

        print(f"\n  Cálculo TOTAL CONSOLIDADO (todos juntos): R$ {divida_final_total_1999:,.2f}")
        print(
            f"  Comparado ao Total FIPE Tabela 2 (R$ {fipe_total_tab2:,.2f}): Diferença R$ {diff_total_consolidado_vs_fipe:,.2f} ({pct_diff_total_consolidado_vs_fipe:.4f}%)")

        if sucesso_excel:
            print(f"\n📊 Resultados salvos com sucesso no arquivo: {caminho_excel}")
        else:
            print(f"\n❌ Houve problema ao salvar o arquivo Excel.")

        print("=" * 70)

    except Exception as e:
        print(f"❌ ERRO GERAL durante a execução: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    executar_replicacao_fipe_completa()
