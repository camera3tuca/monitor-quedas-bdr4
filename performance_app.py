import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import os
import glob

# --- BANCO DE DADOS ---
def init_db():
    conn = sqlite3.connect('portfolio.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS operacoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT,
            ticker TEXT,
            tipo TEXT,
            quantidade REAL,
            preco REAL,
            taxas REAL,
            total REAL,
            corretora TEXT
        )
    ''')
    conn.commit()
    return conn

# Inicializa o banco de dados
conn = init_db()

# --- AUTO-LOAD DADOS (PADRÃO SANTANDER) ---
def auto_load_csvs():
    if not os.path.exists('dados'):
        return

    csv_files = glob.glob('dados/*.csv')
    if not csv_files:
        return

    conn_db = sqlite3.connect('portfolio.db')
    df_existentes = pd.read_sql_query("SELECT data, ticker, tipo, quantidade, preco FROM operacoes", conn_db)

    if not df_existentes.empty:
        existentes_set = set(zip(df_existentes['data'].astype(str), df_existentes['ticker'].astype(str), df_existentes['tipo'].astype(str), df_existentes['quantidade'].astype(float), df_existentes['preco'].astype(float)))
    else:
        existentes_set = set()

    novas_operacoes = []

    def clean_number(x):
        if pd.isna(x): return 0.0
        if isinstance(x, (int, float)): return float(x)
        x = str(x).replace('R$', '').replace(' ', '')
        x = x.replace('.', '').replace(',', '.') if ',' in x else x
        try: return float(x)
        except: return 0.0

    for file in csv_files:
        try:
            try:
                df = pd.read_csv(file, sep=';', skiprows=5, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(file, sep=';', skiprows=5, encoding='latin1')

            if 'Abertura' in df.columns and 'Ativo' in df.columns and 'Lado' in df.columns:
                for _, row in df.iterrows():
                    data = str(row['Abertura'])
                    ticker = str(row['Ativo'])
                    tipo = 'Compra' if str(row['Lado']).strip().upper() == 'C' else 'Venda'

                    qtd = clean_number(row['Qtd Compra'] if tipo == 'Compra' else row['Qtd Venda'])
                    preco = clean_number(row['Preço Compra'] if tipo == 'Compra' else row['Preço Venda'])
                    total = clean_number(row.get('Total', 0.0))
                    if total == 0.0:
                         total = qtd * preco

                    op_key = (data, ticker, tipo, float(qtd), float(preco))
                    if op_key not in existentes_set:
                        novas_operacoes.append({
                            'data': data,
                            'ticker': ticker,
                            'tipo': tipo,
                            'quantidade': qtd,
                            'preco': preco,
                            'taxas': 0.0,
                            'total': total,
                            'corretora': 'Santander (Auto)'
                        })
                        existentes_set.add(op_key)
        except Exception as e:
            print(f"Erro ao auto-carregar {file}: {e}")

    if novas_operacoes:
        df_novas = pd.DataFrame(novas_operacoes)
        df_novas.to_sql('operacoes', conn_db, if_exists='append', index=False)
    conn_db.close()

# Executa auto-load ao iniciar
auto_load_csvs()

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Performance da Carteira",
    page_icon="📈",
    layout="wide"
)

st.markdown('<h1 class="main-title">📈 Performance da Carteira</h1>', unsafe_allow_html=True)
st.markdown('<h3 class="section-header">📁 Importar Notas de Corretagem</h3>', unsafe_allow_html=True)

uploaded_file = st.file_uploader("Faça upload do seu arquivo CSV com as operações", type=["csv"])

is_santander = st.checkbox("Arquivo no padrão Santander (pular primeiras 5 linhas de metadados)?", value=True)

if uploaded_file is not None:
    try:
        if is_santander:
            try:
                df_upload = pd.read_csv(uploaded_file, sep=';', skiprows=5, encoding='utf-8')
            except UnicodeDecodeError:
                uploaded_file.seek(0)
                df_upload = pd.read_csv(uploaded_file, sep=';', skiprows=5, encoding='latin1')
        else:
            try:
                df_upload = pd.read_csv(uploaded_file, sep=None, engine='python')
            except UnicodeDecodeError:
                uploaded_file.seek(0)
                df_upload = pd.read_csv(uploaded_file, sep=None, engine='python', encoding='latin1')

        st.write("Visualização dos dados lidos:")
        st.dataframe(df_upload.head())

        st.markdown("#### Salvar Operações")
        if is_santander:
            st.info("Padrão Santander selecionado: as colunas foram mapeadas automaticamente de acordo com as notas dessa corretora.")
        else:
            st.markdown("Mapeie as colunas do seu CSV (deixe vazio se não existir)")

        colunas_csv = df_upload.columns.tolist()

        c1, c2, c3 = st.columns(3)
        with c1:
            col_data = st.selectbox("Coluna de Data", options=[''] + colunas_csv, index=colunas_csv.index('Abertura')+1 if is_santander and 'Abertura' in colunas_csv else 0)
            col_ticker = st.selectbox("Coluna de Ticker/Ativo", options=[''] + colunas_csv, index=colunas_csv.index('Ativo')+1 if is_santander and 'Ativo' in colunas_csv else 0)
            col_tipo = st.selectbox("Coluna de Tipo (Compra/Venda)", options=[''] + colunas_csv, index=colunas_csv.index('Lado')+1 if is_santander and 'Lado' in colunas_csv else 0)
        with c2:
            col_qtd_c = st.selectbox("Qtd Compra (Santander) ou Qtd", options=[''] + colunas_csv, index=colunas_csv.index('Qtd Compra')+1 if is_santander and 'Qtd Compra' in colunas_csv else 0)
            col_qtd_v = st.selectbox("Qtd Venda (Santander)", options=[''] + colunas_csv, index=colunas_csv.index('Qtd Venda')+1 if is_santander and 'Qtd Venda' in colunas_csv else 0)
            col_preco_c = st.selectbox("Preço Compra (Santander) ou Preço", options=[''] + colunas_csv, index=colunas_csv.index('Preço Compra')+1 if is_santander and 'Preço Compra' in colunas_csv else 0)
            col_preco_v = st.selectbox("Preço Venda (Santander)", options=[''] + colunas_csv, index=colunas_csv.index('Preço Venda')+1 if is_santander and 'Preço Venda' in colunas_csv else 0)
        with c3:
            col_taxas = st.selectbox("Coluna de Taxas (Opcional)", options=[''] + colunas_csv)
            col_total = st.selectbox("Coluna de Total (Opcional)", options=[''] + colunas_csv, index=colunas_csv.index('Total')+1 if is_santander and 'Total' in colunas_csv else 0)

        corretora_nome = st.text_input("Nome da Corretora", value="Santander" if is_santander else "Genérica")

        if st.button("Salvar Operações no Banco de Dados"):
            if col_data and col_ticker and col_tipo and col_qtd_c and col_preco_c:
                # Preparando dados
                df_to_save = pd.DataFrame()
                df_to_save['data'] = df_upload[col_data].astype(str)
                df_to_save['ticker'] = df_upload[col_ticker].astype(str)

                # Adaptação para lógica de Santander 'C' e 'V'
                if is_santander:
                    df_to_save['tipo'] = df_upload[col_tipo].apply(lambda x: 'Compra' if str(x).strip().upper() == 'C' else 'Venda')
                else:
                    df_to_save['tipo'] = df_upload[col_tipo].astype(str)

                # Limpeza de números
                def clean_number(x):
                    if pd.isna(x): return 0.0
                    if isinstance(x, (int, float)): return float(x)
                    x = str(x).replace('R$', '').replace(' ', '')
                    x = x.replace('.', '').replace(',', '.') if ',' in x else x
                    try: return float(x)
                    except: return 0.0

                if is_santander:
                    def get_qtd(row):
                        if str(row[col_tipo]).strip().upper() == 'C':
                            return row[col_qtd_c]
                        else:
                            return row[col_qtd_v]

                    def get_preco(row):
                        if str(row[col_tipo]).strip().upper() == 'C':
                            return row[col_preco_c]
                        else:
                            return row[col_preco_v]

                    df_to_save['quantidade'] = df_upload.apply(get_qtd, axis=1).apply(clean_number)
                    df_to_save['preco'] = df_upload.apply(get_preco, axis=1).apply(clean_number)
                else:
                    df_to_save['quantidade'] = df_upload[col_qtd_c].apply(clean_number)
                    df_to_save['preco'] = df_upload[col_preco_c].apply(clean_number)

                if col_taxas: df_to_save['taxas'] = df_upload[col_taxas].apply(clean_number)
                else: df_to_save['taxas'] = 0.0

                if col_total: df_to_save['total'] = df_upload[col_total].apply(clean_number)
                else: df_to_save['total'] = df_to_save['quantidade'] * df_to_save['preco'] + df_to_save['taxas']

                df_to_save['corretora'] = corretora_nome

                # Salvando no banco
                conn = sqlite3.connect('portfolio.db')
                df_to_save.to_sql('operacoes', conn, if_exists='append', index=False)
                conn.close()
                st.success(f"✅ {len(df_to_save)} operações salvas com sucesso!")
            else:
                st.error("Por favor, selecione as colunas obrigatórias.")
    except Exception as e:
        st.error(f"Erro ao ler o arquivo CSV: {str(e)}")

st.markdown("---")
st.markdown('<h3 class="section-header">💾 Backup e Restauração de Dados</h3>', unsafe_allow_html=True)

col_bkp, col_rst = st.columns(2)

with col_bkp:
    st.markdown("#### Exportar Backup")
    st.write("Baixe o histórico completo de operações registradas no banco de dados local.")
    try:
        conn = sqlite3.connect('portfolio.db')
        df_backup = pd.read_sql_query("SELECT * FROM operacoes", conn)
        conn.close()

        if not df_backup.empty:
            csv = df_backup.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Baixar Backup (CSV)",
                data=csv,
                file_name='portfolio_backup.csv',
                mime='text/csv',
            )
        else:
            st.info("Nenhuma operação para fazer backup.")
    except Exception as e:
        st.error(f"Erro ao gerar backup: {str(e)}")

with col_rst:
    st.markdown("#### Restaurar Backup")
    st.write("Faça upload de um arquivo de backup CSV previamente baixado (portfolio_backup.csv) para restaurar os dados.")
    uploaded_backup = st.file_uploader("Selecione o arquivo de backup", type=["csv"], key="backup_uploader")

    if uploaded_backup is not None:
        if st.button("Restaurar Dados"):
            try:
                df_restore = pd.read_csv(uploaded_backup)
                # Verifica se as colunas essenciais existem no arquivo
                if 'data' in df_restore.columns and 'ticker' in df_restore.columns and 'tipo' in df_restore.columns:
                    conn = sqlite3.connect('portfolio.db')
                    df_restore.to_sql('operacoes', conn, if_exists='replace', index=False)
                    conn.close()
                    st.success("✅ Dados restaurados com sucesso! O aplicativo será recarregado.")
                    import time
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error("Arquivo CSV inválido. Certifique-se de usar o arquivo exportado na ferramenta de backup.")
            except Exception as e:
                st.error(f"Erro ao restaurar backup: {str(e)}")


st.markdown("---")
st.markdown('<h3 class="section-header">📊 Dashboard de Performance</h3>', unsafe_allow_html=True)

# Busca dados do banco para os gráficos
try:
    conn = sqlite3.connect('portfolio.db')
    df_operacoes = pd.read_sql_query("SELECT * FROM operacoes", conn)
    conn.close()

    if not df_operacoes.empty:
        # Processar datas
        try:
            df_operacoes['data_dt'] = pd.to_datetime(df_operacoes['data'], dayfirst=True, errors='coerce')
            df_operacoes['mes_ano'] = df_operacoes['data_dt'].dt.strftime('%Y-%m')
        except:
            df_operacoes['mes_ano'] = 'Desconhecido'

        st.markdown("#### Volume Financeiro Mensal")

        df_valid_dates = df_operacoes.dropna(subset=['data_dt'])
        if not df_valid_dates.empty:
            df_valid_dates['tipo_norm'] = df_valid_dates['tipo'].str.upper().str.strip()
            df_valid_dates['tipo_norm'] = df_valid_dates['tipo_norm'].apply(lambda x: 'Compra' if x.startswith('C') else 'Venda' if x.startswith('V') else x)
            df_valid_dates['total_abs'] = df_valid_dates['total'].abs()

            # Resumo de Métricas
            st.markdown("#### Resumo de Operações")
            vol_compra = df_valid_dates[df_valid_dates['tipo_norm'] == 'Compra']['total_abs'].sum()
            vol_venda = df_valid_dates[df_valid_dates['tipo_norm'] == 'Venda']['total_abs'].sum()
            total_taxas = df_valid_dates['taxas'].sum()

            m1, m2, m3 = st.columns(3)
            m1.metric("Total Comprado", f"R$ {vol_compra:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
            m2.metric("Total Vendido", f"R$ {vol_venda:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
            m3.metric("Total de Taxas", f"R$ {total_taxas:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

            # Gráficos de Volume
            c1, c2 = st.columns(2)
            with c1:
                df_grouped = df_valid_dates.groupby(['mes_ano', 'tipo_norm'])['total_abs'].sum().reset_index()
                fig = px.bar(df_grouped, x='mes_ano', y='total_abs', color='tipo_norm',
                                title='Volume Financeiro (R$) por Mês',
                                labels={'mes_ano': 'Mês', 'total_abs': 'Volume R$', 'tipo_norm': 'Operação'},
                                barmode='group',
                                color_discrete_map={'Compra': '#26a69a', 'Venda': '#ef5350'})
                st.plotly_chart(fig, use_container_width=True)

            with c2:
                df_ativos = df_valid_dates.groupby('ticker')['total_abs'].sum().sort_values(ascending=False).head(10).reset_index()
                fig2 = px.pie(df_ativos, names='ticker', values='total_abs',
                                title='Top 10 Ativos (Distribuição de Volume)',
                                hole=0.4)
                st.plotly_chart(fig2, use_container_width=True)

            # Evolução Temporal
            df_evolucao = df_valid_dates.groupby(['data_dt', 'tipo_norm'])['total_abs'].sum().reset_index()
            fig3 = px.line(df_evolucao, x='data_dt', y='total_abs', color='tipo_norm', markers=True,
                           title='Evolução Diária de Operações',
                           labels={'data_dt': 'Data', 'total_abs': 'Volume R$', 'tipo_norm': 'Operação'},
                           color_discrete_map={'Compra': '#26a69a', 'Venda': '#ef5350'})
            st.plotly_chart(fig3, use_container_width=True)

            # Análise Detalhada por Ativo
            st.markdown("#### Análise Detalhada por Ativo")

            def calc_metricas_ativo(g):
                compras = g[g['tipo_norm'] == 'Compra']
                vendas = g[g['tipo_norm'] == 'Venda']
                qtd_compra = compras['quantidade'].sum()
                qtd_venda = vendas['quantidade'].sum()
                vol_compra = compras['total_abs'].sum()
                vol_venda = vendas['total_abs'].sum()

                preco_medio_compra = vol_compra / qtd_compra if qtd_compra > 0 else 0
                preco_medio_venda = vol_venda / qtd_venda if qtd_venda > 0 else 0

                saldo_qtd = qtd_compra - qtd_venda

                return pd.Series({
                    'Qtd Comprada': qtd_compra,
                    'Preço Médio Compra': preco_medio_compra,
                    'Qtd Vendida': qtd_venda,
                    'Preço Médio Venda': preco_medio_venda,
                    'Saldo Quantidade': saldo_qtd,
                    'Volume Total Movimentado': vol_compra + vol_venda
                })

            df_analise_ativos = df_valid_dates.groupby('ticker').apply(calc_metricas_ativo).reset_index()
            # Formatar para exibição
            df_analise_exib = df_analise_ativos.copy()
            df_analise_exib['Preço Médio Compra'] = df_analise_exib['Preço Médio Compra'].apply(lambda x: f"R$ {x:,.2f}")
            df_analise_exib['Preço Médio Venda'] = df_analise_exib['Preço Médio Venda'].apply(lambda x: f"R$ {x:,.2f}")
            df_analise_exib['Volume Total Movimentado'] = df_analise_exib['Volume Total Movimentado'].apply(lambda x: f"R$ {x:,.2f}")
            st.dataframe(df_analise_exib.sort_values(by='Volume Total Movimentado', ascending=False), use_container_width=True)

            st.markdown("#### Histórico Bruto de Operações")
            st.dataframe(df_operacoes.sort_values(by='id', ascending=False), use_container_width=True)
        else:
            st.warning("Não foi possível processar as datas para gerar o gráfico mensal. Verifique o formato de data do seu CSV.")
            st.dataframe(df_operacoes)
    else:
        st.info("Nenhuma operação registrada no banco de dados. Faça o upload de um CSV acima.")

except Exception as e:
    st.error(f"Erro ao carregar dashboard: {str(e)}")
