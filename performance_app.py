import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px

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

            df_grouped = df_valid_dates.groupby(['mes_ano', 'tipo_norm'])['total'].sum().reset_index()

            fig = px.bar(df_grouped, x='mes_ano', y='total', color='tipo_norm',
                            title='Volume Financeiro (R$) por Mês',
                            labels={'mes_ano': 'Mês', 'total': 'Volume R$', 'tipo_norm': 'Operação'},
                            barmode='group',
                            color_discrete_map={'Compra': '#26a69a', 'Venda': '#ef5350'})
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("#### Ativos Mais Negociados (Volume)")
            df_ativos = df_valid_dates.groupby('ticker')['total'].sum().sort_values(ascending=False).head(10).reset_index()
            fig2 = px.bar(df_ativos, x='ticker', y='total',
                            title='Top 10 Ativos por Volume Financeiro',
                            labels={'ticker': 'Ativo', 'total': 'Volume R$'})
            st.plotly_chart(fig2, use_container_width=True)

            st.markdown("#### Histórico de Operações")
            st.dataframe(df_operacoes.sort_values(by='id', ascending=False))
        else:
            st.warning("Não foi possível processar as datas para gerar o gráfico mensal. Verifique o formato de data do seu CSV.")
            st.dataframe(df_operacoes)
    else:
        st.info("Nenhuma operação registrada no banco de dados. Faça o upload de um CSV acima.")

except Exception as e:
    st.error(f"Erro ao carregar dashboard: {str(e)}")
