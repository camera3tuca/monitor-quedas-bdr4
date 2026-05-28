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

if uploaded_file is not None:
    try:
        # Tenta ler o CSV
        try:
            df_upload = pd.read_csv(uploaded_file, sep=None, engine='python')
        except UnicodeDecodeError:
            uploaded_file.seek(0)
            df_upload = pd.read_csv(uploaded_file, sep=None, engine='python', encoding='latin1')

        st.write("Visualização dos dados lidos:")
        st.dataframe(df_upload.head())

        # Mapeamento genérico de colunas
        st.markdown("#### Mapeie as colunas do seu CSV")
        colunas_csv = df_upload.columns.tolist()

        c1, c2, c3 = st.columns(3)
        with c1:
            col_data = st.selectbox("Coluna de Data", options=[''] + colunas_csv)
            col_ticker = st.selectbox("Coluna de Ticker/Ativo", options=[''] + colunas_csv)
            col_tipo = st.selectbox("Coluna de Tipo (Compra/Venda)", options=[''] + colunas_csv)
        with c2:
            col_qtd = st.selectbox("Coluna de Quantidade", options=[''] + colunas_csv)
            col_preco = st.selectbox("Coluna de Preço", options=[''] + colunas_csv)
        with c3:
            col_taxas = st.selectbox("Coluna de Taxas (Opcional)", options=[''] + colunas_csv)
            col_total = st.selectbox("Coluna de Total (Opcional)", options=[''] + colunas_csv)

        corretora_nome = st.text_input("Nome da Corretora (para organização)", value="Genérica")

        if st.button("Salvar Operações no Banco de Dados"):
            if col_data and col_ticker and col_tipo and col_qtd and col_preco:
                # Preparando dados
                df_to_save = pd.DataFrame()
                df_to_save['data'] = df_upload[col_data].astype(str)
                df_to_save['ticker'] = df_upload[col_ticker].astype(str)
                df_to_save['tipo'] = df_upload[col_tipo].astype(str)

                # Limpeza de números
                def clean_number(x):
                    if pd.isna(x): return 0.0
                    if isinstance(x, (int, float)): return float(x)
                    x = str(x).replace('R$', '').replace(' ', '')
                    x = x.replace('.', '').replace(',', '.') if ',' in x else x
                    try: return float(x)
                    except: return 0.0

                df_to_save['quantidade'] = df_upload[col_qtd].apply(clean_number)
                df_to_save['preco'] = df_upload[col_preco].apply(clean_number)

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
                st.error("Por favor, selecione as colunas obrigatórias (Data, Ticker, Tipo, Quantidade, Preço).")
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
