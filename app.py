import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt
import seaborn as sns
import requests
from datetime import datetime
import pytz
import warnings
import xml.etree.ElementTree as ET
import html as html_lib
import re

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Monitor BDRs - Swing Trade",
    page_icon="📉",
    layout="wide"
)

warnings.filterwarnings('ignore')
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

PERIODO = "1y"  # 1 ano para ter dados suficientes para EMA200 (~252 dias úteis)
TERMINACOES_BDR = ('31', '32', '33', '34', '35', '39')

# Token BRAPI para dados alternativos
BRAPI_TOKEN = "iExnKM1xcbQcYL3cNPhPQ3"  # Token gratuito da BRAPI

# =============================================================================
# FUNÇÕES DE BUSCA E TRADUÇÃO DE NOTÍCIAS
# =============================================================================

def _limpar_html(texto):
    """Remove tags HTML e decodifica entidades."""
    if not texto:
        return ""
    texto = re.sub(r'<[^>]+>', '', texto)
    texto = html_lib.unescape(texto)
    return texto.strip()

def _formatar_data(pub_raw):
    """Converte data RSS para dd/mm/aaaa hh:mm."""
    try:
        for fmt in ('%a, %d %b %Y %H:%M:%S %z', '%a, %d %b %Y %H:%M:%S GMT',
                    '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S%z'):
            try:
                dt = datetime.strptime(pub_raw.strip(), fmt)
                return dt.strftime('%d/%m/%Y %H:%M')
            except Exception:
                continue
        return pub_raw
    except Exception:
        return pub_raw


def _traduzir_com_mymemory(textos):
    """
    Traduz lista de strings (en→pt-BR) via MyMemory — gratuito, sem chave.
    Faz chamadas em paralelo (ThreadPoolExecutor) para reduzir latência.
    """
    if not textos:
        return textos

    def _traduzir_um(texto):
        if not texto or not texto.strip():
            return texto
        try:
            resp = requests.get(
                "https://api.mymemory.translated.net/get",
                params={"q": texto[:450], "langpair": "en|pt-br"},
                timeout=5
            )
            if resp.status_code == 200:
                trad = resp.json().get("responseData", {}).get("translatedText", "")
                if trad and trad.upper() != texto.upper():
                    return trad
        except Exception:
            pass
        return texto

    from concurrent.futures import ThreadPoolExecutor, as_completed
    results = [None] * len(textos)
    with ThreadPoolExecutor(max_workers=6) as ex:
        fut_map = {ex.submit(_traduzir_um, t): i for i, t in enumerate(textos)}
        for fut in as_completed(fut_map):
            results[fut_map[fut]] = fut.result()
    return results


def _buscar_yahoo_rss(ticker_us, max_noticias=8):
    noticias = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/rss+xml, application/xml, text/xml, */*',
    }
    for url in [
        f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker_us}&region=US&lang=en-US",
        f"https://finance.yahoo.com/rss/headline?s={ticker_us}",
    ]:
        try:
            resp = requests.get(url, headers=headers, timeout=8)
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            channel = root.find('channel')
            if channel is None:
                continue
            for item in channel.findall('item')[:max_noticias]:
                titulo = _limpar_html(item.findtext('title', ''))
                if not titulo:
                    continue
                noticias.append({
                    'titulo': titulo,
                    'link': item.findtext('link', ''),
                    'data': _formatar_data(item.findtext('pubDate', '')),
                    'descricao': _limpar_html(item.findtext('description', ''))[:300],
                    'fonte': 'Yahoo Finance'
                })
            if noticias:
                break
        except Exception:
            continue
    return noticias


def _buscar_gurufocus_rss(ticker_us, max_noticias=5):
    noticias = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        resp = requests.get(
            f"https://www.gurufocus.com/news/rss/{ticker_us}",
            headers=headers, timeout=8
        )
        if resp.status_code != 200:
            return []
        root = ET.fromstring(resp.content)
        channel = root.find('channel')
        if channel is None:
            return []
        for item in channel.findall('item')[:max_noticias]:
            titulo = _limpar_html(item.findtext('title', ''))
            if not titulo:
                continue
            noticias.append({
                'titulo': titulo,
                'link': item.findtext('link', ''),
                'data': _formatar_data(item.findtext('pubDate', '')),
                'descricao': _limpar_html(item.findtext('description', ''))[:300],
                'fonte': 'GuruFocus'
            })
    except Exception:
        pass
    return noticias


def _buscar_seekingalpha_rss(ticker_us, max_noticias=5):
    noticias = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    for url in [
        f"https://seekingalpha.com/api/sa/combined/{ticker_us}.xml",
        f"https://seekingalpha.com/symbol/{ticker_us}/feed.xml",
    ]:
        try:
            resp = requests.get(url, headers=headers, timeout=8)
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            channel = root.find('channel')
            items = channel.findall('item') if channel is not None else root.findall('.//item')
            for item in items[:max_noticias]:
                titulo = _limpar_html(item.findtext('title', ''))
                if not titulo:
                    continue
                noticias.append({
                    'titulo': titulo,
                    'link': item.findtext('link', ''),
                    'data': _formatar_data(item.findtext('pubDate', '')),
                    'descricao': _limpar_html(item.findtext('description', ''))[:300],
                    'fonte': 'Seeking Alpha'
                })
            if noticias:
                break
        except Exception:
            continue
    return noticias


def _buscar_marketwatch_rss(ticker_us, max_noticias=5):
    """MarketWatch RSS feed — boa fonte para earnings e análises."""
    noticias = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        resp = requests.get(
            f"https://feeds.marketwatch.com/marketwatch/realtimeheadlines/",
            headers=headers, timeout=8
        )
        if resp.status_code != 200:
            return []
        root = ET.fromstring(resp.content)
        channel = root.find('channel')
        if channel is None:
            return []
        ticker_lower = ticker_us.lower()
        for item in channel.findall('item'):
            titulo = _limpar_html(item.findtext('title', ''))
            desc   = _limpar_html(item.findtext('description', ''))
            # filtra pelo ticker ou nome da empresa
            if ticker_lower not in titulo.lower() and ticker_lower not in desc.lower():
                continue
            if not titulo:
                continue
            noticias.append({
                'titulo': titulo,
                'link': item.findtext('link', ''),
                'data': _formatar_data(item.findtext('pubDate', '')),
                'descricao': desc[:300],
                'fonte': 'MarketWatch'
            })
            if len(noticias) >= max_noticias:
                break
    except Exception:
        pass
    return noticias


def _buscar_google_news_rss(ticker_us, empresa_nome='', max_noticias=6):
    """
    Google News RSS — busca por ticker + nome da empresa.
    Excelente cobertura, inclui fontes diversas (Reuters, Bloomberg, CNBC, etc.).
    """
    noticias = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    # Queries: tenta pelo ticker e depois pelo nome da empresa
    queries = [ticker_us]
    if empresa_nome and empresa_nome != ticker_us:
        nome_curto = empresa_nome.split()[0] if empresa_nome else ''
        if nome_curto:
            queries.append(f"{nome_curto} stock")

    for query in queries:
        try:
            url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}+stock&hl=en-US&gl=US&ceid=US:en"
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            channel = root.find('channel')
            if channel is None:
                continue
            for item in channel.findall('item')[:max_noticias]:
                titulo = _limpar_html(item.findtext('title', ''))
                if not titulo or len(titulo) < 10:
                    continue
                # Extrai a fonte real do título (formato Google: "Título - Fonte")
                fonte_real = 'Google News'
                if ' - ' in titulo:
                    partes = titulo.rsplit(' - ', 1)
                    titulo = partes[0].strip()
                    fonte_real = partes[1].strip()[:30]

                noticias.append({
                    'titulo': titulo,
                    'link': item.findtext('link', ''),
                    'data': _formatar_data(item.findtext('pubDate', '')),
                    'descricao': _limpar_html(item.findtext('description', ''))[:300],
                    'fonte': 'Google News',
                    'fonte_real': fonte_real,
                })
            if len(noticias) >= max_noticias:
                break
        except Exception:
            continue
    return noticias


def _buscar_finviz(ticker_us, max_noticias=5):
    noticias = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        resp = requests.get(
            f"https://finviz.com/quote.ashx?t={ticker_us}",
            headers=headers, timeout=10
        )
        if resp.status_code != 200:
            return []
        matches = re.findall(
            r'<a[^>]+href="(https?://[^"]+)"[^>]*class="[^"]*tab-link[^"]*"[^>]*>([^<]+)</a>',
            resp.text
        )
        datas = re.findall(r'(\w{3}-\d{2}-\d{2}\s+\d{2}:\d{2}(?:AM|PM))', resp.text)
        for i, (link, titulo) in enumerate(matches[:max_noticias]):
            titulo = _limpar_html(titulo)
            if not titulo or len(titulo) < 10:
                continue
            noticias.append({
                'titulo': titulo,
                'link': link,
                'data': datas[i] if i < len(datas) else '',
                'descricao': '',
                'fonte': 'Finviz'
            })
    except Exception:
        pass
    return noticias


@st.cache_data(ttl=900, show_spinner=False)   # cache 15min — notícias mudam mais que dados
def buscar_noticias_com_traducao(ticker_us, empresa_nome=''):
    """
    Agrega notícias de múltiplas fontes em paralelo, deduplica e traduz
    títulos e descrições para português via MyMemory (paralelo).

    Fontes (em ordem de prioridade):
      1. Yahoo Finance RSS     — mais confiável para tickers US
      2. Google News RSS       — maior cobertura, múltiplas fontes
      3. GuruFocus RSS         — análises fundamentalistas
      4. Seeking Alpha RSS     — artigos de análise
      5. MarketWatch RSS       — headlines gerais filtradas pelo ticker
      6. Finviz scraping       — fallback rápido
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Busca todas as fontes em paralelo
    tarefas = {
        'yahoo':       lambda: _buscar_yahoo_rss(ticker_us, 8),
        'google':      lambda: _buscar_google_news_rss(ticker_us, empresa_nome, 6),
        'gurufocus':   lambda: _buscar_gurufocus_rss(ticker_us, 5),
        'seekingalpha':lambda: _buscar_seekingalpha_rss(ticker_us, 5),
        'marketwatch': lambda: _buscar_marketwatch_rss(ticker_us, 4),
        'finviz':      lambda: _buscar_finviz(ticker_us, 5),
    }

    resultados = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        fut_map = {ex.submit(fn): nome for nome, fn in tarefas.items()}
        for fut in as_completed(fut_map, timeout=15):
            nome = fut_map[fut]
            try:
                resultados[nome] = fut.result()
            except Exception:
                resultados[nome] = []

    # Prioridade de montagem
    todas = []
    for fonte in ['yahoo', 'google', 'gurufocus', 'seekingalpha', 'marketwatch', 'finviz']:
        todas += resultados.get(fonte, [])

    # Deduplica por título (primeiros 55 chars, case-insensitive)
    vistos, unicas = set(), []
    for n in todas:
        chave = re.sub(r'\s+', ' ', n['titulo'].lower().strip())[:55]
        if chave and chave not in vistos:
            vistos.add(chave)
            unicas.append(n)

    unicas = unicas[:14]
    if not unicas:
        return []

    # Traduz títulos em paralelo
    titulos_trad = _traduzir_com_mymemory([n['titulo'] for n in unicas])
    for n, t in zip(unicas, titulos_trad):
        if t:
            n['titulo'] = t

    # Traduz descrições não-vazias em paralelo
    descs_idx  = [(i, n['descricao']) for i, n in enumerate(unicas) if n.get('descricao')]
    if descs_idx:
        indices, descs = zip(*descs_idx)
        descs_trad = _traduzir_com_mymemory(list(descs))
        for i, d in zip(indices, descs_trad):
            if d:
                unicas[i]['descricao'] = d

    return unicas


def _renderizar_card_noticia(noticia):
    """Renderiza card de notícia em HTML com design aprimorado."""
    titulo     = noticia.get('titulo', '')
    link       = noticia.get('link', '#')
    data       = noticia.get('data', '')
    desc       = noticia.get('descricao', '')
    fonte      = noticia.get('fonte', '')
    fonte_real = noticia.get('fonte_real', '')   # para Google News

    cores = {
        'Yahoo Finance': ('#eff6ff', '#1d4ed8', '#dbeafe', '📊'),
        'Google News':   ('#f0f9ff', '#0369a1', '#e0f2fe', '🌐'),
        'Seeking Alpha': ('#f0fdf4', '#15803d', '#dcfce7', '📈'),
        'GuruFocus':     ('#fefce8', '#854d0e', '#fef9c3', '🧙'),
        'MarketWatch':   ('#fdf2f8', '#9d174d', '#fce7f3', '📺'),
        'Finviz':        ('#fdf4ff', '#7e22ce', '#f3e8ff', '🔍'),
    }
    bg, cor_fonte, badge_bg, icone = cores.get(fonte, ('#f8fafc', '#475569', '#e2e8f0', '📰'))

    # Exibe fonte real (ex: Reuters, CNBC) quando disponível
    label_fonte = f"{icone} {fonte_real}" if fonte_real and fonte == 'Google News' else f"{icone} {fonte}"

    desc_html = (
        f"<p style='margin:0.35rem 0 0;font-size:0.8rem;color:#64748b;"
        f"line-height:1.45;display:-webkit-box;-webkit-line-clamp:2;"
        f"-webkit-box-orient:vertical;overflow:hidden;'>{desc}</p>"
    ) if desc else ""

    return f"""
    <div style='background:{bg};border:1px solid {badge_bg};
                border-left:3px solid {cor_fonte};
                border-radius:10px;padding:0.85rem 1rem;margin-bottom:0.6rem;'>
        <div style='display:flex;justify-content:space-between;
                    align-items:flex-start;gap:0.5rem;margin-bottom:0.25rem;'>
            <span style='background:{badge_bg};color:{cor_fonte};font-size:0.65rem;
                         font-weight:700;padding:0.12rem 0.5rem;border-radius:999px;
                         white-space:nowrap;flex-shrink:0;'>{label_fonte}</span>
            <span style='font-size:0.68rem;color:#94a3b8;white-space:nowrap;'>{data}</span>
        </div>
        <a href="{link}" target="_blank"
           style='font-size:0.88rem;font-weight:700;color:#1e293b;
                  text-decoration:none;line-height:1.35;display:block;'>{titulo}</a>
        {desc_html}
    </div>"""

# =============================================================================
# MÓDULO DE MACHINE LEARNING — PREVISÃO DE PREÇOS (ISOLADO)
# =============================================================================

@st.cache_data(ttl=1800, show_spinner=False)
def _prever_preco_ml_cached(ticker, dias_previsao=5):
    """Wrapper cacheado para prever_preco_ml — evita re-treino a cada interação."""
    try:
        import yfinance as yf
        df_raw = yf.download(f"{ticker}.SA", period='1y', interval='1d',
                             auto_adjust=True, progress=False, timeout=30)
        if df_raw is None or df_raw.empty:
            return {'erro': 'Sem dados para o modelo.'}
        if isinstance(df_raw.columns, pd.MultiIndex):
            df_raw.columns = df_raw.columns.get_level_values(0)
        # Calcula indicadores necessários
        close = df_raw['Close'].dropna()
        df_raw['EMA20']    = close.ewm(span=20).mean()
        df_raw['EMA50']    = close.ewm(span=50).mean()
        df_raw['EMA200']   = close.ewm(span=200).mean()
        delta = close.diff()
        ganho = delta.clip(lower=0).rolling(14).mean()
        perda = (-delta.clip(upper=0)).rolling(14).mean()
        df_raw['RSI14']    = 100 - (100 / (1 + ganho / perda.replace(0, float('nan'))))
        sma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        df_raw['BB_Lower'] = sma20 - std20 * 2
        df_raw['BB_Upper'] = sma20 + std20 * 2
        ema12 = close.ewm(span=12).mean()
        ema26 = close.ewm(span=26).mean()
        macd  = ema12 - ema26
        df_raw['MACD_Hist'] = macd - macd.ewm(span=9).mean()
        return prever_preco_ml(df_raw.dropna(subset=['Close']), ticker, dias_previsao)
    except Exception as e:
        return {'erro': f'Erro no modelo ML: {str(e)}'}


@st.cache_data(ttl=1800, show_spinner=False)
def _executar_agente_rl_cached(ticker, episodes=8, window_size=5):
    """Wrapper cacheado para executar_agente_rl — evita re-treino a cada interação."""
    try:
        import yfinance as yf
        df_raw = yf.download(f"{ticker}.SA", period='1y', interval='1d',
                             auto_adjust=True, progress=False, timeout=30)
        if df_raw is None or df_raw.empty:
            return {'erro': 'Sem dados para o agente RL.'}
        if isinstance(df_raw.columns, pd.MultiIndex):
            df_raw.columns = df_raw.columns.get_level_values(0)
        close = df_raw['Close'].dropna()
        df_raw['EMA20'] = close.ewm(span=20).mean()
        df_raw['RSI14'] = 50.0   # placeholder — RL usa só Close
        return executar_agente_rl(df_raw.dropna(subset=['Close']), ticker, episodes, window_size)
    except Exception as e:
        return {'erro': f'Erro no agente RL: {str(e)}'}


@st.cache_data(ttl=1800, show_spinner=False)
def _calcular_minervini_cached(ticker):
    """Wrapper cacheado para calcular_minervini."""
    try:
        import yfinance as yf
        df_raw = yf.download(f"{ticker}.SA", period='1y', interval='1d',
                             auto_adjust=True, progress=False, timeout=30)
        if df_raw is None or df_raw.empty:
            return {'erro': 'Sem dados para análise Minervini.'}
        if isinstance(df_raw.columns, pd.MultiIndex):
            df_raw.columns = df_raw.columns.get_level_values(0)
        close = df_raw['Close'].dropna()
        df_raw['EMA20']  = close.ewm(span=20).mean()
        df_raw['RSI14']  = 50.0
        df_raw['Stoch_K']= 50.0
        sma20 = close.rolling(20).mean(); std20 = close.rolling(20).std()
        df_raw['BB_Lower'] = sma20 - std20*2
        df_raw['BB_Upper'] = sma20 + std20*2
        return calcular_minervini(df_raw.dropna(subset=['Close']), ticker)
    except Exception as e:
        return {'erro': f'Erro Minervini: {str(e)}'}


def prever_preco_ml(df_ticker, ticker, dias_previsao=5):
    """
    Ensemble de múltiplos modelos inspirado em framework de previsão de retornos de ações:
      - Gradient Boosting Regressor
      - Random Forest Regressor
      - Extra Trees Regressor
      - Elastic Net (regularização L1+L2)
      - Linear Regression (baseline)

    O melhor modelo (R² no conjunto de teste) é selecionado automaticamente.

    Features (inspiradas no notebook de Stock Price Prediction):
      - Retornos log em múltiplos horizontes: 1d, 5d, 15d, 30d (multi-period returns)
      - EMA20, EMA50, distância do preço às médias
      - RSI14 (oscilador de momento)
      - Volatilidade realizada (10d e 20d)
      - Bollinger %B (posição do preço na banda)
      - MACD Histogram (quando disponível)

    Target: retorno log do próximo dia (shift -1), reconvertido para preço.
    Split temporal 80/20 — sem data leakage.
    """
    try:
        from sklearn.linear_model import LinearRegression, ElasticNet
        from sklearn.ensemble import (GradientBoostingRegressor,
                                      RandomForestRegressor,
                                      ExtraTreesRegressor)
        from sklearn.preprocessing import StandardScaler
        from sklearn.metrics import r2_score, mean_squared_error
        import numpy as np

        df = df_ticker.copy().sort_index()

        # --- Colunas mínimas necessárias ---
        for col in ['Close', 'EMA20', 'RSI14']:
            if col not in df.columns:
                return {'erro': f'Coluna {col} não encontrada nos dados.'}

        df = df.dropna(subset=['Close', 'EMA20', 'RSI14'])

        if len(df) < 80:
            return {'erro': 'Dados insuficientes para treinar o ensemble (mín. 80 dias).'}

        close = df['Close']

        # ── Feature engineering (multi-period returns, inspirado no notebook) ──
        df = df.copy()
        df['LogRet_1d']  = np.log(close).diff(1)
        df['LogRet_5d']  = np.log(close).diff(5)
        df['LogRet_15d'] = np.log(close).diff(15)
        df['LogRet_30d'] = np.log(close).diff(30)

        df['Volatil_10d'] = df['LogRet_1d'].rolling(10).std()
        df['Volatil_20d'] = df['LogRet_1d'].rolling(20).std()

        df['EMA_Dist20']  = (close - df['EMA20']) / df['EMA20']
        if 'EMA50' in df.columns:
            df['EMA_Dist50'] = (close - df['EMA50']) / df['EMA50']
        if 'EMA200' in df.columns:
            df['EMA_Dist200'] = (close - df['EMA200']) / df['EMA200']

        if 'BB_Upper' in df.columns and 'BB_Lower' in df.columns:
            bw = df['BB_Upper'] - df['BB_Lower']
            df['BB_pctB'] = (close - df['BB_Lower']) / bw.replace(0, np.nan)

        if 'MACD_Hist' in df.columns:
            df['MACD_Hist_norm'] = df['MACD_Hist'] / close

        # Target: retorno log do próximo dia
        df['Target_LogRet'] = df['LogRet_1d'].shift(-1)
        df = df.dropna()

        feature_cols = [c for c in [
            'LogRet_1d', 'LogRet_5d', 'LogRet_15d', 'LogRet_30d',
            'Volatil_10d', 'Volatil_20d',
            'RSI14', 'EMA_Dist20',
            'EMA_Dist50', 'EMA_Dist200',
            'BB_pctB', 'MACD_Hist_norm',
        ] if c in df.columns]

        X = df[feature_cols].values
        y = df['Target_LogRet'].values   # target: log-retorno

        # ── Split temporal 80/20 sem embaralhamento ──────────────────────────────
        split   = int(len(X) * 0.80)
        X_train = X[:split];  y_train = y[:split]
        X_test  = X[split:];  y_test  = y[split:]

        # Normalização por StandardScaler (melhor para GBM/RF que MinMax)
        scaler = StandardScaler()
        X_train_sc = scaler.fit_transform(X_train)
        X_test_sc  = scaler.transform(X_test)

        # ── Candidatos: hiperparâmetros ajustados para datasets pequenos (~200 amostras)
        # Menos estimadores + mais regularização + profundidade menor = menos overfitting
        pequeno = split < 150
        leaf_min = max(3, split // 30)

        candidatos = {
            'GradientBoosting': GradientBoostingRegressor(
                n_estimators=80 if pequeno else 150,
                learning_rate=0.08 if pequeno else 0.05,
                max_depth=2 if pequeno else 3,
                subsample=0.7,
                min_samples_leaf=leaf_min,
                random_state=42),
            'RandomForest': RandomForestRegressor(
                n_estimators=100,
                max_depth=3 if pequeno else 5,
                min_samples_leaf=leaf_min,
                max_features='sqrt',
                random_state=42, n_jobs=-1),
            'ExtraTrees': ExtraTreesRegressor(
                n_estimators=100,
                max_depth=3 if pequeno else 5,
                min_samples_leaf=leaf_min,
                max_features='sqrt',
                random_state=42, n_jobs=-1),
            'ElasticNet': ElasticNet(
                alpha=0.01 if pequeno else 0.001,
                l1_ratio=0.5, max_iter=5000),
            'LinearRegression': LinearRegression(),
        }

        # ── Treinar e avaliar cada candidato ─────────────────────────────────────
        resultados_modelos = {}
        for nome, mod in candidatos.items():
            try:
                mod.fit(X_train_sc, y_train)
                r2   = max(0.0, float(r2_score(y_test, mod.predict(X_test_sc))))
                rmse = float(np.sqrt(mean_squared_error(y_test, mod.predict(X_test_sc))))
                resultados_modelos[nome] = {'modelo': mod, 'r2': r2, 'rmse': rmse}
            except Exception:
                continue

        if not resultados_modelos:
            return {'erro': 'Todos os modelos falharam no treinamento.'}

        # Seleciona o melhor por R²
        melhor_nome = max(resultados_modelos, key=lambda n: resultados_modelos[n]['r2'])
        melhor      = resultados_modelos[melhor_nome]
        modelo      = melhor['modelo']
        confianca   = melhor['r2']

        # Ranking completo para exibição
        ranking = sorted(
            [{'nome': n, 'r2': v['r2'], 'rmse': v['rmse']}
             for n, v in resultados_modelos.items()],
            key=lambda x: x['r2'], reverse=True
        )

        # ── Previsão iterativa ────────────────────────────────────────────────────
        ultimo_idx   = -1
        estado       = {col: float(df[col].iloc[ultimo_idx]) for col in feature_cols}
        preco_cur    = float(df['Close'].iloc[ultimo_idx])
        previsoes    = []
        alpha20      = 2 / 21
        ema20_cur    = float(df['EMA20'].iloc[ultimo_idx])
        ema50_cur    = float(df['EMA50'].iloc[ultimo_idx]) if 'EMA50' in df.columns else ema20_cur
        ema200_cur   = float(df['EMA200'].iloc[ultimo_idx]) if 'EMA200' in df.columns else ema20_cur
        rsi_cur      = float(df['RSI14'].iloc[ultimo_idx])
        vol10_cur    = float(df['Volatil_10d'].iloc[ultimo_idx])
        vol20_cur    = float(df['Volatil_20d'].iloc[ultimo_idx])
        logret_5_hist  = list(df['LogRet_1d'].iloc[-5:].values)
        logret_15_hist = list(df['LogRet_1d'].iloc[-15:].values)
        logret_30_hist = list(df['LogRet_1d'].iloc[-30:].values)

        for _ in range(dias_previsao):
            feats = []
            logret_1 = estado.get('LogRet_1d', 0.0)

            logret_5_hist.append(logret_1)
            if len(logret_5_hist) > 5:   logret_5_hist.pop(0)
            logret_15_hist.append(logret_1)
            if len(logret_15_hist) > 15: logret_15_hist.pop(0)
            logret_30_hist.append(logret_1)
            if len(logret_30_hist) > 30: logret_30_hist.pop(0)

            estado_atualizado = {
                'LogRet_1d':      logret_1,
                'LogRet_5d':      sum(logret_5_hist),
                'LogRet_15d':     sum(logret_15_hist),
                'LogRet_30d':     sum(logret_30_hist),
                'Volatil_10d':    vol10_cur,
                'Volatil_20d':    vol20_cur,
                'RSI14':          rsi_cur,
                'EMA_Dist20':     (preco_cur - ema20_cur) / ema20_cur if ema20_cur else 0,
                'EMA_Dist50':     (preco_cur - ema50_cur) / ema50_cur if ema50_cur else 0,
                'EMA_Dist200':    (preco_cur - ema200_cur) / ema200_cur if ema200_cur else 0,
                'BB_pctB':        estado.get('BB_pctB', 0.5),
                'MACD_Hist_norm': estado.get('MACD_Hist_norm', 0.0),
            }
            feats = [estado_atualizado[c] for c in feature_cols]
            entrada_sc   = scaler.transform(np.array([feats]))
            logret_prev  = float(modelo.predict(entrada_sc)[0])

            preco_prev = preco_cur * np.exp(logret_prev)
            previsoes.append(round(preco_prev, 2))

            # Atualiza estado iterativo
            ret_real    = logret_prev
            vol10_cur   = vol10_cur * 0.9 + abs(ret_real) * 0.1
            vol20_cur   = vol20_cur * 0.95 + abs(ret_real) * 0.05
            ema20_cur   = alpha20 * preco_prev + (1 - alpha20) * ema20_cur
            ema50_cur   = (2/51) * preco_prev + (1 - 2/51) * ema50_cur
            ema200_cur  = (2/201) * preco_prev + (1 - 2/201) * ema200_cur
            delta = preco_prev - preco_cur
            ganho = max(delta, 0); perda = max(-delta, 0)
            rsi_cur = min(max(rsi_cur + (ganho - perda) / (preco_cur + 1e-9) * 30, 0), 100)
            estado['LogRet_1d'] = ret_real
            estado['BB_pctB']   = min(max(estado.get('BB_pctB', 0.5) + ret_real * 5, 0), 1)
            preco_cur = preco_prev

        variacao_pct = ((previsoes[-1] - float(df['Close'].iloc[-1])) /
                        float(df['Close'].iloc[-1])) * 100

        if   variacao_pct >  1.5: direcao = "ALTA"
        elif variacao_pct < -1.5: direcao = "BAIXA"
        else:                     direcao = "LATERAL"

        return {
            'erro'          : None,
            'previsoes'     : previsoes,
            'direcao'       : direcao,
            'variacao_pct'  : round(variacao_pct, 2),
            'confianca'     : round(confianca * 100, 1),
            'ultimo_preco'  : round(float(df['Close'].iloc[-1]), 2),
            'melhor_modelo' : melhor_nome,
            'ranking_modelos': ranking,
            'n_features'    : len(feature_cols),
            'features_usadas': feature_cols,
            'n_amostras'    : len(df),
            'rmse_melhor'   : round(melhor['rmse'], 6),
        }

    except ImportError:
        return {'erro': 'scikit-learn não instalado. Adicione scikit-learn ao requirements.txt.'}
    except Exception as e:
        return {'erro': f'Erro no ensemble: {str(e)}'}


def renderizar_painel_ml(resultado_ml, ticker, empresa, dias_previsao=5):
    """
    Renderiza o painel de previsão ML dentro de um st.expander.
    Totalmente isolado — não interfere em nenhuma outra seção.
    """
    with st.expander("🤖 Previsão por Inteligência Artificial — Ensemble de Modelos", expanded=False):

        if resultado_ml.get('erro'):
            st.warning(f"⚠️ {resultado_ml['erro']}")
            return

        direcao        = resultado_ml['direcao']
        variacao       = resultado_ml['variacao_pct']
        confianca      = resultado_ml['confianca']
        previsoes      = resultado_ml['previsoes']
        ult_preco      = resultado_ml['ultimo_preco']
        melhor_modelo  = resultado_ml.get('melhor_modelo', 'N/A')
        ranking        = resultado_ml.get('ranking_modelos', [])
        n_features     = resultado_ml.get('n_features', 0)
        features_usadas = resultado_ml.get('features_usadas', [])
        n_amostras     = resultado_ml.get('n_amostras', 0)
        rmse_melhor    = resultado_ml.get('rmse_melhor', 0)

        # Alerta de qualidade quando R² é muito baixo
        if confianca < 20:
            st.warning(
                f"⚠️ **Ajuste fraco (R² = {confianca:.1f}%):** com apenas ~{n_amostras} amostras "
                f"(1 ano de dados diários), modelos de ensemble tendem a ter R² baixo em séries "
                f"financeiras — que são inerentemente ruidosas. A direção prevista é indicativa, "
                f"não determinística. Use em conjunto com a análise técnica."
            )

        # Ícones por modelo
        icones_modelo = {
            'GradientBoosting':  '🌲',
            'RandomForest':      '🌳',
            'ExtraTrees':        '🌴',
            'ElasticNet':        '🔗',
            'LinearRegression':  '📏',
        }
        icone_melhor = icones_modelo.get(melhor_modelo, '🤖')

        # --- Cabeçalho explicativo ---
        nomes_pt = {
            'GradientBoosting': 'Gradient Boosting',
            'RandomForest':     'Random Forest',
            'ExtraTrees':       'Extra Trees',
            'ElasticNet':       'Elastic Net',
            'LinearRegression': 'Regressão Linear',
        }
        st.markdown(f"""
        <div style='background:linear-gradient(135deg,#1e1b4b 0%,#312e81 100%);
                    padding:1rem 1.4rem;border-radius:12px;margin-bottom:1rem;'>
            <div style='display:flex;align-items:center;gap:0.8rem;margin-bottom:0.6rem;'>
                <span style='font-size:1.8rem;'>{icone_melhor}</span>
                <div>
                    <div style='color:#a5b4fc;font-weight:800;font-size:1rem;'>
                        Modelo selecionado: {nomes_pt.get(melhor_modelo, melhor_modelo)}
                    </div>
                    <div style='color:#c7d2fe;font-size:0.78rem;'>
                        Melhor R² entre 5 algoritmos treinados simultaneamente
                    </div>
                </div>
            </div>
            <p style='margin:0;color:#c7d2fe;font-size:0.80rem;line-height:1.6;'>
                🧠 <strong style='color:#a5b4fc;'>Framework Ensemble:</strong>
                Cinco modelos (Gradient Boosting, Random Forest, Extra Trees, Elastic Net, Regressão Linear)
                foram treinados com <strong>{n_features} features</strong> derivadas de retornos logarítmicos
                em múltiplos horizontes (1d, 5d, 15d, 30d), volatilidade realizada, distância às EMAs,
                RSI14 e Bollinger %B. O target é o retorno log do próximo dia, reconvertido para preço.
                O melhor modelo no conjunto de teste (split 80/20 temporal) é usado para a previsão.
                <br><br>
                ⚠️ <strong style='color:#fbbf24;'>Aviso:</strong>
                Previsões de ML são estimativas estatísticas, não garantias.
                Use como <u>um dos critérios</u> da sua análise, nunca como único sinal.
            </p>
        </div>""", unsafe_allow_html=True)

        # --- Cards de resumo ---
        cfg = {
            "ALTA"   : ("#bbf7d0","#16a34a","#14532d","▲ ALTA PREVISTA",   "#dcfce7"),
            "BAIXA"  : ("#fecaca","#dc2626","#7f1d1d","▼ BAIXA PREVISTA",  "#fef2f2"),
            "LATERAL": ("#fef9c3","#d97706","#78350f","— LATERAL PREVISTA","#fefce8"),
        }
        bg_grad, cor_borda, cor_txt, label_dir, bg_light = cfg[direcao]
        col_dir, col_conf, col_var = st.columns(3)

        with col_dir:
            st.markdown(f"""
            <div style='background:{bg_light};border:2px solid {cor_borda};
                        border-left:6px solid {cor_borda};
                        padding:1rem 0.8rem;border-radius:10px;text-align:center;
                        min-height:115px;display:flex;flex-direction:column;
                        justify-content:center;gap:0.3rem;'>
                <div style='font-size:1.6rem;font-weight:900;color:{cor_borda};
                            letter-spacing:0.02em;line-height:1.1;'>{label_dir}</div>
                <div style='font-size:0.78rem;font-weight:600;color:{cor_txt};
                            opacity:0.8;'>próximos {dias_previsao} dias úteis</div>
            </div>""", unsafe_allow_html=True)

        with col_conf:
            # Barra de progresso CSS + label explicativo claro
            r2_val   = confianca           # já é 0–100
            cor_c    = "#15803d" if r2_val >= 60 else "#b45309" if r2_val >= 30 else "#b91c1c"
            bg_bar_c = "#dcfce7" if r2_val >= 60 else "#fef3c7" if r2_val >= 30 else "#fee2e2"
            nivel    = "Ajuste bom" if r2_val >= 60 else "Ajuste moderado" if r2_val >= 30 else "Ajuste fraco"
            barra_w  = min(int(r2_val), 100)
            st.markdown(f"""
            <div style='background:#f8fafc;border:2px solid #e2e8f0;padding:0.9rem 1rem;
                        border-radius:10px;min-height:115px;
                        display:flex;flex-direction:column;justify-content:center;'>
                <div style='font-size:0.7rem;font-weight:700;color:#94a3b8;
                            text-transform:uppercase;letter-spacing:0.06em;margin-bottom:0.3rem;'>
                    Precisão do Modelo (R²)</div>
                <div style='font-size:2rem;font-weight:900;color:{cor_c};line-height:1;'>
                    {r2_val:.1f}%</div>
                <div style='background:#e2e8f0;border-radius:99px;height:6px;margin:0.45rem 0;'>
                    <div style='background:{cor_c};width:{barra_w}%;height:6px;
                                border-radius:99px;transition:width 0.4s;'></div>
                </div>
                <div style='font-size:0.72rem;color:#64748b;'>
                    {nivel} &nbsp;·&nbsp; {icone_melhor} {nomes_pt.get(melhor_modelo, melhor_modelo)}</div>
                <div style='font-size:0.67rem;color:#94a3b8;margin-top:0.15rem;'>
                    R²=1 perfeito · R²=0 aleatório</div>
            </div>""", unsafe_allow_html=True)

        with col_var:
            sinal_v = "+" if variacao >= 0 else ""
            cor_v   = "#15803d" if variacao > 1.5 else "#b91c1c" if variacao < -1.5 else "#b45309"
            seta_v  = "▲" if variacao > 1.5 else "▼" if variacao < -1.5 else "—"
            st.markdown(f"""
            <div style='background:#f8fafc;border:2px solid #e2e8f0;padding:0.9rem 1rem;
                        border-radius:10px;min-height:115px;
                        display:flex;flex-direction:column;justify-content:center;'>
                <div style='font-size:0.7rem;font-weight:700;color:#94a3b8;
                            text-transform:uppercase;letter-spacing:0.06em;margin-bottom:0.3rem;'>
                    Variação Estimada</div>
                <div style='font-size:2rem;font-weight:900;color:{cor_v};line-height:1;'>
                    {seta_v} {sinal_v}{variacao:.2f}%</div>
                <div style='font-size:0.72rem;color:#64748b;margin-top:0.4rem;'>
                    D0 &rarr; D+{dias_previsao} &nbsp;(previsão iterativa)</div>
                <div style='font-size:0.67rem;color:#94a3b8;margin-top:0.15rem;'>
                    Preço atual: R${ult_preco:.2f}</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

        # --- Ranking dos modelos — colunas Streamlit nativas (evita renderização bruta de HTML) ---
        if ranking:
            st.markdown("**🏆 Desempenho dos Modelos no Conjunto de Teste:**")
            medalhas = ['🥇','🥈','🥉','4°','5°']
            # Primeira linha: top 3
            cols_top = st.columns(3)
            for i, mod_info in enumerate(ranking[:3]):
                cor_rank = "#15803d" if i == 0 else "#64748b"
                borda_r  = "2px solid #22c55e" if i == 0 else "1px solid #e2e8f0"
                bg_rank  = "#f0fdf4" if i == 0 else "#f8fafc"
                r2_disp  = mod_info["r2"] * 100
                rmse_d   = mod_info["rmse"]
                nome_c   = nomes_pt.get(mod_info["nome"], mod_info["nome"])
                barra_w  = min(int(r2_disp * 2), 100)
                bar_cor  = "#22c55e" if i == 0 else "#94a3b8"
                with cols_top[i]:
                    st.markdown(f"""
                    <div style='background:{bg_rank};border:{borda_r};border-radius:10px;
                                padding:0.75rem 0.85rem;'>
                        <div style='font-size:1.1rem;margin-bottom:0.2rem;'>{medalhas[i]}</div>
                        <div style='font-size:0.72rem;font-weight:700;color:{cor_rank};
                                    margin-bottom:0.35rem;'>
                            {icones_modelo.get(mod_info["nome"],"")} {nome_c}</div>
                        <div style='font-size:1.4rem;font-weight:900;color:{cor_rank};
                                    line-height:1;margin-bottom:0.3rem;'>{r2_disp:.1f}%</div>
                        <div style='background:#e2e8f0;border-radius:99px;height:5px;
                                    margin-bottom:0.3rem;'>
                            <div style='background:{bar_cor};width:{barra_w}%;height:5px;
                                        border-radius:99px;'></div></div>
                        <div style='font-size:0.62rem;color:#94a3b8;'>
                            R² &nbsp;·&nbsp; RMSE {rmse_d:.5f}</div>
                    </div>""", unsafe_allow_html=True)
            # Segunda linha: posições 4 e 5
            if len(ranking) > 3:
                cols_bot = st.columns(len(ranking) - 3)
                for i, mod_info in enumerate(ranking[3:]):
                    idx = i + 3
                    r2_disp = mod_info["r2"] * 100
                    rmse_d  = mod_info["rmse"]
                    nome_c  = nomes_pt.get(mod_info["nome"], mod_info["nome"])
                    with cols_bot[i]:
                        st.markdown(f"""
                        <div style='background:#f8fafc;border:1px solid #e2e8f0;
                                    border-radius:10px;padding:0.65rem 0.75rem;'>
                            <div style='font-size:0.9rem;margin-bottom:0.15rem;'>{medalhas[idx]}</div>
                            <div style='font-size:0.7rem;font-weight:700;color:#64748b;
                                        margin-bottom:0.25rem;'>
                                {icones_modelo.get(mod_info["nome"],"")} {nome_c}</div>
                            <div style='font-size:1.2rem;font-weight:800;color:#64748b;
                                        line-height:1;margin-bottom:0.2rem;'>{r2_disp:.1f}%</div>
                            <div style='font-size:0.6rem;color:#94a3b8;'>
                                RMSE {rmse_d:.5f}</div>
                        </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)

        # --- Gráfico ---
        todos_precos = [ult_preco] + previsoes
        todos_labels = ["Hoje"] + [f"D+{i+1}" for i in range(dias_previsao)]
        cor_linha = "#16a34a" if direcao == "ALTA" else "#dc2626" if direcao == "BAIXA" else "#d97706"

        y_min = min(todos_precos) * 0.985
        y_max = max(todos_precos) * 1.015
        if (y_max - y_min) < ult_preco * 0.01:
            y_min = ult_preco * 0.992
            y_max = ult_preco * 1.008

        fig, ax = plt.subplots(figsize=(9, 3.5))
        fig.patch.set_facecolor('#f8fafc')
        ax.set_facecolor('#f8fafc')

        xs = list(range(len(todos_precos)))

        ax.fill_between(xs, todos_precos, y_min, alpha=0.18, color=cor_linha)
        ax.plot(xs, todos_precos,
                color=cor_linha, linewidth=2.5,
                marker='o', markersize=7,
                markerfacecolor='white',
                markeredgecolor=cor_linha, markeredgewidth=2.2,
                zorder=3)
        ax.scatter([0], [ult_preco], color='#6366f1', s=120, zorder=5, label='Hoje')
        ax.axhline(ult_preco, color='#94a3b8', linestyle='--', linewidth=1, alpha=0.5)

        margem = (y_max - y_min)
        for i, p in enumerate(todos_precos):
            ax.annotate(
                f'R${p:.2f}',
                xy=(i, p),
                xytext=(0, 12),
                textcoords='offset points',
                ha='center', va='bottom',
                fontsize=8, color='#1e293b', fontweight='700',
                bbox=dict(boxstyle='round,pad=0.2', facecolor='white',
                          edgecolor='#e2e8f0', alpha=0.85)
            )

        ax.set_ylim(y_min, y_max + margem * 0.45)
        ax.set_xticks(xs)
        ax.set_xticklabels(todos_labels, fontsize=9, color='#475569', fontweight='600')
        ax.set_ylabel('Preço (R$)', fontsize=8.5, color='#64748b')
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'R${v:.2f}'))
        ax.tick_params(axis='y', labelsize=8, colors='#64748b')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#e2e8f0')
        ax.spines['bottom'].set_color('#e2e8f0')
        ax.set_title(
            f'Previsão Ensemble {icone_melhor} {nomes_pt.get(melhor_modelo,melhor_modelo)} — {ticker} ({empresa})',
            fontsize=9.5, color='#334155', pad=12, fontweight='bold')
        ax.grid(axis='y', alpha=0.12, color='#94a3b8')
        plt.tight_layout()
        st.pyplot(fig)
        plt.close(fig)

        # --- Tabela de previsões ---
        st.markdown("**📋 Preços Previstos por Dia:**")
        cols_prev = st.columns(dias_previsao)
        for i, (col, preco) in enumerate(zip(cols_prev, previsoes)):
            delta_pct = ((preco - ult_preco) / ult_preco) * 100
            sinal_d   = "+" if delta_pct >= 0 else ""
            cor_d     = "#15803d" if delta_pct > 0 else "#dc2626" if delta_pct < 0 else "#78350f"
            with col:
                st.markdown(f"""
                <div style='background:#fff;border:1px solid #e2e8f0;border-radius:8px;
                            padding:0.65rem 0.4rem;text-align:center;'>
                    <div style='font-size:0.7rem;color:#94a3b8;font-weight:700;
                                letter-spacing:.05em;'>D+{i+1}</div>
                    <div style='font-size:0.95rem;font-weight:800;color:#1e293b;
                                margin:0.15rem 0;'>R${preco:.2f}</div>
                    <div style='font-size:0.73rem;font-weight:600;color:{cor_d};'>
                        {sinal_d}{delta_pct:.1f}%</div>
                </div>""", unsafe_allow_html=True)

        # --- Features utilizadas ---
        if features_usadas:
            nomes_feat = {
                'LogRet_1d':      'Retorno Log 1d',
                'LogRet_5d':      'Retorno Log 5d',
                'LogRet_15d':     'Retorno Log 15d',
                'LogRet_30d':     'Retorno Log 30d',
                'Volatil_10d':    'Volatilidade 10d',
                'Volatil_20d':    'Volatilidade 20d',
                'RSI14':          'RSI 14',
                'EMA_Dist20':     'Dist. EMA20',
                'EMA_Dist50':     'Dist. EMA50',
                'EMA_Dist200':    'Dist. EMA200',
                'BB_pctB':        'Bollinger %B',
                'MACD_Hist_norm': 'MACD Hist.',
            }
            feat_str = ' &nbsp;|&nbsp; '.join(
                f'<span style="background:#e0e7ff;color:#3730a3;padding:0.1rem 0.4rem;'
                f'border-radius:4px;font-size:0.72rem;font-weight:600;">'
                f'{nomes_feat.get(f,f)}</span>'
                for f in features_usadas
            )
            st.markdown(f"""
            <div style='margin-top:0.8rem;padding:0.7rem 1rem;background:#f1f5f9;
                        border-radius:8px;font-size:0.76rem;color:#64748b;line-height:2;'>
                📐 <strong>Features utilizadas ({n_features}):</strong><br>{feat_str}
            </div>""", unsafe_allow_html=True)

        # --- Legenda de confiança ---
        st.markdown("""
        <div style='margin-top:0.6rem;padding:0.7rem 1rem;background:#f1f5f9;
                    border-radius:8px;font-size:0.76rem;color:#64748b;'>
            📊 <strong>Confiança (R²):</strong>
            &nbsp;🟢 ≥ 60% = Boa &nbsp;|&nbsp;
            🟡 40–60% = Moderada &nbsp;|&nbsp;
            🔴 &lt; 40% = Baixa — use com cautela &nbsp;|&nbsp;
            Split temporal 80/20 sem embaralhamento (sem data leakage)
        </div>""", unsafe_allow_html=True)

# =============================================================================
# REINFORCEMENT LEARNING — AGENTE DE TRADING (Deep Q-Learning simplificado)
# Inspirado em: "Reinforcement Learning Based Trading Strategy" (Machine Learning
# for Asset Managers — Marcos Lopez de Prado / Kaggle notebook)
#
# Framework sem Keras/TensorFlow — usa apenas NumPy para funcionar no Streamlit Cloud.
# O agente aprende uma política ε-greedy com Q-Table aproximada por MLP-NumPy:
#   • Estado  : diferenças de preços numa janela deslizante (window_size=5)
#   • Ações   : 0=Hold, 1=Buy, 2=Sell
#   • Recompensa: PnL realizado na venda (sell_price - bought_price)
#   • Replay  : Experience Replay com mini-batch aleatório (batch_size=32)
#   • Rede    : 2 camadas ocultas (64→32 neurônios), ativação ReLU, saída linear
# =============================================================================

import numpy as np
import random
from collections import deque


def _sigmoid(x):
    return 1 / (1 + np.exp(-np.clip(x, -500, 500)))


def _relu(x):
    return np.maximum(0, x)


def _softmax(x):
    e = np.exp(x - x.max())
    return e / e.sum()


class _QNetwork:
    """Rede neural MLP NumPy puro — com gradient clipping e guarda contra NaN."""
    def __init__(self, state_size, action_size, lr=0.005):
        self.lr = lr
        # He init (melhor para ReLU)
        self.W1 = np.random.randn(state_size, 32) * np.sqrt(2.0 / state_size)
        self.b1 = np.zeros(32)
        self.W2 = np.random.randn(32, 16) * np.sqrt(2.0 / 32)
        self.b2 = np.zeros(16)
        self.W3 = np.random.randn(16, action_size) * np.sqrt(2.0 / 16)
        self.b3 = np.zeros(action_size)
        self._clip = 1.0   # gradient clipping threshold

    def predict(self, x):
        x  = np.asarray(x, dtype=np.float64).flatten()
        h1 = _relu(x  @ self.W1 + self.b1)
        h2 = _relu(h1 @ self.W2 + self.b2)
        out = h2 @ self.W3 + self.b3
        # Substitui NaN por 0 antes de retornar
        return np.nan_to_num(out, nan=0.0, posinf=1.0, neginf=-1.0)

    def train(self, x, y_target):
        x        = np.asarray(x, dtype=np.float64).flatten()
        y_target = np.asarray(y_target, dtype=np.float64)
        # Forward
        h1  = _relu(x  @ self.W1 + self.b1)
        h2  = _relu(h1 @ self.W2 + self.b2)
        out = h2 @ self.W3 + self.b3
        # MSE gradient
        d_out = (out - y_target)                     # shape (action_size,)
        # Backprop camada 3
        dW3 = np.outer(h2, d_out)
        db3 = d_out
        # Backprop camada 2
        d_h2 = (d_out @ self.W3.T) * (h2 > 0)
        dW2  = np.outer(h1, d_h2)
        db2  = d_h2
        # Backprop camada 1
        d_h1 = (d_h2 @ self.W2.T) * (h1 > 0)
        dW1  = np.outer(x, d_h1)
        db1  = d_h1
        # Gradient clipping + atualização
        for p, g in [(self.W3,dW3),(self.b3,db3),
                     (self.W2,dW2),(self.b2,db2),
                     (self.W1,dW1),(self.b1,db1)]:
            g_clipped = np.clip(g, -self._clip, self._clip)
            p -= self.lr * g_clipped
        # Zera NaN que possam ter surgido
        for attr in ['W1','b1','W2','b2','W3','b3']:
            arr = getattr(self, attr)
            if np.any(np.isnan(arr)):
                setattr(self, attr, np.nan_to_num(arr, nan=0.0))


class _RLAgent:
    """Agente DQN com Experience Replay, política ε-greedy e reward normalizado."""
    def __init__(self, state_size, action_size=3):
        self.state_size    = state_size
        self.action_size   = action_size
        self.memory        = deque(maxlen=1000)
        self.inventory     = []
        self.gamma         = 0.95
        self.epsilon       = 1.0
        self.epsilon_min   = 0.01
        self.epsilon_decay = 0.995
        self.model         = _QNetwork(state_size, action_size, lr=0.005)

    def act(self, state, is_eval=False):
        if not is_eval and np.random.rand() <= self.epsilon:
            return np.random.randint(self.action_size)
        q = self.model.predict(state)
        return int(np.argmax(q))

    def exp_replay(self, batch_size):
        if len(self.memory) < batch_size:
            return
        batch = random.sample(list(self.memory), batch_size)
        for state, action, reward, next_state, done in batch:
            target = reward
            if not done:
                next_q  = self.model.predict(next_state)
                target  = reward + self.gamma * float(np.max(next_q))
            q_vals          = self.model.predict(state).copy()
            q_vals[action]  = np.clip(target, -10, 10)   # clamp target
            self.model.train(state, q_vals)
        if self.epsilon > self.epsilon_min:
            self.epsilon *= self.epsilon_decay


def _get_state_rl(prices, t, window):
    """Estado = diferenças de preço sigmoid numa janela de `window` passos."""
    # Pega os últimos `window+1` preços para calcular `window` diferenças
    start  = max(0, t - window + 1)
    block  = prices[start:t + 1].astype(np.float64)
    if len(block) < window + 1:
        block = np.concatenate([np.full(window + 1 - len(block), block[0]), block])
    # `window` diferenças consecutivas normalizadas por sigmoid
    diffs = np.diff(block[-window - 1:])
    return _sigmoid(diffs).astype(np.float32)


def executar_agente_rl(df_ticker, ticker, episodes=12, window_size=5, max_steps=None):
    """
    Treina o agente RL nos dados históricos (80%) e avalia no conjunto de teste (20%).
    """
    try:
        np.random.seed(42)
        random.seed(42)

        df = df_ticker.copy().sort_index()
        if 'Close' not in df.columns or len(df) < 60:
            return {'erro': 'Dados insuficientes para o agente RL (mín. 60 dias).'}

        prices = df['Close'].dropna().values.astype(np.float64)
        # Normaliza preços para escala [0,1] — evita gradientes enormes
        p_min, p_max = prices.min(), prices.max()
        p_range      = p_max - p_min if p_max > p_min else 1.0
        prices_norm  = (prices - p_min) / p_range

        split      = int(len(prices) * 0.80)
        train_raw  = prices[:split]
        train_norm = prices_norm[:split]
        test_raw   = prices[split:]
        test_norm  = prices_norm[split:]

        agent      = _RLAgent(state_size=window_size)
        batch_size = 32
        historico_lucro = []

        # ── TREINAMENTO ───────────────────────────────────────────────────────────
        l = min(len(train_norm) - 1, max_steps or len(train_norm))
        for ep in range(episodes):
            state         = _get_state_rl(train_norm, 0, window_size)
            total_profit  = 0.0
            agent.inventory = []

            for t in range(l):
                action     = agent.act(state)
                next_state = _get_state_rl(train_norm, t + 1, window_size)
                reward     = 0.0

                if action == 1:   # comprar
                    agent.inventory.append(train_raw[t])

                elif action == 2:   # vender
                    if agent.inventory:
                        bought = agent.inventory.pop(0)
                        pnl    = train_raw[t] - bought
                        # Reward normalizado pelo preço — evita escala absurda
                        reward = pnl / (bought + 1e-9)
                        total_profit += pnl
                    else:
                        # Penaliza venda sem estoque
                        reward = -0.01

                done = (t == l - 1)
                # Força venda no último passo se tiver estoque
                if done and agent.inventory:
                    for b in agent.inventory:
                        total_profit += train_raw[t] - b
                    agent.inventory = []

                agent.memory.append((state, action, reward, next_state, done))
                state = next_state

                if len(agent.memory) >= batch_size:
                    agent.exp_replay(batch_size)

            historico_lucro.append(round(total_profit, 2))

        # ── AVALIAÇÃO (test set) ──────────────────────────────────────────────────
        state         = _get_state_rl(test_norm, 0, window_size)
        total_profit_test = 0.0
        agent.inventory   = []
        compras_test      = []
        vendas_test       = []

        for t in range(len(test_norm) - 1):
            action     = agent.act(state, is_eval=True)
            next_state = _get_state_rl(test_norm, t + 1, window_size)

            if action == 1:
                agent.inventory.append(test_raw[t])
                compras_test.append((t, test_raw[t]))
            elif action == 2 and agent.inventory:
                bought  = agent.inventory.pop(0)
                pnl_op  = test_raw[t] - bought
                total_profit_test += pnl_op
                vendas_test.append((t, test_raw[t], pnl_op))

            state = next_state

        # Fecha posições abertas ao final
        if agent.inventory:
            ultimo_p = test_raw[-1]
            for b in agent.inventory:
                pnl_op = ultimo_p - b
                total_profit_test += pnl_op
                vendas_test.append((len(test_raw) - 1, ultimo_p, pnl_op))

        # ── Recomendação: último estado nos dados completos ───────────────────────
        prices_full_norm = prices_norm
        ult_estado = _get_state_rl(prices_full_norm, len(prices_full_norm) - 1, window_size)
        ult_q      = agent.model.predict(ult_estado)
        # Se todos NaN (rede instável), usa Hold
        if np.any(np.isnan(ult_q)):
            ult_q = np.array([0.0, 0.0, 0.0])
        ult_acao   = int(np.argmax(ult_q))
        rec_map    = {0: 'AGUARDAR', 1: 'COMPRAR', 2: 'VENDER'}

        return {
            'erro'                      : None,
            'precos_treino'             : train_raw,
            'precos_teste'              : test_raw,
            'compras_teste'             : compras_test,
            'vendas_teste'              : vendas_test,
            'lucro_treino'              : round(historico_lucro[-1] if historico_lucro else 0, 2),
            'lucro_teste'               : round(total_profit_test, 2),
            'historico_lucro_episodios' : historico_lucro,
            'episodios'                 : episodes,
            'window_size'               : window_size,
            'recomendacao'              : rec_map[ult_acao],
            'q_values'                  : [round(float(v), 4) for v in ult_q],
            'split_idx'                 : split,
            'precos_completos'          : prices,
        }

    except Exception as e:
        return {'erro': f'Erro no agente RL: {str(e)}'}


def renderizar_painel_rl(resultado_rl, ticker, empresa):
    """Renderiza o painel do Agente de Reinforcement Learning."""
    with st.expander("🎮 Agente de Reinforcement Learning — Estratégia de Trading", expanded=False):

        if resultado_rl.get('erro'):
            st.warning(f"⚠️ {resultado_rl['erro']}")
            return

        rec         = resultado_rl['recomendacao']
        lucro_teste = resultado_rl['lucro_teste']
        historico   = resultado_rl['historico_lucro_episodios']
        compras     = resultado_rl['compras_teste']
        vendas      = resultado_rl['vendas_teste']
        precos_test = resultado_rl['precos_teste']
        precos_full = resultado_rl['precos_completos']
        split_idx   = resultado_rl['split_idx']
        q_vals      = resultado_rl['q_values']
        episodios   = resultado_rl['episodios']
        window_size = resultado_rl['window_size']

        # ── Cabeçalho ─────────────────────────────────────────────────────────────
        st.markdown(f"""
        <div style='background:linear-gradient(135deg,#0f172a 0%,#1e3a5f 100%);
                    padding:1rem 1.4rem;border-radius:12px;margin-bottom:1rem;'>
            <div style='display:flex;align-items:center;gap:0.8rem;margin-bottom:0.6rem;'>
                <span style='font-size:2rem;'>🎮</span>
                <div>
                    <div style='color:#93c5fd;font-weight:800;font-size:1rem;'>
                        Deep Q-Learning (DQN) — Agente de Trading
                    </div>
                    <div style='color:#bfdbfe;font-size:0.78rem;'>
                        Treinado por {episodios} episódios · Estado: janela de {window_size} dias · Ações: Hold / Buy / Sell
                    </div>
                </div>
            </div>
            <p style='margin:0;color:#bfdbfe;font-size:0.80rem;line-height:1.65;'>
                🤖 <strong style='color:#93c5fd;'>Como funciona:</strong>
                O agente observa as <em>diferenças de preço</em> numa janela deslizante (estado),
                decide entre <strong>Comprar, Vender ou Aguardar</strong> (ação) e recebe como recompensa
                o <strong>PnL realizado</strong> em cada venda. Uma rede neural MLP (64→32 neurônios, ReLU)
                aproxima a função Q(s,a) — o valor esperado de cada ação em cada estado.
                O treinamento usa <strong>Experience Replay</strong> (buffer de 500 transações, mini-batch de 32)
                e política <strong>ε-greedy</strong> com decaimento progressivo do ε.
                <br><br>
                ⚠️ <strong style='color:#fbbf24;'>Aviso:</strong>
                RL é altamente sensível à qualidade dos dados e ao número de episódios.
                Use como sinal complementar — nunca como único critério de decisão.
            </p>
        </div>""", unsafe_allow_html=True)

        # ── Cards: Recomendação + Lucro Teste + Q-Values ──────────────────────────
        cfg_rec = {
            'COMPRAR' : ('#d4fc79','#96e6a1','#14532d','🚀'),
            'VENDER'  : ('#fca5a5','#ef4444','#7f1d1d','📉'),
            'AGUARDAR': ('#fde047','#fbbf24','#78350f','⏸️'),
        }
        bg1, bg2, cor_txt, icone_rec = cfg_rec[rec]
        c1, c2, c3 = st.columns(3)

        with c1:
            st.markdown(f"""
            <div style='background:linear-gradient(135deg,{bg1} 0%,{bg2} 100%);
                        padding:1.2rem;border-radius:10px;text-align:center;height:120px;
                        display:flex;flex-direction:column;justify-content:center;'>
                <div style='font-size:2rem;'>{icone_rec}</div>
                <div style='font-weight:800;font-size:1rem;color:{cor_txt};'>
                    {rec}</div>
                <div style='font-size:0.74rem;color:{cor_txt};margin-top:0.2rem;'>
                    Recomendação do Agente</div>
            </div>""", unsafe_allow_html=True)

        with c2:
            sinal_l = '+' if lucro_teste >= 0 else ''
            cor_l   = '#15803d' if lucro_teste >= 0 else '#b91c1c'
            st.markdown(f"""
            <div style='background:#f8fafc;border:2px solid #e2e8f0;padding:1.2rem;
                        border-radius:10px;text-align:center;height:120px;
                        display:flex;flex-direction:column;justify-content:center;'>
                <div style='font-size:1.6rem;font-weight:800;color:{cor_l};'>
                    {sinal_l}R${lucro_teste:.2f}</div>
                <div style='font-size:0.8rem;color:#64748b;margin-top:0.25rem;'>
                    Lucro Acumulado (Teste)<br>
                    <span style='font-size:0.7rem;'>{len(compras)} compras · {len(vendas)} vendas</span>
                </div>
            </div>""", unsafe_allow_html=True)

        with c3:
            q_hold = q_vals[0] if not (isinstance(q_vals[0], float) and q_vals[0] != q_vals[0]) else 0.0
            q_buy  = q_vals[1] if not (isinstance(q_vals[1], float) and q_vals[1] != q_vals[1]) else 0.0
            q_sell = q_vals[2] if not (isinstance(q_vals[2], float) and q_vals[2] != q_vals[2]) else 0.0
            melhor_acao = ['Hold','Buy','Sell'][int(np.argmax([q_hold, q_buy, q_sell]))]
            def _qfmt(v):
                return f"{v:+.4f}" if abs(v) < 999 else f"{v:+.1f}"
            st.markdown(f"""
            <div style='background:#f8fafc;border:2px solid #e2e8f0;padding:0.9rem 1rem;
                        border-radius:10px;min-height:120px;
                        display:flex;flex-direction:column;justify-content:center;'>
                <div style='font-size:0.7rem;font-weight:700;color:#94a3b8;
                            text-transform:uppercase;letter-spacing:0.05em;margin-bottom:0.5rem;'>
                    Q-Values (estado atual)</div>
                <div style='font-size:0.85rem;color:#334155;margin-bottom:0.18rem;'>
                    ⏸️ Hold &nbsp;<strong style='color:#475569;'>{_qfmt(q_hold)}</strong></div>
                <div style='font-size:0.85rem;color:#15803d;margin-bottom:0.18rem;'>
                    🛒 Buy &nbsp;&nbsp;<strong>{_qfmt(q_buy)}</strong></div>
                <div style='font-size:0.85rem;color:#b91c1c;margin-bottom:0.35rem;'>
                    💰 Sell &nbsp;&nbsp;<strong>{_qfmt(q_sell)}</strong></div>
                <div style='font-size:0.68rem;color:#94a3b8;'>
                    Maior Q &rarr; ação escolhida: <strong>{melhor_acao}</strong></div>
            </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:0.9rem'></div>", unsafe_allow_html=True)

        # ── Gráfico 1: Evolução do lucro por episódio ─────────────────────────────
        st.markdown("**📈 Aprendizado do Agente — Lucro por Episódio (Treino):**")
        fig_ep, ax_ep = plt.subplots(figsize=(9, 2.8))
        fig_ep.patch.set_facecolor('#f8fafc')
        ax_ep.set_facecolor('#f8fafc')
        xs_ep = list(range(1, len(historico) + 1))
        cores_ep = ['#16a34a' if v >= 0 else '#dc2626' for v in historico]
        ax_ep.bar(xs_ep, historico, color=cores_ep, alpha=0.8, edgecolor='white', linewidth=0.5)
        ax_ep.axhline(0, color='#94a3b8', linewidth=1, linestyle='--')
        for xi, yi in zip(xs_ep, historico):
            ax_ep.annotate(f'R${yi:.0f}', xy=(xi, yi),
                           xytext=(0, 5 if yi >= 0 else -14),
                           textcoords='offset points',
                           ha='center', fontsize=7.5, color='#1e293b', fontweight='600')
        ax_ep.set_xticks(xs_ep)
        ax_ep.set_xticklabels([f'Ep {i}' for i in xs_ep], fontsize=8.5)
        ax_ep.set_ylabel('Lucro (R$)', fontsize=8.5, color='#64748b')
        ax_ep.set_title(f'Evolução do Lucro durante o Treinamento — {ticker}',
                        fontsize=9, color='#334155', pad=8, fontweight='bold')
        ax_ep.spines['top'].set_visible(False)
        ax_ep.spines['right'].set_visible(False)
        ax_ep.grid(axis='y', alpha=0.15)
        plt.tight_layout()
        st.pyplot(fig_ep)
        plt.close(fig_ep)

        # ── Gráfico 2: Comportamento no conjunto de teste (Buy/Sell markers) ───────
        st.markdown("**📊 Simulação no Conjunto de Teste — Sinais de Compra/Venda:**")
        fig_ts, ax_ts = plt.subplots(figsize=(11, 4))
        fig_ts.patch.set_facecolor('#f8fafc')
        ax_ts.set_facecolor('#f8fafc')

        xs_test = list(range(len(precos_test)))
        ax_ts.plot(xs_test, precos_test, color='#334155', linewidth=1.8,
                   label='Preço', zorder=2)

        if compras:
            cx = [c[0] for c in compras]
            cy = [c[1] for c in compras]
            ax_ts.scatter(cx, cy, color='#16a34a', marker='^', s=100,
                          zorder=5, label=f'Compra ({len(compras)})', edgecolors='white', linewidth=0.8)

        if vendas:
            vx = [v[0] for v in vendas]
            vy = [v[1] for v in vendas]
            vc = ['#16a34a' if v[2] >= 0 else '#dc2626' for v in vendas]
            ax_ts.scatter(vx, vy, color=vc, marker='v', s=100,
                          zorder=5, label=f'Venda ({len(vendas)})', edgecolors='white', linewidth=0.8)

        sinal_lt = '+' if lucro_teste >= 0 else ''
        ax_ts.set_title(
            f'Agente RL no Teste — {ticker} | Lucro Total: {sinal_lt}R${lucro_teste:.2f}',
            fontsize=9.5, color='#334155', pad=10, fontweight='bold')
        ax_ts.set_ylabel('Preço (R$)', fontsize=9, color='#64748b')
        ax_ts.set_xlabel(f'Dias (conjunto de teste — últimos {len(precos_test)} pregões)',
                         fontsize=8.5, color='#64748b', labelpad=6)
        ax_ts.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'R${v:.2f}'))
        ax_ts.legend(fontsize=8, framealpha=0.9, loc='upper left')
        ax_ts.spines['top'].set_visible(False)
        ax_ts.spines['right'].set_visible(False)
        ax_ts.grid(alpha=0.12)
        plt.tight_layout()
        st.pyplot(fig_ts)
        plt.close(fig_ts)

        # ── Tabela de operações ────────────────────────────────────────────────────
        if vendas:
            st.markdown("**📋 Operações de Venda no Conjunto de Teste:**")
            ops_data = []
            for i, v in enumerate(vendas):
                pnl = v[2]
                ops_data.append({
                    'Op': f'#{i+1}',
                    'Dia': f'D+{v[0]}',
                    'Preço Venda': f'R${v[1]:.2f}',
                    'PnL': f'{"+" if pnl >= 0 else ""}R${pnl:.2f}',
                    'Resultado': '✅ Lucro' if pnl >= 0 else '❌ Prejuízo',
                })
            import pandas as pd
            df_ops = pd.DataFrame(ops_data)
            st.dataframe(df_ops, use_container_width=True, hide_index=True)

        # ── Legenda técnica ────────────────────────────────────────────────────────
        st.markdown("""
        <div style='margin-top:0.6rem;padding:0.8rem 1rem;background:#f1f5f9;
                    border-radius:8px;font-size:0.76rem;color:#64748b;line-height:1.7;'>
            🧠 <strong>Arquitetura DQN:</strong> MLP 3 camadas (64→32→3 neurônios) · Ativação ReLU · Xavier init
            &nbsp;|&nbsp; 🔄 <strong>Experience Replay:</strong> buffer=500, batch=32
            &nbsp;|&nbsp; 📉 <strong>ε-greedy:</strong> ε=1.0 → 0.05 (decay=0.99)
            &nbsp;|&nbsp; 🎯 <strong>γ (desconto):</strong> 0.95
            &nbsp;|&nbsp; 📊 <strong>Recompensa:</strong> PnL realizado na venda
        </div>""", unsafe_allow_html=True)


# =============================================================================
# ANÁLISE DE FASE — METODOLOGIA MINERVINI / STAN WEINSTEIN
# Baseado em: github.com/camera3tuca/Ryan (Intelligent Stock Screener)
# Referências: "Trade Like a Stock Market Wizard" — Mark Minervini
#              "Secrets for Profiting in Bull and Bear Markets" — Stan Weinstein
#
# O sistema Ryan implementa o Trend Template de Minervini (8 critérios),
# classificação de fase em 4 estágios de Weinstein, Relative Strength vs
# benchmark, e stop loss ATR-based com validação R:R ≥ 2:1.
# Aqui adaptamos esses conceitos para BDRs (preços em BRL, benchmark = IBOV).
# =============================================================================

@st.cache_data(ttl=3600)
def _buscar_ibov():
    """Baixa o IBOV para usar como benchmark de Relative Strength."""
    try:
        df = yf.download('^BVSP', period='1y', interval='1d',
                         auto_adjust=True, progress=False, timeout=30)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df['Close'].dropna()
    except Exception:
        return None


def calcular_minervini(df_ticker, ticker):
    """
    Calcula a análise completa de fase + Trend Template de Minervini para um BDR.

    Retorna dict com:
      fase            : int 1-4
      fase_nome       : str
      fase_cor        : str (hex)
      fase_desc       : str
      criterios       : list[dict]  — os 8 critérios do Trend Template
      criterios_ok    : int (0-8)
      score_forca     : float 0-100
      rs_score        : float 0-10
      rs_slope        : float
      stop_loss       : float
      stop_tipo       : str
      risco_pct       : float
      alvo_2r         : float
      alvo_3r         : float
      rr_ratio        : float
      sma50           : float
      sma150          : float
      sma200          : float
      max_52s         : float
      min_52s         : float
      regime_ibov     : str
      erro            : str | None
    """
    try:
        df = df_ticker.copy().sort_index()
        if 'Close' not in df.columns or len(df) < 60:
            return {'erro': 'Dados insuficientes (mín. 60 dias).'}

        close  = df['Close'].dropna()
        high   = df['High'].dropna()  if 'High'  in df.columns else close
        low    = df['Low'].dropna()   if 'Low'   in df.columns else close
        n      = len(close)

        # ── Médias móveis simples (SMA) ──────────────────────────────────────────
        def sma(s, w):
            return float(s.rolling(w).mean().iloc[-1]) if len(s) >= w else float('nan')

        sma50  = sma(close, 50)
        sma150 = sma(close, 150) if n >= 150 else sma(close, min(n, 100))
        sma200 = sma(close, 200) if n >= 200 else sma(close, min(n, 150))
        preco  = float(close.iloc[-1])

        # Slope da SMA200 (último mês vs 20 pregões atrás)
        if n >= 220:
            sma200_serie = close.rolling(200).mean().dropna()
            slope200 = (float(sma200_serie.iloc[-1]) - float(sma200_serie.iloc[-20])) / float(sma200_serie.iloc[-20]) * 100
        else:
            slope200 = 0.0

        # ── 52 semanas ────────────────────────────────────────────────────────────
        janela_52s = min(n, 252)
        max_52s = float(high.iloc[-janela_52s:].max())
        min_52s = float(low.iloc[-janela_52s:].min())

        # ── Classificação de Fase (Weinstein / Ryan) ──────────────────────────────
        # Fase 2 = sma50 > sma150 > sma200, tudo subindo, preço acima das três
        # Fase 1 = consolidação (sma50 ≈ sma200, preço oscilando)
        # Fase 3 = distribuição (sma50 começa cair, preço abaixo de sma50)
        # Fase 4 = queda (sma50 < sma200, ambas caindo)
        import math
        def _ok(v): return not (math.isnan(v) if isinstance(v, float) else False)

        if _ok(sma50) and _ok(sma150) and _ok(sma200):
            if sma50 > sma150 > sma200 and preco > sma50 and slope200 > 0:
                fase = 2
            elif sma50 < sma200 and slope200 < -0.5:
                fase = 4
            elif preco < sma50 and sma50 < sma150:
                fase = 3
            else:
                fase = 1
        else:
            fase = 1

        fases_cfg = {
            1: ('Base / Acumulação',  '#f59e0b', '⏸️ Ativo em consolidação. Ainda não confirma uptrend. Aguardar rompimento.'),
            2: ('Uptrend Confirmado', '#16a34a', '🚀 Fase de compra! Tendência confirmada com SMAs em cascata e preço acima de todas.'),
            3: ('Distribuição',       '#ea580c', '⚠️ Sinais de topo. Momentum fraquejando. Considere reduzir posição.'),
            4: ('Downtrend',          '#dc2626', '🔴 Tendência de baixa confirmada. Evitar compras. Aguardar nova base.'),
        }
        fase_nome, fase_cor, fase_desc = fases_cfg[fase]

        # ── Trend Template de Minervini (8 critérios) ────────────────────────────
        def c(ok, nome, detalhe):
            return {'ok': ok, 'nome': nome, 'detalhe': detalhe}

        criterios = [
            c(preco > sma150 and preco > sma200,
              'Preço > SMA150 e SMA200',
              f'Preço R${preco:.2f} vs SMA150 R${sma150:.2f} / SMA200 R${sma200:.2f}'),
            c(sma150 > sma200,
              'SMA150 > SMA200',
              f'SMA150 R${sma150:.2f} vs SMA200 R${sma200:.2f}'),
            c(slope200 > 0,
              'SMA200 em tendência de alta ≥ 1 mês',
              f'Slope SMA200: {slope200:+.2f}% no último mês'),
            c(_ok(sma50) and _ok(sma150) and _ok(sma200) and sma50 > sma150 > sma200,
              'SMA50 > SMA150 > SMA200 (cascata)',
              f'SMA50 R${sma50:.2f} > SMA150 R${sma150:.2f} > SMA200 R${sma200:.2f}'),
            c(preco > sma50,
              'Preço > SMA50',
              f'Preço R${preco:.2f} vs SMA50 R${sma50:.2f}'),
            c((preco - min_52s) / (min_52s + 1e-9) >= 0.25,
              'Preço ≥ 25% acima da mínima de 52 semanas',
              f'Mínima 52s: R${min_52s:.2f} | +{(preco/min_52s-1)*100:.1f}% acima'),
            c((max_52s - preco) / (max_52s + 1e-9) <= 0.25,
              'Preço dentro de 25% da máxima de 52 semanas',
              f'Máxima 52s: R${max_52s:.2f} | {(max_52s/preco-1)*100:.1f}% abaixo da máxima'),
            c(True, 'Relative Strength ≥ 7/10',
              'Calculado abaixo contra IBOV'),   # placeholder — atualizado após RS calc
        ]

        # ── Relative Strength vs IBOV ─────────────────────────────────────────────
        ibov = _buscar_ibov()
        rs_slope = 0.0
        rs_score = 5.0   # neutro como default
        if ibov is not None and len(ibov) >= 63:
            try:
                ibov_alinhado = ibov.reindex(close.index, method='ffill').dropna()
                comum = close.index.intersection(ibov_alinhado.index)
                if len(comum) >= 63:
                    s = close.loc[comum].iloc[-63:]
                    b = ibov_alinhado.loc[comum].iloc[-63:]
                    rs_ratio = (s / s.iloc[0]) / (b / b.iloc[0])
                    # slope linear normalizado
                    xs = np.arange(len(rs_ratio))
                    rs_slope = float(np.polyfit(xs, rs_ratio.values, 1)[0] * 63)
                    rs_score = float(np.clip((rs_slope + 0.3) / 0.6 * 10, 0, 10))
            except Exception:
                pass

        # Atualiza critério 8 com RS real
        rs_ok = rs_score >= 7.0
        criterios[7] = c(rs_ok,
                         'Relative Strength ≥ 7/10 vs IBOV',
                         f'RS Score: {rs_score:.1f}/10 | Slope 63d: {rs_slope:+.3f}')

        criterios_ok = sum(1 for cr in criterios if cr['ok'])

        # ── Score de Força (0-100, linear como o Ryan) ────────────────────────────
        # Fase (40 pts) + Trend Template (35 pts) + RS (15 pts) + posição 52s (10 pts)
        score_fase   = {1: 15, 2: 40, 3: 5, 4: 0}[fase]
        score_tt     = round(criterios_ok / 8 * 35, 1)
        score_rs     = round(rs_score / 10 * 15, 1)
        pct_52s      = (preco - min_52s) / max(max_52s - min_52s, 1e-9)   # 0=min, 1=max
        score_52s    = round(min(pct_52s, 1.0) * 10, 1)
        score_forca  = round(score_fase + score_tt + score_rs + score_52s, 1)

        # ── Stop Loss (ATR14 ou mínimo 20 dias, igual ao Ryan) ────────────────────
        if len(high) >= 14 and len(low) >= 14:
            tr = pd.concat([
                high - low,
                (high - close.shift(1)).abs(),
                (low  - close.shift(1)).abs()
            ], axis=1).max(axis=1)
            atr14 = float(tr.rolling(14).mean().iloc[-1])
            stop_atr   = preco - atr14 * 2.0
            stop_swing = float(low.iloc[-20:].min()) * 0.98
            stop_loss  = max(stop_atr, stop_swing)
            stop_tipo  = 'ATR×2' if stop_atr >= stop_swing else 'Mínimo 20d'
        else:
            stop_loss = preco * 0.92
            stop_tipo = 'Estimado (8%)'

        risco_pct = (preco - stop_loss) / preco * 100
        alvo_2r   = preco + (preco - stop_loss) * 2
        alvo_3r   = preco + (preco - stop_loss) * 3
        rr_ratio  = ((alvo_2r - preco) / max(preco - stop_loss, 1e-9))

        # ── Regime do IBOV ────────────────────────────────────────────────────────
        regime_ibov = 'Indisponível'
        if ibov is not None and len(ibov) >= 200:
            ib  = ibov.iloc[-1]
            ib50  = float(ibov.rolling(50).mean().iloc[-1])
            ib200 = float(ibov.rolling(200).mean().iloc[-1])
            if ib > ib50 > ib200:
                regime_ibov = '🟢 Fase 2 — Mercado em alta'
            elif ib < ib50 < ib200:
                regime_ibov = '🔴 Fase 4 — Mercado em queda'
            elif ib < ib50:
                regime_ibov = '🟠 Fase 3 — Distribuição'
            else:
                regime_ibov = '🟡 Fase 1 — Base / Consolidação'

        return {
            'erro'         : None,
            'fase'         : fase,
            'fase_nome'    : fase_nome,
            'fase_cor'     : fase_cor,
            'fase_desc'    : fase_desc,
            'criterios'    : criterios,
            'criterios_ok' : criterios_ok,
            'score_forca'  : score_forca,
            'rs_score'     : rs_score,
            'rs_slope'     : rs_slope,
            'stop_loss'    : round(stop_loss, 2),
            'stop_tipo'    : stop_tipo,
            'risco_pct'    : round(risco_pct, 2),
            'alvo_2r'      : round(alvo_2r, 2),
            'alvo_3r'      : round(alvo_3r, 2),
            'rr_ratio'     : round(rr_ratio, 2),
            'sma50'        : round(sma50, 2)  if _ok(sma50)  else None,
            'sma150'       : round(sma150, 2) if _ok(sma150) else None,
            'sma200'       : round(sma200, 2) if _ok(sma200) else None,
            'max_52s'      : round(max_52s, 2),
            'min_52s'      : round(min_52s, 2),
            'preco'        : round(preco, 2),
            'regime_ibov'  : regime_ibov,
        }

    except Exception as e:
        return {'erro': f'Erro na análise Minervini: {str(e)}'}


def renderizar_painel_minervini(resultado, ticker, empresa):
    """Renderiza a seção de Análise de Fase Minervini dentro de um st.expander."""
    with st.expander("📊 Análise de Fase — Metodologia Minervini / Weinstein", expanded=False):

        if resultado.get('erro'):
            st.warning(f"⚠️ {resultado['erro']}")
            return

        fase         = resultado['fase']
        fase_nome    = resultado['fase_nome']
        fase_cor     = resultado['fase_cor']
        fase_desc    = resultado['fase_desc']
        criterios    = resultado['criterios']
        crit_ok      = resultado['criterios_ok']
        score        = resultado['score_forca']
        rs_score     = resultado['rs_score']
        rs_slope     = resultado['rs_slope']
        stop_loss    = resultado['stop_loss']
        stop_tipo    = resultado['stop_tipo']
        risco_pct    = resultado['risco_pct']
        alvo_2r      = resultado['alvo_2r']
        alvo_3r      = resultado['alvo_3r']
        rr_ratio     = resultado['rr_ratio']
        sma50        = resultado['sma50']
        sma150       = resultado['sma150']
        sma200       = resultado['sma200']
        max_52s      = resultado['max_52s']
        min_52s      = resultado['min_52s']
        preco        = resultado['preco']
        regime_ibov  = resultado['regime_ibov']

        # ── Cabeçalho explicativo ─────────────────────────────────────────────────
        st.markdown(f"""
        <div style='background:linear-gradient(135deg,#0f172a 0%,#1e3a5f 100%);
                    padding:1rem 1.4rem;border-radius:12px;margin-bottom:1rem;'>
            <div style='display:flex;align-items:center;gap:0.8rem;margin-bottom:0.5rem;'>
                <span style='font-size:1.8rem;'>📊</span>
                <div>
                    <div style='color:#93c5fd;font-weight:800;font-size:1rem;'>
                        Trend Template — Mark Minervini (SEPA)</div>
                    <div style='color:#bfdbfe;font-size:0.78rem;'>
                        Fase Weinstein · 8 Critérios · RS vs IBOV · Stop ATR · R:R</div>
                </div>
            </div>
            <p style='margin:0;color:#bfdbfe;font-size:0.79rem;line-height:1.6;'>
                🧠 <strong style='color:#93c5fd;'>Metodologia:</strong>
                Baseada no sistema <em>Ryan</em> (github.com/camera3tuca/Ryan), que implementa o
                <strong>Trend Template de Mark Minervini</strong> — compra apenas ações em
                <strong>Fase 2 confirmada</strong> (SMA50 &gt; SMA150 &gt; SMA200 em cascata ascendente).
                Adaptado para BDRs com benchmark no <strong>IBOV</strong> em vez de SPY.
                Stop Loss calculado por <strong>ATR×2 ou mínimo de 20 pregões</strong>.
                Sinal de compra exige <strong>≥ 7 de 8 critérios</strong> e R:R ≥ 2:1.
            </p>
        </div>""", unsafe_allow_html=True)

        # ── Linha 1: Fase + Score + Regime IBOV ──────────────────────────────────
        c1, c2, c3 = st.columns(3)

        # Card de Fase
        fases_bg = {1:'#fef3c7', 2:'#f0fdf4', 3:'#fff7ed', 4:'#fef2f2'}
        with c1:
            st.markdown(f"""
            <div style='background:{fases_bg[fase]};border:2px solid {fase_cor};
                        border-left:6px solid {fase_cor};
                        padding:0.9rem 1rem;border-radius:10px;min-height:130px;
                        display:flex;flex-direction:column;justify-content:center;'>
                <div style='font-size:0.68rem;font-weight:700;color:#94a3b8;
                            text-transform:uppercase;letter-spacing:.06em;margin-bottom:0.3rem;'>
                    Fase de Weinstein</div>
                <div style='font-size:1.6rem;font-weight:900;color:{fase_cor};line-height:1.1;'>
                    Fase {fase}</div>
                <div style='font-size:0.85rem;font-weight:700;color:{fase_cor};
                            margin:0.2rem 0 0.4rem;'>{fase_nome}</div>
                <div style='font-size:0.7rem;color:#475569;line-height:1.4;'>{fase_desc}</div>
            </div>""", unsafe_allow_html=True)

        # Card de Score de Força
        cor_s = '#15803d' if score >= 70 else '#b45309' if score >= 45 else '#b91c1c'
        bg_s  = '#f0fdf4' if score >= 70 else '#fffbeb' if score >= 45 else '#fef2f2'
        label_s = ('COMPRA FORTE' if score >= 80 else 'BOM' if score >= 65
                   else 'NEUTRO' if score >= 45 else 'FRACO' if score >= 25 else 'EVITAR')
        barra_s = min(int(score), 100)
        with c2:
            st.markdown(f"""
            <div style='background:{bg_s};border:2px solid #e2e8f0;padding:0.9rem 1rem;
                        border-radius:10px;min-height:130px;
                        display:flex;flex-direction:column;justify-content:center;'>
                <div style='font-size:0.68rem;font-weight:700;color:#94a3b8;
                            text-transform:uppercase;letter-spacing:.06em;margin-bottom:0.3rem;'>
                    Score de Força</div>
                <div style='font-size:2rem;font-weight:900;color:{cor_s};line-height:1;'>
                    {score:.0f}/100</div>
                <div style='background:#e2e8f0;border-radius:99px;height:6px;margin:0.4rem 0;'>
                    <div style='background:{cor_s};width:{barra_s}%;height:6px;border-radius:99px;'></div></div>
                <div style='font-size:0.8rem;font-weight:700;color:{cor_s};'>{label_s}</div>
                <div style='font-size:0.65rem;color:#94a3b8;margin-top:0.2rem;'>
                    Fase(40)+TT(35)+RS(15)+52s(10)</div>
            </div>""", unsafe_allow_html=True)

        # Card Regime IBOV
        with c3:
            st.markdown(f"""
            <div style='background:#f8fafc;border:2px solid #e2e8f0;padding:0.9rem 1rem;
                        border-radius:10px;min-height:130px;
                        display:flex;flex-direction:column;justify-content:center;gap:0.5rem;'>
                <div style='font-size:0.68rem;font-weight:700;color:#94a3b8;
                            text-transform:uppercase;letter-spacing:.06em;'>
                    Regime de Mercado</div>
                <div style='font-size:0.9rem;font-weight:700;color:#1e293b;
                            line-height:1.4;'>{regime_ibov}</div>
                <div style='font-size:0.68rem;color:#94a3b8;'>
                    IBOV — benchmark local para RS</div>
                <div style='border-top:1px solid #e2e8f0;padding-top:0.4rem;
                            font-size:0.75rem;color:#334155;'>
                    RS vs IBOV (63d):
                    <strong style='color:{"#16a34a" if rs_score >= 7 else "#dc2626"}'>
                        {rs_score:.1f}/10</strong>
                    &nbsp;<span style='color:{"#16a34a" if rs_slope >= 0 else "#dc2626"}'>
                        ({rs_slope:+.3f} slope)</span>
                </div>
            </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:0.9rem'></div>", unsafe_allow_html=True)

        # ── Trend Template: checklist dos 8 critérios ─────────────────────────────
        st.markdown(f"**📋 Trend Template de Minervini — {crit_ok}/8 critérios atendidos:**")

        # Barra de progresso do template
        tt_pct = int(crit_ok / 8 * 100)
        tt_cor = '#16a34a' if crit_ok >= 7 else '#d97706' if crit_ok >= 5 else '#dc2626'
        tt_label = '✅ SINAL DE COMPRA' if crit_ok >= 7 else '⚠️ PARCIAL' if crit_ok >= 5 else '❌ NÃO ATENDE'
        st.markdown(f"""
        <div style='background:#f1f5f9;border-radius:8px;padding:0.6rem 0.8rem;
                    margin-bottom:0.7rem;display:flex;align-items:center;gap:0.8rem;'>
            <div style='flex:1;background:#e2e8f0;border-radius:99px;height:8px;'>
                <div style='background:{tt_cor};width:{tt_pct}%;height:8px;border-radius:99px;'></div>
            </div>
            <div style='font-size:0.82rem;font-weight:700;color:{tt_cor};white-space:nowrap;'>
                {crit_ok}/8 &nbsp;— {tt_label}</div>
        </div>""", unsafe_allow_html=True)

        # Grid de critérios em 2 colunas
        col_a, col_b = st.columns(2)
        for i, cr in enumerate(criterios):
            icone = '✅' if cr['ok'] else '❌'
            bg_cr = '#f0fdf4' if cr['ok'] else '#fef2f2'
            borda_cr = '#86efac' if cr['ok'] else '#fca5a5'
            cor_cr   = '#15803d' if cr['ok'] else '#b91c1c'
            html_cr = f"""
            <div style='background:{bg_cr};border:1px solid {borda_cr};border-radius:8px;
                        padding:0.55rem 0.7rem;margin-bottom:0.4rem;'>
                <div style='font-size:0.75rem;font-weight:700;color:{cor_cr};'>
                    {icone} {cr['nome']}</div>
                <div style='font-size:0.66rem;color:#64748b;margin-top:0.1rem;'>
                    {cr['detalhe']}</div>
            </div>"""
            if i % 2 == 0:
                col_a.markdown(html_cr, unsafe_allow_html=True)
            else:
                col_b.markdown(html_cr, unsafe_allow_html=True)

        st.markdown("<div style='height:0.6rem'></div>", unsafe_allow_html=True)

        # ── Linha 2: SMAs + 52s ───────────────────────────────────────────────────
        st.markdown("**📐 Médias Móveis e Níveis de Referência:**")
        mc1, mc2, mc3, mc4, mc5 = st.columns(5)
        def _sma_card(col, label, val, preco_atual):
            if val is None:
                col.markdown(f"<div style='text-align:center;font-size:0.7rem;color:#94a3b8;'>{label}<br>—</div>",
                             unsafe_allow_html=True)
                return
            # Preço ACIMA da SMA = bom (verde); abaixo = ruim (vermelho)
            acima = preco_atual > val
            cor   = '#15803d' if acima else '#b91c1c'
            label_pos = '▲ Preço acima' if acima else '▼ Preço abaixo'
            col.markdown(f"""
            <div style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;
                        padding:0.5rem;text-align:center;'>
                <div style='font-size:0.65rem;color:#94a3b8;font-weight:700;
                            text-transform:uppercase;'>{label}</div>
                <div style='font-size:1rem;font-weight:800;color:{cor};'>R${val:.2f}</div>
                <div style='font-size:0.6rem;color:{cor};font-weight:600;'>
                    {label_pos}</div>
            </div>""", unsafe_allow_html=True)

        _sma_card(mc1, 'SMA 50',  sma50,  preco)
        _sma_card(mc2, 'SMA 150', sma150, preco)
        _sma_card(mc3, 'SMA 200', sma200, preco)
        mc4.markdown(f"""
        <div style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;
                    padding:0.5rem;text-align:center;'>
            <div style='font-size:0.65rem;color:#94a3b8;font-weight:700;
                        text-transform:uppercase;'>Máx 52s</div>
            <div style='font-size:1rem;font-weight:800;color:#334155;'>R${max_52s:.2f}</div>
            <div style='font-size:0.6rem;color:#64748b;'>
                {(max_52s/preco-1)*100:.1f}% acima</div>
        </div>""", unsafe_allow_html=True)
        mc5.markdown(f"""
        <div style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;
                    padding:0.5rem;text-align:center;'>
            <div style='font-size:0.65rem;color:#94a3b8;font-weight:700;
                        text-transform:uppercase;'>Mín 52s</div>
            <div style='font-size:1rem;font-weight:800;color:#334155;'>R${min_52s:.2f}</div>
            <div style='font-size:0.6rem;color:#64748b;'>
                {(preco/min_52s-1)*100:.1f}% acima</div>
        </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:0.6rem'></div>", unsafe_allow_html=True)

        # ── Linha 3: Stop Loss + Alvos + R:R ─────────────────────────────────────
        st.markdown("**🎯 Gestão de Risco (Ryan-style ATR):**")
        rc1, rc2, rc3, rc4 = st.columns(4)

        rc1.markdown(f"""
        <div style='background:#fef2f2;border:1px solid #fca5a5;border-radius:8px;
                    padding:0.6rem;text-align:center;'>
            <div style='font-size:0.65rem;font-weight:700;color:#b91c1c;
                        text-transform:uppercase;'>Stop Loss</div>
            <div style='font-size:1.1rem;font-weight:900;color:#b91c1c;'>
                R${stop_loss:.2f}</div>
            <div style='font-size:0.62rem;color:#94a3b8;'>{stop_tipo}</div>
            <div style='font-size:0.68rem;color:#b91c1c;font-weight:600;
                        margin-top:0.2rem;'>Risco: {risco_pct:.1f}%</div>
        </div>""", unsafe_allow_html=True)

        rc2.markdown(f"""
        <div style='background:#f0fdf4;border:1px solid #86efac;border-radius:8px;
                    padding:0.6rem;text-align:center;'>
            <div style='font-size:0.65rem;font-weight:700;color:#15803d;
                        text-transform:uppercase;'>Alvo R:R 2:1</div>
            <div style='font-size:1.1rem;font-weight:900;color:#15803d;'>
                R${alvo_2r:.2f}</div>
            <div style='font-size:0.62rem;color:#94a3b8;'>+{(alvo_2r/preco-1)*100:.1f}% do preço atual</div>
        </div>""", unsafe_allow_html=True)

        rc3.markdown(f"""
        <div style='background:#f0fdf4;border:1px solid #86efac;border-radius:8px;
                    padding:0.6rem;text-align:center;'>
            <div style='font-size:0.65rem;font-weight:700;color:#15803d;
                        text-transform:uppercase;'>Alvo R:R 3:1</div>
            <div style='font-size:1.1rem;font-weight:900;color:#15803d;'>
                R${alvo_3r:.2f}</div>
            <div style='font-size:0.62rem;color:#94a3b8;'>+{(alvo_3r/preco-1)*100:.1f}% do preço atual</div>
        </div>""", unsafe_allow_html=True)

        rr_ok  = rr_ratio >= 2.0
        rr_cor = '#15803d' if rr_ok else '#b91c1c'
        rr_bg  = '#f0fdf4' if rr_ok else '#fef2f2'
        rr_brd = '#86efac' if rr_ok else '#fca5a5'
        rc4.markdown(f"""
        <div style='background:{rr_bg};border:1px solid {rr_brd};border-radius:8px;
                    padding:0.6rem;text-align:center;'>
            <div style='font-size:0.65rem;font-weight:700;color:{rr_cor};
                        text-transform:uppercase;'>R:R Atual</div>
            <div style='font-size:1.3rem;font-weight:900;color:{rr_cor};'>
                {rr_ratio:.1f}:1</div>
            <div style='font-size:0.68rem;font-weight:600;color:{rr_cor};'>
                {"✅ Favorável (≥2:1)" if rr_ok else "❌ Desfavorável (<2:1)"}</div>
        </div>""", unsafe_allow_html=True)

        # ── Rodapé com metodologia ────────────────────────────────────────────────
        st.markdown(f"""
        <div style='margin-top:0.8rem;padding:0.7rem 1rem;background:#f1f5f9;
                    border-radius:8px;font-size:0.74rem;color:#64748b;line-height:1.7;'>
            📚 <strong>Referências:</strong>
            Mark Minervini — <em>Trade Like a Stock Market Wizard</em> (2013) &nbsp;|&nbsp;
            Stan Weinstein — <em>Secrets for Profiting in Bull and Bear Markets</em> &nbsp;|&nbsp;
            Implementação: <em>github.com/camera3tuca/Ryan</em>
            <br>
            ⚠️ <strong>Atenção:</strong>
            Score e fase são indicadores técnicos. Não constituem recomendação de investimento.
            BDRs com menos de 150/200 pregões usam SMAs com janelas reduzidas.
        </div>""", unsafe_allow_html=True)


# =============================================================================
# ESTRATÉGIA TRIPLE SCREEN DE ALEXANDER ELDER
# Referência: https://hw-br.online/education/triple-screen-strategy-3-steps-to-make-profit/
# =============================================================================

def analisar_triple_screen(df_ticker):
    """
    Avalia a Estratégia Triple Screen de Alexander Elder nos dados diários da BDR.
    
    Como o nosso monitor trabalha apenas com timeframe diário (dados de 1 ano),
    adaptamos as três telas conforme a metodologia original de Elder:
      • 1ª Tela (Maré)     → EMA13 diária: inclinação define a tendência dominante
                              + MACD(12,26,9) histograma confirma direção
      • 2ª Tela (Onda)     → EFI(2): oscilador que detecta sobrevenda/sobrecompra
                              na correção dentro da tendência maior
      • 3ª Tela (Execução) → Buy/Sell Stop na máxima/mínima recente (sem indicador)
    
    Retorna dict com:
      tela1, tela2, tela3 — dicts com status, valor e descrição
      veredicto — "COMPRA", "VENDA" ou "AGUARDAR"
      forca     — int 0-3 (quantas telas confirmam)
    """
    try:
        close  = df_ticker['Close'].dropna()
        volume = df_ticker['Volume'].dropna()

        if len(close) < 30:
            return None

        # ── TELA 1: EMA13 + MACD(12,26,9) — identifica a MARÉ ───────────────────────
        # Elder original: slope da EMA13 semanal define a tendência dominante.
        # Adaptação para dados diários: EMA13 diária (≈ tendência de 2-3 semanas).
        # MACD(12,26,9) histograma reforça a direção.
        ema13 = close.ewm(span=13, adjust=False).mean()

        # Inclinação da EMA13 (últimas 3 barras para suavizar ruído)
        ema13_slope = ema13.iloc[-1] - ema13.iloc[-3]  # variação em 3 dias

        # MACD(12,26,9) histograma
        ema12       = close.ewm(span=12, adjust=False).mean()
        ema26       = close.ewm(span=26, adjust=False).mean()
        macd_line   = ema12 - ema26
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist   = macd_line - macd_signal
        macd_val    = macd_hist.iloc[-1]
        macd_slope  = macd_hist.iloc[-1] - macd_hist.iloc[-2]

        ema13_val   = ema13.iloc[-1]
        preco_ult   = close.iloc[-1]

        # Tendência confirmada por EMA13 (preço acima/abaixo) + inclinação + MACD
        alta_confirmada  = (ema13_slope > 0) and (macd_val > 0 or macd_slope > 0)
        baixa_confirmada = (ema13_slope < 0) and (macd_val < 0 or macd_slope < 0)

        pct_dist = ((preco_ult - ema13_val) / ema13_val) * 100  # distância % do preço à EMA13

        if alta_confirmada:
            tela1_status = "ALTA"
            tela1_emoji  = "🟢"
            tela1_desc   = (
                f"EMA13 com inclinação ascendente (+{ema13_slope:+.2f}) e "
                f"MACD(12,26,9) {'positivo' if macd_val > 0 else 'virando para cima'}. "
                f"Preço está {abs(pct_dist):.1f}% {'acima' if pct_dist >= 0 else 'abaixo'} da EMA13. "
                "A MARÉ está de alta — opere apenas compras, aguardando correções (ondas)."
            )
        elif baixa_confirmada:
            tela1_status = "BAIXA"
            tela1_emoji  = "🔴"
            tela1_desc   = (
                f"EMA13 com inclinação descendente ({ema13_slope:+.2f}) e "
                f"MACD(12,26,9) {'negativo' if macd_val < 0 else 'virando para baixo'}. "
                f"Preço está {abs(pct_dist):.1f}% {'abaixo' if pct_dist <= 0 else 'acima'} da EMA13. "
                "A MARÉ está de baixa — opere apenas vendas, aguardando repiques (ondas)."
            )
        else:
            tela1_status = "NEUTRO"
            tela1_emoji  = "🟡"
            tela1_desc   = (
                f"EMA13 sem direção clara (slope: {ema13_slope:+.2f}) ou "
                f"MACD(12,26,9) conflitante (histograma: {macd_val:+.4f}). "
                "Sinais divergentes — aguarde a maré se definir antes de agir."
            )

        # ── TELA 2: EFI(2) — identifica a ONDA (correção dentro da tendência) ────────
        # Elder recomenda o Force Index(2) como oscilador para a 2ª tela.
        # EFI2 = EMA(2) de [(Fechamento atual − Fechamento anterior) × Volume]
        idx_comum = close.index.intersection(volume.index)
        close_c   = close.loc[idx_comum]
        volume_c  = volume.loc[idx_comum]
        efi_bruto = close_c.diff() * volume_c
        efi2      = efi_bruto.ewm(span=2, adjust=False).mean()
        efi2_val  = efi2.iloc[-1]

        # Limiares dinâmicos baseados no desvio padrão histórico do EFI2
        efi2_std  = efi2.std()
        limiar_pos = efi2_std * 0.5   # sobrecompra
        limiar_neg = -efi2_std * 0.5  # sobrevenda

        if efi2_val < limiar_neg:
            tela2_status = "SOBREVENDA"
            tela2_emoji  = "🟢"
            tela2_desc   = (
                f"EFI(2) = {efi2_val:,.0f} (abaixo do limiar {limiar_neg:,.0f}). "
                "A ONDA está em sobrevenda — compradores começando a absorver a pressão vendedora. "
                "Em tendência de alta (1ª Tela), este é o momento de buscar entrada."
            )
        elif efi2_val > limiar_pos:
            tela2_status = "SOBRECOMPRA"
            tela2_emoji  = "🔴"
            tela2_desc   = (
                f"EFI(2) = {efi2_val:,.0f} (acima do limiar {limiar_pos:,.0f}). "
                "A ONDA está em sobrecompra — vendedores começando a pressionar. "
                "Em tendência de baixa (1ª Tela), este é o momento de buscar saída/venda."
            )
        else:
            tela2_status = "NEUTRO"
            tela2_emoji  = "🟡"
            tela2_desc   = (
                f"EFI(2) = {efi2_val:,.0f} (zona neutra: {limiar_neg:,.0f} a {limiar_pos:,.0f}). "
                "Onda em território neutro — aguarde o EFI recuar para sobrevenda (para compra em uptrend) "
                "ou avançar para sobrecompra (para venda em downtrend)."
            )

        # ── TELA 3: EXECUÇÃO — Buy/Sell Stop (sem indicador, ação do preço) ──────────
        preco_atual = close.iloc[-1]
        maxima_rec  = df_ticker['High'].iloc[-5:].max()
        minima_rec  = df_ticker['Low'].iloc[-5:].min()

        if tela1_status == "ALTA" and tela2_status == "SOBREVENDA":
            tela3_status = "COMPRA"
            tela3_emoji  = "🚀"
            stop_loss   = round(minima_rec, 2)
            entrada_ref = round(maxima_rec, 2)
            tela3_desc  = (
                f"✅ Setup de COMPRA confirmado!\n"
                f"• Entrada (Buy Stop): acima de R$ {entrada_ref:.2f} "
                f"(máxima dos últimos 5 dias)\n"
                f"• Stop-Loss: R$ {stop_loss:.2f} "
                f"(mínima dos últimos 5 dias)\n"
                f"• Risco por cota: R$ {(entrada_ref - stop_loss):.2f}\n"
                f"• Lógica Elder: EMA13 de alta + EFI em sobrevenda = "
                "correção esgotada dentro de uptrend. Buy Stop garante que só entramos "
                "se o mercado confirmar a retomada."
            )
        elif tela1_status == "BAIXA" and tela2_status == "SOBRECOMPRA":
            tela3_status = "VENDA"
            tela3_emoji  = "📉"
            stop_loss   = round(maxima_rec, 2)
            entrada_ref = round(minima_rec, 2)
            tela3_desc  = (
                f"⚠️ Setup de VENDA confirmado!\n"
                f"• Entrada (Sell Stop): abaixo de R$ {entrada_ref:.2f} "
                f"(mínima dos últimos 5 dias)\n"
                f"• Stop-Loss: R$ {stop_loss:.2f} "
                f"(máxima dos últimos 5 dias)\n"
                f"• Risco por cota: R$ {(stop_loss - entrada_ref):.2f}\n"
                f"• Lógica Elder: EMA13 de baixa + EFI em sobrecompra = "
                "repique esgotado dentro de downtrend. Sell Stop garante que só entramos "
                "se o mercado confirmar a retomada da queda."
            )
        else:
            tela3_status = "AGUARDAR"
            tela3_emoji  = "⏳"
            pendente = []
            if tela1_status == "NEUTRO":
                pendente.append("1ª Tela: aguardar EMA13 definir direção + MACD confirmar")
            elif tela1_status == "ALTA" and tela2_status != "SOBREVENDA":
                pendente.append("2ª Tela: maré de alta confirmada — aguardar EFI(2) atingir sobrevenda")
            elif tela1_status == "BAIXA" and tela2_status != "SOBRECOMPRA":
                pendente.append("2ª Tela: maré de baixa confirmada — aguardar EFI(2) atingir sobrecompra")
            else:
                pendente.append("Telas 1 e 2 divergentes — aguardar alinhamento")
            tela3_desc = (
                "Setup incompleto. " + " | ".join(pendente) + ".\n"
                "Elder ensina: nunca entre no mercado sem as duas primeiras telas alinhadas. "
                "Paciência é parte da estratégia."
            )

        # Força: 1 ponto por tela alinhada na mesma direção
        forca = 0
        if tela1_status == "ALTA":      forca += 1
        if tela2_status == "SOBREVENDA": forca += 1
        if tela3_status == "COMPRA":    forca += 1

        return {
            'tela1': {'status': tela1_status, 'emoji': tela1_emoji,
                      'valor': round(ema13_slope, 4), 'desc': tela1_desc},
            'tela2': {'status': tela2_status, 'emoji': tela2_emoji,
                      'valor': round(efi2_val, 0), 'desc': tela2_desc},
            'tela3': {'status': tela3_status, 'emoji': tela3_emoji, 'desc': tela3_desc},
            'veredicto': tela3_status,
            'forca': forca,
            'preco_atual': round(preco_atual, 2),
            # Séries para mini-gráficos (últimos 60 dias)
            'serie_close': close.iloc[-60:],
            'serie_macd':  ema13.iloc[-60:],      # usamos EMA13 no mini-gráfico da 1ª tela
            'serie_efi2':  efi2.iloc[-60:],
            'limiar_pos':  limiar_pos,
            'limiar_neg':  limiar_neg,
            'maxima_rec':  round(maxima_rec, 2),
            'minima_rec':  round(minima_rec, 2),
        }

    except Exception:
        return None


def renderizar_triple_screen(resultado, ticker, empresa):
    """
    Renderiza o painel Triple Screen dentro de um st.expander.
    Totalmente isolado — não toca em nenhuma outra seção.
    """
    with st.expander("🖥️ Estratégia Triple Screen — Alexander Elder", expanded=False):

        if resultado is None:
            st.warning("⚠️ Dados insuficientes para calcular o Triple Screen.")
            return

        veredicto = resultado['veredicto']
        forca     = resultado['forca']
        t1        = resultado['tela1']
        t2        = resultado['tela2']
        t3        = resultado['tela3']

        # ── Cabeçalho explicativo ────────────────────────────────────────────────────
        st.markdown("""
        <div style='background:linear-gradient(135deg,#0f2027 0%,#203a43 50%,#2c5364 100%);
                    padding:1rem 1.3rem;border-radius:10px;margin-bottom:1.2rem;'>
            <p style='margin:0;color:#cfd8dc;font-size:0.83rem;line-height:1.65;'>
                🧠 <strong style='color:#80cbc4;'>Como funciona o Triple Screen:</strong>
                Criado por <strong>Alexander Elder</strong> em 1986, combina três "telas" em
                timeframes diferentes para filtrar ruído e confirmar tendências.
                A metáfora do oceano: negocie com a <em>maré</em>, não contra ela.<br><br>
                🌊 <strong style='color:#80deea;'>1ª Tela — A Maré (EMA13 + MACD):</strong>
                A <strong>inclinação da EMA13</strong> define a tendência dominante —
                é a tela mais importante. O MACD(12,26,9) reforça a direção.
                Elder original usa EMA13 <em>semanal</em>;
                adaptamos para <em>diário</em> por ser nosso único timeframe.<br>
                🌀 <strong style='color:#80deea;'>2ª Tela — A Onda (EFI 2):</strong>
                O <strong>Force Index(2)</strong> oscila dentro da tendência maior,
                identificando correções (sobrevenda em uptrend = oportunidade de compra)
                e repiques (sobrecompra em downtrend = oportunidade de venda).<br>
                🎯 <strong style='color:#80deea;'>3ª Tela — A Execução:</strong>
                Sem indicador — usa a <em>ação do preço</em>.
                Buy Stop acima da máxima recente (compra) ou
                Sell Stop abaixo da mínima recente (venda).
                O mercado confirma o movimento — ou a ordem não é executada.
            </p>
        </div>""", unsafe_allow_html=True)

        # ── Veredicto geral ──────────────────────────────────────────────────────────
        cfg_v = {
            "COMPRA":   ("#d4edda", "#155724", "#28a745", "🚀", "SETUP DE COMPRA"),
            "VENDA":    ("#f8d7da", "#721c24", "#dc3545", "📉", "SETUP DE VENDA"),
            "AGUARDAR": ("#fff3cd", "#856404", "#ffc107", "⏳", "AGUARDAR ALINHAMENTO"),
        }
        bg_v, txt_v, brd_v, ico_v, lbl_v = cfg_v[veredicto]

        estrelas = "⭐" * forca + "☆" * (3 - forca)
        st.markdown(f"""
        <div style='background:{bg_v};border:2px solid {brd_v};border-radius:12px;
                    padding:1.1rem 1.4rem;margin-bottom:1.2rem;
                    display:flex;align-items:center;gap:1rem;'>
            <div style='font-size:2.4rem;'>{ico_v}</div>
            <div>
                <div style='font-size:1.2rem;font-weight:800;color:{txt_v};'>{lbl_v}</div>
                <div style='font-size:0.82rem;color:{txt_v};margin-top:0.2rem;'>
                    Força do sinal: {estrelas} &nbsp;({forca}/3 telas alinhadas)
                    &nbsp;|&nbsp; {ticker} — {empresa}
                </div>
            </div>
        </div>""", unsafe_allow_html=True)

        # ── Três painéis das telas + mini-gráficos ──────────────────────────────────
        col1, col2, col3 = st.columns(3)

        cfg_s = {
            "ALTA":        ("#e8f5e9", "#1b5e20", "#43a047"),
            "BAIXA":       ("#ffebee", "#b71c1c", "#e53935"),
            "NEUTRO":      ("#fffde7", "#f57f17", "#fbc02d"),
            "SOBREVENDA":  ("#e8f5e9", "#1b5e20", "#43a047"),
            "SOBRECOMPRA": ("#ffebee", "#b71c1c", "#e53935"),
            "COMPRA":      ("#e8f5e9", "#1b5e20", "#43a047"),
            "VENDA":       ("#ffebee", "#b71c1c", "#e53935"),
            "AGUARDAR":    ("#fffde7", "#f57f17", "#fbc02d"),
        }

        serie_close  = resultado['serie_close']
        serie_macd   = resultado['serie_macd']
        serie_efi2   = resultado['serie_efi2']
        limiar_pos   = resultado['limiar_pos']
        limiar_neg   = resultado['limiar_neg']
        maxima_rec   = resultado['maxima_rec']
        minima_rec   = resultado['minima_rec']
        preco_atual  = resultado['preco_atual']

        for col, tela, num, nome, subtitulo in [
            (col1, t1, "1ª", "Maré",    "EMA13 + MACD(12,26,9)"),
            (col2, t2, "2ª", "Onda",    "EFI(2)"),
            (col3, t3, "3ª", "Execução","Buy/Sell Stop"),
        ]:
            bg_s, txt_s, brd_s = cfg_s.get(tela['status'], ("#f5f5f5","#333","#999"))
            if 'valor' in tela:
                v = tela['valor']
                if abs(v) < 1:
                    valor_fmt = f"{v:+.5f}"
                elif abs(v) >= 1000:
                    valor_fmt = f"{int(v):,}".replace(",", ".")
                else:
                    valor_fmt = f"{v:+.4f}"
            with col:
                valor_linha = (
                    f"<div style='font-size:0.74rem;color:{txt_s};margin-top:0.25rem;"
                    f"font-family:monospace;'>{valor_fmt}</div>"
                ) if 'valor' in tela else ""
                # Card de status
                st.markdown(f"""
                <div style='background:{bg_s};border:1.5px solid {brd_s};
                            border-radius:10px 10px 0 0;padding:0.75rem 0.9rem 0.5rem;'>
                    <div style='font-size:0.68rem;font-weight:700;color:{brd_s};
                                letter-spacing:.08em;text-transform:uppercase;'>
                        {num} TELA — {nome.upper()}
                    </div>
                    <div style='font-size:0.65rem;color:{txt_s};margin-bottom:0.4rem;'>
                        {subtitulo}
                    </div>
                    <div style='display:flex;align-items:center;gap:0.4rem;'>
                        <span style='font-size:1.3rem;line-height:1;'>{tela['emoji']}</span>
                        <span style='font-size:0.9rem;font-weight:800;color:{txt_s};'>
                            {tela['status']}
                        </span>
                    </div>
                    {valor_linha}
                </div>""", unsafe_allow_html=True)

                # Mini-gráfico para cada tela
                fig_mini, ax_m = plt.subplots(figsize=(3.2, 1.6))
                fig_mini.patch.set_facecolor(bg_s)
                ax_m.set_facecolor(bg_s)

                if num == "1ª":
                    # EMA13 sobre preço — inclinação positiva = maré de alta
                    xs   = range(len(serie_close))
                    pr   = serie_close.values
                    em13 = serie_macd.values   # serie_macd agora contém EMA13
                    cor_ema = brd_s
                    ax_m.plot(xs, pr,   color='#607d8b', linewidth=1.0,
                              alpha=0.6, label='Preço')
                    ax_m.plot(xs, em13, color=cor_ema, linewidth=2.0,
                              label='EMA13', zorder=3)
                    # Preenche acima/abaixo da EMA13
                    ax_m.fill_between(xs, pr, em13,
                                      where=(pr >= em13), alpha=0.15,
                                      color='#43a047', interpolate=True)
                    ax_m.fill_between(xs, pr, em13,
                                      where=(pr < em13),  alpha=0.15,
                                      color='#e53935', interpolate=True)
                    ax_m.set_title("EMA13 (Maré)", fontsize=7, color=txt_s, pad=3)

                elif num == "2ª":
                    # EFI(2) — histograma + limiares
                    xs = range(len(serie_efi2))
                    vals = serie_efi2.values
                    cores_b = [brd_s if v >= 0 else '#e53935' for v in vals]
                    ax_m.bar(xs, vals, color=cores_b, alpha=0.7, width=1.0)
                    ax_m.axhline(limiar_pos, color='#e53935', linewidth=0.9,
                                 linestyle='--', alpha=0.8)
                    ax_m.axhline(limiar_neg, color='#43a047', linewidth=0.9,
                                 linestyle='--', alpha=0.8)
                    ax_m.axhline(0, color='#90a4ae', linewidth=0.7, linestyle='-')
                    ax_m.set_title("EFI(2)", fontsize=7, color=txt_s, pad=3)

                else:
                    # Tela 3 — preço dos últimos 20 dias + faixas de Buy/Sell Stop
                    close_20 = serie_close.iloc[-20:]
                    xs = range(len(close_20))
                    vals = close_20.values
                    cor_linha = '#43a047' if t3['status'] == 'COMPRA' else \
                                '#e53935' if t3['status'] == 'VENDA' else '#f57f17'
                    ax_m.plot(xs, vals, color=cor_linha, linewidth=1.5, zorder=3)
                    ax_m.axhline(maxima_rec, color='#43a047', linewidth=1.0,
                                 linestyle='--', alpha=0.9,
                                 label=f'Buy Stop R${maxima_rec:.2f}')
                    ax_m.axhline(minima_rec, color='#e53935', linewidth=1.0,
                                 linestyle='--', alpha=0.9,
                                 label=f'Stop R${minima_rec:.2f}')
                    ax_m.axhline(preco_atual, color='#607d8b', linewidth=0.8,
                                 linestyle=':', alpha=0.7)
                    ax_m.fill_between(xs, maxima_rec, minima_rec,
                                      alpha=0.07, color=cor_linha)
                    ax_m.set_title("Preço + Stop", fontsize=7, color=txt_s, pad=3)

                for spine in ax_m.spines.values():
                    spine.set_visible(False)
                ax_m.set_xticks([])
                ax_m.tick_params(axis='y', labelsize=6, colors=txt_s, length=0)
                ax_m.yaxis.set_major_formatter(
                    plt.FuncFormatter(lambda v, _:
                        f'{v/1e6:.1f}M' if abs(v) >= 1e6 else
                        f'{v/1e3:.0f}K' if abs(v) >= 1e3 else
                        f'{v:.2f}'))
                plt.tight_layout(pad=0.3)
                st.pyplot(fig_mini, use_container_width=True)
                plt.close(fig_mini)

                # Borda inferior arredondada para fechar visualmente com o card
                st.markdown(f"""
                <div style='background:{bg_s};border:1.5px solid {brd_s};
                            border-top:none;border-radius:0 0 10px 10px;
                            height:6px;margin-top:-4px;'></div>
                """, unsafe_allow_html=True)

        # ── Detalhamento por tela ────────────────────────────────────────────────────
        st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)

        for tela, num, icone, titulo in [
            (t1, "1ª", "🌊", "Tela — Identificação da Maré (EMA13 + MACD 12,26,9)"),
            (t2, "2ª", "🌀", "Tela — Sinal de Entrada pela Onda (EFI 2)"),
            (t3, "3ª", "🎯", "Tela — Execução (Ordem Stop)"),
        ]:
            bg_d, txt_d, _ = cfg_s.get(tela['status'], ("#f8fafc","#334155","#cbd5e1"))
            st.markdown(f"""
            <div style='background:{bg_d};border-left:4px solid;
                        border-color:{cfg_s.get(tela["status"],("","","#999"))[2]};
                        border-radius:0 8px 8px 0;padding:0.8rem 1rem;
                        margin-bottom:0.6rem;'>
                <div style='font-weight:700;font-size:0.88rem;color:{txt_d};
                            margin-bottom:0.35rem;'>{icone} {num} {titulo}</div>
                <div style='font-size:0.82rem;color:{txt_d};line-height:1.55;
                            white-space:pre-wrap;'>{tela['desc']}</div>
            </div>""", unsafe_allow_html=True)

        # ── Nota educacional ─────────────────────────────────────────────────────────
        st.markdown("""
        <div style='margin-top:0.8rem;padding:0.7rem 1rem;background:#eceff1;
                    border-radius:8px;font-size:0.76rem;color:#546e7a;line-height:1.5;'>
            📖 <strong>Importante:</strong> O Triple Screen foi concebido para múltiplos
            timeframes. Como este monitor usa apenas dados <em>diários</em>, a 1ª tela
            representa a tendência de <strong>médio prazo</strong> (últimas semanas) e a
            2ª tela a oscilação de <strong>curto prazo</strong> (últimos dias).
            Para máxima precisão, confirme sempre no gráfico semanal (1ª tela) e
            no gráfico horário (2ª tela) antes de executar qualquer ordem.
            &nbsp;|&nbsp;
            <a href="https://hw-br.online/education/triple-screen-strategy-3-steps-to-make-profit/"
               target="_blank" style='color:#0288d1;'>Leia o artigo completo ↗</a>
        </div>""", unsafe_allow_html=True)

# =============================================================================
# MAPEAMENTO BDR → TICKER US PARA DADOS FUNDAMENTALISTAS
# =============================================================================
BDR_TO_US_MAP = {
    'A1AP34': 'AAP',
    'A1DC34': 'ADC',
    'A1DI34': 'ADI',
    'A1EP34': 'AEP',
    'A1ES34': 'AES',
    'A1FL34': 'AFL',
    'A1IV34': 'AIV',
    'A1KA34': 'AKAM',
    'A1LB34': 'ALB',
    'A1LK34': 'ALK',
    'A1LL34': 'BFH',
    'A1MD34': 'AMD',
    'A1MP34': 'AMP',
    'A1MT34': 'AMAT',
    'A1NE34': 'ANET',
    'A1PH34': 'APH',
    'A1PL34': 'APLD',
    'A1PO34': 'APO',
    'A1PP34': 'APP',
    'A1RE34': 'ARE',
    'A1RG34': 'ARGX',
    'A1SU34': 'AIZ',
    'A1TH34': 'ATHM',
    'A1VB34': 'AVB',
    'A1WK34': 'AWK',
    'A1ZN34': 'AZN',
    'A2MB34': 'AMBA',
    'A2RR34': 'ARWR',
    'A2RW34': 'ARW',
    'A2SO34': 'ASO',
    'A2XO34': 'AXON',
    'A2ZT34': 'AZTA',
    'AADA39': 'AADA',
    'AALL34': 'AAL',
    'AAPL34': 'AAPL',
    'ABBV34': 'ABBV',
    'ABGD39': 'ABGD',
    'ABTT34': 'ABT',
    'ABUD34': 'BUD',
    'ACNB34': 'ACN',
    'ACWX39': 'ACWX',
    'ADBE34': 'ADBE',
    'AIRB34': 'ABNB',
    'AMGN34': 'AMGN',
    'AMZO34': 'AMZN',
    'APTV34': 'APTV',
    'ARGT39': 'ARGT',
    'ARMT34': 'MT',
    'ARNC34': 'HWM',
    'ASML34': 'ASML',
    'ATTB34': 'T',
    'AURA33': 'ORA',
    'AVGO34': 'AVGO',
    'AWII34': 'AWI',
    'AXPB34': 'AXP',
    'B1AM34': 'BN',
    'B1AX34': 'BAX',
    'B1BW34': 'BBWI',
    'B1CS34': 'BCS',
    'B1FC34': 'BF-B',
    'B1IL34': 'BILI',
    'B1LL34': 'BALL',
    'B1MR34': 'BMRN',
    'B1NT34': 'BNTX',
    'B1PP34': 'BP',
    'B1RF34': 'BR',
    'B1SA34': 'BSAC',
    'B1TI34': 'BTI',
    'B2AH34': 'BAH',
    'B2HI34': 'BILL',
    'B2LN34': 'BL',
    'B2MB34': 'BMBL',
    'B2RK34': 'BRKR',
    'B2UR34': 'BURL',
    'B2YN34': 'BYND',
    'BAAX39': 'BAAX',
    'BABA34': 'BABA',
    'BACW39': 'BACW',
    'BAER39': 'BAER',
    'BAGG39': 'BAGG',
    'BAIQ39': 'BAIQ',
    'BAOR39': 'BAOR',
    'BARY39': 'BARY',
    'BASK39': 'BASK',
    'BBER39': 'BBER',
    'BBJP39': 'BBJP',
    'BBUG39': 'BBUG',
    'BCAT39': 'BCAT',
    'BCHI39': 'BCHI',
    'BCIR39': 'BCIR',
    'BCLO39': 'BCLO',
    'BCNY39': 'BCNY',
    'BCOM39': 'BCOM',
    'BCPX39': 'BCPX',
    'BCSA34': 'SAN',
    'BCTE39': 'BCTE',
    'BCWV39': 'BCWV',
    'BDVD39': 'BDVD',
    'BDVE39': 'BDVE',
    'BDVY39': 'BDVY',
    'BECH39': 'BECH',
    'BEEM39': 'BEEM',
    'BEFA39': 'BEFA',
    'BEFG39': 'BEFG',
    'BEFV39': 'BEFV',
    'BEGD39': 'BEGD',
    'BEGE39': 'BEGE',
    'BEGU39': 'BEGU',
    'BEIS39': 'BEIS',
    'BEMV39': 'BEMV',
    'BEPP39': 'BEPP',
    'BEPU39': 'BEPU',
    'BERK34': 'BRK-B',
    'BEWA39': 'BEWA',
    'BEWC39': 'BEWC',
    'BEWD39': 'BEWD',
    'BEWG39': 'BEWG',
    'BEWH39': 'BEWH',
    'BEWJ39': 'BEWJ',
    'BEWL39': 'BEWL',
    'BEWP39': 'BEWP',
    'BEWS39': 'BEWS',
    'BEWW39': 'BEWW',
    'BEWY39': 'BEWY',
    'BEWZ39': 'BEWZ',
    'BEZA39': 'BEZA',
    'BEZU39': 'BEZU',
    'BFAV39': 'BFAV',
    'BFLO39': 'BFLO',
    'BFXI39': 'BFXI',
    'BGLC39': 'BGLC',
    'BGOV39': 'BGOV',
    'BGOZ39': 'BGOZ',
    'BGRT39': 'BGRT',
    'BGWH39': 'BGWH',
    'BHEF39': 'BHEF',
    'BHER39': 'BHER',
    'BHVN34': 'BHVN',
    'BHYC39': 'BHYC',
    'BHYG39': 'BHYG',
    'BIAI39': 'BIAI',
    'BIAU39': 'BIAU',
    'BIBB39': 'BIBB',
    'BICL39': 'BICL',
    'BIDU34': 'BIDU',
    'BIEF39': 'BIEF',
    'BIEI39': 'BIEI',
    'BIEM39': 'BIEM',
    'BIEO39': 'BIEO',
    'BIEU39': 'BIEU',
    'BIEV39': 'BIEV',
    'BIGF39': 'BIGF',
    'BIGS39': 'BIGS',
    'BIHE39': 'BIHE',
    'BIHF39': 'BIHF',
    'BIHI39': 'BIHI',
    'BIIB34': 'BIIB',
    'BIJH39': 'BIJH',
    'BIJR39': 'BIJR',
    'BIJS39': 'BIJS',
    'BIJT39': 'BIJT',
    'BILF39': 'BILF',
    'BIPC39': 'BIPC',
    'BIPZ39': 'BIPZ',
    'BITB39': 'BITB',
    'BITO39': 'BITO',
    'BIUS39': 'BIUS',
    'BIVB39': 'BIVB',
    'BIVE39': 'BIVE',
    'BIVW39': 'BIVW',
    'BIWF39': 'BIWF',
    'BIWM39': 'BIWM',
    'BIXG39': 'BIXG',
    'BIXJ39': 'BIXJ',
    'BIXN39': 'BIXN',
    'BIXU39': 'BIXU',
    'BIYE39': 'BIYE',
    'BIYF39': 'BIYF',
    'BIYJ39': 'BIYJ',
    'BIYT39': 'BIYT',
    'BIYW39': 'BIYW',
    'BIYZ39': 'BIYZ',
    'BJQU39': 'BJQU',
    'BKCH39': 'BKCH',
    'BKNG34': 'BKNG',
    'BKWB39': 'BKWB',
    'BKXI39': 'BKXI',
    'BLAK34': 'BLAK',
    'BLBT39': 'BLBT',
    'BLPX39': 'BLPX',
    'BLQD39': 'BLQD',
    'BMTU39': 'BMTU',
    'BMYB34': 'BMYB',
    'BNDA39': 'BNDA',
    'BOAC34': 'BOAC',
    'BOEF39': 'BOEF',
    'BOEI34': 'BOEI',
    'BONY34': 'BONY',
    'BOTZ39': 'BOTZ',
    'BOXP34': 'BOXP',
    'BPIC39': 'BPIC',
    'BPVE39': 'BPVE',
    'BQQW39': 'BQQW',
    'BQUA39': 'BQUA',
    'BQYL39': 'BQYL',
    'BSCZ39': 'BSCZ',
    'BSDV39': 'BSDV',
    'BSHV39': 'BSHV',
    'BSHY39': 'BSHY',
    'BSIL39': 'BSIL',
    'BSIZ39': 'BSIZ',
    'BSLV39': 'BSLV',
    'BSOC39': 'BSOC',
    'BSOX39': 'BSOX',
    'BSRE39': 'BSRE',
    'BTFL39': 'BTFL',
    'BTIP39': 'BTIP',
    'BTLT39': 'BTLT',
    'BURA39': 'BURA',
    'BURT39': 'BURT',
    'BUSM39': 'BUSM',
    'BUSR39': 'BUSR',
    'BUTL39': 'BUTL',
    'C1AB34': 'CABO',
    'C1AG34': 'CAG',
    'C1AH34': 'CAH',
    'C1BL34': 'CB',
    'C1BR34': 'CBRE',
    'C1CJ34': 'CCJ',
    'C1CL34': 'CCL',
    'C1CO34': 'COR',
    'C1DN34': 'CDNS',
    'C1FG34': 'CFG',
    'C1GP34': 'CSGP',
    'C1HR34': 'CHRW',
    'C1IC34': 'CI',
    'C1MG34': 'CMG',
    'C1MI34': 'CMI',
    'C1MS34': 'CMS',
    'C1NC34': 'CNC',
    'C1OO34': 'COO',
    'C1PB34': 'CPB',
    'C1RH34': 'CRH',
    'C2AC34': 'CACI',
    'C2CA34': 'KOF',
    'C2GN34': 'CGNX',
    'C2HD34': 'CHDN',
    'C2OI34': 'COIN',
    'C2OL34': 'CIBR',
    'C2OU34': 'COUR',
    'C2RN34': 'CRNC',
    'C2RS34': 'CRSP',
    'C2RW34': 'CRWD',
    'C2ZR34': 'CZR',
    'CAON34': 'CAON',
    'CATP34': 'CATP',
    'CHCM34': 'CHCM',
    'CHDC34': 'CHDC',
    'CHME34': 'CHME',
    'CHVX34': 'CVX',
    'CLOV34': 'CLOV',
    'CLXC34': 'CLXC',
    'CNIC34': 'CNIC',
    'COCA34': 'COCA',
    'COLG34': 'COLG',
    'COPH34': 'COPH',
    'COTY34': 'COTY',
    'COWC34': 'COWC',
    'CPRL34': 'CPRL',
    'CRIN34': 'CRIN',
    'CSCO34': 'CSCO',
    'CSXC34': 'CSXC',
    'CTGP34': 'CTGP',
    'CTSH34': 'CTSH',
    'CVSH34': 'CVSH',
    'D1DG34': 'DDOG',
    'D1EX34': 'DXCM',
    'D1LR34': 'DLR',
    'D1OC34': 'DOCU',
    'D1OW34': 'DOW',
    'D1VN34': 'DVN',
    'D2AR34': 'DAR',
    'D2AS34': 'DASH',
    'D2NL34': 'DNLI',
    'D2OC34': 'DOCS',
    'D2OX34': 'DOX',
    'D2PZ34': 'DPZ',
    'DBAG34': 'DBAG',
    'DDNB34': 'DDNB',
    'DEEC34': 'DEEC',
    'DEFT31': 'DEFT',
    'DEOP34': 'DEOP',
    'DGCO34': 'DGCO',
    'DHER34': 'DHER',
    'DISB34': 'DIS',
    'DOLL39': 'DOLL',
    'DTCR39': 'DTCR',
    'DUOL34': 'DUOL',
    'DVAI34': 'DVAI',
    'E1CO34': 'EC',
    'E1DU34': 'EDU',
    'E1LV34': 'ELV',
    'E1MN34': 'EMN',
    'E1MR34': 'EMR',
    'E1OG34': 'EOG',
    'E1QN34': 'EQNR',
    'E1RI34': 'ERIC',
    'E1TN34': 'ETN',
    'E1WL34': 'EW',
    'E2AG34': 'EXP',
    'E2EF34': 'EEFT',
    'E2NP34': 'ENPH',
    'E2ST34': 'ESTC',
    'E2TS34': 'ETSY',
    'EAIN34': 'EAIN',
    'EBAY34': 'EBAY',
    'EIDO39': 'EIDO',
    'ELCI34': 'ELCI',
    'EPHE39': 'EPHE',
    'EQIX34': 'EQIX',
    'ETHA39': 'ETHA',
    'EVEB31': 'EVEB',
    'EVTC31': 'EVTC',
    'EWJV39': 'EWJV',
    'EXGR34': 'EXGR',
    'EXPB31': 'EXPB',
    'EXXO34': 'EXXO',
    'F1AN34': 'FANG',
    'F1IS34': 'FI',
    'F1MC34': 'FMC',
    'F1NI34': 'FIS',
    'F1SL34': 'FSLY',
    'F1TN34': 'FTNT',
    'F2IC34': 'FICO',
    'F2IV34': 'FIVN',
    'F2NV34': 'FNV',
    'F2RS34': 'FRSH',
    'FASL34': 'FASL',
    'FBOK34': 'META',
    'FCXO34': 'FCXO',
    'FDMO34': 'FDMO',
    'FDXB34': 'FDXB',
    'FSLR34': 'FSLR',
    'G1AM34': 'GLPI',
    'G1AR34': 'IT',
    'G1DS34': 'GDS',
    'G1FI34': 'GFI',
    'G1LO34': 'GLOB',
    'G1LW34': 'GLW',
    'G1MI34': 'GIS',
    'G1PI34': 'GPN',
    'G1RM34': 'GRMN',
    'G1SK34': 'GSK',
    'G1TR39': 'G1TR',
    'G1WW34': 'GWW',
    'G2DD34': 'GDDY',
    'G2DI33': 'G2D',
    'G2EV34': 'GEV',
    'GDBR34': 'GDBR',
    'GDXB39': 'GDXB',
    'GEOO34': 'GEOO',
    'GILD34': 'GILD',
    'GMCO34': 'GMCO',
    'GOGL34': 'GOOGL',
    'GOGL35': 'GOOG',
    'GPRK34': 'GPRK',
    'GPRO34': 'GPRO',
    'GPSI34': 'GPSI',
    'GROP31': 'GROP',
    'GSGI34': 'GSGI',
    'H1AS34': 'HAS',
    'H1CA34': 'HCA',
    'H1DB34': 'HDB',
    'H1II34': 'HII',
    'H1OG34': 'HOG',
    'H1PE34': 'HPE',
    'H1RL34': 'HRL',
    'H1SB34': 'HSBC',
    'H1UM34': 'HUM',
    'H2TA34': 'HR',
    'H2UB34': 'HUBS',
    'HALI34': 'HALI',
    'HOME34': 'HOME',
    'HOND34': 'HOND',
    'HPQB34': 'HPQB',
    'HYEM39': 'HYEM',
    'I1AC34': 'IAC',
    'I1DX34': 'IDXX',
    'I1EX34': 'IEX',
    'I1FO34': 'INFY',
    'I1LM34': 'ILMN',
    'I1NC34': 'INCY',
    'I1PC34': 'IP',
    'I1PG34': 'IPGP',
    'I1QV34': 'IQV',
    'I1QY34': 'IQ',
    'I1RM34': 'IRM',
    'I1RP34': 'TT',
    'I1SR34': 'ISRG',
    'I2NG34': 'INGR',
    'I2NV34': 'INVH',
    'IBIT39': 'IBIT',
    'IBKR34': 'IBKR',
    'ICLR34': 'ICLR',
    'INBR32': 'INTR',
    'INTU34': 'INTU',
    'ITLC34': 'ITLC',
    'J1EG34': 'J',
    'J2BL34': 'JBL',
    'JBSS32': 'JBSS',
    'JDCO34': 'JD',
    'JNJB34': 'JNJB',
    'JPMC34': 'JPMC',
    'K1BF34': 'KB',
    'K1LA34': 'KLAC',
    'K1MX34': 'KMX',
    'K1SG34': 'KEYS',
    'K1SS34': 'KSS',
    'K1TC34': 'KT',
    'K2CG34': 'KC',
    'KHCB34': 'KHCB',
    'KMBB34': 'KMBB',
    'KMIC34': 'KMIC',
    'L1EG34': 'LEG',
    'L1EN34': 'LEN',
    'L1HX34': 'LHX',
    'L1MN34': 'LUMN',
    'L1NC34': 'LNC',
    'L1RC34': 'LRCX',
    'L1WH34': 'LW',
    'L1YG34': 'LYG',
    'L1YV34': 'LYV',
    'L2PL34': 'LPLA',
    'L2SC34': 'LSCC',
    'LBRD34': 'LBRD',
    'LILY34': 'LILY',
    'LOWC34': 'LOWC',
    'M1AA34': 'MAA',
    'M1CH34': 'MCHP',
    'M1CK34': 'MCK',
    'M1DB34': 'MDB',
    'M1HK34': 'MHK',
    'M1MC34': 'MMC',
    'M1NS34': 'MNST',
    'M1RN34': 'MRNA',
    'M1SC34': 'MSCI',
    'M1SI34': 'MSI',
    'M1TA34': 'META',
    'M1TC34': 'MTCH',
    'M1TT34': 'MAR',
    'M1UF34': 'MUFG',
    'M2KS34': 'MKSI',
    'M2PM34': 'MP',
    'M2PR34': 'MPWR',
    'M2PW34': 'MPW',
    'M2RV34': 'MRVL',
    'M2ST34': 'MSTR',
    'MACY34': 'MACY',
    'MCDC34': 'MCDC',
    'MCOR34': 'MCOR',
    'MDLZ34': 'MDLZ',
    'MDTC34': 'MDTC',
    'MELI34': 'MELI',
    'MKLC34': 'MKLC',
    'MMMC34': 'MMMC',
    'MOOO34': 'MOOO',
    'MOSC34': 'MOSC',
    'MRCK34': 'MRCK',
    'MSBR34': 'MSBR',
    'MSCD34': 'MA',
    'MSFT34': 'MSFT',
    'MUTC34': 'MU',
    'N1BI34': 'NBIX',
    'N1CL34': 'NCLH',
    'N1DA34': 'NDAQ',
    'N1EM34': 'NEM',
    'N1GG34': 'NGG',
    'N1IS34': 'NI',
    'N1OW34': 'NOW',
    'N1RG34': 'NRG',
    'N1TA34': 'NTAP',
    'N1UE34': 'NUE',
    'N1VO34': 'NVO',
    'N1VR34': 'NVR',
    'N1VS34': 'NVS',
    'N1WG34': 'NWG',
    'N1XP34': 'NXPI',
    'N2ET34': 'NET',
    'N2LY34': 'NLY',
    'N2TN34': 'NTNX',
    'N2VC34': 'NVCR',
    'NETE34': 'NETE',
    'NEXT34': 'NEXT',
    'NFLX34': 'NFLX',
    'NIKE34': 'NIKE',
    'NMRH34': 'NMRH',
    'NOCG34': 'NOCG',
    'NOKI34': 'NOKI',
    'NVDC34': 'NVDA',
    'O1DF34': 'ODFL',
    'O1KT34': 'OKTA',
    'O2HI34': 'OHI',
    'O2NS34': 'ON',
    'ORCL34': 'ORCL',
    'ORLY34': 'ORLY',
    'OXYP34': 'OXYP',
    'P1AC34': 'PCAR',
    'P1AY34': 'PAYX',
    'P1DD34': 'PDD',
    'P1EA34': 'DOC',
    'P1GR34': 'PGR',
    'P1KX34': 'PKX',
    'P1LD34': 'PLD',
    'P1NW34': 'PNW',
    'P1PL34': 'PPL',
    'P1RG34': 'PRGO',
    'P1SX34': 'PSX',
    'P2AN34': 'PANW',
    'P2AT34': 'PATH',
    'P2AX34': 'PAX',
    'P2EG34': 'PEGA',
    'P2EN34': 'PENN',
    'P2IN34': 'PINS',
    'P2LT34': 'PLTR',
    'P2ST34': 'PSTG',
    'P2TC34': 'PTC',
    'PAGS34': 'PAGS',
    'PEPB34': 'PEPB',
    'PFIZ34': 'PFIZ',
    'PGCO34': 'PGCO',
    'PHGN34': 'PHGN',
    'PHMO34': 'PHMO',
    'PNCS34': 'PNCS',
    'PRXB31': 'PRXB',
    'PSKY34': 'PSKY',
    'PYPL34': 'PYPL',
    'Q2SC34': 'QS',
    'QCOM34': 'QCOM',
    'QUBT34': 'QUBT',
    'R1DY34': 'RDY',
    'R1EG34': 'REG',
    'R1EL34': 'RELX',
    'R1HI34': 'RHI',
    'R1IN34': 'O',
    'R1KU34': 'ROKU',
    'R1MD34': 'RMD',
    'R1OP34': 'ROP',
    'R1SG34': 'RSG',
    'R1YA34': 'RYAAY',
    'R2BL34': 'RBLX',
    'R2NG34': 'RNG',
    'R2PD34': 'RPD',
    'REGN34': 'REGN',
    'RGTI34': 'RGTI',
    'RIGG34': 'RIGG',
    'RIOT34': 'RIOT',
    'ROST34': 'ROST',
    'ROXO34': 'NU',
    'RSSL39': 'RSSL',
    'RYTT34': 'RYTT',
    'S1BA34': 'SBAC',
    'S1BS34': 'SBSW',
    'S1HW34': 'SHW',
    'S1KM34': 'SKM',
    'S1LG34': 'SLG',
    'S1NA34': 'SNA',
    'S1NP34': 'SNPS',
    'S1OU34': 'LUV',
    'S1PO34': 'SPOT',
    'S1RE34': 'SRE',
    'S1TX34': 'STX',
    'S1WK34': 'SWK',
    'S1YY34': 'SYY',
    'S2CH34': 'SQM',
    'S2EA34': 'SE',
    'S2ED34': 'SEDG',
    'S2FM34': 'SFM',
    'S2GM34': 'SGML',
    'S2HO34': 'SHOP',
    'S2NA34': 'SNAP',
    'S2NW34': 'SNOW',
    'S2TA34': 'STAG',
    'S2UI34': 'SUI',
    'S2YN34': 'SYNA',
    'SAPP34': 'SAPP',
    'SBUB34': 'SBUB',
    'SCHW34': 'SCHW',
    'SIVR39': 'SIVR',
    'SLBG34': 'SLBG',
    'SLXB39': 'SLXB',
    'SMIN39': 'SMIN',
    'SNEC34': 'SNEC',
    'SOLN39': 'SOLN',
    'SPGI34': 'SPGI',
    'SSFO34': 'SSFO',
    'STMN34': 'STMN',
    'STOC34': 'STOC',
    'STZB34': 'STZB',
    'T1AL34': 'TAL',
    'T1AM34': 'TEAM',
    'T1EV34': 'TEVA',
    'T1LK34': 'TLK',
    'T1MU34': 'TMUS',
    'T1OW34': 'AMT',
    'T1RI34': 'TRIP',
    'T1SC34': 'TSCO',
    'T1SO34': 'SO',
    'T1TW34': 'TTWO',
    'T1WL34': 'TWLO',
    'T2DH34': 'TDOC',
    'T2ER34': 'TER',
    'T2RM34': 'TRMB',
    'T2TD34': 'TTD',
    'T2YL34': 'TYL',
    'TAKP34': 'TAKP',
    'TBIL39': 'TBIL',
    'TMCO34': 'TMCO',
    'TMOS34': 'TMOS',
    'TOPB39': 'TOPB',
    'TPRY34': 'TPRY',
    'TRVC34': 'TRVC',
    'TSLA34': 'TSLA',
    'TSMC34': 'TSMC',
    'TSNF34': 'TSNF',
    'TXSA34': 'TXSA',
    'U1AI34': 'UA',
    'U1AL34': 'UAL',
    'U1BE34': 'UBER',
    'U1DR34': 'UDR',
    'U1HS34': 'UHS',
    'U1RI34': 'URI',
    'U2PS34': 'UPST',
    'U2PW34': 'UPWK',
    'U2ST34': 'U',
    'U2TH34': 'UTHR',
    'UBSG34': 'UBSG',
    'ULEV34': 'ULEV',
    'UNHH34': 'UNHH',
    'UPAC34': 'UPAC',
    'USBC34': 'USBC',
    'V1MC34': 'VMC',
    'V1NO34': 'VNO',
    'V1OD34': 'VOD',
    'V1RS34': 'VRSK',
    'V1RT34': 'VRT',
    'V1SA34': 'V',
    'V1ST34': 'VST',
    'V1TA34': 'VTR',
    'V2EE34': 'VEEV',
    'V2TX34': 'VTEX',
    'VERZ34': 'VERZ',
    'VISA34': 'VISA',
    'VLOE34': 'VLOE',
    'VRSN34': 'VRSN',
    'W1BD34': 'WBD',
    'W1BO34': 'WB',
    'W1DC34': 'WDC',
    'W1EL34': 'WELL',
    'W1HR34': 'WHR',
    'W1MB34': 'WMB',
    'W1MC34': 'WM',
    'W1MG34': 'WMG',
    'W1YC34': 'WY',
    'W2ST34': 'WST',
    'W2YF34': 'W',
    'WABC34': 'WABC',
    'WALM34': 'WALM',
    'WFCO34': 'WFCO',
    'WUNI34': 'WUNI',
    'X1YZ34': 'SQ',
    'XPBR31': 'XPBR',
    'XRPV39': 'XRPV',
    'Y2PF34': 'YPF',
    'YUMR34': 'YUMR',
    'Z1BR34': 'ZBRA',
    'Z1OM34': 'ZM',
    'Z1TA34': 'ZETA',
    'Z1TS34': 'ZTS',
    'Z2LL34': 'Z',
    'Z2SC34': 'ZS'
}

def mapear_ticker_us(ticker_bdr):
    """
    Mapeia BDR para o ticker US da empresa mãe.
    Usa BDR_TO_US_MAP completo (678 empresas) derivado do NOMES_BDRS.
    Fallback: remove sufixo numérico (cobre novos BDRs ainda não mapeados).
    """
    if ticker_bdr in BDR_TO_US_MAP:
        return BDR_TO_US_MAP[ticker_bdr]
    # Fallback para BDRs recém-listados não cobertos pelo mapa
    stripped = ticker_bdr.rstrip('0123456789')
    # Se sobrar dígito no meio, retorna o BDR original (OpenBB pode resolver pelo nome)
    return stripped

def calcular_score_fundamentalista(info):
    """
    Calcula score 0-100 baseado em métricas fundamentalistas
    Retorna: (score, detalhes_dict)
    """
    score = 50  # Base neutra
    detalhes = {
        'pe_ratio': {'valor': None, 'pontos': 0, 'criterio': ''},
        'dividend_yield': {'valor': None, 'pontos': 0, 'criterio': ''},
        'revenue_growth': {'valor': None, 'pontos': 0, 'criterio': ''},
        'recomendacao': {'valor': None, 'pontos': 0, 'criterio': ''},
        'market_cap': {'valor': None, 'pontos': 0, 'criterio': ''},
    }
    
    try:
        # P/E Ratio (15 pontos)
        pe = info.get('trailingPE') or info.get('forwardPE')
        if pe:
            detalhes['pe_ratio']['valor'] = pe
            if 10 <= pe <= 25:
                detalhes['pe_ratio']['pontos'] = 15
                detalhes['pe_ratio']['criterio'] = 'Ótimo (10-25)'
                score += 15
            elif 5 <= pe < 10 or 25 < pe <= 35:
                detalhes['pe_ratio']['pontos'] = 10
                detalhes['pe_ratio']['criterio'] = 'Bom (5-10 ou 25-35)'
                score += 10
            elif pe < 5:
                detalhes['pe_ratio']['pontos'] = 5
                detalhes['pe_ratio']['criterio'] = 'Baixo (<5)'
                score += 5
            elif pe > 50:
                detalhes['pe_ratio']['pontos'] = -10
                detalhes['pe_ratio']['criterio'] = 'Muito alto (>50)'
                score -= 10
            else:
                detalhes['pe_ratio']['criterio'] = 'Regular (35-50)'
        
        # Dividend Yield (10 pontos)
        div_yield = info.get('dividendYield')
        if div_yield:
            detalhes['dividend_yield']['valor'] = div_yield
            if div_yield > 0.04:
                detalhes['dividend_yield']['pontos'] = 10
                detalhes['dividend_yield']['criterio'] = 'Excelente (>4%)'
                score += 10
            elif div_yield > 0.02:
                detalhes['dividend_yield']['pontos'] = 5
                detalhes['dividend_yield']['criterio'] = 'Bom (>2%)'
                score += 5
            else:
                detalhes['dividend_yield']['criterio'] = 'Baixo (<2%)'
        
        # Crescimento de Receita (15 pontos)
        rev_growth = info.get('revenueGrowth')
        if rev_growth:
            detalhes['revenue_growth']['valor'] = rev_growth
            if rev_growth > 0.20:
                detalhes['revenue_growth']['pontos'] = 15
                detalhes['revenue_growth']['criterio'] = 'Excelente (>20%)'
                score += 15
            elif rev_growth > 0.10:
                detalhes['revenue_growth']['pontos'] = 10
                detalhes['revenue_growth']['criterio'] = 'Muito bom (>10%)'
                score += 10
            elif rev_growth > 0.05:
                detalhes['revenue_growth']['pontos'] = 5
                detalhes['revenue_growth']['criterio'] = 'Bom (>5%)'
                score += 5
            elif rev_growth < -0.10:
                detalhes['revenue_growth']['pontos'] = -10
                detalhes['revenue_growth']['criterio'] = 'Negativo (<-10%)'
                score -= 10
            else:
                detalhes['revenue_growth']['criterio'] = 'Estável'
        
        # Recomendação (10 pontos)
        rec = info.get('recommendationKey', '')
        detalhes['recomendacao']['valor'] = rec
        if rec == 'strong_buy':
            detalhes['recomendacao']['pontos'] = 10
            detalhes['recomendacao']['criterio'] = 'Compra Forte'
            score += 10
        elif rec == 'buy':
            detalhes['recomendacao']['pontos'] = 5
            detalhes['recomendacao']['criterio'] = 'Compra'
            score += 5
        elif rec == 'hold':
            detalhes['recomendacao']['criterio'] = 'Manter'
        elif rec == 'sell':
            detalhes['recomendacao']['pontos'] = -5
            detalhes['recomendacao']['criterio'] = 'Venda'
            score -= 5
        elif rec == 'strong_sell':
            detalhes['recomendacao']['pontos'] = -10
            detalhes['recomendacao']['criterio'] = 'Venda Forte'
            score -= 10
        
        # Market Cap (10 pontos)
        mcap = info.get('marketCap')
        if mcap:
            detalhes['market_cap']['valor'] = mcap
            if mcap > 1e12:
                detalhes['market_cap']['pontos'] = 10
                detalhes['market_cap']['criterio'] = 'Mega Cap (>$1T)'
                score += 10
            elif mcap > 100e9:
                detalhes['market_cap']['pontos'] = 5
                detalhes['market_cap']['criterio'] = 'Large Cap (>$100B)'
                score += 5
            elif mcap > 10e9:
                detalhes['market_cap']['criterio'] = 'Mid Cap (>$10B)'
            else:
                detalhes['market_cap']['criterio'] = 'Small Cap (<$10B)'
    
    except Exception:
        pass
    
    return max(0, min(100, score)), detalhes

def buscar_dados_brapi(ticker_bdr):
    """
    Busca dados da BDR diretamente na BRAPI (B3)
    Retorna dict com dados ou None
    """
    try:
        url = f"https://brapi.dev/api/quote/{ticker_bdr}?token={BRAPI_TOKEN}"
        response = requests.get(url, timeout=10)
        
        if response.status_code != 200:
            return None
        
        data = response.json()
        
        if 'results' not in data or len(data['results']) == 0:
            return None
        
        result = data['results'][0]
        
        # Extrair dados disponíveis
        return {
            'preco': result.get('regularMarketPrice'),
            'variacao': result.get('regularMarketChangePercent'),
            'volume': result.get('regularMarketVolume'),
            'market_cap': result.get('marketCap'),
            'setor': result.get('sector', 'N/A'),
            'nome': result.get('longName', ticker_bdr),
            'cambio': result.get('currency', 'BRL'),
        }
    except Exception:
        return None

def calcular_score_brapi(dados_brapi):
    """
    Calcula score baseado em dados da BRAPI (mais limitados)
    """
    score = 50
    detalhes = {
        'fonte': {'valor': 'BRAPI (B3)', 'pontos': 0, 'criterio': 'Dados da BDR na B3'},
        'market_cap': {'valor': None, 'pontos': 0, 'criterio': ''},
        'volume': {'valor': None, 'pontos': 0, 'criterio': ''},
    }
    
    # Market Cap (20 pontos)
    mcap = dados_brapi.get('market_cap')
    if mcap:
        detalhes['market_cap']['valor'] = mcap
        mcap_b = mcap / 1e9
        if mcap_b > 100:
            detalhes['market_cap']['pontos'] = 20
            detalhes['market_cap']['criterio'] = 'Large Cap (>$100B)'
            score += 20
        elif mcap_b > 10:
            detalhes['market_cap']['pontos'] = 10
            detalhes['market_cap']['criterio'] = 'Mid Cap (>$10B)'
            score += 10
        else:
            detalhes['market_cap']['criterio'] = 'Small Cap (<$10B)'
    
    # Volume (10 pontos - liquidez na B3)
    volume = dados_brapi.get('volume')
    if volume:
        detalhes['volume']['valor'] = volume
        if volume > 1000000:
            detalhes['volume']['pontos'] = 10
            detalhes['volume']['criterio'] = 'Alta liquidez (>1M)'
            score += 10
        elif volume > 100000:
            detalhes['volume']['pontos'] = 5
            detalhes['volume']['criterio'] = 'Boa liquidez (>100K)'
            score += 5
        else:
            detalhes['volume']['criterio'] = 'Baixa liquidez (<100K)'
    
    return max(0, min(100, score)), detalhes

FMP_API_KEY = "tBsRam74Ac6bZRWS3C8HY83C6not17Uh"

def buscar_dados_openbb(ticker_us):
    """
    Busca dados fundamentalistas via OpenBB SDK (openbb-finance).
    Retorna um dict compatível com o formato do Yahoo Finance ou None.
    """
    try:
        from openbb import obb

        # Configura chave FMP em tempo de execução
        try:
            obb.user.credentials.fmp_api_key = FMP_API_KEY
        except Exception:
            pass

        info = {}

        # --- Perfil / visão geral ---
        try:
            profile = obb.equity.profile(symbol=ticker_us, provider="fmp")
            if profile and profile.results:
                r = profile.results[0]
                info['marketCap']   = getattr(r, 'mkt_cap', None)
                info['sector']      = getattr(r, 'sector', None)
                info['industry']    = getattr(r, 'industry', None)
                info['symbol']      = ticker_us
        except Exception:
            pass

        # --- Métricas fundamentais ---
        try:
            metrics = obb.equity.fundamental.metrics(symbol=ticker_us, provider="fmp")
            if metrics and metrics.results:
                m = metrics.results[0]
                info['trailingPE']    = getattr(m, 'pe_ratio', None)
                info['dividendYield'] = getattr(m, 'dividend_yield', None)
                info['revenueGrowth'] = getattr(m, 'revenue_growth', None)
        except Exception:
            pass

        # --- Recomendação de analistas ---
        try:
            rec = obb.equity.estimates.consensus(symbol=ticker_us, provider="fmp")
            if rec and rec.results:
                cons = rec.results[0]
                raw = str(getattr(cons, 'consensus', '') or '').lower().replace(' ', '_')
                # normaliza para o padrão Yahoo: strong_buy / buy / hold / sell / strong_sell
                mapping = {
                    'strong_buy': 'strong_buy', 'strongbuy': 'strong_buy',
                    'buy': 'buy', 'overweight': 'buy', 'outperform': 'buy',
                    'hold': 'hold', 'neutral': 'hold', 'market_perform': 'hold',
                    'sell': 'sell', 'underweight': 'sell', 'underperform': 'sell',
                    'strong_sell': 'strong_sell',
                }
                info['recommendationKey'] = mapping.get(raw, raw) if raw else None
        except Exception:
            pass

        # Só retorna se tiver ao menos market cap ou P/E
        if info.get('marketCap') or info.get('trailingPE'):
            return info

    except ImportError:
        pass
    except Exception:
        pass

    return None


NOMES_BDRS = {
    'A1AP34': 'Advance Auto Parts, Inc.',
    'A1DC34': 'Agree Realty Corp',
    'A1DI34': 'Analog Devices, Inc.',
    'A1EP34': 'American Electric Power Company, Inc.',
    'A1ES34': 'AES Corporation',
    'A1FL34': 'Aflac Incorporated',
    'A1IV34': 'Apartment Investment and Management Company',
    'A1KA34': 'Akamai Technologies, Inc.',
    'A1LB34': 'Albemarle Corporation',
    'A1LK34': 'Alaska Air Group, Inc.',
    'A1LL34': 'Bread Financial Holdings, Inc.',
    'A1MD34': 'Advanced Micro Devices, Inc.',
    'A1MP34': 'Ameriprise Financial, Inc.',
    'A1MT34': 'Applied Materials, Inc.',
    'A1NE34': 'Arista Networks Inc',
    'A1PH34': 'Amphenol Corporation',
    'A1PL34': 'Applied Digital Corporation',
    'A1PO34': 'Apollo Global Management Inc',
    'A1PP34': 'AppLovin Corp.',
    'A1RE34': 'Alexandria Real Estate Equities Inc',
    'A1RG34': 'argenx SE ADR',
    'A1SU34': 'Assurant, Inc.',
    'A1TH34': 'Autohome Inc. ADR',
    'A1VB34': 'AvalonBay Communities, Inc.',
    'A1WK34': 'American Water Works Co Inc',
    'A1ZN34': 'AstraZeneca PLC ADR',
    'A2MB34': 'Ambarella, Inc.',
    'A2RR34': 'Arrowhead Pharmaceuticals, Inc.',
    'A2RW34': 'Arrows Electronics Inc',
    'A2SO34': 'Academy Sports and Outdoors Inc',
    'A2XO34': 'Axon Enterprise Inc',
    'A2ZT34': 'Azenta Inc',
    'AADA39': '21Shares Ltd ETP',
    'AALL34': 'American Airlines Group Inc.',
    'AAPL34': 'Apple Inc.',
    'ABBV34': 'AbbVie, Inc.',
    'ABGD39': 'abrdn Gold ETF Trust',
    'ABTT34': 'Abbott Laboratories',
    'ABUD34': 'Anheuser-Busch InBev SA/NV ADR',
    'ACNB34': 'Accenture PLC',
    'ACWX39': 'iShares MSCI ACWI ex US ETF',
    'ADBE34': 'Adobe Inc.',
    'AIRB34': 'Airbnb, Inc.',
    'AMGN34': 'Amgen Inc.',
    'AMZO34': 'Amazon.com, Inc.',
    'APTV34': 'Aptiv PLC',
    'ARGT39': 'Global X MSCI Argentina ETF',
    'ARMT34': 'ArcelorMittal SA',
    'ARNC34': 'Howmet Aerospace Inc',
    'ASML34': 'ASML Holding NV ADR',
    'ATTB34': 'AT&T Inc',
    'AURA33': 'Aura Minerals Inc',
    'AVGO34': 'Broadcom Inc.',
    'AWII34': 'Armstrong World Industries, Inc.',
    'AXPB34': 'American Express Co',
    'B1AM34': 'Brookfield Corporation',
    'B1AX34': 'Baxter International Inc.',
    'B1BW34': 'Bath & Body Works, Inc.',
    'B1CS34': 'Barclays PLC ADR',
    'B1FC34': 'Brown-Forman Corporation',
    'B1IL34': 'Bilibili, Inc. ADR',
    'B1LL34': 'Ball Corporation',
    'B1MR34': 'Biomarin Pharmaceutical Inc.',
    'B1NT34': 'BioNTech SE ADR',
    'B1PP34': 'BP PLC',
    'B1RF34': 'Broadridge Financial Solutions, Inc.',
    'B1SA34': 'Banco Santander Chile ADR',
    'B1TI34': 'British American Tobacco PLC ADR',
    'B2AH34': 'Booz Allen Hamilton Holding Corp Class A',
    'B2HI34': 'BILL Holdings, Inc.',
    'B2LN34': 'BlackLine, Inc.',
    'B2MB34': 'Bumble, Inc.',
    'B2RK34': 'Bruker Corporation',
    'B2UR34': 'Burlington Stores, Inc.',
    'B2YN34': 'Beyond Meat, Inc.',
    'BAAX39': 'iShares MSCI All Country Asia ex Japan ETF',
    'BABA34': 'Alibaba Group Holding Limited ADR',
    'BACW39': 'iShares MSCI ACWI ETF',
    'BAER39': 'iShares U.S. Aerospace & Defense ETF',
    'BAGG39': 'iShares Core U.S. Aggregate Bond ETF',
    'BAIQ39': 'Global X Artificial Intelligence & Technology ETF',
    'BAOR39': 'iShares Core Growth Allocation ETF',
    'BARY39': 'iShares Future AI & Tech ETF',
    'BASK39': '21Shares Ltd ETP',
    'BBER39': 'JPMorgan BetaBuilders Europe Fund',
    'BBJP39': 'JPMorgan BetaBuilders Japan Fund',
    'BBUG39': 'Global X Cybersecurity ETF',
    'BCAT39': 'Global X S&P 500 Catholic Values Custom ETF',
    'BCHI39': 'iShares MSCI China ETF',
    'BCIR39': 'First Trust NASDAQ Cybersecurity ETF',
    'BCLO39': 'Global X Cloud Computing ETF',
    'BCNY39': 'iShares MSCI China A ETF',
    'BCOM39': 'iShares GSCI Commodity Dynamic Roll Strategy ETF',
    'BCPX39': 'Global X Copper Miners ETF',
    'BCSA34': 'Banco Santander SA ADR',
    'BCTE39': 'Global X CleanTech ETF',
    'BCWV39': 'iShares MSCI Global Min Vol Factor ETF',
    'BDVD39': 'Global X Superdividend U.S. ETF',
    'BDVE39': 'iShares Emerging Markets Dividend ETF',
    'BDVY39': 'iShares Select Dividend ETF',
    'BECH39': 'iShares MSCI Chile ETF',
    'BEEM39': 'iShares MSCI Emerging Markets ETF',
    'BEFA39': 'iShares MSCI EAFE ETF',
    'BEFG39': 'iShares MSCI EAFE Growth ETF',
    'BEFV39': 'iShares MSCI EAFE Value ETF',
    'BEGD39': 'iShares ESG Aware MSCI EAFE ETF',
    'BEGE39': 'iShares ESG Aware MSCI EM ETF',
    'BEGU39': 'iShares Trust iShares ESG Aware MSCI USA ETF',
    'BEIS39': 'iShares MSCI Israel ETF',
    'BEMV39': 'iShares MSCI Emerging Markets Min Vol Factor ETF',
    'BEPP39': 'iShares MSCI Pacific ex Japan ETF',
    'BEPU39': 'iShares MSCI Peru and Global Exposure ETF',
    'BERK34': 'Berkshire Hathaway Inc. B',
    'BEWA39': 'iShares MSCI Australia ETF',
    'BEWC39': 'iShares MSCI Canada ETF',
    'BEWD39': 'iShares MSCI Sweden ETF',
    'BEWG39': 'iShares MSCI Germany ETF',
    'BEWH39': 'iShares MSCI Hong Kong ETF',
    'BEWJ39': 'iShares MSCI Japan ETF',
    'BEWL39': 'iShares MSCI Switzerland ETF',
    'BEWP39': 'iShares MSCI Spain ETF',
    'BEWS39': 'iShares MSCI Singapore ETF',
    'BEWW39': 'iShares MSCI Mexico ETF',
    'BEWY39': 'iShares MSCI South Korea Capped ETF',
    'BEWZ39': 'iShares MSCI Brazil ETF',
    'BEZA39': 'iShares MSCI South Africa ETF',
    'BEZU39': 'iShares MSCI Eurozone ETF',
    'BFAV39': 'iShares MSCI EAFE Min Vol Factor ETF',
    'BFLO39': 'iShares Floating Rate Bond ETF',
    'BFXI39': 'iShares China Large-Cap ETF',
    'BGLC39': 'iShares Global 100 ETF',
    'BGOV39': 'iShares US Treasury Bond ETF',
    'BGOZ39': 'iShares 25+ Year Treasury STRIPS Bond ETF',
    'BGRT39': 'iShares Global REIT ETF',
    'BGWH39': 'iShares Core Dividend Growth ETF',
    'BHEF39': 'iShares Currency Hedged MSCI EAFE ETF',
    'BHER39': 'Global X Video Games & Esports ETF',
    'BHVN34': 'Biohaven Research Ltd',
    'BHYC39': 'iShares 0-5 Year High Yield Corporate Bond ETF',
    'BHYG39': 'iShares iBoxx USD High Yield Corporate Bond ETF',
    'BIAI39': 'iShares U.S. Broker-Dealers & Securities Exchanges ETF',
    'BIAU39': 'iShares Gold Trust',
    'BIBB39': 'iShares Biotechnology ETF',
    'BICL39': 'iShares Global Clean Energy ETF',
    'BIDU34': 'Baidu, Inc. ADR',
    'BIEF39': 'iShares Core MSCI EAFE ETF',
    'BIEI39': 'iShares 3-7 Year Treasury Bond ETF',
    'BIEM39': 'iShares Core MSCI Emerging Markets ETF',
    'BIEO39': 'iShares US Oil & Gas Exploration & Production ETF',
    'BIEU39': 'iShares Core MSCI Europe ETF',
    'BIEV39': 'iShares Europe ETF',
    'BIGF39': 'iShares Global Infrastructure ETF',
    'BIGS39': 'iShares 1-5 Year Investment Grade Corporate BondETF',
    'BIHE39': 'iShares US Pharmaceuticals ETF',
    'BIHF39': 'iShares US Healthcare Providers ETF',
    'BIHI39': 'iShares US Medical Devices ETF',
    'BIIB34': 'Biogen Inc.',
    'BIJH39': 'iShares Core S&P Mid-Cap ETF',
    'BIJR39': 'iShares Core S&P Small-Cap ETF',
    'BIJS39': 'iShares S&P Small-Cap 600 Value ETF',
    'BIJT39': 'iShares S&P Small-Cap 600 Growth ETF',
    'BILF39': 'iShares Latin America 40 ETF',
    'BIPC39': 'iShares Core MSCI Pacific ETF',
    'BIPZ39': 'PIMCO Broad US TIPS Index Exchange-Traded Fund',
    'BITB39': 'iShares US Home Construction ETF',
    'BITO39': 'iShares Core S&P Total U.S. Stock Market ETF',
    'BIUS39': 'iShares Core Total USD Bond Market ETF',
    'BIVB39': 'iShares Core S&P 500 ETF',
    'BIVE39': 'iShares S&P 500 Value ETF',
    'BIVW39': 'iShares S&P 500 Growth ETF',
    'BIWF39': 'iShares Russell 1000 Growth ETF',
    'BIWM39': 'iShares Russell 2000 ETF',
    'BIXG39': 'iShares Global Financials ETF',
    'BIXJ39': 'iShares Global Healthcare ETF',
    'BIXN39': 'iShares Global Tech ETF',
    'BIXU39': 'iShares Core MSCI Total International Stock ETF',
    'BIYE39': 'iShares US Energy ETF',
    'BIYF39': 'iShares US Financials ETF',
    'BIYJ39': 'iShares US Industrials ETF',
    'BIYT39': 'iShares 7-10 Year Treasury Bond ETF',
    'BIYW39': 'iShares US Technology ETF',
    'BIYZ39': 'iShares US Telecommunications ETF',
    'BJQU39': 'JPMorgan U.S. Quality Factor ETF',
    'BKCH39': 'Global X Blockchain ETF',
    'BKNG34': 'Booking Holdings Inc.',
    'BKWB39': 'KraneShares CSI China Internet ETF',
    'BKXI39': 'iShares Global Consumer Staples ETF',
    'BLAK34': 'BlackRock, Inc.',
    'BLBT39': 'Global X Lithium & Battery Tech ETF',
    'BLPX39': 'Global X MLP & Energy Infrastructure ETF',
    'BLQD39': 'iShares iBoxx USD Investment Grade Corporate Bond ETF',
    'BMTU39': 'iShares MSCI USA Momentum Factor ETF',
    'BMYB34': 'Bristol-Myers Squibb Company',
    'BNDA39': 'iShares MSCI India ETF',
    'BOAC34': 'Bank of America Corp',
    'BOEF39': 'iShares S&P 100 ETF',
    'BOEI34': 'Boeing Company',
    'BONY34': 'Bank of New York Mellon Corp',
    'BOTZ39': 'Global X Robotics & Artificial Intelligence ETF',
    'BOXP34': 'BXP Inc',
    'BPIC39': 'iShares MSCI Global Metals & Mining Producers ETF',
    'BPVE39': 'Global X US Infrastructure Development ETF',
    'BQQW39': 'First Trust NASDAQ-100 Equal Weighted Index Fund',
    'BQUA39': 'iShares MSCI USA Quality Factor ETF',
    'BQYL39': 'Global X NASDAQ 100 Covered Call ETF',
    'BSCZ39': 'iShares MSCI EAFE Small-Cap ETF',
    'BSDV39': 'Global X Superdividend ETF',
    'BSHV39': 'iShares Short Treasury Bond ETF',
    'BSHY39': 'iShares 1-3 Year Treasury Bond ETF',
    'BSIL39': 'Global X Silver Miners ETF',
    'BSIZ39': 'iShares MSCI USA Size Factor ETF',
    'BSLV39': 'iShares Silver Trust',
    'BSOC39': 'Global X Social Media ETF',
    'BSOX39': 'iShares Semiconductor ETF',
    'BSRE39': 'Global X SuperDividend REIT ETF',
    'BTFL39': 'iShares Treasury Floating Rate Bond ETF',
    'BTIP39': 'iShares TIPS Bond ETF',
    'BTLT39': 'iShares 20+ Year Treasury Bond ETF',
    'BURA39': 'Global X Uranium ETF',
    'BURT39': 'iShares MSCI World ETF',
    'BUSM39': 'iShares MSCI USA Minimum Volatility ETF',
    'BUSR39': 'iShares Core US REIT ETF',
    'BUTL39': 'iShares US Utilities ETF',
    'C1AB34': 'Cable One, Inc.',
    'C1AG34': 'Conagra Brands, Inc.',
    'C1AH34': 'Cardinal Health, Inc.',
    'C1BL34': 'Chubb Limited',
    'C1BR34': 'CBRE Group, Inc.',
    'C1CJ34': 'Cameco Corporation',
    'C1CL34': 'Carnival Corporation',
    'C1CO34': 'Cencora, Inc.',
    'C1DN34': 'Cadence Design Systems, Inc.',
    'C1FG34': 'Citizens Financial Group, Inc.',
    'C1GP34': 'CoStar Group, Inc.',
    'C1HR34': 'C.H.Robinson Worldwide Inc',
    'C1IC34': 'Cigna Group',
    'C1MG34': 'Chipotle Mexican Grill, Inc.',
    'C1MI34': 'Cummins Inc. (Ex. Cummins Engine Inc)',
    'C1MS34': 'CMS Energy Corporation',
    'C1NC34': 'Centene Corporation',
    'C1OO34': 'Cooper Companies, Inc.',
    'C1PB34': 'Campbell\'s Company',
    'C1RH34': 'CRH public limited company',
    'C2AC34': 'CACI International Inc',
    'C2CA34': 'Coca-Cola Femsa SAB de CV ADR',
    'C2GN34': 'Cognex Corp',
    'C2HD34': 'Churchill Downs Inc',
    'C2OI34': 'Coinbase Global, Inc.',
    'C2OL34': 'Grupo Cibest S.A. ADR',
    'C2OU34': 'Coursera Inc',
    'C2RN34': 'Cerence Inc.',
    'C2RS34': 'CRISPR Therapeutics AG',
    'C2RW34': 'CrowdStrike Holdings, Inc.',
    'C2ZR34': 'Caesars Entertainment, Inc.',
    'CAON34': 'Capital One Financial Corp',
    'CATP34': 'Caterpillar Inc',
    'CHCM34': 'Charter Communications, Inc.',
    'CHDC34': 'Church & Dwight Co., Inc.',
    'CHME34': 'CME Group Inc',
    'CHVX34': 'Chevron Corporation',
    'CLOV34': 'Clover Health Investments Corp.',
    'CLXC34': 'Clorox Co',
    'CNIC34': 'Canadian National Railway Co',
    'COCA34': 'Coca-Cola Company',
    'COLG34': 'Colgate-Palmolive Co',
    'COPH34': 'ConocoPhillips',
    'COTY34': 'Coty Inc.',
    'COWC34': 'Costco Wholesale Corporation',
    'CPRL34': 'Canadian Pacific Kansas City Limited',
    'CRIN34': 'Carter\'s Incorporated',
    'CSCO34': 'Cisco Systems, Inc.',
    'CSXC34': 'CSX Corporation',
    'CTGP34': 'Citigroup Inc.',
    'CTSH34': 'Cognizant Technology Solutions Corporation',
    'CVSH34': 'CVS Health Corp',
    'D1DG34': 'Datadog, Inc.',
    'D1EX34': 'DexCom, Inc.',
    'D1LR34': 'Digital Realty Trust, Inc.',
    'D1OC34': 'DocuSign, Inc.',
    'D1OW34': 'Dow, Inc.',
    'D1VN34': 'Devon Energy Corporation',
    'D2AR34': 'Darling Ingredients Inc',
    'D2AS34': 'DoorDash, Inc.',
    'D2NL34': 'Denali Therapeutics Inc',
    'D2OC34': 'Doximity, Inc.',
    'D2OX34': 'Amdocs Ltd',
    'D2PZ34': 'Domino\'s Pizza, Inc.',
    'DBAG34': 'Deutsche Bank AG',
    'DDNB34': 'DuPont de Nemours, Inc.',
    'DEEC34': 'Deere & Co',
    'DEFT31': 'DeFi Technologies Inc',
    'DEOP34': 'Diageo PLC ADR',
    'DGCO34': 'Dollar General Corporation',
    'DHER34': 'Danaher Corp',
    'DISB34': 'Walt Disney Company',
    'DOLL39': 'iShares 0-3 Month Treasury Bond ETF',
    'DTCR39': 'Global X Data Center REITs & Digital Infrastructure ETF',
    'DUOL34': 'Duolingo, Inc.',
    'DVAI34': 'DaVita Inc.',
    'E1CO34': 'Ecopetrol SA ADR',
    'E1DU34': 'New Oriental Education & Technology Group, Inc.',
    'E1LV34': 'Elevance Health, Inc.',
    'E1MN34': 'Eastman Chemical Company',
    'E1MR34': 'Emerson Electric Co.',
    'E1OG34': 'EOG Resources, Inc.',
    'E1QN34': 'Equinor ASA ADR',
    'E1RI34': 'Telefonaktiebolaget LM Ericsson ADR B',
    'E1TN34': 'Eaton Corp. PlcShs',
    'E1WL34': 'Edwards Lifesciences Corp',
    'E2AG34': 'EAGLE MATERIALS INC',
    'E2EF34': 'Euronet Worldwide Inc',
    'E2NP34': 'Enphase Energy, Inc.',
    'E2ST34': 'Elastic NV',
    'E2TS34': 'Etsy, Inc.',
    'EAIN34': 'Electronic Arts Inc.',
    'EBAY34': 'eBay Inc.',
    'EIDO39': 'iShares MSCI Indonesia ETF',
    'ELCI34': 'Estee Lauder Companies Inc',
    'EPHE39': 'iShares MSCI Philippines ETF',
    'EQIX34': 'Equinix Inc',
    'ETHA39': 'iShares Ethereum Trust',
    'EVEB31': 'Eve Holding Inc',
    'EVTC31': 'EVERTEC, Inc.',
    'EWJV39': 'iShares MSCI Japan Value ETF',
    'EXGR34': 'Expedia Group, Inc.',
    'EXPB31': 'Experian PLC Sponsored',
    'EXXO34': 'Exxon Mobil Corp',
    'F1AN34': 'Diamondback Energy, Inc.',
    'F1IS34': 'Fiserv, Inc.',
    'F1MC34': 'FMC Corp',
    'F1NI34': 'Fidelity National Information Services, Inc.',
    'F1SL34': 'Fastly, Inc.',
    'F1TN34': 'Fortinet, Inc.',
    'F2IC34': 'Fair Isaac Corporation',
    'F2IV34': 'Five9 Inc',
    'F2NV34': 'Franco-Nevada Corporation',
    'F2RS34': 'Freshworks, Inc.',
    'FASL34': 'Fastenal Company',
    'FCXO34': 'Freeport-McMoRan, Inc.',
    'FDMO34': 'Ford Motor Company',
    'FDXB34': 'FedEx Corporation',
    'FSLR34': 'First Solar, Inc.',
    'G1AM34': 'Gaming and Leisure Properties Inc',
    'G1AR34': 'Gartner, Inc.',
    'G1DS34': 'GDS Holdings Ltd. ADR A',
    'G1FI34': 'Gold Fields Limited',
    'G1LO34': 'Globant Sa',
    'G1LW34': 'Corning Inc',
    'G1MI34': 'General Mills, Inc.',
    'G1PI34': 'Global Payments Inc.',
    'G1RM34': 'Garmin Ltd.',
    'G1SK34': 'GSK PLC ADR',
    'G1TR39': 'abrdn Precious Metals Basket ETF Trust',
    'G1WW34': 'W.W. Grainger, Inc.',
    'G2DD34': 'GoDaddy, Inc.',
    'G2DI33': 'G2D Investments, Ltd.',
    'G2EV34': 'GE Vernova Inc',
    'GDBR34': 'General Dynamics Corp',
    'GDXB39': 'VanEck Gold Miners ETF',
    'GEOO34': 'GE Aerospace',
    'GILD34': 'Gilead Sciences, Inc',
    'GMCO34': 'General Motors Company',
    'GOGL34': 'Alphabet Inc',
    'GOGL35': 'Alphabet Inc',
    'GPRK34': 'GeoPark Ltd',
    'GPRO34': 'GoPro, Inc.',
    'GPSI34': 'Gap Inc.',
    'GROP31': 'Brazil Potash Corp',
    'GSGI34': 'Goldman Sachs Group, Inc.',
    'H1AS34': 'Hasbro, Inc.',
    'H1CA34': 'HCA Healthcare Inc',
    'H1DB34': 'HDFC Bank Limited',
    'H1II34': 'Huntington Ingalls Industries Inc',
    'H1OG34': 'Harley-Davidson Inc',
    'H1PE34': 'Hewlett Packard Enterprise Co.',
    'H1RL34': 'Hormel Foods Corporation',
    'H1SB34': 'HSBC Holdings Plc',
    'H1UM34': 'Humana Inc',
    'H2TA34': 'Healthcare Realty Trust Incorporated',
    'H2UB34': 'HubSpot, Inc.',
    'HALI34': 'Halliburton Company Shs',
    'HOME34': 'Home Depot Inc',
    'HOND34': 'Honda Motor Co., Ltd. ADR',
    'HPQB34': 'HP Inc.',
    'HYEM39': 'VanEck Emerging Markets High Yield Bond ETF',
    'I1AC34': 'IAC Inc.',
    'I1DX34': 'IDEXX Laboratories, Inc.',
    'I1EX34': 'IDEX Corporation',
    'I1FO34': 'Infosys Limited',
    'I1LM34': 'Illumina, Inc.',
    'I1NC34': 'Incyte Corporation',
    'I1PC34': 'International Paper Company',
    'I1PG34': 'IPG Photonics Corp',
    'I1QV34': 'IQVIA Holdings Inc',
    'I1QY34': 'iQIYI, Inc.',
    'I1RM34': 'Iron Mountain REIT Inc',
    'I1RP34': 'Trane Technologies plc',
    'I1SR34': 'Intuitive Surgical, Inc.',
    'I2NG34': 'Ingredion Inc',
    'I2NV34': 'Invitation Homes, Inc.',
    'IBIT39': 'IShares Bitcoin Trust',
    'IBKR34': 'Interactive Brokers Group, Inc.',
    'ICLR34': 'Icon PLC',
    'INBR32': 'Inter & Co., Inc.',
    'INTU34': 'Intuit Corp',
    'ITLC34': 'Intel Corporation',
    'J1EG34': 'Jacobs Solutions Inc.',
    'J2BL34': 'Jabil Inc.',
    'JBSS32': 'JBS N.V.',
    'JDCO34': 'JD.com, Inc. ADR',
    'JNJB34': 'Johnson & Johnson',
    'JPMC34': 'JPMorgan Chase & Co.',
    'K1BF34': 'KB Financial Group Inc',
    'K1LA34': 'KLA Corporation',
    'K1MX34': 'CarMax, Inc.',
    'K1SG34': 'Keysight Technologies, Inc.',
    'K1SS34': 'Kohl\'s Corporation',
    'K1TC34': 'KT Corporation',
    'K2CG34': 'Kingsoft Cloud Holdings Ltd. ADR',
    'KHCB34': 'Kraft Heinz Company',
    'KMBB34': 'Kimberly-Clark Corp',
    'KMIC34': 'Kinder Morgan Inc',
    'L1EG34': 'Leggett & Platt Inc',
    'L1EN34': 'Lennar Corporation',
    'L1HX34': 'L3Harris Technologies Inc',
    'L1MN34': 'Lumen Technologies, Inc.',
    'L1NC34': 'Lincoln National Corp',
    'L1RC34': 'Lam Research Corporation',
    'L1WH34': 'Lamb Weston Holdings, Inc.',
    'L1YG34': 'Lloyds Banking Group PLC',
    'L1YV34': 'Live Nation Entertainment, Inc.',
    'L2PL34': 'LPL Financial Holdings Inc',
    'L2SC34': 'Lattice Semiconductor Corp',
    'LBRD34': 'Liberty Broadband Corp.',
    'LILY34': 'Eli Lilly & Co',
    'LOWC34': 'Lowe\'s Companies Inc',
    'M1AA34': 'Mid-America Apartment Communities, Inc.',
    'M1CH34': 'Microchip Technology Incorporated',
    'M1CK34': 'McKesson Corporation',
    'M1DB34': 'MongoDB, Inc.',
    'M1HK34': 'Mohawk Industries, Inc.',
    'M1MC34': 'Marsh & McLennan Companies, Inc.',
    'M1NS34': 'Monster Beverage Corporation',
    'M1RN34': 'Moderna, Inc.',
    'M1SC34': 'MSCI Inc.',
    'M1SI34': 'Motorola Solutions, Inc.',
    'M1TA34': 'Meta Platforms Inc',
    'M1TC34': 'Match Group, Inc.',
    'M1TT34': 'Marriott International, Inc. (New)',
    'M1UF34': 'Mitsubishi UFJ Financial Group, Inc.',
    'M2KS34': 'MKS Inc',
    'M2PM34': 'MP Materials Corp',
    'M2PR34': 'Monolithic Power Systems, Inc.',
    'M2PW34': 'Medical Properties Trust, Inc.',
    'M2RV34': 'Marvell Technology, Inc.',
    'M2ST34': 'Strategy Inc',
    'MACY34': 'Macy\'s, Inc.',
    'MCDC34': 'McDonald\'s Corporation',
    'MCOR34': 'Moody\'s Corporation',
    'MDLZ34': 'Mondelez International, Inc.',
    'MDTC34': 'Medtronic plc',
    'MELI34': 'MercadoLibre, Inc.',
    'MKLC34': 'Markel Group Inc.',
    'MMMC34': '3M Company',
    'MOOO34': 'Altria Group, Inc.',
    'MOSC34': 'Mosaic Co',
    'MRCK34': 'Merck & Co., Inc.',
    'MSBR34': 'Morgan Stanley',
    'MSCD34': 'Mastercard Inc',
    'MSFT34': 'Microsoft Corp',
    'MUTC34': 'Micron Technology Inc',
    'N1BI34': 'Neurocrine Biosciences, Inc.',
    'N1CL34': 'Norwegian Cruise Line Holdings Ltd.',
    'N1DA34': 'Nasdaq, Inc.',
    'N1EM34': 'Newmont Corporation',
    'N1GG34': 'National Grid PLC',
    'N1IS34': 'Nisource Inc',
    'N1OW34': 'ServiceNow, Inc.',
    'N1RG34': 'NRG Energy, Inc.',
    'N1TA34': 'NetApp, Inc.',
    'N1UE34': 'Nucor Corporation',
    'N1VO34': 'Novo Nordisk A/S ADR B',
    'N1VR34': 'NVR, Inc.',
    'N1VS34': 'Novartis AG',
    'N1WG34': 'NatWest Group Plc',
    'N1XP34': 'NXP Semiconductors NV',
    'N2ET34': 'Cloudflare Inc',
    'N2LY34': 'Annaly Capital Management, Inc.',
    'N2TN34': 'Nutanix, Inc.',
    'N2VC34': 'NovoCure Ltd.',
    'NETE34': 'Netease Inc ADR',
    'NEXT34': 'NextEra Energy, Inc.',
    'NFLX34': 'Netflix, Inc.',
    'NIKE34': 'NIKE, Inc.',
    'NMRH34': 'Nomura Holdings, Inc. ADR',
    'NOCG34': 'Northrop Grumman Corp.',
    'NOKI34': 'Nokia Oyj',
    'NVDC34': 'NVIDIA Corporation',
    'O1DF34': 'Old Dominion Freight Line, Inc.',
    'O1KT34': 'Okta, Inc.',
    'O2HI34': 'Omega Healthcare Investors Inc',
    'O2NS34': 'ON Semiconductor Corporation',
    'ORCL34': 'Oracle Corp',
    'ORLY34': 'O\'Reilly Automotive Inc',
    'OXYP34': 'Occidental Petroleum Corp',
    'P1AC34': 'PACCAR Inc',
    'P1AY34': 'Paychex, Inc.',
    'P1DD34': 'PDD Holdings Inc. ADR A',
    'P1EA34': 'Healthpeak Properties, Inc.',
    'P1GR34': 'Progressive Corporation',
    'P1KX34': 'POSCO Holdings Inc. ADR',
    'P1LD34': 'Prologis, Inc.',
    'P1NW34': 'Pinnacle West Capital Corp',
    'P1PL34': 'PPL Corporation',
    'P1RG34': 'Perrigo Company PLC',
    'P1SX34': 'Phillips 66',
    'P2AN34': 'Palo Alto Networks, Inc.',
    'P2AT34': 'UiPath, Inc.',
    'P2AX34': 'Patria Investments Ltd.',
    'P2EG34': 'Pegasystems Inc.',
    'P2EN34': 'PENN Entertainment, Inc.',
    'P2IN34': 'Pinterest, Inc.',
    'P2LT34': 'Palantir Technologies Inc.',
    'P2ST34': 'Pure Storage, Inc.',
    'P2TC34': 'PTC Inc.',
    'PAGS34': 'PagSeguro Digital Ltd.',
    'PEPB34': 'PepsiCo, Inc.',
    'PFIZ34': 'Pfizer Inc',
    'PGCO34': 'Procter & Gamble Co',
    'PHGN34': 'Koninklijke Philips N.V. ADR',
    'PHMO34': 'Philip Morris International Inc.',
    'PNCS34': 'PNC Financial Services Group, Inc.',
    'PRXB31': 'Prosus N.V. ADR Sponsored',
    'PSKY34': 'Paramount Skydance Corporation',
    'PYPL34': 'PayPal Holdings, Inc.',
    'Q2SC34': 'QuantumScape Corporation',
    'QCOM34': 'QUALCOMM Incorporated',
    'QUBT34': 'Quantum Computing Inc',
    'R1DY34': 'Dr Reddy\'S Laboratories Ltd ADR',
    'R1EG34': 'Regency Centers Corporation',
    'R1EL34': 'RELX PLC',
    'R1HI34': 'Robert Half Inc.',
    'R1IN34': 'Realty Income Corporation',
    'R1KU34': 'Roku, Inc.',
    'R1MD34': 'ResMed Inc.',
    'R1OP34': 'Roper Technologies, Inc.',
    'R1SG34': 'Republic Services, Inc.',
    'R1YA34': 'Ryanair Holdings PLC',
    'R2BL34': 'Roblox Corp.',
    'R2NG34': 'RingCentral, Inc.',
    'R2PD34': 'Rapid7 Inc',
    'REGN34': 'Regeneron Pharmaceuticals, Inc.Shs',
    'RGTI34': 'Rigetti Computing, Inc.',
    'RIGG34': 'Transocean Ltd.',
    'RIOT34': 'Rio Tinto PLC ADR',
    'ROST34': 'Ross Stores, Inc.',
    'ROXO34': 'Nu Holdings Ltd.',
    'RSSL39': 'Global X RUSSELL 2000 ETF',
    'RYTT34': 'RTX Corporation',
    'S1BA34': 'SBA Communications Corp.',
    'S1BS34': 'Sibanye Stillwater Limited',
    'S1HW34': 'Sherwin-Williams Company',
    'S1KM34': 'SK Telecom Co., Ltd.',
    'S1LG34': 'SL Green Realty Corp.',
    'S1NA34': 'Snap-On Incorporated',
    'S1NP34': 'Synopsys, Inc.',
    'S1OU34': 'Southwest Airlines Co.',
    'S1PO34': 'Spotify Technology S.A.',
    'S1RE34': 'Sempra',
    'S1TX34': 'Seagate Technology Holdings PLC',
    'S1WK34': 'Stanley Black & Decker, Inc.',
    'S1YY34': 'Sysco Corporation',
    'S2CH34': 'Sociedad Quimica y Minera de Chile SA SOQUIMICH ADR',
    'S2EA34': 'Sea Limited ADR A',
    'S2ED34': 'SolarEdge Technologies, Inc.',
    'S2FM34': 'Sprouts Farmers Market, Inc.',
    'S2GM34': 'Sigma Lithium Corporation',
    'S2HO34': 'Shopify, Inc.',
    'S2NA34': 'Snap, Inc.',
    'S2NW34': 'Snowflake, Inc.',
    'S2TA34': 'STAG Industrial, Inc.',
    'S2UI34': 'Sun Communities, Inc.',
    'S2YN34': 'Synaptics Inc',
    'SAPP34': 'SAP SE ADR',
    'SBUB34': 'Starbucks Corporation',
    'SCHW34': 'Charles Schwab Corp',
    'SIVR39': 'abrdn Silver ETF Trust',
    'SLBG34': 'SLB Limited',
    'SLXB39': 'VanEck Steel ETF',
    'SMIN39': 'iShares MSCI India Small Cap Index Fund',
    'SNEC34': 'Sony Group Corporation ADR',
    'SOLN39': '21Shares Ltd ETP',
    'SPGI34': 'S&P Global Inc',
    'SSFO34': 'Salesforce, Inc.',
    'STMN34': 'STMicroelectronics NV ADR',
    'STOC34': 'StoneCo Ltd.',
    'STZB34': 'Constellation Brands, Inc.',
    'T1AL34': 'TAL Education Group ADR A',
    'T1AM34': 'Atlassian Corp',
    'T1EV34': 'Teva Pharmaceutical Industries Ltd',
    'T1LK34': 'PT Telkom Indonesia (Persero) TbkADR B',
    'T1MU34': 'T-Mobile US, Inc.',
    'T1OW34': 'American Tower Corporation',
    'T1RI34': 'TripAdvisor, Inc.',
    'T1SC34': 'Tractor Supply Company',
    'T1SO34': 'Southern Company',
    'T1TW34': 'Take-Two Interactive Software, Inc.',
    'T1WL34': 'Twilio, Inc.',
    'T2DH34': 'Teladoc Health, Inc.',
    'T2ER34': 'Teradyne, Inc.',
    'T2RM34': 'Trimble Inc',
    'T2TD34': 'Trade Desk, Inc.',
    'T2YL34': 'Tyler Technologies Inc',
    'TAKP34': 'Takeda Pharmaceutical Co. Ltd.',
    'TBIL39': 'Global X 1-3 Month T-Bill ETF',
    'TMCO34': 'Toyota Motor Corp ADR',
    'TMOS34': 'Thermo Fisher Scientific Inc.',
    'TOPB39': 'iShares Top 20 US Stocks ETF',
    'TPRY34': 'Tapestry Inc',
    'TRVC34': 'Travelers Companies Inc',
    'TSLA34': 'Tesla, Inc.',
    'TSMC34': 'Taiwan Semiconductor Manufacturing Co., Ltd. ADR',
    'TSNF34': 'Tyson Foods, Inc.',
    'TXSA34': 'Ternium S.A. ADR',
    'U1AI34': 'Under Armour, Inc.',
    'U1AL34': 'United Airlines Holdings, Inc.',
    'U1BE34': 'Uber Technologies, Inc.',
    'U1DR34': 'UDR, Inc.',
    'U1HS34': 'Universal Health Services, Inc.',
    'U1RI34': 'United Rentals, Inc.',
    'U2PS34': 'Upstart Holdings, Inc.',
    'U2PW34': 'Upwork, Inc.',
    'U2ST34': 'Unity Software, Inc.',
    'U2TH34': 'United Therapeutics Corporation',
    'UBSG34': 'UBS Group AG',
    'ULEV34': 'Unilever PLC ADR',
    'UNHH34': 'Unitedhealth Group Inc',
    'UPAC34': 'Union Pacific Corp',
    'USBC34': 'U.S. Bancorp',
    'V1MC34': 'Vulcan Materials Company',
    'V1NO34': 'Vornado Realty Trust',
    'V1OD34': 'Vodafone Group Public Limited Company',
    'V1RS34': 'Verisk Analytics, Inc.',
    'V1RT34': 'Vertiv Holdings LLC',
    'V1ST34': 'Vistra Corp',
    'V1TA34': 'Ventas, Inc.',
    'V2EE34': 'Veeva Systems Inc',
    'V2TX34': 'VTEX',
    'VERZ34': 'Verizon Communications Inc',
    'VISA34': 'Visa Inc.',
    'VLOE34': 'Valero Energy Corp',
    'VRSN34': 'VeriSign, Inc.',
    'W1BD34': 'Warner Bros. Discovery, Inc.',
    'W1BO34': 'Weibo Corp.',
    'W1DC34': 'Western Digital Corporation',
    'W1EL34': 'Welltower Inc.',
    'W1HR34': 'Whirlpool Corporation',
    'W1MB34': 'Williams Companies, Inc.',
    'W1MC34': 'Waste Management, Inc.',
    'W1MG34': 'Warner Music Group Corp.',
    'W1YC34': 'Weyerhaeuser Company',
    'W2ST34': 'West Pharmaceutical Services Inc',
    'W2YF34': 'Wayfair, Inc.',
    'WABC34': 'Western Alliance Bancorp',
    'WALM34': 'Walmart Inc',
    'WFCO34': 'Wells Fargo & Company',
    'WUNI34': 'Western Union Company',
    'X1YZ34': 'Block, Inc.',
    'XPBR31': 'XP Inc.',
    'XRPV39': 'Valour Inc. Structured Product',
    'Y2PF34': 'YPF SA',
    'YUMR34': 'Yum! Brands, Inc.',
    'Z1BR34': 'Zebra Technologies Corporation',
    'Z1OM34': 'Zoom Communications, Inc.',
    'Z1TA34': 'Zeta Global Holdings Corp.',
    'Z1TS34': 'Zoetis, Inc.',
    'Z2LL34': 'Zillow Group, Inc.',
    'Z2SC34': 'Zscaler, Inc.',
}

@st.cache_data(ttl=3600, show_spinner=False)
def buscar_dados_fundamentalistas(ticker_bdr):
    """
    Busca dados fundamentalistas com fallback em cascata:
    1. Yahoo Finance com ticker US mapeado (empresa mãe)
    2. Yahoo Finance com variantes do ticker (sufixo .SA removido, etc.)
    3. OpenBB / FMP com chave configurada
    4. BRAPI como último recurso
    """
    ticker_us = mapear_ticker_us(ticker_bdr)

    def _score_from_yf_info(info, fonte_label, ticker_label):
        """Processa info do yFinance e devolve dict padronizado ou None."""
        if not info or len(info) < 5:
            return None
        # Aceita mesmo sem marketCap — basta ter algum dado útil
        if not any([
            info.get('marketCap'),
            info.get('trailingPE'),
            info.get('forwardPE'),
            info.get('revenueGrowth'),
        ]):
            return None

        score = 50
        det = {}

        # P/E
        pe = info.get('trailingPE') or info.get('forwardPE')
        if pe and isinstance(pe, (int, float)):
            det['pe_ratio'] = {'valor': round(pe, 2), 'pontos': 0, 'criterio': ''}
            if 10 <= pe <= 25:   score += 15; det['pe_ratio'].update(pontos=15, criterio='Ótimo (10-25)')
            elif 5 <= pe <= 35:  score += 10; det['pe_ratio'].update(pontos=10, criterio='Bom (5-10 ou 25-35)')
            elif pe < 5:         score +=  5; det['pe_ratio'].update(pontos=5,  criterio='Baixo (<5)')
            elif pe > 50:        score -= 10; det['pe_ratio'].update(pontos=-10, criterio='Muito alto (>50)')
            else:                              det['pe_ratio']['criterio'] = 'Regular (35-50)'
        else:
            det['pe_ratio'] = {'valor': None, 'pontos': 0, 'criterio': ''}

        # Dividend Yield
        dy = info.get('dividendYield')
        if dy and isinstance(dy, (int, float)):
            det['dividend_yield'] = {'valor': dy, 'pontos': 0, 'criterio': ''}
            if dy > 0.04:   score += 10; det['dividend_yield'].update(pontos=10, criterio='Excelente (>4%)')
            elif dy > 0.02: score +=  5; det['dividend_yield'].update(pontos=5,  criterio='Bom (>2%)')
            else:                        det['dividend_yield']['criterio'] = 'Baixo (<2%)'
        else:
            det['dividend_yield'] = {'valor': None, 'pontos': 0, 'criterio': ''}

        # Revenue Growth
        rg = info.get('revenueGrowth')
        if rg and isinstance(rg, (int, float)):
            det['revenue_growth'] = {'valor': rg, 'pontos': 0, 'criterio': ''}
            if rg > 0.20:    score += 15; det['revenue_growth'].update(pontos=15,  criterio='Excelente (>20%)')
            elif rg > 0.10:  score += 10; det['revenue_growth'].update(pontos=10,  criterio='Muito bom (>10%)')
            elif rg > 0.05:  score +=  5; det['revenue_growth'].update(pontos=5,   criterio='Bom (>5%)')
            elif rg < -0.10: score -= 10; det['revenue_growth'].update(pontos=-10, criterio='Negativo (<-10%)')
            else:                         det['revenue_growth']['criterio'] = 'Estável'
        else:
            det['revenue_growth'] = {'valor': None, 'pontos': 0, 'criterio': ''}

        # Recomendação
        rec = info.get('recommendationKey', '')
        pts_rec = {'strong_buy': 10, 'buy': 5, 'hold': 0, 'sell': -5, 'strong_sell': -10}
        crit_rec = {'strong_buy': 'Compra Forte', 'buy': 'Compra', 'hold': 'Manter',
                    'sell': 'Venda', 'strong_sell': 'Venda Forte'}
        score += pts_rec.get(rec, 0)
        det['recomendacao'] = {
            'valor': rec,
            'pontos': pts_rec.get(rec, 0),
            'criterio': crit_rec.get(rec, rec.replace('_', ' ').title() if rec else ''),
        }

        # Market Cap
        mc = info.get('marketCap')
        if mc and isinstance(mc, (int, float)):
            det['market_cap'] = {'valor': mc, 'pontos': 0, 'criterio': ''}
            if mc > 1e12:    score += 10; det['market_cap'].update(pontos=10, criterio='Mega Cap (>$1T)')
            elif mc > 100e9: score +=  5; det['market_cap'].update(pontos=5,  criterio='Large Cap (>$100B)')
            elif mc > 10e9:               det['market_cap']['criterio'] = 'Mid Cap (>$10B)'
            else:                         det['market_cap']['criterio'] = 'Small Cap (<$10B)'
        else:
            det['market_cap'] = {'valor': None, 'pontos': 0, 'criterio': ''}

        score = max(0, min(100, score))

        return {
            'fonte': fonte_label,
            'ticker_fonte': ticker_label,
            'score': score,
            'detalhes': det,
            'pe_ratio':       det['pe_ratio']['valor'],
            'dividend_yield': det['dividend_yield']['valor'],
            'market_cap':     det['market_cap']['valor'],
            'revenue_growth': det['revenue_growth']['valor'],
            'recomendacao':   det['recomendacao']['valor'],
            'setor':          info.get('sector', 'N/A'),
        }

    # ------------------------------------------------------------------
    # TENTATIVA 1: Yahoo Finance — busca pelo NOME da empresa mãe
    # ------------------------------------------------------------------
    # Esta é a abordagem mais confiável: usa o nome completo da empresa
    # para encontrar o ticker correto no Yahoo Finance, independente
    # de erros no BDR_TO_US_MAP.
    try:
        nome_empresa = NOMES_BDRS.get(ticker_bdr, '')
        # Remove sufixos comuns de BDRs (ADR, PLC, Inc., Corp., etc.)
        # para melhorar a precisão da busca
        nome_limpo = nome_empresa
        for sufixo in [' ADR', ' ADS', ' Ordinary Shares', ' Class A', ' Class B',
                       ' Class C', ' A Shares', ' B Shares']:
            nome_limpo = nome_limpo.replace(sufixo, '')
        nome_limpo = nome_limpo.strip()

        if nome_limpo:
            try:
                resultado_busca = yf.Search(nome_limpo, max_results=5)
                quotes = resultado_busca.quotes if hasattr(resultado_busca, 'quotes') else []
                # Filtra apenas ações US (exchange NYSE, NASDAQ, etc.)
                tickers_encontrados = []
                for q in quotes:
                    tipo = q.get('quoteType', '')
                    exchange = q.get('exchange', '')
                    symbol = q.get('symbol', '')
                    # Aceita ações e ADRs em bolsas americanas
                    if tipo in ('EQUITY',) and '.' not in symbol and exchange in (
                        'NMS', 'NYQ', 'NGM', 'NCM', 'ASE', 'PCX', 'BTS', 'NAS', 'NYSE', 'NASDAQ'
                    ):
                        tickers_encontrados.append(symbol)

                for t in tickers_encontrados[:3]:  # testa até 3 candidatos
                    try:
                        info = yf.Ticker(t).info
                        resultado = _score_from_yf_info(info, f'Yahoo Finance — {t} ({nome_limpo})', t)
                        if resultado:
                            return resultado
                    except Exception:
                        continue
            except Exception:
                pass
    except Exception:
        pass

    # ------------------------------------------------------------------
    # TENTATIVA 2: Yahoo Finance — ticker US do mapa (fallback direto)
    # ------------------------------------------------------------------
    try:
        tickers_tentar = [ticker_us]
        if '-' in ticker_us:
            tickers_tentar.append(ticker_us.replace('-', '.'))

        for t in tickers_tentar:
            try:
                info = yf.Ticker(t).info
                resultado = _score_from_yf_info(info, f'Yahoo Finance — {t}', t)
                if resultado:
                    return resultado
            except Exception:
                continue
    except Exception:
        pass

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # TENTATIVA 3: OpenBB / FMP — empresa mãe
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    try:
        info_obb = buscar_dados_openbb(ticker_us)
        resultado = _score_from_yf_info(info_obb, f'OpenBB / FMP — {ticker_us}', ticker_us)
        if resultado:
            return resultado
    except Exception:
        pass

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # TENTATIVA 4: BRAPI — BDR na B3 (último recurso)
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    try:
        dados_brapi = buscar_dados_brapi(ticker_bdr)
        if dados_brapi:
            score, detalhes = calcular_score_brapi(dados_brapi)
            return {
                'fonte': 'BRAPI (BDR na B3)',
                'ticker_fonte': ticker_bdr,
                'score': score,
                'detalhes': detalhes,
                'pe_ratio': None,
                'dividend_yield': None,
                'market_cap': dados_brapi.get('market_cap'),
                'revenue_growth': None,
                'recomendacao': None,
                'setor': dados_brapi.get('setor', 'N/A'),
                'volume_b3': dados_brapi.get('volume'),
            }
    except Exception:
        pass

    return None

# Dicionário de nomes de BDRs (677 empresas - atualizado em 2026-02-06)

# --- FUNÇÕES ---

@st.cache_data(ttl=1800)
def buscar_dados(tickers):
    if not tickers: return pd.DataFrame()
    sa_tickers = [f"{t}.SA" for t in tickers]
    try:
        # Mantendo o método que você gosta (rápido)
        df = yf.download(sa_tickers, period=PERIODO, auto_adjust=True, progress=False, timeout=60)
        if df.empty: return pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = pd.MultiIndex.from_tuples([(c[0], c[1].replace(".SA", "")) for c in df.columns])
        return df.dropna(axis=1, how='all')
    except Exception: return pd.DataFrame()

@st.cache_data(ttl=1800)
def buscar_dados_horario(ticker):
    """
    Baixa dados de 60 minutos reais do yfinance para um único ticker.
    Limite do yfinance: até 60 dias para interval='60m'.
    Calcula indicadores básicos no índice horário.
    Retorna DataFrame com DatetimeIndex horário ou None se falhar.
    """
    try:
        df = yf.download(
            f"{ticker}.SA",
            period='60d',
            interval='60m',
            auto_adjust=True,
            progress=False,
            timeout=30
        )
        if df is None or df.empty:
            return None
        # Remove MultiIndex se existir
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.dropna(subset=['Close'])
        df = df.sort_index()
        # Calcula indicadores básicos no timeframe horário
        close = df['Close']
        df['EMA20']    = close.ewm(span=20).mean()
        df['EMA50']    = close.ewm(span=50).mean()
        df['EMA200']   = close.ewm(span=200).mean()
        delta          = close.diff()
        ganho          = delta.clip(lower=0).rolling(14).mean()
        perda          = -delta.clip(upper=0).rolling(14).mean()
        rs             = ganho / perda.replace(0, float('nan'))
        df['RSI14']    = 100 - (100 / (1 + rs))
        sma            = close.rolling(20).mean()
        std            = close.rolling(20).std()
        df['BB_Lower'] = sma - std * 2
        df['BB_Upper'] = sma + std * 2
        low14          = df['Low'].rolling(14).min()
        high14         = df['High'].rolling(14).max()
        rng            = (high14 - low14).replace(0, float('nan'))
        df['Stoch_K']  = 100 * (close - low14) / rng
        ema12          = close.ewm(span=12).mean()
        ema26          = close.ewm(span=26).mean()
        macd           = ema12 - ema26
        df['MACD_Hist']= macd - macd.ewm(span=9).mean()
        return df
    except Exception:
        return None


    """Busca os nomes das empresas diretamente do Yahoo Finance"""
    mapa_nomes = {}
    
    # Processar em lotes pequenos para não sobrecarregar
    total = len(tickers)
    
    if total > 0:
        progresso_nomes = st.progress(0, text="Buscando nomes das empresas...")
        
        for i, ticker in enumerate(tickers):
            try:
                # Atualizar progresso a cada 5 tickers
                if i % 5 == 0:
                    progresso_nomes.progress(min((i + 1) / total, 1.0), 
                                            text=f"Buscando nomes... {i+1}/{total}")
                
                ticker_yf = yf.Ticker(f"{ticker}.SA")
                info = ticker_yf.info
                
                # Tentar pegar o nome na ordem de preferência
                nome = (info.get('longName') or 
                       info.get('shortName') or 
                       ticker)
                
                mapa_nomes[ticker] = nome
            except:
                # Se falhar, usar o ticker mesmo
                mapa_nomes[ticker] = ticker
        
        progresso_nomes.empty()
    
    return mapa_nomes

def calcular_indicadores(df):
    df_calc = df.copy()
    tickers = df_calc.columns.get_level_values(1).unique()
    
    progresso = st.progress(0)
    total = len(tickers)
    
    for i, ticker in enumerate(tickers):
        progresso.progress((i + 1) / total)
        try:
            close = df_calc[('Close', ticker)]
            high = df_calc[('High', ticker)]
            low = df_calc[('Low', ticker)]
            
            # RSI 14
            delta = close.diff()
            ganho = delta.clip(lower=0).rolling(14).mean()
            perda = -delta.clip(upper=0).rolling(14).mean()
            rs = ganho / perda
            df_calc[('RSI14', ticker)] = 100 - (100 / (1 + rs))

            # ESTOCÁSTICO 14 (%K)
            lowest_low = low.rolling(window=14).min()
            highest_high = high.rolling(window=14).max()
            stoch_k = 100 * ((close - lowest_low) / (highest_high - lowest_low))
            df_calc[('Stoch_K', ticker)] = stoch_k

            # Médias e Bollinger
            df_calc[('EMA20', ticker)] = close.ewm(span=20).mean()
            df_calc[('EMA50', ticker)] = close.ewm(span=50).mean()
            df_calc[('EMA200', ticker)] = close.ewm(span=200).mean()
            sma = close.rolling(20).mean()
            std = close.rolling(20).std()
            df_calc[('BB_Lower', ticker)] = sma - (std * 2)
            df_calc[('BB_Upper', ticker)] = sma + (std * 2)

            # MACD
            ema_12 = close.ewm(span=12).mean()
            ema_26 = close.ewm(span=26).mean()
            macd = ema_12 - ema_26
            signal = macd.ewm(span=9).mean()
            df_calc[('MACD_Hist', ticker)] = macd - signal
        except: continue
            
    progresso.empty()
    return df_calc

def calcular_fibonacci(df_ticker):
    try:
        if len(df_ticker) < 50: return None
        high = df_ticker['High'].max()
        low = df_ticker['Low'].min()
        diff = high - low
        return {'61.8%': low + (diff * 0.618)} 
    except: return None

def gerar_sinal(row_ticker, df_ticker):
    sinais = []
    score = 0
    explicacoes = []  # Nova lista para explicações didáticas
    
    def classificar(s):
        if s >= 4: return "Muito Alta"
        if s >= 2: return "Alta"
        if s >= 1: return "Média"
        return "Baixa"

    try:
        close = row_ticker.get('Close')
        rsi = row_ticker.get('RSI14')
        stoch = row_ticker.get('Stoch_K')
        macd_hist = row_ticker.get('MACD_Hist')
        bb_lower = row_ticker.get('BB_Lower')
        
        # Sinais de Reversão
        if pd.notna(rsi):
            if rsi < 30:
                sinais.append("RSI Oversold")
                explicacoes.append(f"📉 RSI em {rsi:.1f} (< 30): Forte sobrevenda, possível reversão iminente")
                score += 3
            elif rsi < 40:
                sinais.append("RSI Baixo")
                explicacoes.append(f"📊 RSI em {rsi:.1f} (< 40): Sobrevenda moderada")
                score += 1
        
        if pd.notna(stoch):
            if stoch < 20:
                sinais.append("Stoch. Fundo")
                explicacoes.append(f"📉 Estocástico em {stoch:.1f} (< 20): Muito sobrevendido, reversão provável")
                score += 2
            
        if pd.notna(macd_hist) and macd_hist > 0:
            sinais.append("MACD Virando")
            explicacoes.append("🔄 MACD positivo: Momentum de alta começando")
            score += 1
            
        if pd.notna(close) and pd.notna(bb_lower):
            if close < bb_lower:
                sinais.append("Abaixo BB")
                explicacoes.append(f"⚠️ Preço abaixo da Banda de Bollinger: Sobrevenda extrema")
                score += 2
            elif close < bb_lower * 1.02:
                sinais.append("Suporte BB")
                explicacoes.append("🎯 Preço próximo da Banda Inferior: Zona de suporte")
                score += 1

        fibo = calcular_fibonacci(df_ticker)
        if fibo and (fibo['61.8%'] * 0.99 <= close <= fibo['61.8%'] * 1.01):
            sinais.append("Fibo 61.8%")
            explicacoes.append("⭐ Preço na Zona de Ouro do Fibonacci (61.8%): Ponto ideal de reversão!")
            score += 2

        return sinais, score, classificar(score), explicacoes
    except:
        return [], 0, "Indefinida", []

def analisar_oportunidades(df_calc, mapa_nomes):
    resultados = []
    tickers = df_calc.columns.get_level_values(1).unique()

    for ticker in tickers:
        try:
            df_ticker = df_calc.xs(ticker, axis=1, level=1).dropna()
            if len(df_ticker) < 50: continue

            last = df_ticker.iloc[-1]
            anterior = df_ticker.iloc[-2]
            
            preco = last.get('Close')
            preco_ant = anterior.get('Close')
            preco_open = last.get('Open')
            volume = last.get('Volume')
            
            if pd.isna(preco) or pd.isna(preco_ant): continue

            # Variações
            queda_dia = ((preco - preco_ant) / preco_ant) * 100
            gap = ((preco_open - preco_ant) / preco_ant) * 100
            
            if queda_dia >= 0: continue 

            sinais, score, classificacao, explicacoes = gerar_sinal(last, df_ticker)
            
            # I.S.
            rsi = last.get('RSI14', 50)
            stoch = last.get('Stoch_K', 50)
            is_index = ((100 - rsi) + (100 - stoch)) / 2

            # === RANKING DE LIQUIDEZ (0-10) ===
            try:
                n = min(20, len(df_ticker))
                vol_serie = df_ticker['Volume'].tail(n)
                vol_medio = vol_serie.mean()
                if pd.isna(vol_medio): vol_medio = 0

                # Gaps: dias em que abertura difere >1% do fechamento anterior
                n_gaps = 0
                for i in range(1, min(n + 1, len(df_ticker))):
                    c_ant = df_ticker['Close'].iloc[-i-1]
                    o_at  = df_ticker['Open'].iloc[-i]
                    if c_ant > 0 and abs((o_at - c_ant) / c_ant) * 100 > 1:
                        n_gaps += 1

                # Consistência: proporção de dias com volume ≥ 80% da média
                consist = sum(1 for v in vol_serie if pd.notna(v) and v >= vol_medio * 0.8) / n if n > 0 else 0

                # Score 0-100
                liq = 0
                # Volume (40 pts)
                if   vol_medio > 500000: liq += 40
                elif vol_medio > 100000: liq += 35
                elif vol_medio >  50000: liq += 30
                elif vol_medio >  10000: liq += 25
                elif vol_medio >   5000: liq += 20
                elif vol_medio >   1000: liq += 15
                elif vol_medio >    100: liq += 10
                else:                    liq += 5
                # Gaps (30 pts — menos é melhor)
                if   n_gaps == 0: liq += 30
                elif n_gaps <= 2: liq += 25
                elif n_gaps <= 5: liq += 20
                elif n_gaps <= 8: liq += 15
                elif n_gaps <=12: liq += 10
                else:             liq += 5
                # Consistência (30 pts)
                if   consist >= 0.75: liq += 30
                elif consist >= 0.50: liq += 20
                elif consist >= 0.25: liq += 10
                else:                 liq += 5

                ranking_liq = max(0, min(10, round(liq / 10)))
            except Exception:
                ranking_liq = 1

            # Tratamento de Nome
            nome_completo = mapa_nomes.get(ticker, ticker)
            
            # Se o nome completo for igual ao ticker, significa que não conseguimos o nome real
            if nome_completo == ticker:
                # Usar o ticker sem processar
                nome_curto = ticker
            else:
                # Processar o nome normalmente
                palavras = nome_completo.split()
                ignore_list = ['INC', 'CORP', 'LTD', 'S.A.', 'GMBH', 'PLC', 'GROUP', 'HOLDINGS', 'CO', 'LLC']
                palavras_uteis = [p for p in palavras if p.upper().replace('.', '').replace(',', '') not in ignore_list]
                
                if len(palavras_uteis) > 0:
                    nome_curto = " ".join(palavras_uteis[:2])
                else:
                    nome_curto = nome_completo
                    
                nome_curto = nome_curto.replace(',', '').title()

            resultados.append({
                'Ticker': ticker,
                'Empresa': nome_curto,
                'Preco': preco,
                'Volume': volume,
                'Queda_Dia': queda_dia,
                'Gap': gap,
                'IS': is_index,
                'RSI14': rsi,
                'Stoch': stoch,
                'Potencial': classificacao,
                'Score': score,
                'Sinais': ", ".join(sinais) if sinais else "-",
                'Explicacoes': explicacoes,
                'Liquidez': int(ranking_liq)
            })
        except: continue
    return resultados

def plotar_grafico(df_ticker, ticker, empresa, rsi, is_val,
                   timeframe='Diário', zoom_periods=None, tipo_grafico='Linha',
                   df_horario=None):
    """
    Plota o gráfico técnico principal com suporte a:
      - timeframe  : 'Horário (60min)' | 'Diário' | 'Semanal' | 'Mensal'
      - zoom_periods: número de barras a exibir (None = tudo)
      - tipo_grafico: 'Linha' | 'Candlestick'
      - df_horario : DataFrame com dados reais de 60min (opcional, usado no timeframe horário)
    """
    import matplotlib.dates as mdates
    from matplotlib.patches import Rectangle

    colunas_necessarias = ['Close', 'Open', 'High', 'Low', 'Volume',
                           'EMA20', 'RSI14', 'Stoch_K', 'BB_Lower', 'BB_Upper']
    colunas_opcionais   = ['EMA50', 'EMA200', 'MACD_Hist']

    # ── Seleciona e reamostrar os dados conforme timeframe ───────────────────────
    tf_label = timeframe

    if timeframe == 'Horário (60min)':
        # Usa dados reais de 60min se disponíveis, senão avisa
        if df_horario is not None and not df_horario.empty:
            colunas_pres = [c for c in colunas_necessarias + colunas_opcionais
                            if c in df_horario.columns]
            df = df_horario[colunas_pres].dropna(subset=['Close']).copy()
            tf_label = 'Horário (60min) — últimos 60 dias'
        else:
            # Fallback: dados diários dos últimos 30 pregões
            colunas_pres = [c for c in colunas_necessarias + colunas_opcionais
                            if c in df_ticker.columns]
            df = df_ticker[colunas_pres].dropna(subset=['Close']).copy()
            df = df.iloc[-30:]
            tf_label = 'Horário (60min) — dados diários (fallback)'
    else:
        colunas_pres = [c for c in colunas_necessarias + colunas_opcionais
                        if c in df_ticker.columns]
        df_full = df_ticker[colunas_pres].dropna(subset=['Close', 'EMA20']).copy()
        df_full = df_full.sort_index()

        if timeframe == 'Semanal':
            agg = {c: ('first' if c == 'Open' else 'max' if c == 'High' else
                       'min' if c == 'Low' else 'last' if c == 'Close' else
                       'sum' if c == 'Volume' else 'last') for c in colunas_pres}
            df = df_full.resample('W').agg(agg).dropna(subset=['Close'])
        elif timeframe == 'Mensal':
            agg = {c: ('first' if c == 'Open' else 'max' if c == 'High' else
                       'min' if c == 'Low' else 'last' if c == 'Close' else
                       'sum' if c == 'Volume' else 'last') for c in colunas_pres}
            df = df_full.resample('ME').agg(agg).dropna(subset=['Close'])
        else:   # Diário
            df = df_full.copy()

    # ── Aplicar Zoom ─────────────────────────────────────────────────────────────
    if zoom_periods and zoom_periods < len(df):
        df = df.iloc[-zoom_periods:].copy()

    close  = df['Close']
    ema20  = df['EMA20']  if 'EMA20'  in df.columns else None
    ema50  = df['EMA50']  if 'EMA50'  in df.columns else None
    ema200 = df['EMA200'] if 'EMA200' in df.columns else None
    datas  = df.index

    fig, axes = plt.subplots(3, 1, figsize=(12, 9), sharex=True,
                             gridspec_kw={'height_ratios': [3, 1, 1]})

    # ── Fibonacci ────────────────────────────────────────────────────────────────
    high = df['High'].max() if 'High' in df.columns else close.max()
    low  = df['Low'].min()  if 'Low'  in df.columns else close.min()
    diff = high - low

    fib_levels = {
        '0%':    high,
        '23.6%': high - diff * 0.236,
        '38.2%': high - diff * 0.382,
        '50%':   high - diff * 0.500,
        '61.8%': high - diff * 0.618,
        '78.6%': high - diff * 0.786,
        '100%':  low,
    }
    fib_colors = {
        '0%':    '#e74c3c', '23.6%': '#e67e22', '38.2%': '#f39c12',
        '50%':   '#3498db', '61.8%': '#2ecc71',
        '78.6%': '#1abc9c', '100%':  '#9b59b6',
    }

    # ── Painel 1: Preço + EMAs + Fibonacci + Bollinger ───────────────────────────
    ax1 = axes[0]

    # Bollinger (fundo, discreta)
    if 'BB_Lower' in df.columns and 'BB_Upper' in df.columns:
        ax1.fill_between(datas, df['BB_Lower'], df['BB_Upper'],
                         alpha=0.07, color='#607d8b', zorder=0)

    # Fibonacci
    for nivel, preco_fib in fib_levels.items():
        cor = fib_colors[nivel]
        ax1.axhline(preco_fib, color=cor, linestyle='--', linewidth=0.9,
                    alpha=0.55, zorder=1)
        ax1.text(datas[-1], preco_fib, f' Fib {nivel}',
                 fontsize=7.5, color=cor, va='center',
                 bbox=dict(boxstyle='round,pad=0.2', facecolor='white',
                           edgecolor=cor, alpha=0.75))

    # Zona de ouro (61.8%)
    ax1.axhspan(fib_levels['61.8%'] * 0.99, fib_levels['61.8%'] * 1.01,
                alpha=0.12, color='#2ecc71', zorder=0, label='Zona de Ouro')

    # EMAs — plotadas antes do preço para não sobrepor
    if ema20 is not None:
        ax1.plot(datas, ema20,  label='EMA20',  color='#2962FF',
                 linewidth=1.4, alpha=0.9, zorder=3)
    if ema50 is not None:
        ema50_alinhada = ema50.reindex(datas)
        ax1.plot(datas, ema50_alinhada, label='EMA50', color='#FF6D00',
                 linewidth=1.4, alpha=0.85, zorder=3)
    if ema200 is not None:
        ema200_alinhada = ema200.reindex(datas)
        ax1.plot(datas, ema200_alinhada, label='EMA200', color='#00695C',
                 linewidth=1.8, alpha=0.8, zorder=3)

    # ── Candlestick ou Linha ──────────────────────────────────────────────────
    if tipo_grafico == 'Candlestick' and 'Open' in df.columns and 'High' in df.columns and 'Low' in df.columns:
        # Largura dinâmica das velas conforme número de barras
        n_barras = len(datas)
        if n_barras <= 30:
            largura_vela = 0.6
        elif n_barras <= 60:
            largura_vela = 0.5
        elif n_barras <= 120:
            largura_vela = 0.4
        else:
            largura_vela = 0.3

        # Converter datas para números matplotlib para posicionamento
        import matplotlib.dates as mdates_inner
        datas_num = mdates_inner.date2num(datas.to_pydatetime())
        largura_dias = largura_vela  # em unidades de dias matplotlib

        for i, (data_num, row_c) in enumerate(zip(datas_num, df.itertuples())):
            op  = getattr(row_c, 'Open',  None)
            hi  = getattr(row_c, 'High',  None)
            lo  = getattr(row_c, 'Low',   None)
            cl  = getattr(row_c, 'Close', None)
            if any(v is None or (hasattr(v, '__class__') and v.__class__.__name__ == 'float' and str(v) == 'nan') for v in [op, hi, lo, cl]):
                continue
            import math
            if any(math.isnan(float(v)) for v in [op, hi, lo, cl]):
                continue

            cor_vela   = '#26a69a' if float(cl) >= float(op) else '#ef5350'
            cor_pavio  = '#26a69a' if float(cl) >= float(op) else '#ef5350'
            corpo_min  = min(float(op), float(cl))
            corpo_max  = max(float(op), float(cl))
            corpo_h    = max(corpo_max - corpo_min, 0.0001)

            # Pavio (sombra)
            ax1.plot([data_num, data_num], [float(lo), float(hi)],
                     color=cor_pavio, linewidth=0.8, zorder=4)
            # Corpo
            rect = Rectangle(
                (data_num - largura_dias / 2, corpo_min),
                largura_dias, corpo_h,
                facecolor=cor_vela, edgecolor=cor_pavio,
                linewidth=0.4, zorder=5
            )
            ax1.add_patch(rect)

        # Ponto de fechamento mais recente destacado
        ultimo_num = datas_num[-1]
        ax1.scatter([ultimo_num], [close.iloc[-1]], color='#e74c3c',
                    s=50, zorder=7)
    else:
        # Linha padrão
        ax1.plot(datas, close, label='Close', color='#1a1a2e',
                 linewidth=2.2, zorder=5)
        # Ponto de fechamento mais recente destacado
        ax1.scatter([datas[-1]], [close.iloc[-1]], color='#e74c3c',
                    s=40, zorder=6)

    # Tendência atual
    ult_close = close.iloc[-1]
    ult_ema20 = ema20.iloc[-1]
    ult_ema50  = ema50.reindex(datas).iloc[-1]  if ema50  is not None else None
    ult_ema200 = ema200.reindex(datas).iloc[-1] if ema200 is not None else None

    if ult_ema50 is not None and ult_ema200 is not None:
        if ult_close > ult_ema20 > ult_ema50 > ult_ema200:
            status = "🟢 Tendência Forte de Alta"
        elif ult_close > ult_ema20 and ult_close > ult_ema50 and ult_close > ult_ema200:
            status = "🟢 Acima das 3 EMAs"
        elif ult_close < ult_ema20 and ult_close < ult_ema50 and ult_close < ult_ema200:
            status = "🔴 Abaixo das 3 EMAs"
        else:
            status = "🟡 Tendência Mista"
    else:
        status = "🟢 Acima EMA20" if ult_close > ult_ema20 else "🔴 Abaixo EMA20"

    # Fibonacci mais próximo
    nivel_mais_proximo = min(fib_levels, key=lambda n: abs(ult_close - fib_levels[n]))

    zoom_info = f' | Zoom: {zoom_periods} barras' if zoom_periods else ''
    ax1.set_title(
        f'{ticker} - {empresa} | {tf_label} | I.S.: {is_val:.0f} | {status} | Próx. Fib: {nivel_mais_proximo}{zoom_info}',
        fontweight='bold', fontsize=10, pad=6)
    ax1.legend(loc='upper left', fontsize=7, framealpha=0.92, ncol=3)
    ax1.grid(True, alpha=0.18, zorder=0)
    ax1.set_ylabel('Preço (R$)', fontsize=9)

    # ── Painel 2: RSI ─────────────────────────────────────────────────────────────
    ax2 = axes[1]
    if 'RSI14' in df.columns:
        rsi_vals = df['RSI14'].reindex(datas)
        ax2.plot(datas, rsi_vals, color='#FF6F00', linewidth=1.5, label='RSI14')
        ax2.axhline(30, color='#F44336', linestyle='--', linewidth=1, alpha=0.7)
        ax2.axhline(70, color='#4CAF50', linestyle='--', linewidth=1, alpha=0.7)
        ax2.fill_between(datas, 0,  30, alpha=0.15, color='#F44336')
        ax2.fill_between(datas, 70, 100, alpha=0.15, color='#4CAF50')
    ax2.set_ylabel('RSI', fontsize=9)
    ax2.set_ylim(0, 100)
    ax2.grid(True, alpha=0.18)

    # ── Painel 3: Estocástico ─────────────────────────────────────────────────────
    ax3 = axes[2]
    if 'Stoch_K' in df.columns:
        stoch_vals = df['Stoch_K'].reindex(datas)
        ax3.plot(datas, stoch_vals, color='#9C27B0', linewidth=1.5, label='Stoch %K')
        ax3.axhline(20, color='#F44336', linestyle='--', linewidth=1, alpha=0.7)
        ax3.axhline(80, color='#4CAF50', linestyle='--', linewidth=1, alpha=0.7)
        ax3.fill_between(datas, 0,  20, alpha=0.15, color='#F44336')
        ax3.fill_between(datas, 80, 100, alpha=0.15, color='#4CAF50')
    ax3.set_ylabel('Stoch', fontsize=9)
    ax3.set_ylim(0, 100)
    ax3.grid(True, alpha=0.18)

    # ── Eixo X — formatação exata por timeframe ──────────────────────────────────
    ax3 = axes[2]
    n_barras = len(df)

    if timeframe == 'Horário (60min)':
        # Horas: "10h", "11h", "12h" — major = cada hora, minor = nada
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%d/%b %Hh'))
        # Intervalo adaptativo: 1h se poucos dados, senão a cada 4h ou 8h
        if n_barras <= 48:
            ax3.xaxis.set_major_locator(mdates.HourLocator(interval=2))
        elif n_barras <= 200:
            ax3.xaxis.set_major_locator(mdates.HourLocator(interval=4))
        else:
            ax3.xaxis.set_major_locator(mdates.HourLocator(interval=8))

    elif timeframe == 'Diário':
        # Dias: "11/Mai", "12/Mai", etc.
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%d/%b'))
        # Intervalo adaptativo por quantidade de barras visíveis
        if n_barras <= 30:
            ax3.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        elif n_barras <= 90:
            ax3.xaxis.set_major_locator(mdates.DayLocator(interval=5))
        elif n_barras <= 180:
            ax3.xaxis.set_major_locator(mdates.DayLocator(interval=10))
        else:
            ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
            ax3.xaxis.set_major_formatter(mdates.DateFormatter('%b/%y'))

    elif timeframe == 'Semanal':
        # Semanas: "W19/Mai", início de cada semana
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%d/%b/%y'))
        if n_barras <= 26:
            ax3.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=1))
        else:
            ax3.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=2))

    elif timeframe == 'Mensal':
        # Meses: "Jan/25", "Fev/25", etc. — um rótulo por mês
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%b/%y'))
        ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=1))

    plt.setp(ax3.xaxis.get_majorticklabels(),
             rotation=40, ha='right', fontsize=9, fontweight='600', color='#334155')
    ax3.tick_params(axis='x', which='major', pad=6, length=4, color='#94a3b8')
    ax3.set_xlabel('', fontsize=0)   # sem label redundante; o formato já é autoexplicativo

    # Grade vertical suave em todos os painéis
    for ax in axes:
        ax.xaxis.grid(True, alpha=0.10, color='#94a3b8', linestyle='-')

    plt.tight_layout(rect=[0, 0, 1, 1])
    fig.subplots_adjust(bottom=0.13)
    return fig

# Estilização
def estilizar_is(val):
    if val >= 75: return 'background-color: #d32f2f; color: white; font-weight: bold'
    elif val >= 60: return 'background-color: #ffa726; color: black'
    else: return 'color: #888888'

def estilizar_potencial(val):
    if val == 'Muito Alta': return 'background-color: #2e7d32; color: white; font-weight: bold' 
    elif val == 'Alta': return 'background-color: #66bb6a; color: black; font-weight: bold'
    elif val == 'Média': return 'background-color: #ffa726; color: black'
    elif val == 'Baixa': return 'background-color: #e0e0e0; color: black' 
    return ''

def estilizar_liquidez(val):
    """Degradê vermelho→amarelo→verde para ranking 0-10"""
    paleta = {
        0:  ('#7f0000', 'white'),
        1:  ('#c62828', 'white'),
        2:  ('#ef5350', 'white'),
        3:  ('#ff7043', 'white'),
        4:  ('#ffa726', 'black'),
        5:  ('#fdd835', 'black'),
        6:  ('#d4e157', 'black'),
        7:  ('#9ccc65', 'black'),
        8:  ('#66bb6a', 'black'),
        9:  ('#2e7d32', 'white'),
        10: ('#1b5e20', 'white'),
    }
    try:
        v = int(val)
    except Exception:
        v = 0
    bg, fg = paleta.get(v, ('#9e9e9e', 'white'))
    return (f'background-color: {bg}; color: {fg}; '
            f'font-weight: 900; font-size: 1.1em; text-align: center;')

def estilizar_fundamentalista(val):
    """Estilo para classificação fundamentalista"""
    cores = {
        '🌟': ('#1b5e20', 'white'),  # Excelente
        '✅': ('#2e7d32', 'white'),   # Bom
        '⚖️': ('#fdd835', 'black'),   # Neutro
        '⚠️': ('#ff7043', 'white'),   # Atenção
        '🔴': ('#c62828', 'white'),   # Evitar
        '—': ('#e0e0e0', 'black'),   # N/A
    }
    bg, fg = cores.get(val, ('#e0e0e0', 'black'))
    return (f'background-color: {bg}; color: {fg}; '
            f'font-weight: 900; font-size: 1.2em; text-align: center;')

# --- LAYOUT DO APP ---

# CSS customizado para aparência profissional
st.markdown("""
<style>
    /* Cabeçalho principal */
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .main-title {
        color: white;
        font-size: 2.5rem;
        font-weight: 700;
        margin: 0;
        text-align: center;
    }
    .main-subtitle {
        color: rgba(255, 255, 255, 0.9);
        font-size: 1.1rem;
        text-align: center;
        margin-top: 0.5rem;
    }
    
    /* Cards de métricas */
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #667eea;
    }
    
    /* Melhorar botões */
    .stButton > button {
        width: 100%;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        font-weight: 600;
        border: none;
        padding: 0.75rem 2rem;
        border-radius: 8px;
        transition: all 0.3s ease;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
    }
    
    /* Melhorar checkboxes */
    .stCheckbox {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    }
    
    /* Seções */
    .section-header {
        color: #667eea;
        font-size: 1.5rem;
        font-weight: 600;
        margin-top: 2rem;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #667eea;
    }
    
    /* Tabela */
    .dataframe {
        border-radius: 8px;
        overflow: hidden;
    }
    
    /* Info boxes */
    .stAlert {
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

# Cabeçalho profissional
from datetime import datetime
import pytz

# Obter data e hora do Brasil
fuso_brasil = pytz.timezone('America/Sao_Paulo')
agora = datetime.now(fuso_brasil)
data_hora_analise = agora.strftime("%d/%m/%Y às %H:%M:%S")
dia_semana = agora.strftime("%A")
dias_pt = {
    'Monday': 'Segunda-feira',
    'Tuesday': 'Terça-feira', 
    'Wednesday': 'Quarta-feira',
    'Thursday': 'Quinta-feira',
    'Friday': 'Sexta-feira',
    'Saturday': 'Sábado',
    'Sunday': 'Domingo'
}
dia_semana_pt = dias_pt.get(dia_semana, dia_semana)

st.markdown(f"""
<div class="main-header">
    <h1 class="main-title">📊 Monitor BDR - Swing Trade Pro</h1>
    <p class="main-subtitle">Análise Técnica Avançada | Rastreamento de Oportunidades em Tempo Real</p>
    <p style="color: rgba(255, 255, 255, 0.8); font-size: 0.9rem; text-align: center; margin-top: 0.5rem;">
        🕐 {dia_semana_pt}, {data_hora_analise} (Horário de Brasília)
    </p>
</div>
""", unsafe_allow_html=True)

# Barra de informações
col_info1, col_info2, col_info3 = st.columns(3)
with col_info1:
    st.markdown("**📈 Estratégia:** Reversão em Sobrevenda")
with col_info2:
    st.markdown("**🎯 Foco:** BDRs em Queda com Potencial")
with col_info3:
    st.markdown("**⏱️ Timeframe:** 6 Meses | Diário")

st.markdown("---")

# Seção educacional (expansível)
with st.expander("📚 Guia dos Indicadores - Entenda os Sinais", expanded=False):
    st.markdown("""
    ### 🎯 Índice de Sobrevenda (I.S.)
    **O que é:** Combina RSI e Estocástico para medir o nível de sobrevenda.
    - **75-100**: 🔴 Muito sobrevendido (alta probabilidade de reversão)
    - **60-75**: 🟠 Sobrevendido moderado
    - **< 60**: ⚪ Não sobrevendido
    
    ### 📉 RSI (Relative Strength Index)
    **O que é:** Mede a força do movimento de preço (0-100).
    - **< 30**: 🟢 Zona de sobrevenda (possível reversão para alta)
    - **30-70**: Zona neutra
    - **> 70**: 🔴 Zona de sobrecompra (possível reversão para baixa)
    
    ### 📊 Estocástico
    **O que é:** Compara o preço de fechamento com a faixa de preços recente.
    - **< 20**: 🟢 Muito sobrevendido (sinal de compra potencial)
    - **20-80**: Zona neutra
    - **> 80**: 🔴 Sobrecomprado (cuidado)
    
    ### 📈 MACD (Moving Average Convergence Divergence)
    **O que é:** Mostra a relação entre duas médias móveis.
    - **Virando positivo**: 🟢 Momento de alta começando
    - **Histograma crescente**: Força compradora aumentando
    
    ### 🎨 Bandas de Bollinger
    **O que é:** Envelope de volatilidade ao redor da média.
    - **Preço abaixo da banda inferior**: 🟢 Sobrevendido (possível reversão)
    - **Preço na banda superior**: 🔴 Sobrecomprado
    
    ### 🌟 Fibonacci (61.8% - Zona de Ouro)
    **O que é:** Níveis onde o preço tende a encontrar suporte/resistência.
    - **61.8%**: ⭐ Nível mais importante - alta probabilidade de reversão
    - **38.2% e 50%**: Suportes intermediários
    - **Próximo de um nível**: Atenção para possível reversão
    
    ### 📊 Médias Móveis (EMAs)
    **O que é:** Mostram a direção da tendência.
    - **Preço acima das 3 EMAs**: 🟢 Tendência de alta consolidada
    - **EMA20 > EMA50 > EMA200**: Alinhamento de alta (ideal!)
    - **Preço caindo MAS acima das EMAs**: 📈 Correção em tendência de alta (oportunidade!)

    ### 🖥️ Estratégia Triple Screen (Alexander Elder, 1986)
    **O que é:** Método de 3 camadas publicado por Elder na *Futures Magazine* em 1986. Combina indicadores de tendência com osciladores em timeframes diferentes, eliminando os pontos fracos de cada um. A metáfora: negocie *com* a maré, não contra ela.

    **As 3 Telas (adaptadas para dados diários):**
    - 🌊 **1ª Tela — A Maré (EMA13 + MACD 12,26,9):** Elder usa a **inclinação da EMA13 semanal** como filtro principal de tendência. Como nosso monitor só tem dados diários, usamos a EMA13 diária. O MACD(12,26,9) confirma. EMA13 subindo + MACD positivo = maré de alta. Só opere na direção da maré.
    - 🌀 **2ª Tela — A Onda (EFI 2):** Elder recomenda o **Force Index(2)** ou outro oscilador (Stoch, Williams %R) no timeframe intermediário. Identifica *correções* dentro da tendência maior. Em uptrend: aguarde o EFI cair para sobrevenda → oportunidade de compra. Em downtrend: aguarde sobrecompra → oportunidade de venda.
    - 🎯 **3ª Tela — A Execução (Buy/Sell Stop):** Sem indicador — pura ação do preço. Em setup de compra: coloque um Buy Stop 1 tick acima da máxima anterior. Se o mercado subir e acionar o stop, você entra. Se continuar caindo, a ordem não é executada. Stop-Loss na mínima recente.

    **Sinal completo de compra:** EMA13 subindo + EFI em sobrevenda + Buy Stop acionado  
    **Sinal completo de venda:** EMA13 caindo + EFI em sobrecompra + Sell Stop acionado

    ### 💡 Como Usar Este Monitor
    1. **Filtre** por EMAs para encontrar correções em tendências de alta
    2. **Procure** I.S. alto (>75) = forte sobrevenda
    3. **Confirme** com RSI < 30 e Estocástico < 20
    4. **Verifique** se está próximo de Fibonacci 61.8%
    5. **Aplique o Triple Screen:** veja se a maré e a onda estão alinhadas
    6. **Entre** somente quando as 3 telas confirmarem! 🚀
    """)

st.markdown("---")

if st.button("🔄 Atualizar Análise", type="primary"):
    with st.spinner("Conectando à API e baixando dados..."):
        # Usar dicionário local de BDRs em vez de buscar da BRAPI
        lista_bdrs = list(NOMES_BDRS.keys())
        
        df = buscar_dados(lista_bdrs)
        
        if df.empty:
            st.error("Erro ao carregar dados. Se o Yahoo tiver bloqueado, aguarde alguns minutos.")
            st.stop()
        
    # Calcular indicadores
    with st.spinner("Calculando indicadores técnicos..."):
        df_calc = calcular_indicadores(df)
        
    # Analisar oportunidades usando dicionário local
    with st.spinner("Analisando oportunidades..."):
        oportunidades = analisar_oportunidades(df_calc, NOMES_BDRS)
        
        if oportunidades:
            # Atualizar os nomes nas oportunidades (já processados em analisar_oportunidades)
            # Salvar no session_state
            st.session_state['oportunidades'] = oportunidades
            st.session_state['df_calc'] = df_calc

# Verificar se há dados no session_state
if 'oportunidades' in st.session_state and 'df_calc' in st.session_state:
    oportunidades = st.session_state['oportunidades']
    df_calc = st.session_state['df_calc']
    
    # Criar DataFrame das oportunidades
    df_res = pd.DataFrame(oportunidades)
    df_res = df_res.sort_values(by='Queda_Dia', ascending=True)
    
    st.success(f"✅ {len(oportunidades)} oportunidades detectadas!")
    
    # --- FILTROS COM DESIGN PROFISSIONAL ---
    st.markdown('<h3 class="section-header">🎯 Filtros de Tendência</h3>', unsafe_allow_html=True)
    
    st.markdown("""
    <div style='background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); 
                padding: 1rem; border-radius: 8px; margin-bottom: 1rem;'>
        <p style='margin: 0; color: #334155; font-weight: 500;'>
            💡 <strong>Dica:</strong> Selecione as médias móveis para filtrar BDRs em correção dentro de tendências de alta
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    col_filtro1, col_filtro2, col_filtro3 = st.columns(3)
    
    with col_filtro1:
        filtrar_ema20 = st.checkbox(
            "📈 Acima da EMA20", 
            value=False,
            help="Preço acima da EMA20 (curto prazo)"
        )
    
    with col_filtro2:
        filtrar_ema50 = st.checkbox(
            "📊 Acima da EMA50", 
            value=False,
            help="Preço acima da EMA50 (médio prazo)"
        )
    
    with col_filtro3:
        filtrar_ema200 = st.checkbox(
            "📉 Acima da EMA200", 
            value=False,
            help="Preço acima da EMA200 (longo prazo)"
        )

    # Slider de liquidez
    st.markdown("**💧 Liquidez mínima:**")
    ranking_min_liq = st.slider(
        "0 = sem filtro  |  10 = máxima exigência",
        min_value=0, max_value=10, value=0, step=1,
        help="Filtra BDRs pelo ranking de liquidez 0-10. Quanto maior, menor o risco de gaps e volume baixo."
    )
    
    # Aplicar filtros se algum selecionado
    if filtrar_ema20 or filtrar_ema50 or filtrar_ema200 or ranking_min_liq > 0:
        df_res_filtrado = []
        contadores = {'ema20': 0, 'ema50': 0, 'ema200': 0, 'sem_dados': 0}
        
        for opp in oportunidades:
            ticker = opp['Ticker']
            try:
                df_ticker = df_calc.xs(ticker, axis=1, level=1).dropna()
                
                # Verificar tamanho mínimo
                tam = len(df_ticker)
                if tam < 20:
                    contadores['sem_dados'] += 1
                    continue
                
                ultimo_close = df_ticker['Close'].iloc[-1]
                
                # Verificar cada condição separadamente
                passa_filtro = True
                
                # Filtro EMA20
                if filtrar_ema20:
                    if 'EMA20' in df_ticker.columns and tam >= 20:
                        ultima_ema20 = df_ticker['EMA20'].iloc[-1]
                        if pd.notna(ultima_ema20) and ultimo_close > ultima_ema20:
                            contadores['ema20'] += 1
                        else:
                            passa_filtro = False
                    else:
                        passa_filtro = False
                
                # Filtro EMA50
                if filtrar_ema50 and passa_filtro:
                    if 'EMA50' in df_ticker.columns and tam >= 50:
                        ultima_ema50 = df_ticker['EMA50'].iloc[-1]
                        if pd.notna(ultima_ema50) and ultimo_close > ultima_ema50:
                            contadores['ema50'] += 1
                        else:
                            passa_filtro = False
                    else:
                        passa_filtro = False
                
                # Filtro EMA200
                if filtrar_ema200 and passa_filtro:
                    # EMA200 precisa de pelo menos 50 períodos para ser significativa
                    if 'EMA200' in df_ticker.columns and tam >= 50:
                        ultima_ema200 = df_ticker['EMA200'].iloc[-1]
                        if pd.notna(ultima_ema200) and ultimo_close > ultima_ema200:
                            contadores['ema200'] += 1
                        else:
                            passa_filtro = False
                    else:
                        passa_filtro = False

                # Filtro de Liquidez
                if ranking_min_liq > 0 and passa_filtro:
                    if opp.get('Liquidez', 0) < ranking_min_liq:
                        passa_filtro = False
                
                # Adicionar se passou em todos os filtros
                if passa_filtro:
                    df_res_filtrado.append(opp)
                    
            except Exception as e:
                contadores['sem_dados'] += 1
                continue
        
        if df_res_filtrado:
            df_res = pd.DataFrame(df_res_filtrado)
            df_res = df_res.sort_values(by='Queda_Dia', ascending=True)
            
            # Mensagem personalizada com estatísticas
            filtros_ativos = []
            if filtrar_ema20:
                filtros_ativos.append(f"EMA20 ({contadores['ema20']} ✓)")
            if filtrar_ema50:
                filtros_ativos.append(f"EMA50 ({contadores['ema50']} ✓)")
            if filtrar_ema200:
                filtros_ativos.append(f"EMA200 ({contadores['ema200']} ✓)")
            
            st.markdown(f"""
            <div style='background: linear-gradient(135deg, #d4fc79 0%, #96e6a1 100%); 
                        padding: 1rem; border-radius: 8px; margin: 1rem 0;'>
                <p style='margin: 0; color: #166534; font-weight: 600; font-size: 1.1rem;'>
                    ✅ {len(df_res)} BDRs encontradas | Filtros ativos: {' + '.join(filtros_ativos)}
                </p>
            </div>
            """, unsafe_allow_html=True)
        else:
            # Mostrar estatísticas de debug
            filtros_ativos = []
            if filtrar_ema20:
                filtros_ativos.append(f"EMA20 ({contadores['ema20']} acima)")
            if filtrar_ema50:
                filtros_ativos.append(f"EMA50 ({contadores['ema50']} acima)")
            if filtrar_ema200:
                filtros_ativos.append(f"EMA200 ({contadores['ema200']} acima)")
            
            st.markdown(f"""
            <div style='background: linear-gradient(135deg, #ffeaa7 0%, #fdcb6e 100%); 
                        padding: 1rem; border-radius: 8px; margin: 1rem 0;'>
                <p style='margin: 0; color: #7c3626; font-weight: 600;'>
                    ⚠️ Nenhuma BDR passou em TODOS os filtros combinados
                </p>
                <p style='margin: 0.5rem 0 0 0; color: #7c3626; font-size: 0.9rem;'>
                    📊 {' | '.join(filtros_ativos)} | {contadores['sem_dados']} sem dados suficientes
                </p>
            </div>
            """, unsafe_allow_html=True)
            df_res = pd.DataFrame()  # DataFrame vazio
    
    if not df_res.empty:
        # --- TABELA INTERATIVA ---
        st.markdown('<h3 class="section-header">📊 Oportunidades Detectadas</h3>', unsafe_allow_html=True)
        
        st.markdown("""
        <div style='background: #f8fafc; padding: 0.75rem; border-radius: 6px; margin-bottom: 1rem; border-left: 4px solid #667eea;'>
            <p style='margin: 0; color: #475569; font-size: 0.95rem;'>
                💡 <strong>Dica:</strong> Clique em qualquer linha da tabela para visualizar o gráfico técnico completo
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        evento = st.dataframe(
            df_res.style.map(estilizar_potencial, subset=['Potencial'])
                        .map(estilizar_is, subset=['IS'])
                        .map(estilizar_liquidez, subset=['Liquidez'])
            .format({
                'Preco': 'R$ {:.2f}',
                'Volume': '{:,.0f}',
                'Queda_Dia': '{:.2f}%',
                'Gap': '{:.2f}%',
                'IS': '{:.0f}',
                'RSI14': '{:.0f}',
                'Stoch': '{:.0f}',
                'Liquidez': '{:.0f}'
            }),
            column_order=("Ticker", "Empresa", "Liquidez", "Preco", "Queda_Dia", "IS", "Volume", "Gap", "Potencial", "Score", "Sinais"),
            column_config={
                "Empresa": st.column_config.TextColumn("Empresa", width="medium"),
                "Liquidez": st.column_config.NumberColumn("💧 Liq.", width="small",
                    help="Ranking de Liquidez 0-10 (🔴 baixa → 🟢 alta)"),
                "IS": st.column_config.NumberColumn("I.S.", help="Índice de Sobrevenda"),
                "Volume": st.column_config.NumberColumn("Vol.", help="Volume Financeiro"),
                "Score": st.column_config.ProgressColumn("Força", format="%d", min_value=0, max_value=10),
                "Potencial": st.column_config.Column("Sinal"),
                "Sinais": st.column_config.TextColumn("Sinais Técnicos", width="large")
            },
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row"
        )
        
        # --- GRÁFICO INTERATIVO ---
        if evento.selection and evento.selection.rows:
            st.markdown("---")
            linha_selecionada = evento.selection.rows[0]
            row = df_res.iloc[linha_selecionada]
            ticker = row['Ticker']
            
            st.markdown(f'<h3 class="section-header">📈 Análise Técnica: {ticker} - {row["Empresa"]}</h3>', unsafe_allow_html=True)
            
            try:
                df_ticker = df_calc.xs(ticker, axis=1, level=1).dropna()
                
                # ── Controles do gráfico ─────────────────────────────────────────
                st.markdown("**⚙️ Configurações do Gráfico:**")
                ctrl_col1, ctrl_col2, ctrl_col3, ctrl_col4, ctrl_col5 = st.columns([2, 1, 1, 1, 1])

                with ctrl_col1:
                    timeframe_sel = st.radio(
                        "📅 Timeframe",
                        options=['Horário (60min)', 'Diário', 'Semanal', 'Mensal'],
                        index=1,
                        horizontal=True,
                        key=f"tf_{ticker}"
                    )

                with ctrl_col2:
                    tipo_graf = st.selectbox(
                        "📊 Tipo",
                        options=['Linha', 'Candlestick'],
                        index=0,
                        key=f"tipo_{ticker}"
                    )

                with ctrl_col3:
                    zoom_opcoes = {
                        'Tudo': None,
                        '3M':  63,
                        '1M':  21,
                        '15d': 15,
                        '5d':  5,
                    }
                    zoom_label = st.selectbox(
                        "🔍 Período visível",
                        options=list(zoom_opcoes.keys()),
                        index=0,
                        key=f"zoom_sel_{ticker}"
                    )
                    zoom_val = zoom_opcoes[zoom_label]

                with ctrl_col4:
                    if st.button("🔎+  Zoom In", key=f"zin_{ticker}"):
                        current_zoom = st.session_state.get(f'zoom_custom_{ticker}', None)
                        if current_zoom is None:
                            st.session_state[f'zoom_custom_{ticker}'] = max(10, len(df_calc.xs(ticker, axis=1, level=1).dropna()) // 2)
                        else:
                            st.session_state[f'zoom_custom_{ticker}'] = max(10, int(current_zoom * 0.6))

                with ctrl_col5:
                    if st.button("🔍−  Zoom Out", key=f"zout_{ticker}"):
                        current_zoom = st.session_state.get(f'zoom_custom_{ticker}', None)
                        if current_zoom is None:
                            pass
                        else:
                            new_zoom = int(current_zoom * 1.6)
                            total = len(df_calc.xs(ticker, axis=1, level=1).dropna())
                            if new_zoom >= total:
                                st.session_state[f'zoom_custom_{ticker}'] = None
                            else:
                                st.session_state[f'zoom_custom_{ticker}'] = new_zoom

                # Zoom: dropdown tem prioridade; botões ajustam dentro do dropdown "Tudo"
                zoom_final = zoom_val
                if zoom_val is None and f'zoom_custom_{ticker}' in st.session_state:
                    zoom_final = st.session_state[f'zoom_custom_{ticker}']
                elif zoom_val is not None:
                    # Resetar zoom customizado quando usuário escolhe período fixo
                    st.session_state.pop(f'zoom_custom_{ticker}', None)

                # Layout: gráfico maior à esquerda, info à direita
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    # Busca dados horários reais quando necessário
                    df_horario = None
                    if timeframe_sel == 'Horário (60min)':
                        with st.spinner('Carregando dados horários (60min)...'):
                            df_horario = buscar_dados_horario(ticker)
                        if df_horario is None:
                            st.caption('⚠️ Dados horários indisponíveis — usando fallback diário.')

                    fig = plotar_grafico(df_ticker, ticker, row['Empresa'], row['RSI14'], row['IS'],
                                         timeframe=timeframe_sel,
                                         zoom_periods=zoom_final,
                                         tipo_grafico=tipo_graf,
                                         df_horario=df_horario)
                    st.pyplot(fig)
                
                with col2:
                    potencial = row['Potencial']
                    
                    # Card de potencial
                    if "Alta" in potencial:
                        cor_bg = "linear-gradient(135deg, #d4fc79 0%, #96e6a1 100%)"
                        cor_texto = "#166534"
                        icone = "🟢"
                    elif "Média" in potencial:
                        cor_bg = "linear-gradient(135deg, #ffeaa7 0%, #fdcb6e 100%)"
                        cor_texto = "#7c3626"
                        icone = "🟡"
                    else:
                        cor_bg = "linear-gradient(135deg, #dfe6e9 0%, #b2bec3 100%)"
                        cor_texto = "#2d3436"
                        icone = "⚪"
                    
                    st.markdown(f"""
                    <div style='background: {cor_bg}; padding: 1rem; border-radius: 8px; margin-bottom: 1rem;'>
                        <h2 style='margin: 0; color: {cor_texto}; text-align: center;'>
                            {icone} {potencial}
                        </h2>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    st.metric("💰 Preço Atual", f"R$ {row['Preco']:.2f}")
                    st.metric("📉 Queda no Dia", f"{row['Queda_Dia']:.2f}%", delta_color="inverse")
                    st.metric("🎯 I.S. (Sobrevenda)", f"{row['IS']:.0f}/100")
                    
                    if row['Gap'] < -1:
                        st.metric("⚡ Gap de Abertura", f"{row['Gap']:.2f}%", delta_color="inverse")
                    
                    st.markdown(f"**⭐ Score:** {row['Score']}/10")
                    st.markdown(f"**📊 Volume:** {row['Volume']:,.0f}")
                    
                    # Sinais técnicos
                    st.markdown("""
                    <div style='background: #e0e7ff; padding: 0.75rem; border-radius: 6px; margin-top: 1rem;'>
                        <p style='margin: 0; font-weight: 600; color: #3730a3; font-size: 0.9rem;'>
                            📋 Sinais Detectados
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
                    st.markdown(f"<p style='font-size: 0.85rem; color: #475569;'>{row['Sinais']}</p>", unsafe_allow_html=True)
                    
                    # Explicações didáticas
                    if 'Explicacoes' in row and row['Explicacoes']:
                        st.markdown("""
                        <div style='background: #fef3c7; padding: 0.75rem; border-radius: 6px; margin-top: 1rem;'>
                            <p style='margin: 0; font-weight: 600; color: #92400e; font-size: 0.9rem;'>
                                💡 O que isso significa?
                            </p>
                        </div>
                        """, unsafe_allow_html=True)
                        for explicacao in row['Explicacoes']:
                            st.markdown(f"<p style='font-size: 0.82rem; color: #92400e; margin: 0.3rem 0;'>• {explicacao}</p>", unsafe_allow_html=True)
                    
            except Exception as e:
                st.error(f"❌ Erro ao carregar gráfico: {e}")

            # === ESTRATÉGIA TRIPLE SCREEN ===
            st.markdown("---")
            try:
                df_ticker_ts = df_calc.xs(ticker, axis=1, level=1).dropna()
                resultado_ts = analisar_triple_screen(df_ticker_ts)
            except Exception:
                resultado_ts = None
            renderizar_triple_screen(resultado_ts, ticker, row['Empresa'])

            # === PAINEL FUNDAMENTALISTA (ABAIXO DO GRÁFICO) ===
            st.markdown("---")
            st.markdown('<h3 class="section-header">📊 Análise Fundamentalista</h3>', unsafe_allow_html=True)
            
            with st.spinner(f"Buscando dados fundamentalistas de {ticker}..."):
                fund_data = buscar_dados_fundamentalistas(ticker)
            
            if fund_data:
                # Card com score em porcentagem
                score = fund_data['score']
                fonte = fund_data.get('fonte', 'Yahoo Finance')
                ticker_fonte = fund_data.get('ticker_fonte', ticker)
                
                if score >= 80:
                    cor_fundo = "linear-gradient(135deg, #d4fc79 0%, #96e6a1 100%)"
                    cor_texto = "#166534"
                    label = "EXCELENTE"
                elif score >= 65:
                    cor_fundo = "linear-gradient(135deg, #a7f3d0 0%, #6ee7b7 100%)"
                    cor_texto = "#065f46"
                    label = "BOM"
                elif score >= 50:
                    cor_fundo = "linear-gradient(135deg, #fde047 0%, #fbbf24 100%)"
                    cor_texto = "#92400e"
                    label = "NEUTRO"
                elif score >= 35:
                    cor_fundo = "linear-gradient(135deg, #fdcb6e 0%, #ff7043 100%)"
                    cor_texto = "#7c3626"
                    label = "ATENÇÃO"
                else:
                    cor_fundo = "linear-gradient(135deg, #ef5350 0%, #c62828 100%)"
                    cor_texto = "white"
                    label = "EVITAR"
                
                st.markdown(f"""
                <div style='background: {cor_fundo}; padding: 1.5rem; border-radius: 12px; margin-bottom: 1.5rem;'>
                    <div style='text-align: center;'>
                        <h1 style='margin: 0; color: {cor_texto}; font-size: 4rem; font-weight: 900;'>{score:.0f}%</h1>
                        <p style='margin: 0.5rem 0 0 0; color: {cor_texto}; font-size: 1.5rem; font-weight: 600;'>
                            {label}
                        </p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                # Fonte dos dados
                if 'BRAPI' in fonte:
                    st.info(f"📡 **Fonte:** {fonte} | Ticker: **{ticker_fonte}**\n\n⚠️ *Dados limitados disponíveis para esta BDR. Score baseado em Market Cap e Volume na B3.*")
                else:
                    st.success(f"📡 **Fonte:** {fonte} | Ticker US: **{ticker_fonte}**")
                
                # Métricas em colunas
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown("### 📈 Valuation")
                    if fund_data.get('pe_ratio'):
                        st.metric("P/E Ratio", f"{fund_data['pe_ratio']:.2f}")
                    else:
                        st.metric("P/E Ratio", "N/A")
                    
                    if fund_data.get('market_cap'):
                        mcap_b = fund_data['market_cap'] / 1e9
                        if mcap_b >= 1000:
                            st.metric("Market Cap", f"${mcap_b/1000:.2f}T")
                        else:
                            st.metric("Market Cap", f"${mcap_b:.1f}B")
                    else:
                        st.metric("Market Cap", "N/A")
                
                with col2:
                    st.markdown("### 💰 Rentabilidade")
                    if fund_data.get('dividend_yield'):
                        st.metric("Dividend Yield", f"{fund_data['dividend_yield']*100:.2f}%")
                    else:
                        st.metric("Dividend Yield", "N/A")
                    
                    if fund_data.get('revenue_growth'):
                        growth = fund_data['revenue_growth'] * 100
                        st.metric("Crescimento Receita", f"{growth:+.1f}%",
                                 delta=f"{growth:.1f}%" if growth > 0 else None)
                    elif fund_data.get('volume_b3'):
                        st.metric("Volume B3", f"{fund_data['volume_b3']:,.0f}")
                    else:
                        st.metric("Crescimento Receita", "N/A")
                
                with col3:
                    st.markdown("### 🎯 Info")
                    rec = fund_data.get('recomendacao')
                    if rec and rec != 'N/A':
                        rec_map = {
                            'strong_buy': ('🟢 COMPRA FORTE', 'green'),
                            'buy': ('🟢 Compra', 'green'),
                            'hold': ('🟡 Manter', 'orange'),
                            'sell': ('🔴 Venda', 'red'),
                            'strong_sell': ('🔴 VENDA FORTE', 'red'),
                        }
                        rec_texto, rec_cor = rec_map.get(rec, (rec.upper(), 'gray'))
                        st.markdown(f"**Analistas:**")
                        st.markdown(f"<h3 style='color: {rec_cor}; margin: 0;'>{rec_texto}</h3>", unsafe_allow_html=True)
                    
                    if fund_data.get('setor') and fund_data['setor'] != 'N/A':
                        st.markdown(f"**Setor:**")
                        st.markdown(f"<p style='font-size: 1.1rem; margin: 0;'>{fund_data['setor']}</p>", unsafe_allow_html=True)
                    else:
                        st.markdown("**Setor:** N/A")
                
                # Detalhamento da Pontuação
                st.markdown("---")
                st.markdown("### 📋 Detalhamento da Pontuação")
                
                detalhes = fund_data.get('detalhes', {})
                
                # Criar tabela de detalhamento
                dados_tabela = []
                
                # Verificar se tem dados BRAPI ou Yahoo
                if 'fonte' in detalhes and 'BRAPI' in detalhes['fonte'].get('valor', ''):
                    # Dados da BRAPI
                    fonte_det = detalhes.get('fonte', {})
                    dados_tabela.append({
                        'Métrica': 'Fonte de Dados',
                        'Valor': fonte_det.get('valor', 'BRAPI'),
                        'Pontos': '-',
                        'Avaliação': fonte_det.get('criterio', 'Dados da B3')
                    })
                    
                    mcap_det = detalhes.get('market_cap', {})
                    if mcap_det.get('valor'):
                        mcap_val = mcap_det['valor'] / 1e9
                        if mcap_val >= 1000:
                            valor_str = f"${mcap_val/1000:.2f}T"
                        else:
                            valor_str = f"${mcap_val:.1f}B"
                        dados_tabela.append({
                            'Métrica': 'Market Cap',
                            'Valor': valor_str,
                            'Pontos': f"{mcap_det['pontos']:+d}/20",
                            'Avaliação': mcap_det.get('criterio', '-')
                        })
                    
                    vol_det = detalhes.get('volume', {})
                    if vol_det.get('valor'):
                        dados_tabela.append({
                            'Métrica': 'Volume B3',
                            'Valor': f"{vol_det['valor']:,.0f}",
                            'Pontos': f"{vol_det['pontos']:+d}/10",
                            'Avaliação': vol_det.get('criterio', '-')
                        })
                else:
                    # Dados do Yahoo Finance (completos)
                    pe_det = detalhes.get('pe_ratio', {})
                    if pe_det.get('valor'):
                        dados_tabela.append({
                            'Métrica': 'P/E Ratio',
                            'Valor': f"{pe_det['valor']:.2f}",
                            'Pontos': f"{pe_det['pontos']:+d}/15",
                            'Avaliação': pe_det.get('criterio', '-')
                        })
                    
                    div_det = detalhes.get('dividend_yield', {})
                    if div_det.get('valor'):
                        dados_tabela.append({
                            'Métrica': 'Dividend Yield',
                            'Valor': f"{div_det['valor']*100:.2f}%",
                            'Pontos': f"{div_det['pontos']:+d}/10",
                            'Avaliação': div_det.get('criterio', '-')
                        })
                    
                    rev_det = detalhes.get('revenue_growth', {})
                    if rev_det.get('valor') is not None:
                        dados_tabela.append({
                            'Métrica': 'Crescimento Receita',
                            'Valor': f"{rev_det['valor']*100:+.1f}%",
                            'Pontos': f"{rev_det['pontos']:+d}/15",
                            'Avaliação': rev_det.get('criterio', '-')
                        })
                    
                    rec_det = detalhes.get('recomendacao', {})
                    if rec_det.get('valor'):
                        dados_tabela.append({
                            'Métrica': 'Recomendação Analistas',
                            'Valor': rec_det['valor'].replace('_', ' ').title(),
                            'Pontos': f"{rec_det['pontos']:+d}/10",
                            'Avaliação': rec_det.get('criterio', '-')
                        })
                    
                    mcap_det = detalhes.get('market_cap', {})
                    if mcap_det.get('valor'):
                        mcap_val = mcap_det['valor'] / 1e9
                        if mcap_val >= 1000:
                            valor_str = f"${mcap_val/1000:.2f}T"
                        else:
                            valor_str = f"${mcap_val:.1f}B"
                        dados_tabela.append({
                            'Métrica': 'Market Cap',
                            'Valor': valor_str,
                            'Pontos': f"{mcap_det['pontos']:+d}/10",
                            'Avaliação': mcap_det.get('criterio', '-')
                        })
                
                if dados_tabela:
                    df_detalhes = pd.DataFrame(dados_tabela)
                    st.dataframe(
                        df_detalhes,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Métrica": st.column_config.TextColumn("Métrica", width="medium"),
                            "Valor": st.column_config.TextColumn("Valor Atual", width="small"),
                            "Pontos": st.column_config.TextColumn("Pontos", width="small"),
                            "Avaliação": st.column_config.TextColumn("Avaliação", width="medium"),
                        }
                    )
                    
                    st.caption(f"**Score Total:** {score:.0f}/100 (Base: 50 + Bônus/Penalidades)")
                else:
                    st.warning("Não há detalhes disponíveis para esta análise.")
                
            else:
                st.warning(f"⚠️ Não foi possível obter dados fundamentalistas para {ticker}")
                ticker_us = mapear_ticker_us(ticker)
                st.info(f"""
                💡 **Por que isso acontece?**
                
                - Ticker BDR: `{ticker}`
                - Ticker US mapeado: `{ticker_us}`
                
                **Tentativas realizadas:**
                1. ❌ Yahoo Finance (empresa mãe) - Sem dados
                2. ❌ OpenBB / FMP (empresa mãe) - Sem dados
                3. ❌ BRAPI (BDR na B3) - Sem dados
                
                **Possíveis causas:**
                - BDR muito nova ou com baixíssimo volume
                - Ticker não listado ou delisted
                - Dados ainda não disponíveis nas APIs públicas
                
                **Solução:** Infelizmente este ticker não possui dados fundamentalistas disponíveis nas fontes consultadas.
                """)

            # === MÓDULO DE MACHINE LEARNING ===
            st.markdown("---")
            # Usa wrapper cacheado — só re-treina se o ticker mudar
            resultado_ml = _prever_preco_ml_cached(ticker, dias_previsao=5)
            renderizar_painel_ml(resultado_ml, ticker, row['Empresa'], dias_previsao=5)

            # === MÓDULO DE REINFORCEMENT LEARNING ===
            resultado_rl = _executar_agente_rl_cached(ticker, episodes=8, window_size=5)
            renderizar_painel_rl(resultado_rl, ticker, row['Empresa'])

            # === ANÁLISE DE FASE — METODOLOGIA MINERVINI ===
            resultado_mv = _calcular_minervini_cached(ticker)
            renderizar_painel_minervini(resultado_mv, ticker, row['Empresa'])

            # === SEÇÃO DE NOTÍCIAS ===
            st.markdown("---")
            st.markdown('<h3 class="section-header">📰 Últimas Notícias da Empresa</h3>', unsafe_allow_html=True)

            ticker_us_news    = mapear_ticker_us(ticker)
            empresa_nome_news = ticker_us_news
            setor_news        = ''
            if fund_data:
                empresa_nome_news = fund_data.get('nome', ticker_us_news) or ticker_us_news
                setor_news        = fund_data.get('setor', '') or ''

            # Cabeçalho compacto com info + botão atualizar
            hc1, hc2 = st.columns([4, 1])
            with hc1:
                st.markdown(
                    f"🔎 **{empresa_nome_news}** &nbsp;·&nbsp; `{ticker_us_news}`"
                    + (f" &nbsp;·&nbsp; *{setor_news}*" if setor_news else ""),
                    unsafe_allow_html=True
                )
            with hc2:
                if st.button("🔄 Atualizar", key=f"btn_news_{ticker}", use_container_width=True):
                    buscar_noticias_com_traducao.clear()

            with st.spinner("Buscando notícias em paralelo..."):
                noticias_lista = buscar_noticias_com_traducao(ticker_us_news, empresa_nome_news)

            if noticias_lista:
                # Contagem por fonte
                from collections import Counter
                contagem_fontes = Counter(n['fonte'] for n in noticias_lista)
                fontes_str = ' · '.join(
                    f"{f} ({c})" for f, c in contagem_fontes.most_common()
                )
                st.caption(f"✅ {len(noticias_lista)} notícias · {fontes_str} · 🌐 Traduzidas para português")

                # Cards em 2 colunas
                col_n1, col_n2 = st.columns(2)
                metade = (len(noticias_lista) + 1) // 2
                with col_n1:
                    for noticia in noticias_lista[:metade]:
                        st.markdown(_renderizar_card_noticia(noticia), unsafe_allow_html=True)
                with col_n2:
                    for noticia in noticias_lista[metade:]:
                        st.markdown(_renderizar_card_noticia(noticia), unsafe_allow_html=True)

                # Links rápidos
                st.markdown("---")
                st.markdown("**🔗 Acompanhe nas fontes originais:**")
                lc = st.columns(5)
                links_fontes = [
                    ("📊 Yahoo Finance", f"https://finance.yahoo.com/quote/{ticker_us_news}/news/"),
                    ("🌐 Google News",   f"https://news.google.com/search?q={ticker_us_news}+stock&hl=pt-BR"),
                    ("📈 Seeking Alpha", f"https://seekingalpha.com/symbol/{ticker_us_news}/news"),
                    ("🔍 Finviz",        f"https://finviz.com/quote.ashx?t={ticker_us_news}"),
                    ("🧙 GuruFocus",     f"https://www.gurufocus.com/news/{ticker_us_news}"),
                ]
                for col, (label, url) in zip(lc, links_fontes):
                    col.markdown(f"[{label}]({url})")
            else:
                st.warning(
                    f"⚠️ Nenhuma notícia encontrada para **{empresa_nome_news}** (`{ticker_us_news}`). "
                    f"Acesse diretamente nas fontes abaixo:"
                )
                lc2 = st.columns(5)
                links_fontes = [
                    ("📊 Yahoo Finance", f"https://finance.yahoo.com/quote/{ticker_us_news}/news/"),
                    ("🌐 Google News",   f"https://news.google.com/search?q={ticker_us_news}+stock&hl=pt-BR"),
                    ("📈 Seeking Alpha", f"https://seekingalpha.com/symbol/{ticker_us_news}/news"),
                    ("🔍 Finviz",        f"https://finviz.com/quote.ashx?t={ticker_us_news}"),
                    ("🧙 GuruFocus",     f"https://www.gurufocus.com/news/{ticker_us_news}"),
                ]
                for col, (label, url) in zip(lc2, links_fontes):
                    col.markdown(f"[{label}]({url})")

        else:
            st.markdown("""
            <div style='background: linear-gradient(135deg, #e0e7ff 0%, #c7d2fe 100%); 
                        padding: 2rem; border-radius: 8px; text-align: center; margin: 2rem 0;'>
                <p style='margin: 0; color: #3730a3; font-size: 1.1rem; font-weight: 500;'>
                    👆 Selecione uma BDR na tabela acima para visualizar a análise técnica completa
                </p>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #ffeaa7 0%, #fdcb6e 100%); 
                    padding: 2rem; border-radius: 8px; text-align: center;'>
            <h3 style='margin: 0; color: #7c3626;'>📊 Nenhuma oportunidade detectada</h3>
            <p style='margin: 0.5rem 0 0 0; color: #7c3626;'>
                Aguarde novas oportunidades ou ajuste os critérios de filtro
            </p>
        </div>
        """, unsafe_allow_html=True)

# Rodapé profissional
st.markdown("---")
st.markdown("""
<div style='text-align: center; padding: 2rem 0; color: #64748b;'>
    <p style='margin: 0; font-size: 0.9rem;'>
        <strong>Monitor BDR - Swing Trade Pro</strong> | Powered by Python, yFinance & Streamlit
    </p>
    <p style='margin: 0.5rem 0 0 0; font-size: 0.8rem;'>
        ⚠️ Este sistema é apenas para fins educacionais. Não constitui recomendação de investimento.
    </p>
</div>
""", unsafe_allow_html=True)
