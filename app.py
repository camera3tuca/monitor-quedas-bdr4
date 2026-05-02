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

PERIODO = "1y"  # Mantido por compatibilidade
TERMINACOES_BDR = ('31', '32', '33', '34', '35', '39')

# Token BRAPI para dados alternativos
BRAPI_TOKEN = "iExnKM1xcbQcYL3cNPhPQ3"  

# =============================================================================
# FUNÇÕES DE BUSCA E TRADUÇÃO DE NOTÍCIAS
# =============================================================================

def _limpar_html(texto):
    if not texto:
        return ""
    texto = re.sub(r'<[^>]+>', '', texto)
    texto = html_lib.unescape(texto)
    return texto.strip()

def _formatar_data(pub_raw):
    try:
        dt = datetime.strptime(pub_raw, '%a, %d %b %Y %H:%M:%S %z')
        return dt.strftime('%d/%m/%Y %H:%M')
    except Exception:
        return pub_raw

def _traduzir_com_mymemory(textos):
    if not textos:
        return textos
    traduzidos = []
    for texto in textos:
        if not texto or not texto.strip():
            traduzidos.append(texto)
            continue
        try:
            resp = requests.get(
                "https://api.mymemory.translated.net/get",
                params={"q": texto[:500], "langpair": "en|pt-br"},
                timeout=6
            )
            if resp.status_code == 200:
                data = resp.json()
                traducao = data.get("responseData", {}).get("translatedText", "")
                if traducao and traducao.upper() != texto.upper():
                    traduzidos.append(traducao)
                else:
                    traduzidos.append(texto)
            else:
                traduzidos.append(texto)
        except Exception:
            traduzidos.append(texto)
    return traduzidos

def _buscar_yahoo_rss(ticker_us, max_noticias=8):
    noticias = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
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
                    'descricao': _limpar_html(item.findtext('description', ''))[:280],
                    'fonte': 'Yahoo Finance'
                })
            if noticias:
                break
        except Exception:
            continue
    return noticias

def _buscar_gurufocus_rss(ticker_us, max_noticias=6):
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
                'descricao': _limpar_html(item.findtext('description', ''))[:280],
                'fonte': 'GuruFocus'
            })
    except Exception:
        pass
    return noticias

def _buscar_seekingalpha_rss(ticker_us, max_noticias=6):
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
                    'descricao': _limpar_html(item.findtext('description', ''))[:280],
                    'fonte': 'Seeking Alpha'
                })
            if noticias:
                break
        except Exception:
            continue
    return noticias

def _buscar_finviz(ticker_us, max_noticias=6):
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

def buscar_noticias_com_traducao(ticker_us):
    todas = []
    todas += _buscar_yahoo_rss(ticker_us, max_noticias=8)
    if len(todas) < 4:
        todas += _buscar_gurufocus_rss(ticker_us, max_noticias=6)
    if len(todas) < 6:
        todas += _buscar_seekingalpha_rss(ticker_us, max_noticias=6)
    if len(todas) < 4:
        todas += _buscar_finviz(ticker_us, max_noticias=6)

    vistos, unicas = set(), []
    for n in todas:
        chave = n['titulo'].lower()[:60]
        if chave not in vistos:
            vistos.add(chave)
            unicas.append(n)
    unicas = unicas[:12]

    if not unicas:
        return []

    titulos_orig = [n['titulo'] for n in unicas]
    titulos_trad = _traduzir_com_mymemory(titulos_orig)
    for n, t in zip(unicas, titulos_trad):
        n['titulo'] = t

    idx_desc = [(i, n['descricao']) for i, n in enumerate(unicas) if n.get('descricao')]
    if idx_desc:
        indices, descs = zip(*idx_desc)
        descs_trad = _traduzir_com_mymemory(list(descs))
        for i, d in zip(indices, descs_trad):
            unicas[i]['descricao'] = d

    return unicas

def _renderizar_card_noticia(noticia):
    titulo  = noticia.get('titulo', '')
    link    = noticia.get('link', '#')
    data    = noticia.get('data', '')
    desc    = noticia.get('descricao', '')
    fonte   = noticia.get('fonte', '')
    cores = {
        'Yahoo Finance': ('#eff6ff', '#1d4ed8', '#dbeafe'),
        'Seeking Alpha': ('#f0fdf4', '#15803d', '#dcfce7'),
        'GuruFocus':     ('#fefce8', '#854d0e', '#fef9c3'),
        'Finviz':        ('#fdf4ff', '#7e22ce', '#f3e8ff'),
    }
    bg, cor_fonte, badge_bg = cores.get(fonte, ('#f8fafc', '#475569', '#e2e8f0'))
    desc_html = (
        f"<p style='margin:0.4rem 0 0 0;font-size:0.82rem;color:#64748b;"
        f"line-height:1.4;'>{desc}</p>"
    ) if desc else ""
    return f"""
    <div style='background:{bg};border:1px solid {badge_bg};border-radius:10px;
                padding:1rem 1.1rem;margin-bottom:0.75rem;'>
        <div style='display:flex;justify-content:space-between;
                    align-items:flex-start;gap:0.5rem;'>
            <a href="{link}" target="_blank"
               style='font-size:0.92rem;font-weight:600;color:#1e293b;
                      text-decoration:none;line-height:1.35;flex:1;'>{titulo}</a>
            <span style='background:{badge_bg};color:{cor_fonte};font-size:0.70rem;
                         font-weight:700;padding:0.15rem 0.55rem;border-radius:999px;
                         white-space:nowrap;margin-left:0.5rem;'>{fonte}</span>
        </div>
        {desc_html}
        <p style='margin:0.5rem 0 0 0;font-size:0.75rem;color:#94a3b8;'>🕐 {data}</p>
    </div>"""

# =============================================================================
# MÓDULO DE MACHINE LEARNING — PREVISÃO DE PREÇOS (ISOLADO)
# =============================================================================

def prever_preco_ml(df_ticker, ticker, dias_previsao=5):
    try:
        from sklearn.linear_model import LinearRegression
        from sklearn.preprocessing import MinMaxScaler
        import numpy as np

        df = df_ticker.copy()

        colunas_necessarias = ['Close', 'EMA20', 'RSI14']
        for col in colunas_necessarias:
            if col not in df.columns:
                return {'erro': f'Coluna {col} não encontrada nos dados.'}

        df = df[['Close', 'EMA20', 'RSI14'] +
                (['EMA50'] if 'EMA50' in df.columns else [])].copy()
        df = df.dropna()

        if len(df) < 60:
            return {'erro': 'Dados insuficientes para treinar o modelo (mín. 60 dias).'}

        df['Retorno']  = df['Close'].pct_change()          
        df['Volatil']  = df['Close'].pct_change().rolling(10).std()  
        df['EMA_Dist'] = (df['Close'] - df['EMA20']) / df['EMA20']  

        df['Target'] = df['Close'].shift(-1)
        df = df.dropna()

        feature_cols = ['Close', 'EMA20', 'RSI14', 'Retorno', 'Volatil', 'EMA_Dist']
        if 'EMA50' in df.columns:
            feature_cols.append('EMA50')

        X = df[feature_cols].values
        y = df['Target'].values

        scaler_X = MinMaxScaler()
        scaler_y = MinMaxScaler()
        X_sc = scaler_X.fit_transform(X)
        y_sc = scaler_y.fit_transform(y.reshape(-1, 1)).ravel()

        split    = int(len(X_sc) * 0.8)
        X_train  = X_sc[:split];  y_train = y_sc[:split]
        X_test   = X_sc[split:];  y_test  = y_sc[split:]

        modelo = LinearRegression()
        modelo.fit(X_train, y_train)
        confianca = max(0.0, float(modelo.score(X_test, y_test)))

        ultimo = df.iloc[-1]
        preco_cur  = float(ultimo['Close'])
        ema20_cur  = float(ultimo['EMA20'])
        rsi_cur    = float(ultimo['RSI14'])
        ret_cur    = float(ultimo['Retorno'])
        vol_cur    = float(ultimo['Volatil'])
        ema50_cur  = float(ultimo['EMA50']) if 'EMA50' in df.columns else ema20_cur

        alpha20 = 2 / (20 + 1)
        alpha50 = 2 / (50 + 1)
        previsoes = []

        for _ in range(dias_previsao):
            ema_dist = (preco_cur - ema20_cur) / ema20_cur if ema20_cur else 0

            row_feats = [preco_cur, ema20_cur, rsi_cur, ret_cur, vol_cur, ema_dist]
            if 'EMA50' in df.columns:
                row_feats.append(ema50_cur)

            entrada_sc = scaler_X.transform(np.array([row_feats]))
            prev_sc    = modelo.predict(entrada_sc)
            preco_prev = float(scaler_y.inverse_transform(prev_sc.reshape(-1, 1))[0][0])
            previsoes.append(round(preco_prev, 2))

            ret_cur    = (preco_prev - preco_cur) / preco_cur if preco_cur else 0
            vol_cur    = vol_cur * 0.9 + abs(ret_cur) * 0.1   
            ema20_cur  = alpha20 * preco_prev + (1 - alpha20) * ema20_cur
            ema50_cur  = alpha50 * preco_prev + (1 - alpha50) * ema50_cur
            delta      = preco_prev - preco_cur
            ganho      = max(delta, 0);  perda = max(-delta, 0)
            rsi_cur    = min(max(rsi_cur + (ganho - perda) / (preco_cur + 1e-9) * 30, 0), 100)
            preco_cur  = preco_prev

        variacao_pct = ((previsoes[-1] - float(df.iloc[-1]['Close'])) /
                        float(df.iloc[-1]['Close'])) * 100

        if   variacao_pct >  1.5: direcao = "ALTA"
        elif variacao_pct < -1.5: direcao = "BAIXA"
        else:                     direcao = "LATERAL"

        return {
            'erro'        : None,
            'previsoes'   : previsoes,
            'direcao'     : direcao,
            'variacao_pct': round(variacao_pct, 2),
            'confianca'   : round(confianca * 100, 1),
            'ultimo_preco': round(float(df.iloc[-1]['Close']), 2),
        }

    except ImportError:
        return {'erro': 'scikit-learn não instalado. Adicione scikit-learn ao requirements.txt.'}
    except Exception as e:
        return {'erro': f'Erro no modelo: {str(e)}'}


def renderizar_painel_ml(resultado_ml, ticker, empresa, dias_previsao=5):
    with st.expander("🤖 Previsão por Inteligência Artificial (Machine Learning)", expanded=False):

        if resultado_ml.get('erro'):
            st.warning(f"⚠️ {resultado_ml['erro']}")
            return

        direcao   = resultado_ml['direcao']
        variacao  = resultado_ml['variacao_pct']
        confianca = resultado_ml['confianca']
        previsoes = resultado_ml['previsoes']
        ult_preco = resultado_ml['ultimo_preco']

        st.markdown("""
        <div style='background:linear-gradient(135deg,#1e1b4b 0%,#312e81 100%);
                    padding:1rem 1.2rem;border-radius:10px;margin-bottom:1rem;'>
            <p style='margin:0;color:#c7d2fe;font-size:0.82rem;line-height:1.6;'>
                🧠 <strong style='color:#a5b4fc;'>Como funciona:</strong>
                Um modelo de <strong>Regressão Linear</strong> foi treinado agora mesmo
                com os dados históricos deste ativo, usando 6 variáveis:
                <em>Preço de Fechamento, EMA20, EMA50, RSI14,
                Retorno Diário e Volatilidade (10d)</em>.
                A previsão é feita de forma iterativa — a saída de D+1 alimenta D+2, e assim por diante.
                <br><br>
                ⚠️ <strong style='color:#fbbf24;'>Aviso:</strong>
                Previsões de ML são estimativas estatísticas, não garantias.
                Use como <u>um dos critérios</u> da sua análise, nunca como único sinal.
            </p>
        </div>""", unsafe_allow_html=True)

        cfg = {
            "ALTA"   : ("#d4fc79","#96e6a1","#14532d","🚀","ALTA PREVISTA"),
            "BAIXA"  : ("#fca5a5","#ef4444","#7f1d1d","📉","BAIXA PREVISTA"),
            "LATERAL": ("#fde047","#fbbf24","#78350f","➡️","LATERAL PREVISTA"),
        }
        bg1, bg2, cor_txt, icone, label = cfg[direcao]
        col_dir, col_conf, col_var = st.columns(3)

        with col_dir:
            st.markdown(f"""
            <div style='background:linear-gradient(135deg,{bg1} 0%,{bg2} 100%);
                        padding:1.2rem;border-radius:10px;text-align:center;height:110px;
                        display:flex;flex-direction:column;justify-content:center;'>
                <div style='font-size:2rem;'>{icone}</div>
                <div style='font-weight:800;font-size:1.05rem;color:{cor_txt};'>{label}</div>
                <div style='font-size:0.78rem;color:{cor_txt};margin-top:0.2rem;'>
                    próximos {dias_previsao} dias</div>
            </div>""", unsafe_allow_html=True)

        with col_conf:
            cor_c = "#15803d" if confianca >= 60 else "#b45309" if confianca >= 40 else "#b91c1c"
            nivel = "Boa" if confianca >= 60 else "Moderada" if confianca >= 40 else "Baixa"
            st.markdown(f"""
            <div style='background:#f8fafc;border:2px solid #e2e8f0;padding:1.2rem;
                        border-radius:10px;text-align:center;height:110px;
                        display:flex;flex-direction:column;justify-content:center;'>
                <div style='font-size:1.9rem;font-weight:800;color:{cor_c};'>{confianca:.0f}%</div>
                <div style='font-size:0.8rem;color:#64748b;margin-top:0.2rem;'>
                    Confiança ({nivel})<br>
                    <span style='font-size:0.7rem;'>(R² no conjunto de teste)</span>
                </div>
            </div>""", unsafe_allow_html=True)

        with col_var:
            sinal_v = "+" if variacao >= 0 else ""
            cor_v   = "#15803d" if variacao > 1.5 else "#b91c1c" if variacao < -1.5 else "#b45309"
            st.markdown(f"""
            <div style='background:#f8fafc;border:2px solid #e2e8f0;padding:1.2rem;
                        border-radius:10px;text-align:center;height:110px;
                        display:flex;flex-direction:column;justify-content:center;'>
                <div style='font-size:1.9rem;font-weight:800;color:{cor_v};'>
                    {sinal_v}{variacao:.2f}%</div>
                <div style='font-size:0.8rem;color:#64748b;margin-top:0.2rem;'>
                    Variação Prevista<br>
                    <span style='font-size:0.7rem;'>(D0 → D+{dias_previsao})</span>
                </div>
            </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

        todos_precos = [ult_preco] + previsoes
        todos_labels = ["Hoje"] + [f"D+{i+1}" for i in range(dias_previsao)]
        cor_linha = "#16a34a" if direcao == "ALTA" else "#dc2626" if direcao == "BAIXA" else "#d97706"

        y_min = min(todos_precos) * 0.985
        y_max = max(todos_precos) * 1.015
        if (y_max - y_min) < ult_preco * 0.01:
            y_min = ult_preco * 0.992
            y_max = ult_preco * 1.008

        fig, ax = plt.subplots(figsize=(7, 3.2))
        fig.patch.set_facecolor('#f8fafc')
        ax.set_facecolor('#f8fafc')

        xs = list(range(len(todos_precos)))

        ax.fill_between(xs, todos_precos, y_min,
                        alpha=0.18, color=cor_linha)
        ax.plot(xs, todos_precos,
                color=cor_linha, linewidth=2.5,
                marker='o', markersize=6,
                markerfacecolor='white',
                markeredgecolor=cor_linha, markeredgewidth=2,
                zorder=3)
        ax.scatter([0], [ult_preco], color='#6366f1', s=110, zorder=5)
        ax.axhline(ult_preco, color='#94a3b8',
                   linestyle='--', linewidth=1, alpha=0.5)

        margem = (y_max - y_min)
        for i, p in enumerate(todos_precos):
            offset_y = margem * 0.12
            ax.annotate(
                f'R${p:.2f}',
                xy=(i, p),
                xytext=(0, offset_y),
                textcoords='offset points',
                ha='center', va='bottom',
                fontsize=7.5, color='#1e293b', fontweight='600',
            )

        ax.set_ylim(y_min, y_max + margem * 0.35)   
        ax.set_xticks(xs)
        ax.set_xticklabels(todos_labels, fontsize=8.5, color='#475569')
        ax.set_ylabel('Preço (R$)', fontsize=8, color='#64748b')
        ax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda v, _: f'R${v:.2f}'))
        ax.tick_params(axis='y', labelsize=7.5, colors='#64748b')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#e2e8f0')
        ax.spines['bottom'].set_color('#e2e8f0')
        ax.set_title(
            f'Previsão ML — {ticker}  ({empresa})',
            fontsize=9, color='#334155', pad=10)
        plt.tight_layout()
        st.pyplot(fig)
        plt.close(fig)

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

        st.markdown("""
        <div style='margin-top:1rem;padding:0.7rem 1rem;background:#f1f5f9;
                    border-radius:8px;font-size:0.76rem;color:#64748b;'>
            📐 <strong>Confiança (R²):</strong>
            &nbsp;🟢 ≥ 60% = Boa &nbsp;|&nbsp;
            🟡 40–60% = Moderada &nbsp;|&nbsp;
            🔴 &lt; 40% = Baixa — use com cautela
        </div>""", unsafe_allow_html=True)

# =============================================================================
# ESTRATÉGIA TRIPLE SCREEN DE ALEXANDER ELDER
# =============================================================================

def analisar_triple_screen(df_ticker):
    try:
        close  = df_ticker['Close'].dropna()
        volume = df_ticker['Volume'].dropna()

        if len(close) < 30:
            return None

        ema13 = close.ewm(span=13, adjust=False).mean()
        ema13_slope = ema13.iloc[-1] - ema13.iloc[-3]  

        ema12       = close.ewm(span=12, adjust=False).mean()
        ema26       = close.ewm(span=26, adjust=False).mean()
        macd_line   = ema12 - ema26
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist   = macd_line - macd_signal
        macd_val    = macd_hist.iloc[-1]
        macd_slope  = macd_hist.iloc[-1] - macd_hist.iloc[-2]

        ema13_val   = ema13.iloc[-1]
        preco_ult   = close.iloc[-1]

        alta_confirmada  = (ema13_slope > 0) and (macd_val > 0 or macd_slope > 0)
        baixa_confirmada = (ema13_slope < 0) and (macd_val < 0 or macd_slope < 0)

        pct_dist = ((preco_ult - ema13_val) / ema13_val) * 100 

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

        idx_comum = close.index.intersection(volume.index)
        close_c   = close.loc[idx_comum]
        volume_c  = volume.loc[idx_comum]
        efi_bruto = close_c.diff() * volume_c
        efi2      = efi_bruto.ewm(span=2, adjust=False).mean()
        efi2_val  = efi2.iloc[-1]

        efi2_std  = efi2.std()
        limiar_pos = efi2_std * 0.5   
        limiar_neg = -efi2_std * 0.5  

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
            'serie_close': close.iloc[-60:],
            'serie_macd':  ema13.iloc[-60:],      
            'serie_efi2':  efi2.iloc[-60:],
            'limiar_pos':  limiar_pos,
            'limiar_neg':  limiar_neg,
            'maxima_rec':  round(maxima_rec, 2),
            'minima_rec':  round(minima_rec, 2),
        }

    except Exception:
        return None

def renderizar_triple_screen(resultado, ticker, empresa):
    with st.expander("🖥️ Estratégia Triple Screen — Alexander Elder", expanded=False):

        if resultado is None:
            st.warning("⚠️ Dados insuficientes para calcular o Triple Screen.")
            return

        veredicto = resultado['veredicto']
        forca     = resultado['forca']
        t1        = resultado['tela1']
        t2        = resultado['tela2']
        t3        = resultado['tela3']

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

                fig_mini, ax_m = plt.subplots(figsize=(3.2, 1.6))
                fig_mini.patch.set_facecolor(bg_s)
                ax_m.set_facecolor(bg_s)

                if num == "1ª":
                    xs   = range(len(serie_close))
                    pr   = serie_close.values
                    em13 = serie_macd.values   
                    cor_ema = brd_s
                    ax_m.plot(xs, pr,   color='#607d8b', linewidth=1.0,
                              alpha=0.6, label='Preço')
                    ax_m.plot(xs, em13, color=cor_ema, linewidth=2.0,
                              label='EMA13', zorder=3)
                    ax_m.fill_between(xs, pr, em13,
                                      where=(pr >= em13), alpha=0.15,
                                      color='#43a047', interpolate=True)
                    ax_m.fill_between(xs, pr, em13,
                                      where=(pr < em13),  alpha=0.15,
                                      color='#e53935', interpolate=True)
                    ax_m.set_title("EMA13 (Maré)", fontsize=7, color=txt_s, pad=3)

                elif num == "2ª":
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

                st.markdown(f"""
                <div style='background:{bg_s};border:1.5px solid {brd_s};
                            border-top:none;border-radius:0 0 10px 10px;
                            height:6px;margin-top:-4px;'></div>
                """, unsafe_allow_html=True)

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
