import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
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

PERIODO = "1y"
TERMINACOES_BDR = ('31', '32', '33', '34', '35', '39')
BRAPI_TOKEN = "iExnKM1xcbQcYL3cNPhPQ3"

# =============================================================================
# TIMEFRAMES DISPONÍVEIS PARA O GRÁFICO
# =============================================================================
TIMEFRAME_MAP = {
    "5 min":   {"interval": "5m",  "period": "5d",  "label": "5 Minutos (últimos 5 dias)"},
    "60 min":  {"interval": "60m", "period": "60d", "label": "60 Minutos (últimos 60 dias)"},
    "Diário":  {"interval": "1d",  "period": "1y",  "label": "Diário (1 ano)"},
    "Semanal": {"interval": "1wk", "period": "2y",  "label": "Semanal (2 anos)"},
    "Mensal":  {"interval": "1mo", "period": "5y",  "label": "Mensal (5 anos)"},
}

# =============================================================================
# NOTÍCIAS
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
    headers = {'User-Agent': 'Mozilla/5.0'}
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
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        resp = requests.get(f"https://www.gurufocus.com/news/rss/{ticker_us}", headers=headers, timeout=8)
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
    headers = {'User-Agent': 'Mozilla/5.0'}
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
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        resp = requests.get(f"https://finviz.com/quote.ashx?t={ticker_us}", headers=headers, timeout=10)
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
                'titulo': titulo, 'link': link,
                'data': datas[i] if i < len(datas) else '',
                'descricao': '', 'fonte': 'Finviz'
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
    titulo = noticia.get('titulo', '')
    link   = noticia.get('link', '#')
    data   = noticia.get('data', '')
    desc   = noticia.get('descricao', '')
    fonte  = noticia.get('fonte', '')
    cores  = {
        'Yahoo Finance': ('#eff6ff', '#1d4ed8', '#dbeafe'),
        'Seeking Alpha': ('#f0fdf4', '#15803d', '#dcfce7'),
        'GuruFocus':     ('#fefce8', '#854d0e', '#fef9c3'),
        'Finviz':        ('#fdf4ff', '#7e22ce', '#f3e8ff'),
    }
    bg, cor_fonte, badge_bg = cores.get(fonte, ('#f8fafc', '#475569', '#e2e8f0'))
    desc_html = (
        f"<p style='margin:0.4rem 0 0 0;font-size:0.82rem;color:#64748b;line-height:1.4;'>{desc}</p>"
    ) if desc else ""
    return f"""
    <div style='background:{bg};border:1px solid {badge_bg};border-radius:10px;
                padding:1rem 1.1rem;margin-bottom:0.75rem;'>
        <div style='display:flex;justify-content:space-between;align-items:flex-start;gap:0.5rem;'>
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
# MACHINE LEARNING
# =============================================================================

def prever_preco_ml(df_ticker, ticker, dias_previsao=5):
    try:
        from sklearn.linear_model import LinearRegression
        from sklearn.preprocessing import MinMaxScaler

        df = df_ticker.copy()
        for col in ['Close', 'EMA20', 'RSI14']:
            if col not in df.columns:
                return {'erro': f'Coluna {col} não encontrada.'}

        df = df[['Close', 'EMA20', 'RSI14'] +
                (['EMA50'] if 'EMA50' in df.columns else [])].copy().dropna()

        if len(df) < 60:
            return {'erro': 'Dados insuficientes (mín. 60 dias).'}

        df['Retorno']  = df['Close'].pct_change()
        df['Volatil']  = df['Close'].pct_change().rolling(10).std()
        df['EMA_Dist'] = (df['Close'] - df['EMA20']) / df['EMA20']
        df['Target']   = df['Close'].shift(-1)
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

        split   = int(len(X_sc) * 0.8)
        modelo  = LinearRegression()
        modelo.fit(X_sc[:split], y_sc[:split])
        confianca = max(0.0, float(modelo.score(X_sc[split:], y_sc[split:])))

        ultimo    = df.iloc[-1]
        preco_cur = float(ultimo['Close'])
        ema20_cur = float(ultimo['EMA20'])
        rsi_cur   = float(ultimo['RSI14'])
        ret_cur   = float(ultimo['Retorno'])
        vol_cur   = float(ultimo['Volatil'])
        ema50_cur = float(ultimo['EMA50']) if 'EMA50' in df.columns else ema20_cur
        alpha20   = 2 / (20 + 1)
        alpha50   = 2 / (50 + 1)
        previsoes = []

        for _ in range(dias_previsao):
            ema_dist  = (preco_cur - ema20_cur) / ema20_cur if ema20_cur else 0
            row_feats = [preco_cur, ema20_cur, rsi_cur, ret_cur, vol_cur, ema_dist]
            if 'EMA50' in df.columns:
                row_feats.append(ema50_cur)
            entrada_sc = scaler_X.transform(np.array([row_feats]))
            preco_prev = float(scaler_y.inverse_transform(
                modelo.predict(entrada_sc).reshape(-1, 1))[0][0])
            previsoes.append(round(preco_prev, 2))
            ret_cur   = (preco_prev - preco_cur) / preco_cur if preco_cur else 0
            vol_cur   = vol_cur * 0.9 + abs(ret_cur) * 0.1
            ema20_cur = alpha20 * preco_prev + (1 - alpha20) * ema20_cur
            ema50_cur = alpha50 * preco_prev + (1 - alpha50) * ema50_cur
            delta     = preco_prev - preco_cur
            rsi_cur   = min(max(rsi_cur + (max(delta,0) - max(-delta,0)) / (preco_cur + 1e-9) * 30, 0), 100)
            preco_cur = preco_prev

        variacao_pct = ((previsoes[-1] - float(df.iloc[-1]['Close'])) / float(df.iloc[-1]['Close'])) * 100
        direcao = "ALTA" if variacao_pct > 1.5 else "BAIXA" if variacao_pct < -1.5 else "LATERAL"

        return {
            'erro': None, 'previsoes': previsoes, 'direcao': direcao,
            'variacao_pct': round(variacao_pct, 2),
            'confianca': round(confianca * 100, 1),
            'ultimo_preco': round(float(df.iloc[-1]['Close']), 2),
        }
    except ImportError:
        return {'erro': 'scikit-learn não instalado.'}
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
                🧠 <strong style='color:#a5b4fc;'>Regressão Linear</strong> treinada com
                Close, EMA20, EMA50, RSI14, Retorno Diário e Volatilidade (10d).<br>
                ⚠️ <strong style='color:#fbbf24;'>Aviso:</strong>
                Estimativas estatísticas — não são garantias.
            </p>
        </div>""", unsafe_allow_html=True)

        cfg = {
            "ALTA":    ("#d4fc79","#96e6a1","#14532d","🚀","ALTA PREVISTA"),
            "BAIXA":   ("#fca5a5","#ef4444","#7f1d1d","📉","BAIXA PREVISTA"),
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
                <div style='font-size:0.78rem;color:{cor_txt};'>próximos {dias_previsao} dias</div>
            </div>""", unsafe_allow_html=True)

        with col_conf:
            cor_c = "#15803d" if confianca >= 60 else "#b45309" if confianca >= 40 else "#b91c1c"
            nivel = "Boa" if confianca >= 60 else "Moderada" if confianca >= 40 else "Baixa"
            st.markdown(f"""
            <div style='background:#f8fafc;border:2px solid #e2e8f0;padding:1.2rem;
                        border-radius:10px;text-align:center;height:110px;
                        display:flex;flex-direction:column;justify-content:center;'>
                <div style='font-size:1.9rem;font-weight:800;color:{cor_c};'>{confianca:.0f}%</div>
                <div style='font-size:0.8rem;color:#64748b;'>Confiança ({nivel})<br>
                    <span style='font-size:0.7rem;'>(R² teste)</span></div>
            </div>""", unsafe_allow_html=True)

        with col_var:
            sinal_v = "+" if variacao >= 0 else ""
            cor_v = "#15803d" if variacao > 1.5 else "#b91c1c" if variacao < -1.5 else "#b45309"
            st.markdown(f"""
            <div style='background:#f8fafc;border:2px solid #e2e8f0;padding:1.2rem;
                        border-radius:10px;text-align:center;height:110px;
                        display:flex;flex-direction:column;justify-content:center;'>
                <div style='font-size:1.9rem;font-weight:800;color:{cor_v};'>
                    {sinal_v}{variacao:.2f}%</div>
                <div style='font-size:0.8rem;color:#64748b;'>Variação Prevista<br>
                    <span style='font-size:0.7rem;'>(D0 → D+{dias_previsao})</span></div>
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
        margem = y_max - y_min

        fig, ax = plt.subplots(figsize=(7, 3.2))
        fig.patch.set_facecolor('#f8fafc')
        ax.set_facecolor('#f8fafc')
        xs = list(range(len(todos_precos)))
        ax.fill_between(xs, todos_precos, y_min, alpha=0.18, color=cor_linha)
        ax.plot(xs, todos_precos, color=cor_linha, linewidth=2.5, marker='o', markersize=6,
                markerfacecolor='white', markeredgecolor=cor_linha, markeredgewidth=2, zorder=3)
        ax.scatter([0], [ult_preco], color='#6366f1', s=110, zorder=5)
        ax.axhline(ult_preco, color='#94a3b8', linestyle='--', linewidth=1, alpha=0.5)
        for i, p in enumerate(todos_precos):
            ax.annotate(f'R${p:.2f}', xy=(i, p), xytext=(0, margem * 0.12),
                        textcoords='offset points', ha='center', va='bottom',
                        fontsize=7.5, color='#1e293b', fontweight='600')
        ax.set_ylim(y_min, y_max + margem * 0.35)
        ax.set_xticks(xs)
        ax.set_xticklabels(todos_labels, fontsize=8.5, color='#475569')
        ax.set_ylabel('Preço (R$)', fontsize=8, color='#64748b')
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'R${v:.2f}'))
        ax.tick_params(axis='y', labelsize=7.5, colors='#64748b')
        for sp in ['top','right']: ax.spines[sp].set_visible(False)
        ax.spines['left'].set_color('#e2e8f0')
        ax.spines['bottom'].set_color('#e2e8f0')
        ax.set_title(f'Previsão ML — {ticker} ({empresa})', fontsize=9, color='#334155', pad=10)
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
                    <div style='font-size:0.7rem;color:#94a3b8;font-weight:700;'>D+{i+1}</div>
                    <div style='font-size:0.95rem;font-weight:800;color:#1e293b;'>R${preco:.2f}</div>
                    <div style='font-size:0.73rem;font-weight:600;color:{cor_d};'>
                        {sinal_d}{delta_pct:.1f}%</div>
                </div>""", unsafe_allow_html=True)

        st.markdown("""
        <div style='margin-top:1rem;padding:0.7rem 1rem;background:#f1f5f9;
                    border-radius:8px;font-size:0.76rem;color:#64748b;'>
            📐 <strong>Confiança (R²):</strong>
            🟢 ≥ 60% = Boa | 🟡 40–60% = Moderada | 🔴 &lt;40% = Baixa
        </div>""", unsafe_allow_html=True)


# =============================================================================
# TRIPLE SCREEN
# =============================================================================

def analisar_triple_screen(df_ticker):
    try:
        close  = df_ticker['Close'].dropna()
        volume = df_ticker['Volume'].dropna()
        if len(close) < 30:
            return None

        ema13       = close.ewm(span=13, adjust=False).mean()
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
        pct_dist    = ((preco_ult - ema13_val) / ema13_val) * 100

        alta_confirmada  = (ema13_slope > 0) and (macd_val > 0 or macd_slope > 0)
        baixa_confirmada = (ema13_slope < 0) and (macd_val < 0 or macd_slope < 0)

        if alta_confirmada:
            tela1_status = "ALTA"; tela1_emoji = "🟢"
            tela1_desc   = (
                f"EMA13 ascendente (+{ema13_slope:+.2f}), MACD {'positivo' if macd_val>0 else 'virando para cima'}. "
                f"Preço {abs(pct_dist):.1f}% {'acima' if pct_dist>=0 else 'abaixo'} da EMA13. Maré de ALTA."
            )
        elif baixa_confirmada:
            tela1_status = "BAIXA"; tela1_emoji = "🔴"
            tela1_desc   = (
                f"EMA13 descendente ({ema13_slope:+.2f}), MACD {'negativo' if macd_val<0 else 'virando para baixo'}. "
                f"Preço {abs(pct_dist):.1f}% {'abaixo' if pct_dist<=0 else 'acima'} da EMA13. Maré de BAIXA."
            )
        else:
            tela1_status = "NEUTRO"; tela1_emoji = "🟡"
            tela1_desc   = f"EMA13 sem direção clara (slope:{ema13_slope:+.2f}), MACD conflitante. Aguarde."

        idx_comum = close.index.intersection(volume.index)
        efi_bruto = close.loc[idx_comum].diff() * volume.loc[idx_comum]
        efi2      = efi_bruto.ewm(span=2, adjust=False).mean()
        efi2_val  = efi2.iloc[-1]
        efi2_std  = efi2.std()
        limiar_pos = efi2_std * 0.5
        limiar_neg = -efi2_std * 0.5

        if efi2_val < limiar_neg:
            tela2_status = "SOBREVENDA"; tela2_emoji = "🟢"
            tela2_desc   = f"EFI(2)={efi2_val:,.0f} < {limiar_neg:,.0f}. Sobrevenda — compradores absorvendo pressão."
        elif efi2_val > limiar_pos:
            tela2_status = "SOBRECOMPRA"; tela2_emoji = "🔴"
            tela2_desc   = f"EFI(2)={efi2_val:,.0f} > {limiar_pos:,.0f}. Sobrecompra — vendedores pressionando."
        else:
            tela2_status = "NEUTRO"; tela2_emoji = "🟡"
            tela2_desc   = f"EFI(2)={efi2_val:,.0f} (zona neutra). Aguarde sinal mais claro."

        preco_atual = close.iloc[-1]
        maxima_rec  = df_ticker['High'].iloc[-5:].max()
        minima_rec  = df_ticker['Low'].iloc[-5:].min()

        if tela1_status == "ALTA" and tela2_status == "SOBREVENDA":
            tela3_status = "COMPRA"; tela3_emoji = "🚀"
            tela3_desc   = (
                f"✅ Setup COMPRA!\n"
                f"• Buy Stop: acima de R${maxima_rec:.2f}\n"
                f"• Stop-Loss: R${minima_rec:.2f}\n"
                f"• Risco: R${maxima_rec - minima_rec:.2f}/cota"
            )
        elif tela1_status == "BAIXA" and tela2_status == "SOBRECOMPRA":
            tela3_status = "VENDA"; tela3_emoji = "📉"
            tela3_desc   = (
                f"⚠️ Setup VENDA!\n"
                f"• Sell Stop: abaixo de R${minima_rec:.2f}\n"
                f"• Stop-Loss: R${maxima_rec:.2f}\n"
                f"• Risco: R${maxima_rec - minima_rec:.2f}/cota"
            )
        else:
            tela3_status = "AGUARDAR"; tela3_emoji = "⏳"
            tela3_desc   = "Setup incompleto — aguarde alinhamento entre as telas 1 e 2."

        forca = sum([tela1_status == "ALTA", tela2_status == "SOBREVENDA", tela3_status == "COMPRA"])

        return {
            'tela1': {'status': tela1_status, 'emoji': tela1_emoji, 'valor': round(ema13_slope,4), 'desc': tela1_desc},
            'tela2': {'status': tela2_status, 'emoji': tela2_emoji, 'valor': round(efi2_val,0),    'desc': tela2_desc},
            'tela3': {'status': tela3_status, 'emoji': tela3_emoji, 'desc': tela3_desc},
            'veredicto': tela3_status, 'forca': forca, 'preco_atual': round(preco_atual,2),
            'serie_close': close.iloc[-60:], 'serie_macd': ema13.iloc[-60:],
            'serie_efi2': efi2.iloc[-60:], 'limiar_pos': limiar_pos, 'limiar_neg': limiar_neg,
            'maxima_rec': round(maxima_rec,2), 'minima_rec': round(minima_rec,2),
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
        t1, t2, t3 = resultado['tela1'], resultado['tela2'], resultado['tela3']

        st.markdown("""
        <div style='background:linear-gradient(135deg,#0f2027 0%,#203a43 50%,#2c5364 100%);
                    padding:1rem 1.3rem;border-radius:10px;margin-bottom:1.2rem;'>
            <p style='margin:0;color:#cfd8dc;font-size:0.83rem;line-height:1.65;'>
                🧠 <strong style='color:#80cbc4;'>Triple Screen (Elder, 1986):</strong>
                🌊 1ª Tela: EMA13+MACD (maré) &nbsp;|&nbsp;
                🌀 2ª Tela: EFI(2) (onda) &nbsp;|&nbsp;
                🎯 3ª Tela: Buy/Sell Stop (execução)
            </p>
        </div>""", unsafe_allow_html=True)

        cfg_v = {
            "COMPRA":   ("#d4edda","#155724","#28a745","🚀","SETUP DE COMPRA"),
            "VENDA":    ("#f8d7da","#721c24","#dc3545","📉","SETUP DE VENDA"),
            "AGUARDAR": ("#fff3cd","#856404","#ffc107","⏳","AGUARDAR ALINHAMENTO"),
        }
        bg_v, txt_v, brd_v, ico_v, lbl_v = cfg_v[veredicto]
        estrelas = "⭐" * forca + "☆" * (3 - forca)

        st.markdown(f"""
        <div style='background:{bg_v};border:2px solid {brd_v};border-radius:12px;
                    padding:1.1rem 1.4rem;margin-bottom:1.2rem;display:flex;align-items:center;gap:1rem;'>
            <div style='font-size:2.4rem;'>{ico_v}</div>
            <div>
                <div style='font-size:1.2rem;font-weight:800;color:{txt_v};'>{lbl_v}</div>
                <div style='font-size:0.82rem;color:{txt_v};margin-top:0.2rem;'>
                    Força: {estrelas} ({forca}/3) | {ticker} — {empresa}
                </div>
            </div>
        </div>""", unsafe_allow_html=True)

        cfg_s = {
            "ALTA":       ("#e8f5e9","#1b5e20","#43a047"),
            "BAIXA":      ("#ffebee","#b71c1c","#e53935"),
            "NEUTRO":     ("#fffde7","#f57f17","#fbc02d"),
            "SOBREVENDA": ("#e8f5e9","#1b5e20","#43a047"),
            "SOBRECOMPRA":("#ffebee","#b71c1c","#e53935"),
            "COMPRA":     ("#e8f5e9","#1b5e20","#43a047"),
            "VENDA":      ("#ffebee","#b71c1c","#e53935"),
            "AGUARDAR":   ("#fffde7","#f57f17","#fbc02d"),
        }

        col1, col2, col3 = st.columns(3)
        serie_close = resultado['serie_close']
        serie_macd  = resultado['serie_macd']
        serie_efi2  = resultado['serie_efi2']
        limiar_pos  = resultado['limiar_pos']
        limiar_neg  = resultado['limiar_neg']
        maxima_rec  = resultado['maxima_rec']
        minima_rec  = resultado['minima_rec']
        preco_atual = resultado['preco_atual']

        for col, tela, num, nome, subtitulo in [
            (col1, t1, "1ª", "Maré",    "EMA13 + MACD(12,26,9)"),
            (col2, t2, "2ª", "Onda",    "EFI(2)"),
            (col3, t3, "3ª", "Execução","Buy/Sell Stop"),
        ]:
            bg_s, txt_s, brd_s = cfg_s.get(tela['status'], ("#f5f5f5","#333","#999"))
            valor_linha = ""
            if 'valor' in tela:
                v = tela['valor']
                vf = f"{v:+.5f}" if abs(v)<1 else (f"{int(v):,}".replace(",",".") if abs(v)>=1000 else f"{v:+.4f}")
                valor_linha = (f"<div style='font-size:0.74rem;color:{txt_s};"
                               f"margin-top:0.25rem;font-family:monospace;'>{vf}</div>")
            with col:
                st.markdown(f"""
                <div style='background:{bg_s};border:1.5px solid {brd_s};
                            border-radius:10px 10px 0 0;padding:0.75rem 0.9rem 0.5rem;'>
                    <div style='font-size:0.68rem;font-weight:700;color:{brd_s};
                                letter-spacing:.08em;text-transform:uppercase;'>
                        {num} TELA — {nome.upper()}</div>
                    <div style='font-size:0.65rem;color:{txt_s};margin-bottom:0.4rem;'>{subtitulo}</div>
                    <div style='display:flex;align-items:center;gap:0.4rem;'>
                        <span style='font-size:1.3rem;'>{tela['emoji']}</span>
                        <span style='font-size:0.9rem;font-weight:800;color:{txt_s};'>{tela['status']}</span>
                    </div>{valor_linha}
                </div>""", unsafe_allow_html=True)

                fig_m, ax_m = plt.subplots(figsize=(3.2, 1.6))
                fig_m.patch.set_facecolor(bg_s)
                ax_m.set_facecolor(bg_s)

                if num == "1ª":
                    xs = range(len(serie_close))
                    ax_m.plot(xs, serie_close.values, color='#607d8b', linewidth=1.0, alpha=0.6)
                    ax_m.plot(xs, serie_macd.values, color=brd_s, linewidth=2.0, zorder=3)
                    ax_m.fill_between(xs, serie_close.values, serie_macd.values,
                                      where=(serie_close.values >= serie_macd.values),
                                      alpha=0.15, color='#43a047', interpolate=True)
                    ax_m.fill_between(xs, serie_close.values, serie_macd.values,
                                      where=(serie_close.values < serie_macd.values),
                                      alpha=0.15, color='#e53935', interpolate=True)
                    ax_m.set_title("EMA13 (Maré)", fontsize=7, color=txt_s, pad=3)
                elif num == "2ª":
                    xs   = range(len(serie_efi2))
                    vals = serie_efi2.values
                    ax_m.bar(xs, vals, color=[brd_s if v>=0 else '#e53935' for v in vals], alpha=0.7, width=1.0)
                    ax_m.axhline(limiar_pos, color='#e53935', linewidth=0.9, linestyle='--', alpha=0.8)
                    ax_m.axhline(limiar_neg, color='#43a047', linewidth=0.9, linestyle='--', alpha=0.8)
                    ax_m.axhline(0, color='#90a4ae', linewidth=0.7)
                    ax_m.set_title("EFI(2)", fontsize=7, color=txt_s, pad=3)
                else:
                    cl20 = serie_close.iloc[-20:].values
                    xs   = range(len(cl20))
                    cor_l = '#43a047' if t3['status']=='COMPRA' else '#e53935' if t3['status']=='VENDA' else '#f57f17'
                    ax_m.plot(xs, cl20, color=cor_l, linewidth=1.5, zorder=3)
                    ax_m.axhline(maxima_rec, color='#43a047', linewidth=1.0, linestyle='--', alpha=0.9)
                    ax_m.axhline(minima_rec, color='#e53935', linewidth=1.0, linestyle='--', alpha=0.9)
                    ax_m.axhline(preco_atual, color='#607d8b', linewidth=0.8, linestyle=':', alpha=0.7)
                    ax_m.fill_between(xs, maxima_rec, minima_rec, alpha=0.07, color=cor_l)
                    ax_m.set_title("Preço + Stop", fontsize=7, color=txt_s, pad=3)

                for sp in ax_m.spines.values(): sp.set_visible(False)
                ax_m.set_xticks([])
                ax_m.tick_params(axis='y', labelsize=6, colors=txt_s, length=0)
                ax_m.yaxis.set_major_formatter(plt.FuncFormatter(lambda v,_:
                    f'{v/1e6:.1f}M' if abs(v)>=1e6 else f'{v/1e3:.0f}K' if abs(v)>=1e3 else f'{v:.2f}'))
                plt.tight_layout(pad=0.3)
                st.pyplot(fig_m, use_container_width=True)
                plt.close(fig_m)

                st.markdown(f"""
                <div style='background:{bg_s};border:1.5px solid {brd_s};border-top:none;
                            border-radius:0 0 10px 10px;height:6px;margin-top:-4px;'></div>""",
                            unsafe_allow_html=True)

        st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)
        for tela, num, icone, titulo in [
            (t1,"1ª","🌊","Tela — Identificação da Maré (EMA13 + MACD 12,26,9)"),
            (t2,"2ª","🌀","Tela — Sinal de Entrada pela Onda (EFI 2)"),
            (t3,"3ª","🎯","Tela — Execução (Ordem Stop)"),
        ]:
            bg_d, txt_d, brd_d = cfg_s.get(tela['status'], ("#f8fafc","#334155","#cbd5e1"))
            st.markdown(f"""
            <div style='background:{bg_d};border-left:4px solid {brd_d};
                        border-radius:0 8px 8px 0;padding:0.8rem 1rem;margin-bottom:0.6rem;'>
                <div style='font-weight:700;font-size:0.88rem;color:{txt_d};margin-bottom:0.35rem;'>
                    {icone} {num} {titulo}</div>
                <div style='font-size:0.82rem;color:{txt_d};line-height:1.55;
                            white-space:pre-wrap;'>{tela['desc']}</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("""
        <div style='margin-top:0.8rem;padding:0.7rem 1rem;background:#eceff1;
                    border-radius:8px;font-size:0.76rem;color:#546e7a;'>
            📖 Confirme sempre no gráfico semanal (1ª tela) e horário (2ª tela) antes de executar.
            <a href="https://hw-br.online/education/triple-screen-strategy-3-steps-to-make-profit/"
               target="_blank" style='color:#0288d1;'>Leia mais ↗</a>
        </div>""", unsafe_allow_html=True)


# =============================================================================
# MAPEAMENTO BDR → TICKER US E NOMES
# =============================================================================
BDR_TO_US_MAP = {
    'A1AP34':'AAP','A1DC34':'ADC','A1DI34':'ADI','A1EP34':'AEP','A1ES34':'AES',
    'A1FL34':'AFL','A1IV34':'AIV','A1KA34':'AKAM','A1LB34':'ALB','A1LK34':'ALK',
    'A1LL34':'BFH','A1MD34':'AMD','A1MP34':'AMP','A1MT34':'AMAT','A1NE34':'ANET',
    'A1PH34':'APH','A1PL34':'APLD','A1PO34':'APO','A1PP34':'APP','A1RE34':'ARE',
    'A1RG34':'ARGX','A1SU34':'AIZ','A1TH34':'ATHM','A1VB34':'AVB','A1WK34':'AWK',
    'A1ZN34':'AZN','A2MB34':'AMBA','A2RR34':'ARWR','A2RW34':'ARW','A2SO34':'ASO',
    'A2XO34':'AXON','A2ZT34':'AZTA','AALL34':'AAL','AAPL34':'AAPL','ABBV34':'ABBV',
    'ABTT34':'ABT','ABUD34':'BUD','ACNB34':'ACN','ADBE34':'ADBE','AIRB34':'ABNB',
    'AMGN34':'AMGN','AMZO34':'AMZN','APTV34':'APTV','ARMT34':'MT','ARNC34':'HWM',
    'ASML34':'ASML','ATTB34':'T','AVGO34':'AVGO','AWII34':'AWI','AXPB34':'AXP',
    'B1AM34':'BN','B1AX34':'BAX','B1BW34':'BBWI','B1CS34':'BCS','B1FC34':'BF-B',
    'B1IL34':'BILI','B1LL34':'BALL','B1MR34':'BMRN','B1NT34':'BNTX','B1PP34':'BP',
    'B1RF34':'BR','B1SA34':'BSAC','B1TI34':'BTI','B2AH34':'BAH','B2HI34':'BILL',
    'B2LN34':'BL','B2MB34':'BMBL','B2RK34':'BRKR','B2UR34':'BURL','B2YN34':'BYND',
    'BABA34':'BABA','BCSA34':'SAN','BERK34':'BRK-B','BHVN34':'BHVN','BIDU34':'BIDU',
    'BIIB34':'BIIB','BKNG34':'BKNG','BLAK34':'BLAK','BMYB34':'BMYB','BOAC34':'BOAC',
    'BOEI34':'BOEI','BONY34':'BONY','BOXP34':'BOXP','C1AB34':'CABO','C1AG34':'CAG',
    'C1AH34':'CAH','C1BL34':'CB','C1BR34':'CBRE','C1CJ34':'CCJ','C1CL34':'CCL',
    'C1CO34':'COR','C1DN34':'CDNS','C1FG34':'CFG','C1GP34':'CSGP','C1HR34':'CHRW',
    'C1IC34':'CI','C1MG34':'CMG','C1MI34':'CMI','C1MS34':'CMS','C1NC34':'CNC',
    'C1OO34':'COO','C1PB34':'CPB','C1RH34':'CRH','C2AC34':'CACI','C2CA34':'KOF',
    'C2GN34':'CGNX','C2HD34':'CHDN','C2OI34':'COIN','C2OU34':'COUR','C2RS34':'CRSP',
    'C2RW34':'CRWD','C2ZR34':'CZR','CAON34':'CAON','CATP34':'CATP','CHCM34':'CHCM',
    'CHDC34':'CHDC','CHME34':'CHME','CHVX34':'CVX','CLOV34':'CLOV','CLXC34':'CLXC',
    'CNIC34':'CNIC','COCA34':'COCA','COLG34':'COLG','COPH34':'COPH','COTY34':'COTY',
    'COWC34':'COWC','CPRL34':'CPRL','CSCO34':'CSCO','CSXC34':'CSXC','CTGP34':'CTGP',
    'CTSH34':'CTSH','CVSH34':'CVSH','D1DG34':'DDOG','D1EX34':'DXCM','D1LR34':'DLR',
    'D1OC34':'DOCU','D1OW34':'DOW','D1VN34':'DVN','D2AR34':'DAR','D2AS34':'DASH',
    'D2NL34':'DNLI','D2OC34':'DOCS','D2OX34':'DOX','D2PZ34':'DPZ','DBAG34':'DBAG',
    'DDNB34':'DDNB','DEEC34':'DEEC','DEOP34':'DEOP','DGCO34':'DGCO','DHER34':'DHER',
    'DISB34':'DIS','DUOL34':'DUOL','DVAI34':'DVAI','E1CO34':'EC','E1DU34':'EDU',
    'E1LV34':'ELV','E1MN34':'EMN','E1MR34':'EMR','E1OG34':'EOG','E1QN34':'EQNR',
    'E1RI34':'ERIC','E1TN34':'ETN','E1WL34':'EW','E2NP34':'ENPH','E2ST34':'ESTC',
    'E2TS34':'ETSY','EAIN34':'EAIN','EBAY34':'EBAY','ELCI34':'ELCI','EQIX34':'EQIX',
    'EXXO34':'EXXO','F1AN34':'FANG','F1IS34':'FI','F1MC34':'FMC','F1NI34':'FIS',
    'F1SL34':'FSLY','F1TN34':'FTNT','F2IC34':'FICO','F2IV34':'FIVN','F2NV34':'FNV',
    'F2RS34':'FRSH','FASL34':'FASL','FBOK34':'META','FCXO34':'FCXO','FDMO34':'FDMO',
    'FDXB34':'FDXB','FSLR34':'FSLR','G1AM34':'GLPI','G1AR34':'IT','G1DS34':'GDS',
    'G1FI34':'GFI','G1LO34':'GLOB','G1LW34':'GLW','G1MI34':'GIS','G1PI34':'GPN',
    'G1RM34':'GRMN','G1SK34':'GSK','G1WW34':'GWW','G2DD34':'GDDY','G2EV34':'GEV',
    'GDBR34':'GDBR','GEOO34':'GEOO','GILD34':'GILD','GMCO34':'GMCO','GOGL34':'GOOGL',
    'GOGL35':'GOOG','GPRO34':'GPRO','GPSI34':'GPSI','GSGI34':'GSGI','H1AS34':'HAS',
    'H1CA34':'HCA','H1DB34':'HDB','H1II34':'HII','H1OG34':'HOG','H1PE34':'HPE',
    'H1RL34':'HRL','H1SB34':'HSBC','H1UM34':'HUM','H2UB34':'HUBS','HALI34':'HALI',
    'HOME34':'HOME','HOND34':'HOND','HPQB34':'HPQB','I1AC34':'IAC','I1DX34':'IDXX',
    'I1EX34':'IEX','I1FO34':'INFY','I1LM34':'ILMN','I1NC34':'INCY','I1PC34':'IP',
    'I1PG34':'IPGP','I1QV34':'IQV','I1QY34':'IQ','I1RM34':'IRM','I1RP34':'TT',
    'I1SR34':'ISRG','I2NG34':'INGR','I2NV34':'INVH','IBKR34':'IBKR','ICLR34':'ICLR',
    'INBR32':'INTR','INTU34':'INTU','ITLC34':'ITLC','J1EG34':'J','J2BL34':'JBL',
    'JBSS32':'JBSS','JDCO34':'JD','JNJB34':'JNJB','JPMC34':'JPMC','K1BF34':'KB',
    'K1LA34':'KLAC','K1MX34':'KMX','K1SG34':'KEYS','K1SS34':'KSS','K1TC34':'KT',
    'KHCB34':'KHCB','KMBB34':'KMBB','KMIC34':'KMIC','L1EG34':'LEG','L1EN34':'LEN',
    'L1HX34':'LHX','L1MN34':'LUMN','L1NC34':'LNC','L1RC34':'LRCX','L1WH34':'LW',
    'L1YG34':'LYG','L1YV34':'LYV','L2PL34':'LPLA','L2SC34':'LSCC','LBRD34':'LBRD',
    'LILY34':'LILY','LOWC34':'LOWC','M1AA34':'MAA','M1CH34':'MCHP','M1CK34':'MCK',
    'M1DB34':'MDB','M1HK34':'MHK','M1MC34':'MMC','M1NS34':'MNST','M1RN34':'MRNA',
    'M1SC34':'MSCI','M1SI34':'MSI','M1TA34':'META','M1TC34':'MTCH','M1TT34':'MAR',
    'M1UF34':'MUFG','M2PR34':'MPWR','M2RV34':'MRVL','M2ST34':'MSTR','MACY34':'MACY',
    'MCDC34':'MCDC','MCOR34':'MCOR','MDLZ34':'MDLZ','MDTC34':'MDTC','MELI34':'MELI',
    'MMMC34':'MMMC','MOOO34':'MOOO','MOSC34':'MOSC','MRCK34':'MRCK','MSBR34':'MSBR',
    'MSCD34':'MA','MSFT34':'MSFT','MUTC34':'MU','N1BI34':'NBIX','N1CL34':'NCLH',
    'N1DA34':'NDAQ','N1EM34':'NEM','N1GG34':'NGG','N1IS34':'NI','N1OW34':'NOW',
    'N1RG34':'NRG','N1TA34':'NTAP','N1UE34':'NUE','N1VO34':'NVO','N1VR34':'NVR',
    'N1VS34':'NVS','N1WG34':'NWG','N1XP34':'NXPI','N2ET34':'NET','N2LY34':'NLY',
    'N2TN34':'NTNX','N2VC34':'NVCR','NETE34':'NETE','NEXT34':'NEXT','NFLX34':'NFLX',
    'NIKE34':'NIKE','NOCG34':'NOCG','NVDC34':'NVDA','O1DF34':'ODFL','O1KT34':'OKTA',
    'O2HI34':'OHI','O2NS34':'ON','ORCL34':'ORCL','ORLY34':'ORLY','OXYP34':'OXYP',
    'P1AC34':'PCAR','P1AY34':'PAYX','P1DD34':'PDD','P1GR34':'PGR','P1KX34':'PKX',
    'P1LD34':'PLD','P1NW34':'PNW','P1PL34':'PPL','P1RG34':'PRGO','P1SX34':'PSX',
    'P2AN34':'PANW','P2AT34':'PATH','P2IN34':'PINS','P2LT34':'PLTR','P2ST34':'PSTG',
    'P2TC34':'PTC','PAGS34':'PAGS','PEPB34':'PEPB','PFIZ34':'PFIZ','PGCO34':'PGCO',
    'PHMO34':'PHMO','PYPL34':'PYPL','QCOM34':'QCOM','QUBT34':'QUBT','R1DY34':'RDY',
    'R1EG34':'REG','R1EL34':'RELX','R1HI34':'RHI','R1IN34':'O','R1KU34':'ROKU',
    'R1MD34':'RMD','R1OP34':'ROP','R1SG34':'RSG','R1YA34':'RYAAY','R2BL34':'RBLX',
    'R2NG34':'RNG','REGN34':'REGN','RGTI34':'RGTI','RIOT34':'RIOT','ROST34':'ROST',
    'ROXO34':'NU','RYTT34':'RYTT','S1BA34':'SBAC','S1HW34':'SHW','S1KM34':'SKM',
    'S1NA34':'SNA','S1NP34':'SNPS','S1OU34':'LUV','S1PO34':'SPOT','S1RE34':'SRE',
    'S1TX34':'STX','S1WK34':'SWK','S1YY34':'SYY','S2CH34':'SQM','S2EA34':'SE',
    'S2ED34':'SEDG','S2FM34':'SFM','S2HO34':'SHOP','S2NA34':'SNAP','S2NW34':'SNOW',
    'S2TA34':'STAG','SAPP34':'SAPP','SBUB34':'SBUB','SCHW34':'SCHW','SLBG34':'SLBG',
    'SNEC34':'SNEC','SPGI34':'SPGI','SSFO34':'SSFO','STOC34':'STOC','T1AL34':'TAL',
    'T1AM34':'TEAM','T1EV34':'TEVA','T1MU34':'TMUS','T1OW34':'AMT','T1RI34':'TRIP',
    'T1SC34':'TSCO','T1SO34':'SO','T1TW34':'TTWO','T1WL34':'TWLO','T2DH34':'TDOC',
    'T2ER34':'TER','T2RM34':'TRMB','T2TD34':'TTD','T2YL34':'TYL','TMCO34':'TMCO',
    'TMOS34':'TMOS','TPRY34':'TPRY','TRVC34':'TRVC','TSLA34':'TSLA','TSMC34':'TSMC',
    'U1AI34':'UA','U1AL34':'UAL','U1BE34':'UBER','U1DR34':'UDR','U1HS34':'UHS',
    'U1RI34':'URI','U2PS34':'UPST','U2PW34':'UPWK','U2ST34':'U','U2TH34':'UTHR',
    'UBSG34':'UBSG','ULEV34':'ULEV','UNHH34':'UNHH','UPAC34':'UPAC','USBC34':'USBC',
    'V1MC34':'VMC','V1NO34':'VNO','V1OD34':'VOD','V1RS34':'VRSK','V1RT34':'VRT',
    'V1SA34':'V','V1ST34':'VST','V1TA34':'VTR','V2EE34':'VEEV','V2TX34':'VTEX',
    'VERZ34':'VERZ','VISA34':'VISA','VLOE34':'VLOE','VRSN34':'VRSN','W1BD34':'WBD',
    'W1BO34':'WB','W1DC34':'WDC','W1EL34':'WELL','W1HR34':'WHR','W1MB34':'WMB',
    'W1MC34':'WM','W1YC34':'WY','W2ST34':'WST','W2YF34':'W','WABC34':'WABC',
    'WALM34':'WALM','WFCO34':'WFCO','WUNI34':'WUNI','X1YZ34':'SQ','XPBR31':'XPBR',
    'Y2PF34':'YPF','YUMR34':'YUMR','Z1BR34':'ZBRA','Z1OM34':'ZM','Z1TA34':'ZETA',
    'Z1TS34':'ZTS','Z2LL34':'Z','Z2SC34':'ZS',
}

def mapear_ticker_us(ticker_bdr):
    if ticker_bdr in BDR_TO_US_MAP:
        return BDR_TO_US_MAP[ticker_bdr]
    return ticker_bdr.rstrip('0123456789')

NOMES_BDRS = {
    'AAPL34':'Apple Inc.','MSFT34':'Microsoft Corp','AMZO34':'Amazon.com, Inc.',
    'GOGL34':'Alphabet Inc','GOGL35':'Alphabet Inc','NVDC34':'NVIDIA Corporation',
    'TSLA34':'Tesla, Inc.','FBOK34':'Meta Platforms Inc','AVGO34':'Broadcom Inc.',
    'LILY34':'Eli Lilly & Co','ASML34':'ASML Holding NV ADR','JNJB34':'Johnson & Johnson',
    'NFLX34':'Netflix, Inc.','DISB34':'Walt Disney Company','ABBV34':'AbbVie, Inc.',
    'ABTT34':'Abbott Laboratories','BOAC34':'Bank of America Corp','JPMC34':'JPMorgan Chase & Co.',
    'AXPB34':'American Express Co','COWC34':'Costco Wholesale Corporation','HOME34':'Home Depot Inc',
    'MCDC34':'McDonald\'s Corporation','COLG34':'Colgate-Palmolive Co','COCA34':'Coca-Cola Company',
    'PEPB34':'PepsiCo, Inc.','PFIZ34':'Pfizer Inc','MRCK34':'Merck & Co., Inc.',
    'BMYB34':'Bristol-Myers Squibb Company','AMGN34':'Amgen Inc.','GILD34':'Gilead Sciences, Inc',
    'REGN34':'Regeneron Pharmaceuticals, Inc.','BIIB34':'Biogen Inc.','CSCO34':'Cisco Systems, Inc.',
    'INTU34':'Intuit Corp','ADBE34':'Adobe Inc.','ORCL34':'Oracle Corp','QCOM34':'QUALCOMM Incorporated',
    'ITLC34':'Intel Corporation','MUTC34':'Micron Technology Inc','A1MD34':'Advanced Micro Devices, Inc.',
    'AIRB34':'Airbnb, Inc.','BKNG34':'Booking Holdings Inc.','PYPL34':'PayPal Holdings, Inc.',
    'EBAY34':'eBay Inc.','MELI34':'MercadoLibre, Inc.','PAGS34':'PagSeguro Digital Ltd.',
    'STOC34':'StoneCo Ltd.','ROXO34':'Nu Holdings Ltd.','INBR32':'Inter & Co., Inc.',
    'XPBR31':'XP Inc.','JBSS32':'JBS N.V.','TSMC34':'Taiwan Semiconductor Manufacturing Co., Ltd. ADR',
    'MSBR34':'Morgan Stanley','GSGI34':'Goldman Sachs Group, Inc.','SCHW34':'Charles Schwab Corp',
    'WFCO34':'Wells Fargo & Company','USBC34':'U.S. Bancorp','BONY34':'Bank of New York Mellon Corp',
    'BERK34':'Berkshire Hathaway Inc. B','BLAK34':'BlackRock, Inc.','CAON34':'Capital One Financial Corp',
    'CHVX34':'Chevron Corporation','EXXO34':'Exxon Mobil Corp','COPH34':'ConocoPhillips',
    'OXYP34':'Occidental Petroleum Corp','SLBG34':'SLB Limited','HALI34':'Halliburton Company',
    'BOEI34':'Boeing Company','RYTT34':'RTX Corporation','NOCG34':'Northrop Grumman Corp.',
    'GDBR34':'General Dynamics Corp','GEOO34':'GE Aerospace','CATP34':'Caterpillar Inc',
    'DEEC34':'Deere & Co','UPAC34':'Union Pacific Corp','CPRL34':'Canadian Pacific Kansas City Limited',
    'FDXB34':'FedEx Corporation','WALM34':'Walmart Inc','LOWC34':'Lowe\'s Companies Inc',
    'ROST34':'Ross Stores, Inc.','ORLY34':'O\'Reilly Automotive Inc','SBUB34':'Starbucks Corporation',
    'MCOR34':'Moody\'s Corporation','SPGI34':'S&P Global Inc','MSCD34':'Mastercard Inc',
    'VISA34':'Visa Inc.','VERZ34':'Verizon Communications Inc','ATTB34':'AT&T Inc',
    'TMOS34':'Thermo Fisher Scientific Inc.','DHER34':'Danaher Corp','I1SR34':'Intuitive Surgical, Inc.',
    'MDTC34':'Medtronic plc','UNHH34':'Unitedhealth Group Inc','CVSH34':'CVS Health Corp',
    'H1UM34':'Humana Inc','E1LV34':'Elevance Health, Inc.','C1NC34':'Centene Corporation',
    'C1IC34':'Cigna Group','SSFO34':'Salesforce, Inc.','D1DG34':'Datadog, Inc.',
    'C2RW34':'CrowdStrike Holdings, Inc.','N2ET34':'Cloudflare Inc','P2AN34':'Palo Alto Networks, Inc.',
    'F1TN34':'Fortinet, Inc.','O1KT34':'Okta, Inc.','S2NW34':'Snowflake, Inc.',
    'M1DB34':'MongoDB, Inc.','N2TN34':'Nutanix, Inc.','Z2SC34':'Zscaler, Inc.',
    'T1AM34':'Atlassian Corp','H2UB34':'HubSpot, Inc.','D2AS34':'DoorDash, Inc.',
    'U1BE34':'Uber Technologies, Inc.','S1PO34':'Spotify Technology S.A.',
    'DUOL34':'Duolingo, Inc.','R2BL34':'Roblox Corp.','S2HO34':'Shopify, Inc.',
    'E2TS34':'Etsy, Inc.','P2LT34':'Palantir Technologies Inc.','R1KU34':'Roku, Inc.',
    'T2TD34':'Trade Desk, Inc.','U2PS34':'Upstart Holdings, Inc.','T1WL34':'Twilio, Inc.',
    'R2NG34':'RingCentral, Inc.','T2DH34':'Teladoc Health, Inc.','S2NA34':'Snap, Inc.',
    'P2IN34':'Pinterest, Inc.','B2YN34':'Beyond Meat, Inc.','C2OI34':'Coinbase Global, Inc.',
    'M1RN34':'Moderna, Inc.','B1NT34':'BioNTech SE ADR','RGTI34':'Rigetti Computing, Inc.',
    'QUBT34':'Quantum Computing Inc','D1EX34':'DexCom, Inc.','I1SR34':'Intuitive Surgical, Inc.',
    'NEXT34':'NextEra Energy, Inc.','E2NP34':'Enphase Energy, Inc.','FSLR34':'First Solar, Inc.',
    'A1MD34':'Advanced Micro Devices, Inc.','BIDU34':'Baidu, Inc. ADR','BABA34':'Alibaba Group Holding Limited ADR',
    'JDCO34':'JD.com, Inc. ADR','N1VO34':'Novo Nordisk A/S ADR','HOND34':'Honda Motor Co., Ltd. ADR',
    'TMCO34':'Toyota Motor Corp ADR','ASML34':'ASML Holding NV ADR','SNEC34':'Sony Group Corporation ADR',
    'SAPP34':'SAP SE ADR','UBSG34':'UBS Group AG','DBAG34':'Deutsche Bank AG',
    'A1ZN34':'AstraZeneca PLC ADR','G1SK34':'GSK PLC ADR','ULEV34':'Unilever PLC ADR',
    'H1SB34':'HSBC Holdings Plc','N1GG34':'National Grid PLC','B1PP34':'BP PLC',
    'B1TI34':'British American Tobacco PLC ADR','ABUD34':'Anheuser-Busch InBev SA/NV ADR',
    'EQIX34':'Equinix Inc','D1LR34':'Digital Realty Trust, Inc.','I1RM34':'Iron Mountain REIT Inc',
    'T1OW34':'American Tower Corporation','S1BA34':'SBA Communications Corp.',
    'ACNB34':'Accenture PLC','C1DN34':'Cadence Design Systems, Inc.',
    'S1NP34':'Synopsys, Inc.','F2IC34':'Fair Isaac Corporation','G1AR34':'Gartner, Inc.',
    'A1KA34':'Akamai Technologies, Inc.','IBKR34':'Interactive Brokers Group, Inc.',
    'ICLR34':'Icon PLC','G1LO34':'Globant Sa','P2AX34':'Patria Investments Ltd.',
    # Garantir que todos os BDR_TO_US_MAP tenham pelo menos um nome básico
}

# Preencher nomes faltantes com fallback
for k in BDR_TO_US_MAP:
    if k not in NOMES_BDRS:
        us = BDR_TO_US_MAP[k]
        NOMES_BDRS[k] = us  # usa o ticker US como nome provisório

# =============================================================================
# DADOS FUNDAMENTALISTAS
# =============================================================================

FMP_API_KEY = "tBsRam74Ac6bZRWS3C8HY83C6not17Uh"

def buscar_dados_brapi(ticker_bdr):
    try:
        url = f"https://brapi.dev/api/quote/{ticker_bdr}?token={BRAPI_TOKEN}"
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None
        data = response.json()
        if 'results' not in data or len(data['results']) == 0:
            return None
        result = data['results'][0]
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
    score = 50
    detalhes = {
        'fonte': {'valor': 'BRAPI (B3)', 'pontos': 0, 'criterio': 'Dados da BDR na B3'},
        'market_cap': {'valor': None, 'pontos': 0, 'criterio': ''},
        'volume': {'valor': None, 'pontos': 0, 'criterio': ''},
    }
    mcap = dados_brapi.get('market_cap')
    if mcap:
        detalhes['market_cap']['valor'] = mcap
        mcap_b = mcap / 1e9
        if mcap_b > 100:
            detalhes['market_cap'].update(pontos=20, criterio='Large Cap (>$100B)'); score += 20
        elif mcap_b > 10:
            detalhes['market_cap'].update(pontos=10, criterio='Mid Cap (>$10B)'); score += 10
        else:
            detalhes['market_cap']['criterio'] = 'Small Cap (<$10B)'
    volume = dados_brapi.get('volume')
    if volume:
        detalhes['volume']['valor'] = volume
        if volume > 1000000:
            detalhes['volume'].update(pontos=10, criterio='Alta liquidez (>1M)'); score += 10
        elif volume > 100000:
            detalhes['volume'].update(pontos=5, criterio='Boa liquidez (>100K)'); score += 5
        else:
            detalhes['volume']['criterio'] = 'Baixa liquidez (<100K)'
    return max(0, min(100, score)), detalhes

def buscar_dados_openbb(ticker_us):
    try:
        from openbb import obb
        try:
            obb.user.credentials.fmp_api_key = FMP_API_KEY
        except Exception:
            pass
        info = {}
        try:
            profile = obb.equity.profile(symbol=ticker_us, provider="fmp")
            if profile and profile.results:
                r = profile.results[0]
                info['marketCap'] = getattr(r, 'mkt_cap', None)
                info['sector']    = getattr(r, 'sector', None)
                info['symbol']    = ticker_us
        except Exception:
            pass
        try:
            metrics = obb.equity.fundamental.metrics(symbol=ticker_us, provider="fmp")
            if metrics and metrics.results:
                m = metrics.results[0]
                info['trailingPE']    = getattr(m, 'pe_ratio', None)
                info['dividendYield'] = getattr(m, 'dividend_yield', None)
                info['revenueGrowth'] = getattr(m, 'revenue_growth', None)
        except Exception:
            pass
        if info.get('marketCap') or info.get('trailingPE'):
            return info
    except Exception:
        pass
    return None

@st.cache_data(ttl=3600, show_spinner=False)
def buscar_dados_fundamentalistas(ticker_bdr):
    ticker_us = mapear_ticker_us(ticker_bdr)

    def _score_from_yf_info(info, fonte_label, ticker_label):
        if not info or len(info) < 5:
            return None
        if not any([info.get('marketCap'), info.get('trailingPE'),
                    info.get('forwardPE'), info.get('revenueGrowth')]):
            return None
        score = 50
        det   = {}

        pe = info.get('trailingPE') or info.get('forwardPE')
        if pe and isinstance(pe, (int, float)):
            det['pe_ratio'] = {'valor': round(pe,2), 'pontos': 0, 'criterio': ''}
            if 10<=pe<=25:   score+=15; det['pe_ratio'].update(pontos=15, criterio='Ótimo (10-25)')
            elif 5<=pe<=35:  score+=10; det['pe_ratio'].update(pontos=10, criterio='Bom')
            elif pe<5:       score+=5;  det['pe_ratio'].update(pontos=5,  criterio='Baixo (<5)')
            elif pe>50:      score-=10; det['pe_ratio'].update(pontos=-10,criterio='Muito alto (>50)')
            else:                       det['pe_ratio']['criterio'] = 'Regular'
        else:
            det['pe_ratio'] = {'valor': None, 'pontos': 0, 'criterio': ''}

        dy = info.get('dividendYield')
        if dy and isinstance(dy, (int, float)):
            det['dividend_yield'] = {'valor': dy, 'pontos': 0, 'criterio': ''}
            if dy>0.04:   score+=10; det['dividend_yield'].update(pontos=10, criterio='Excelente (>4%)')
            elif dy>0.02: score+=5;  det['dividend_yield'].update(pontos=5,  criterio='Bom (>2%)')
            else:                    det['dividend_yield']['criterio'] = 'Baixo (<2%)'
        else:
            det['dividend_yield'] = {'valor': None, 'pontos': 0, 'criterio': ''}

        rg = info.get('revenueGrowth')
        if rg and isinstance(rg, (int, float)):
            det['revenue_growth'] = {'valor': rg, 'pontos': 0, 'criterio': ''}
            if rg>0.20:    score+=15; det['revenue_growth'].update(pontos=15, criterio='Excelente (>20%)')
            elif rg>0.10:  score+=10; det['revenue_growth'].update(pontos=10, criterio='Muito bom (>10%)')
            elif rg>0.05:  score+=5;  det['revenue_growth'].update(pontos=5,  criterio='Bom (>5%)')
            elif rg<-0.10: score-=10; det['revenue_growth'].update(pontos=-10,criterio='Negativo (<-10%)')
            else:                     det['revenue_growth']['criterio'] = 'Estável'
        else:
            det['revenue_growth'] = {'valor': None, 'pontos': 0, 'criterio': ''}

        rec = info.get('recommendationKey', '')
        pts_rec  = {'strong_buy':10,'buy':5,'hold':0,'sell':-5,'strong_sell':-10}
        crit_rec = {'strong_buy':'Compra Forte','buy':'Compra','hold':'Manter',
                    'sell':'Venda','strong_sell':'Venda Forte'}
        score += pts_rec.get(rec, 0)
        det['recomendacao'] = {'valor': rec, 'pontos': pts_rec.get(rec,0),
                               'criterio': crit_rec.get(rec, rec.replace('_',' ').title() if rec else '')}

        mc = info.get('marketCap')
        if mc and isinstance(mc, (int, float)):
            det['market_cap'] = {'valor': mc, 'pontos': 0, 'criterio': ''}
            if mc>1e12:    score+=10; det['market_cap'].update(pontos=10, criterio='Mega Cap (>$1T)')
            elif mc>100e9: score+=5;  det['market_cap'].update(pontos=5,  criterio='Large Cap (>$100B)')
            elif mc>10e9:             det['market_cap']['criterio'] = 'Mid Cap (>$10B)'
            else:                     det['market_cap']['criterio'] = 'Small Cap (<$10B)'
        else:
            det['market_cap'] = {'valor': None, 'pontos': 0, 'criterio': ''}

        return {
            'fonte': fonte_label, 'ticker_fonte': ticker_label,
            'score': max(0, min(100, score)), 'detalhes': det,
            'pe_ratio': det['pe_ratio']['valor'],
            'dividend_yield': det['dividend_yield']['valor'],
            'market_cap': det['market_cap']['valor'],
            'revenue_growth': det['revenue_growth']['valor'],
            'recomendacao': det['recomendacao']['valor'],
            'setor': info.get('sector', 'N/A'),
        }

    # Tentativa 1: Yahoo Finance por nome
    try:
        nome_empresa = NOMES_BDRS.get(ticker_bdr, '')
        nome_limpo   = nome_empresa
        for suf in [' ADR', ' ADS', ' Ordinary Shares', ' Class A', ' Class B', ' Class C']:
            nome_limpo = nome_limpo.replace(suf, '')
        nome_limpo = nome_limpo.strip()
        if nome_limpo:
            res_busca = yf.Search(nome_limpo, max_results=5)
            quotes    = res_busca.quotes if hasattr(res_busca, 'quotes') else []
            tickers_us_candidatos = [
                q['symbol'] for q in quotes
                if q.get('quoteType') == 'EQUITY'
                and '.' not in q.get('symbol', '')
                and q.get('exchange', '') in ('NMS','NYQ','NGM','NCM','ASE','PCX','BTS','NAS','NYSE','NASDAQ')
            ]
            for t in tickers_us_candidatos[:3]:
                info    = yf.Ticker(t).info
                resultado = _score_from_yf_info(info, f'Yahoo Finance — {t}', t)
                if resultado:
                    return resultado
    except Exception:
        pass

    # Tentativa 2: Yahoo Finance por ticker mapeado
    try:
        for t in ([ticker_us, ticker_us.replace('-','.')] if '-' in ticker_us else [ticker_us]):
            info      = yf.Ticker(t).info
            resultado = _score_from_yf_info(info, f'Yahoo Finance — {t}', t)
            if resultado:
                return resultado
    except Exception:
        pass

    # Tentativa 3: OpenBB/FMP
    try:
        info_obb  = buscar_dados_openbb(ticker_us)
        resultado = _score_from_yf_info(info_obb, f'OpenBB/FMP — {ticker_us}', ticker_us)
        if resultado:
            return resultado
    except Exception:
        pass

    # Tentativa 4: BRAPI
    try:
        dados_brapi = buscar_dados_brapi(ticker_bdr)
        if dados_brapi:
            score, detalhes = calcular_score_brapi(dados_brapi)
            return {
                'fonte': 'BRAPI (BDR na B3)', 'ticker_fonte': ticker_bdr,
                'score': score, 'detalhes': detalhes,
                'pe_ratio': None, 'dividend_yield': None,
                'market_cap': dados_brapi.get('market_cap'),
                'revenue_growth': None, 'recomendacao': None,
                'setor': dados_brapi.get('setor', 'N/A'),
                'volume_b3': dados_brapi.get('volume'),
            }
    except Exception:
        pass

    return None


# =============================================================================
# DADOS PRINCIPAIS (scan diário para tabela de oportunidades)
# =============================================================================

@st.cache_data(ttl=1800)
def buscar_dados(tickers):
    if not tickers:
        return pd.DataFrame()
    sa_tickers = [f"{t}.SA" for t in tickers]
    try:
        df = yf.download(sa_tickers, period=PERIODO, auto_adjust=True, progress=False, timeout=60)
        if df.empty:
            return pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = pd.MultiIndex.from_tuples([(c[0], c[1].replace(".SA","")) for c in df.columns])
        return df.dropna(axis=1, how='all')
    except Exception:
        return pd.DataFrame()


def calcular_indicadores(df):
    df_calc = df.copy()
    tickers = df_calc.columns.get_level_values(1).unique()
    progresso = st.progress(0)
    total = len(tickers)
    for i, ticker in enumerate(tickers):
        progresso.progress((i + 1) / total)
        try:
            close = df_calc[('Close', ticker)]
            high  = df_calc[('High',  ticker)]
            low   = df_calc[('Low',   ticker)]
            delta = close.diff()
            ganho = delta.clip(lower=0).rolling(14).mean()
            perda = -delta.clip(upper=0).rolling(14).mean()
            rs    = ganho / perda
            df_calc[('RSI14',    ticker)] = 100 - (100 / (1 + rs))
            ll = low.rolling(14).min()
            hh = high.rolling(14).max()
            df_calc[('Stoch_K',  ticker)] = 100 * ((close - ll) / (hh - ll))
            df_calc[('EMA20',    ticker)] = close.ewm(span=20).mean()
            df_calc[('EMA50',    ticker)] = close.ewm(span=50).mean()
            df_calc[('EMA200',   ticker)] = close.ewm(span=200).mean()
            sma = close.rolling(20).mean()
            std = close.rolling(20).std()
            df_calc[('BB_Lower', ticker)] = sma - std * 2
            df_calc[('BB_Upper', ticker)] = sma + std * 2
            e12 = close.ewm(span=12).mean()
            e26 = close.ewm(span=26).mean()
            macd = e12 - e26
            sig  = macd.ewm(span=9).mean()
            df_calc[('MACD_Hist',ticker)] = macd - sig
        except Exception:
            continue
    progresso.empty()
    return df_calc


# =============================================================================
# BUSCA DE DADOS PARA O GRÁFICO (timeframe dinâmico)
# =============================================================================

@st.cache_data(ttl=300, show_spinner=False)
def buscar_dados_grafico(ticker, timeframe_key):
    """
    Busca OHLCV para o ticker no timeframe especificado.
    Retorna DataFrame simples (não multi-index) com colunas OHLCV, ou None.
    """
    params = TIMEFRAME_MAP.get(timeframe_key, TIMEFRAME_MAP["Diário"])
    try:
        df = yf.download(
            f"{ticker}.SA",
            interval=params["interval"],
            period=params["period"],
            auto_adjust=True,
            progress=False,
            timeout=30,
        )
        if df.empty:
            return None
        # Flatten multi-index se existir
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]
        df = df[['Open','High','Low','Close','Volume']].dropna()
        df.index = pd.to_datetime(df.index)
        return df
    except Exception:
        return None


def calcular_indicadores_simples(df):
    """
    Calcula indicadores técnicos em DataFrame simples (OHLCV).
    Retorna o mesmo DF com colunas adicionais.
    """
    df    = df.copy()
    close = df['Close']
    high  = df['High']
    low   = df['Low']

    # RSI 14
    delta = close.diff()
    ganho = delta.clip(lower=0).rolling(14).mean()
    perda = -delta.clip(upper=0).rolling(14).mean()
    df['RSI14']   = 100 - (100 / (1 + ganho / perda))

    # Estocástico %K 14
    ll = low.rolling(14).min()
    hh = high.rolling(14).max()
    df['Stoch_K'] = 100 * ((close - ll) / (hh - ll))

    # EMAs
    df['EMA20']  = close.ewm(span=20,  adjust=False).mean()
    df['EMA50']  = close.ewm(span=50,  adjust=False).mean()
    df['EMA200'] = close.ewm(span=200, adjust=False).mean()

    # Bollinger
    sma = close.rolling(20).mean()
    std = close.rolling(20).std()
    df['BB_Lower'] = sma - std * 2
    df['BB_Upper'] = sma + std * 2

    # MACD
    e12 = close.ewm(span=12, adjust=False).mean()
    e26 = close.ewm(span=26, adjust=False).mean()
    macd = e12 - e26
    df['MACD_Hist'] = macd - macd.ewm(span=9, adjust=False).mean()

    return df


# =============================================================================
# DESENHO DE CANDLESTICKS
# =============================================================================

def _desenhar_candles(ax, df, width_frac=0.7):
    """Desenha candlesticks OHLC em eixo matplotlib."""
    if len(df) == 0:
        return

    datas_num = mdates.date2num(df.index.to_pydatetime())
    spacing   = np.median(np.diff(datas_num)) if len(datas_num) > 1 else 1.0
    width     = spacing * width_frac

    COR_ALTA  = '#26a69a'
    COR_BAIXA = '#ef5350'

    opens  = df['Open'].values
    highs  = df['High'].values
    lows   = df['Low'].values
    closes = df['Close'].values

    for d, o, h, l, c in zip(datas_num, opens, highs, lows, closes):
        if any(pd.isna(v) for v in [o, h, l, c]):
            continue
        cor      = COR_ALTA if c >= o else COR_BAIXA
        body_bot = min(o, c)
        body_h   = max(abs(c - o), (h - l) * 0.005)  # altura mínima visível
        rect = mpatches.Rectangle(
            (d - width / 2, body_bot), width, body_h,
            facecolor=cor, edgecolor=cor, linewidth=0.3, zorder=4
        )
        ax.add_patch(rect)
        ax.plot([d, d], [l, body_bot],          color=cor, linewidth=0.8, zorder=3)
        ax.plot([d, d], [body_bot + body_h, h], color=cor, linewidth=0.8, zorder=3)

    ax.set_xlim(datas_num[0] - width, datas_num[-1] + width)


# =============================================================================
# GRÁFICO PRINCIPAL — suporta Linha ou Candles + qualquer timeframe
# =============================================================================

def plotar_grafico(df_ticker, ticker, empresa, rsi_val, is_val, tipo_grafico="Linha"):
    """
    Plota o gráfico técnico principal.

    Parâmetros
    ----------
    df_ticker   : DataFrame com OHLCV + indicadores calculados
    ticker      : Código da BDR
    empresa     : Nome da empresa
    rsi_val     : RSI atual (para título)
    is_val      : Índice de Sobrevenda (para título)
    tipo_grafico: "Linha" ou "Candles"
    """
    # ── Seleciona e alinha colunas ────────────────────────────────────────────
    necessarias = ['Close','Open','High','Low','Volume','EMA20','RSI14','Stoch_K','BB_Lower','BB_Upper']
    opcionais   = ['EMA50','EMA200','MACD_Hist']
    presentes   = [c for c in necessarias + opcionais if c in df_ticker.columns]
    df = df_ticker[presentes].copy()
    df = df.dropna(subset=['Close','EMA20']).sort_index()

    close  = df['Close']
    ema20  = df['EMA20']
    ema50  = df['EMA50']  if 'EMA50'  in df.columns else None
    ema200 = df['EMA200'] if 'EMA200' in df.columns else None
    datas  = df.index

    fig, axes = plt.subplots(3, 1, figsize=(10, 8), sharex=True,
                             gridspec_kw={'height_ratios': [3, 1, 1]})

    # ── Fibonacci ─────────────────────────────────────────────────────────────
    high_max = df['High'].max() if 'High' in df.columns else close.max()
    low_min  = df['Low'].min()  if 'Low'  in df.columns else close.min()
    diff = high_max - low_min
    fib_levels = {
        '0%':    high_max,
        '23.6%': high_max - diff * 0.236,
        '38.2%': high_max - diff * 0.382,
        '50%':   high_max - diff * 0.500,
        '61.8%': high_max - diff * 0.618,
        '78.6%': high_max - diff * 0.786,
        '100%':  low_min,
    }
    fib_colors = {
        '0%':'#e74c3c','23.6%':'#e67e22','38.2%':'#f39c12',
        '50%':'#3498db','61.8%':'#2ecc71','78.6%':'#1abc9c','100%':'#9b59b6',
    }

    ax1 = axes[0]

    # Bollinger (fundo)
    if 'BB_Lower' in df.columns and 'BB_Upper' in df.columns:
        ax1.fill_between(datas, df['BB_Lower'], df['BB_Upper'],
                         alpha=0.07, color='#607d8b', zorder=0)

    # Fibonacci
    for nivel, preco_fib in fib_levels.items():
        cor = fib_colors[nivel]
        ax1.axhline(preco_fib, color=cor, linestyle='--', linewidth=0.9, alpha=0.55, zorder=1)
        ax1.text(datas[-1], preco_fib, f' Fib {nivel}', fontsize=7.5, color=cor, va='center',
                 bbox=dict(boxstyle='round,pad=0.2', facecolor='white', edgecolor=cor, alpha=0.75))

    ax1.axhspan(fib_levels['61.8%'] * 0.99, fib_levels['61.8%'] * 1.01,
                alpha=0.12, color='#2ecc71', zorder=0)

    # EMAs (antes do preço para não sobrepor)
    ax1.plot(datas, ema20, label='EMA20',  color='#2962FF', linewidth=1.4, alpha=0.9, zorder=3)
    if ema50 is not None:
        ax1.plot(datas, ema50.reindex(datas),  label='EMA50',  color='#FF6D00', linewidth=1.4, alpha=0.85, zorder=3)
    if ema200 is not None:
        ax1.plot(datas, ema200.reindex(datas), label='EMA200', color='#00695C', linewidth=1.8, alpha=0.8,  zorder=3)

    # ── Preço: Candles ou Linha ───────────────────────────────────────────────
    if tipo_grafico == "Candles" and all(c in df.columns for c in ['Open','High','Low','Close']):
        _desenhar_candles(ax1, df)
        ax1.set_xlim(mdates.date2num(datas[0].to_pydatetime()) - 1,
                     mdates.date2num(datas[-1].to_pydatetime()) + 1)
    else:
        ax1.plot(datas, close, label='Close', color='#1a1a2e', linewidth=2.0, zorder=5)
        ax1.scatter([datas[-1]], [close.iloc[-1]], color='#e74c3c', s=40, zorder=6)

    # ── Tendência ─────────────────────────────────────────────────────────────
    ult_close  = close.iloc[-1]
    ult_ema20  = ema20.iloc[-1]
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

    nivel_mais_proximo = min(fib_levels, key=lambda n: abs(ult_close - fib_levels[n]))

    tipo_label = "🕯 Candles" if tipo_grafico == "Candles" else "📈 Linha"
    ax1.set_title(
        f'{ticker} - {empresa} | I.S.: {is_val:.0f} | {status} | Fib: {nivel_mais_proximo} | {tipo_label}',
        fontweight='bold', fontsize=9, pad=6
    )
    ax1.legend(loc='upper left', fontsize=7, framealpha=0.92, ncol=3)
    ax1.grid(True, alpha=0.18, zorder=0)
    ax1.set_ylabel('Preço (R$)', fontsize=9)

    # ── RSI ───────────────────────────────────────────────────────────────────
    ax2 = axes[1]
    if 'RSI14' in df.columns:
        ax2.plot(datas, df['RSI14'].reindex(datas), color='#FF6F00', linewidth=1.5, label='RSI14')
        ax2.axhline(30, color='#F44336', linestyle='--', linewidth=1, alpha=0.7)
        ax2.axhline(70, color='#4CAF50', linestyle='--', linewidth=1, alpha=0.7)
        ax2.fill_between(datas, 0,  30, alpha=0.15, color='#F44336')
        ax2.fill_between(datas, 70, 100, alpha=0.15, color='#4CAF50')
    ax2.set_ylabel('RSI', fontsize=9)
    ax2.set_ylim(0, 100)
    ax2.grid(True, alpha=0.18)

    # ── Estocástico ───────────────────────────────────────────────────────────
    ax3 = axes[2]
    if 'Stoch_K' in df.columns:
        ax3.plot(datas, df['Stoch_K'].reindex(datas), color='#9C27B0', linewidth=1.5, label='Stoch %K')
        ax3.axhline(20, color='#F44336', linestyle='--', linewidth=1, alpha=0.7)
        ax3.axhline(80, color='#4CAF50', linestyle='--', linewidth=1, alpha=0.7)
        ax3.fill_between(datas, 0,  20, alpha=0.15, color='#F44336')
        ax3.fill_between(datas, 80, 100, alpha=0.15, color='#4CAF50')
    ax3.set_ylabel('Stoch', fontsize=9)
    ax3.set_ylim(0, 100)
    ax3.grid(True, alpha=0.18)

    # ── Eixo X legível ────────────────────────────────────────────────────────
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
    ax3.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=30, ha='right', fontsize=8)
    ax3.set_xlabel('Data', fontsize=9)

    plt.tight_layout()
    return fig


# =============================================================================
# ANÁLISE DE OPORTUNIDADES
# =============================================================================

def calcular_fibonacci(df_ticker):
    try:
        if len(df_ticker) < 50:
            return None
        high = df_ticker['High'].max()
        low  = df_ticker['Low'].min()
        diff = high - low
        return {'61.8%': low + diff * 0.618}
    except Exception:
        return None

def gerar_sinal(row_ticker, df_ticker):
    sinais, score, explicacoes = [], 0, []

    def classificar(s):
        if s >= 4: return "Muito Alta"
        if s >= 2: return "Alta"
        if s >= 1: return "Média"
        return "Baixa"

    try:
        close    = row_ticker.get('Close')
        rsi      = row_ticker.get('RSI14')
        stoch    = row_ticker.get('Stoch_K')
        macd_h   = row_ticker.get('MACD_Hist')
        bb_lower = row_ticker.get('BB_Lower')

        if pd.notna(rsi):
            if rsi < 30:
                sinais.append("RSI Oversold")
                explicacoes.append(f"📉 RSI em {rsi:.1f} (< 30): Forte sobrevenda")
                score += 3
            elif rsi < 40:
                sinais.append("RSI Baixo")
                explicacoes.append(f"📊 RSI em {rsi:.1f} (< 40): Sobrevenda moderada")
                score += 1

        if pd.notna(stoch) and stoch < 20:
            sinais.append("Stoch. Fundo")
            explicacoes.append(f"📉 Estocástico em {stoch:.1f} (< 20): Muito sobrevendido")
            score += 2

        if pd.notna(macd_h) and macd_h > 0:
            sinais.append("MACD Virando")
            explicacoes.append("🔄 MACD positivo: Momentum de alta começando")
            score += 1

        if pd.notna(close) and pd.notna(bb_lower):
            if close < bb_lower:
                sinais.append("Abaixo BB")
                explicacoes.append("⚠️ Abaixo da Banda Bollinger: Sobrevenda extrema")
                score += 2
            elif close < bb_lower * 1.02:
                sinais.append("Suporte BB")
                explicacoes.append("🎯 Próximo da Banda Inferior: Zona de suporte")
                score += 1

        fibo = calcular_fibonacci(df_ticker)
        if fibo and pd.notna(close) and (fibo['61.8%'] * 0.99 <= close <= fibo['61.8%'] * 1.01):
            sinais.append("Fibo 61.8%")
            explicacoes.append("⭐ Na Zona de Ouro do Fibonacci (61.8%): Ponto ideal de reversão!")
            score += 2

        return sinais, score, classificar(score), explicacoes
    except Exception:
        return [], 0, "Indefinida", []

def analisar_oportunidades(df_calc, mapa_nomes):
    resultados = []
    tickers    = df_calc.columns.get_level_values(1).unique()

    for ticker in tickers:
        try:
            df_ticker = df_calc.xs(ticker, axis=1, level=1).dropna()
            if len(df_ticker) < 50:
                continue

            last     = df_ticker.iloc[-1]
            anterior = df_ticker.iloc[-2]
            preco    = last.get('Close')
            preco_ant= anterior.get('Close')
            volume   = last.get('Volume')
            preco_op = last.get('Open')

            if pd.isna(preco) or pd.isna(preco_ant):
                continue

            queda_dia = ((preco - preco_ant) / preco_ant) * 100
            gap       = ((preco_op - preco_ant) / preco_ant) * 100
            if queda_dia >= 0:
                continue

            sinais, score, classificacao, explicacoes = gerar_sinal(last, df_ticker)
            rsi   = last.get('RSI14', 50)
            stoch = last.get('Stoch_K', 50)
            is_index = ((100 - rsi) + (100 - stoch)) / 2

            # Ranking de liquidez 0-10
            try:
                n = min(20, len(df_ticker))
                vol_serie = df_ticker['Volume'].tail(n)
                vol_medio = vol_serie.mean() or 0
                n_gaps = sum(
                    1 for i in range(1, min(n+1, len(df_ticker)))
                    if df_ticker['Close'].iloc[-i-1] > 0 and
                    abs((df_ticker['Open'].iloc[-i] - df_ticker['Close'].iloc[-i-1]) /
                        df_ticker['Close'].iloc[-i-1]) * 100 > 1
                )
                consist = sum(1 for v in vol_serie if pd.notna(v) and v >= vol_medio * 0.8) / n if n > 0 else 0
                liq = 0
                liq += 40 if vol_medio>500000 else 35 if vol_medio>100000 else 30 if vol_medio>50000 else \
                       25 if vol_medio>10000  else 20 if vol_medio>5000   else 15 if vol_medio>1000  else \
                       10 if vol_medio>100    else 5
                liq += 30 if n_gaps==0 else 25 if n_gaps<=2 else 20 if n_gaps<=5 else \
                       15 if n_gaps<=8 else 10 if n_gaps<=12 else 5
                liq += 30 if consist>=0.75 else 20 if consist>=0.50 else 10 if consist>=0.25 else 5
                ranking_liq = max(0, min(10, round(liq / 10)))
            except Exception:
                ranking_liq = 1

            nome_completo = mapa_nomes.get(ticker, ticker)
            if nome_completo == ticker:
                nome_curto = ticker
            else:
                ignore = {'INC','CORP','LTD','S.A.','GMBH','PLC','GROUP','HOLDINGS','CO','LLC'}
                uteis  = [p for p in nome_completo.split()
                           if p.upper().replace('.','').replace(',','') not in ignore]
                nome_curto = " ".join(uteis[:2]).replace(',','').title() if uteis else nome_completo

            resultados.append({
                'Ticker': ticker, 'Empresa': nome_curto,
                'Preco': preco, 'Volume': volume,
                'Queda_Dia': queda_dia, 'Gap': gap,
                'IS': is_index, 'RSI14': rsi, 'Stoch': stoch,
                'Potencial': classificacao, 'Score': score,
                'Sinais': ", ".join(sinais) if sinais else "-",
                'Explicacoes': explicacoes, 'Liquidez': int(ranking_liq)
            })
        except Exception:
            continue
    return resultados


# =============================================================================
# ESTILIZAÇÃO
# =============================================================================

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
    paleta = {
        0:('#7f0000','white'), 1:('#c62828','white'), 2:('#ef5350','white'),
        3:('#ff7043','white'), 4:('#ffa726','black'), 5:('#fdd835','black'),
        6:('#d4e157','black'), 7:('#9ccc65','black'), 8:('#66bb6a','black'),
        9:('#2e7d32','white'),10:('#1b5e20','white'),
    }
    try:
        v = int(val)
    except Exception:
        v = 0
    bg, fg = paleta.get(v, ('#9e9e9e','white'))
    return f'background-color:{bg};color:{fg};font-weight:900;font-size:1.1em;text-align:center;'


# =============================================================================
# LAYOUT DO APP
# =============================================================================

st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem; border-radius: 10px; margin-bottom: 2rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .main-title    { color:white; font-size:2.5rem; font-weight:700; margin:0; text-align:center; }
    .main-subtitle { color:rgba(255,255,255,0.9); font-size:1.1rem; text-align:center; margin-top:0.5rem; }
    .section-header {
        color:#667eea; font-size:1.5rem; font-weight:600;
        margin-top:2rem; margin-bottom:1rem;
        padding-bottom:0.5rem; border-bottom:2px solid #667eea;
    }
    .stButton > button {
        width:100%;
        background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);
        color:white; font-weight:600; border:none;
        padding:0.75rem 2rem; border-radius:8px; transition:all 0.3s ease;
    }
    .stButton > button:hover { transform:translateY(-2px); box-shadow:0 4px 12px rgba(102,126,234,0.4); }
    .chart-controls {
        background:linear-gradient(135deg,#f0f4ff 0%,#e8f0fe 100%);
        border:1.5px solid #c7d2fe; border-radius:10px;
        padding:0.9rem 1.2rem; margin-bottom:0.8rem;
    }
</style>
""", unsafe_allow_html=True)

fuso_brasil    = pytz.timezone('America/Sao_Paulo')
agora          = datetime.now(fuso_brasil)
data_hora      = agora.strftime("%d/%m/%Y às %H:%M:%S")
dias_pt        = {'Monday':'Segunda-feira','Tuesday':'Terça-feira','Wednesday':'Quarta-feira',
                  'Thursday':'Quinta-feira','Friday':'Sexta-feira','Saturday':'Sábado','Sunday':'Domingo'}
dia_semana_pt  = dias_pt.get(agora.strftime("%A"), agora.strftime("%A"))

st.markdown(f"""
<div class="main-header">
    <h1 class="main-title">📊 Monitor BDR - Swing Trade Pro</h1>
    <p class="main-subtitle">Análise Técnica Avançada | Rastreamento de Oportunidades em Tempo Real</p>
    <p style="color:rgba(255,255,255,0.8);font-size:0.9rem;text-align:center;margin-top:0.5rem;">
        🕐 {dia_semana_pt}, {data_hora} (Horário de Brasília)
    </p>
</div>
""", unsafe_allow_html=True)

col_info1, col_info2, col_info3 = st.columns(3)
with col_info1: st.markdown("**📈 Estratégia:** Reversão em Sobrevenda")
with col_info2: st.markdown("**🎯 Foco:** BDRs em Queda com Potencial")
with col_info3: st.markdown("**⏱️ Timeframe:** 1 Ano | Diário")
st.markdown("---")

with st.expander("📚 Guia dos Indicadores - Entenda os Sinais", expanded=False):
    st.markdown("""
    ### 🎯 Índice de Sobrevenda (I.S.) | 📉 RSI < 30 = Sobrevenda | 📊 Estocástico < 20 = Sobrevenda
    ### 📈 MACD positivo → Momentum de alta | 🎨 Abaixo Bollinger → Sobrevenda extrema
    ### 🌟 Fibonacci 61.8% = Zona de Ouro | 📊 EMAs = Tendência dominante
    ### 🖥️ Triple Screen (Elder 1986): EMA13+MACD → EFI2 → Buy/Sell Stop
    ### 🕯 Candles: Verde = fechamento > abertura | Vermelho = fechamento < abertura
    """)

st.markdown("---")

if st.button("🔄 Atualizar Análise", type="primary"):
    with st.spinner("Conectando à API e baixando dados..."):
        lista_bdrs = list(NOMES_BDRS.keys())
        df         = buscar_dados(lista_bdrs)
        if df.empty:
            st.error("Erro ao carregar dados. Aguarde alguns minutos e tente novamente.")
            st.stop()

    with st.spinner("Calculando indicadores técnicos..."):
        df_calc = calcular_indicadores(df)

    with st.spinner("Analisando oportunidades..."):
        oportunidades = analisar_oportunidades(df_calc, NOMES_BDRS)
        if oportunidades:
            st.session_state['oportunidades'] = oportunidades
            st.session_state['df_calc']       = df_calc

if 'oportunidades' in st.session_state and 'df_calc' in st.session_state:
    oportunidades = st.session_state['oportunidades']
    df_calc       = st.session_state['df_calc']

    df_res = pd.DataFrame(oportunidades).sort_values(by='Queda_Dia', ascending=True)
    st.success(f"✅ {len(oportunidades)} oportunidades detectadas!")

    # ── Filtros ───────────────────────────────────────────────────────────────
    st.markdown('<h3 class="section-header">🎯 Filtros de Tendência</h3>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    with c1: filtrar_ema20  = st.checkbox("📈 Acima da EMA20",  value=False)
    with c2: filtrar_ema50  = st.checkbox("📊 Acima da EMA50",  value=False)
    with c3: filtrar_ema200 = st.checkbox("📉 Acima da EMA200", value=False)

    st.markdown("**💧 Liquidez mínima:**")
    ranking_min_liq = st.slider("0 = sem filtro  |  10 = máxima exigência",
                                min_value=0, max_value=10, value=0, step=1)

    if filtrar_ema20 or filtrar_ema50 or filtrar_ema200 or ranking_min_liq > 0:
        df_res_filtrado = []
        cnt = {'ema20': 0, 'ema50': 0, 'ema200': 0, 'sem_dados': 0}

        for opp in oportunidades:
            t = opp['Ticker']
            try:
                df_t = df_calc.xs(t, axis=1, level=1).dropna()
                tam  = len(df_t)
                if tam < 20:
                    cnt['sem_dados'] += 1
                    continue

                uc   = df_t['Close'].iloc[-1]
                ok   = True

                if filtrar_ema20:
                    e20 = df_t['EMA20'].iloc[-1] if ('EMA20' in df_t.columns and tam >= 20) else None
                    if e20 is not None and pd.notna(e20) and uc > e20: cnt['ema20'] += 1
                    else: ok = False

                if filtrar_ema50 and ok:
                    e50 = df_t['EMA50'].iloc[-1] if ('EMA50' in df_t.columns and tam >= 50) else None
                    if e50 is not None and pd.notna(e50) and uc > e50: cnt['ema50'] += 1
                    else: ok = False

                if filtrar_ema200 and ok:
                    e200 = df_t['EMA200'].iloc[-1] if ('EMA200' in df_t.columns and tam >= 50) else None
                    if e200 is not None and pd.notna(e200) and uc > e200: cnt['ema200'] += 1
                    else: ok = False

                if ranking_min_liq > 0 and ok:
                    if opp.get('Liquidez', 0) < ranking_min_liq:
                        ok = False

                if ok:
                    df_res_filtrado.append(opp)
            except Exception:
                cnt['sem_dados'] += 1

        if df_res_filtrado:
            df_res = pd.DataFrame(df_res_filtrado).sort_values(by='Queda_Dia', ascending=True)
            ativos = ([f"EMA20 ({cnt['ema20']}✓)"]  if filtrar_ema20  else []) + \
                     ([f"EMA50 ({cnt['ema50']}✓)"]  if filtrar_ema50  else []) + \
                     ([f"EMA200 ({cnt['ema200']}✓)"] if filtrar_ema200 else [])
            st.markdown(f"""
            <div style='background:linear-gradient(135deg,#d4fc79 0%,#96e6a1 100%);
                        padding:1rem;border-radius:8px;margin:1rem 0;'>
                <p style='margin:0;color:#166534;font-weight:600;font-size:1.1rem;'>
                    ✅ {len(df_res)} BDRs | Filtros: {' + '.join(ativos) if ativos else 'Liquidez'}
                </p>
            </div>""", unsafe_allow_html=True)
        else:
            st.warning("⚠️ Nenhuma BDR passou em todos os filtros combinados.")
            df_res = pd.DataFrame()

    if not df_res.empty:
        st.markdown('<h3 class="section-header">📊 Oportunidades Detectadas</h3>', unsafe_allow_html=True)
        st.info("💡 Clique em qualquer linha para visualizar o gráfico técnico completo")

        evento = st.dataframe(
            df_res.style
                  .map(estilizar_potencial, subset=['Potencial'])
                  .map(estilizar_is,        subset=['IS'])
                  .map(estilizar_liquidez,  subset=['Liquidez'])
                  .format({'Preco':'R$ {:.2f}','Volume':'{:,.0f}','Queda_Dia':'{:.2f}%',
                           'Gap':'{:.2f}%','IS':'{:.0f}','RSI14':'{:.0f}',
                           'Stoch':'{:.0f}','Liquidez':'{:.0f}'}),
            column_order=("Ticker","Empresa","Liquidez","Preco","Queda_Dia",
                          "IS","Volume","Gap","Potencial","Score","Sinais"),
            column_config={
                "Empresa":   st.column_config.TextColumn("Empresa", width="medium"),
                "Liquidez":  st.column_config.NumberColumn("💧 Liq.", width="small",
                             help="Ranking de Liquidez 0-10"),
                "IS":        st.column_config.NumberColumn("I.S.", help="Índice de Sobrevenda"),
                "Volume":    st.column_config.NumberColumn("Vol."),
                "Score":     st.column_config.ProgressColumn("Força", format="%d", min_value=0, max_value=10),
                "Potencial": st.column_config.Column("Sinal"),
                "Sinais":    st.column_config.TextColumn("Sinais Técnicos", width="large"),
            },
            use_container_width=True, hide_index=True,
            on_select="rerun", selection_mode="single-row"
        )

        # ── DETALHE DO TICKER SELECIONADO ─────────────────────────────────────
        if evento.selection and evento.selection.rows:
            st.markdown("---")
            row    = df_res.iloc[evento.selection.rows[0]]
            ticker = row['Ticker']

            st.markdown(
                f'<h3 class="section-header">📈 {ticker} — {row["Empresa"]}</h3>',
                unsafe_allow_html=True
            )

            # ── *** CONTROLES DE GRÁFICO *** ──────────────────────────────────
            st.markdown("""
            <div class="chart-controls">
                <strong>⚙️ Configurações do Gráfico</strong>
            </div>""", unsafe_allow_html=True)

            ctrl1, ctrl2, ctrl3 = st.columns([2, 2, 2])

            with ctrl1:
                timeframe_sel = st.selectbox(
                    "⏱️ Timeframe",
                    options=list(TIMEFRAME_MAP.keys()),
                    index=2,   # "Diário" como padrão
                    key=f"tf_{ticker}",
                    help=(
                        "5 min → últimos 5 dias | "
                        "60 min → últimos 60 dias | "
                        "Diário → 1 ano | "
                        "Semanal → 2 anos | "
                        "Mensal → 5 anos"
                    )
                )

            with ctrl2:
                tipo_grafico = st.radio(
                    "📊 Tipo de Gráfico",
                    options=["Linha", "Candles"],
                    index=0,
                    horizontal=True,
                    key=f"tipo_{ticker}",
                    help="Candles mostram Abertura/Máxima/Mínima/Fechamento. Verde = alta, Vermelho = baixa."
                )

            with ctrl3:
                st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
                label_tf = TIMEFRAME_MAP[timeframe_sel]['label']
                st.info(f"📅 {label_tf}")

            # ── Carrega dados do timeframe escolhido ──────────────────────────
            with st.spinner(f"Carregando dados {timeframe_sel}..."):
                df_grafico_raw = buscar_dados_grafico(ticker, timeframe_sel)

            if df_grafico_raw is not None and len(df_grafico_raw) >= 10:
                df_grafico = calcular_indicadores_simples(df_grafico_raw)
            else:
                # Fallback: dados diários já em memória
                aviso_tf = (f"⚠️ Dados insuficientes para **{timeframe_sel}** "
                            f"(mín. 10 barras). Exibindo gráfico Diário.")
                st.warning(aviso_tf)
                df_ticker_base = df_calc.xs(ticker, axis=1, level=1).dropna()
                df_grafico     = df_ticker_base

            # ── Layout: gráfico + painel lateral ──────────────────────────────
            col_graf, col_info = st.columns([3, 1])

            with col_graf:
                try:
                    fig = plotar_grafico(
                        df_grafico, ticker, row['Empresa'],
                        row['RSI14'], row['IS'],
                        tipo_grafico=tipo_grafico
                    )
                    st.pyplot(fig)
                    plt.close(fig)
                except Exception as e:
                    st.error(f"❌ Erro ao gerar gráfico: {e}")

            with col_info:
                potencial = row['Potencial']
                if "Alta" in potencial:
                    cor_bg, cor_txt, icone = "linear-gradient(135deg,#d4fc79 0%,#96e6a1 100%)", "#166534", "🟢"
                elif "Média" in potencial:
                    cor_bg, cor_txt, icone = "linear-gradient(135deg,#ffeaa7 0%,#fdcb6e 100%)", "#7c3626", "🟡"
                else:
                    cor_bg, cor_txt, icone = "linear-gradient(135deg,#dfe6e9 0%,#b2bec3 100%)", "#2d3436", "⚪"

                st.markdown(f"""
                <div style='background:{cor_bg};padding:1rem;border-radius:8px;margin-bottom:1rem;'>
                    <h2 style='margin:0;color:{cor_txt};text-align:center;'>{icone} {potencial}</h2>
                </div>""", unsafe_allow_html=True)

                st.metric("💰 Preço Atual",      f"R$ {row['Preco']:.2f}")
                st.metric("📉 Queda no Dia",     f"{row['Queda_Dia']:.2f}%", delta_color="inverse")
                st.metric("🎯 I.S. (Sobrevenda)", f"{row['IS']:.0f}/100")
                if row['Gap'] < -1:
                    st.metric("⚡ Gap de Abertura", f"{row['Gap']:.2f}%", delta_color="inverse")
                st.markdown(f"**⭐ Score:** {row['Score']}/10")
                st.markdown(f"**📊 Volume:** {row['Volume']:,.0f}")

                st.markdown("""
                <div style='background:#e0e7ff;padding:0.75rem;border-radius:6px;margin-top:1rem;'>
                    <p style='margin:0;font-weight:600;color:#3730a3;font-size:0.9rem;'>
                        📋 Sinais Detectados</p>
                </div>""", unsafe_allow_html=True)
                st.markdown(
                    f"<p style='font-size:0.85rem;color:#475569;'>{row['Sinais']}</p>",
                    unsafe_allow_html=True
                )

                if 'Explicacoes' in row and row['Explicacoes']:
                    st.markdown("""
                    <div style='background:#fef3c7;padding:0.75rem;border-radius:6px;margin-top:1rem;'>
                        <p style='margin:0;font-weight:600;color:#92400e;font-size:0.9rem;'>
                            💡 O que isso significa?</p>
                    </div>""", unsafe_allow_html=True)
                    for exp in row['Explicacoes']:
                        st.markdown(
                            f"<p style='font-size:0.82rem;color:#92400e;margin:0.3rem 0;'>• {exp}</p>",
                            unsafe_allow_html=True
                        )

            # ── Triple Screen ─────────────────────────────────────────────────
            st.markdown("---")
            try:
                df_ts    = df_calc.xs(ticker, axis=1, level=1).dropna()
                res_ts   = analisar_triple_screen(df_ts)
            except Exception:
                res_ts = None
            renderizar_triple_screen(res_ts, ticker, row['Empresa'])

            # ── Fundamentalista ───────────────────────────────────────────────
            st.markdown("---")
            st.markdown('<h3 class="section-header">📊 Análise Fundamentalista</h3>', unsafe_allow_html=True)

            with st.spinner(f"Buscando dados fundamentalistas de {ticker}..."):
                fund_data = buscar_dados_fundamentalistas(ticker)

            if fund_data:
                score  = fund_data['score']
                fonte  = fund_data.get('fonte', 'Yahoo Finance')
                t_fonte= fund_data.get('ticker_fonte', ticker)

                if score >= 80:   cf, ct, lbl = "linear-gradient(135deg,#d4fc79 0%,#96e6a1 100%)", "#166534", "EXCELENTE"
                elif score >= 65: cf, ct, lbl = "linear-gradient(135deg,#a7f3d0 0%,#6ee7b7 100%)", "#065f46", "BOM"
                elif score >= 50: cf, ct, lbl = "linear-gradient(135deg,#fde047 0%,#fbbf24 100%)", "#92400e", "NEUTRO"
                elif score >= 35: cf, ct, lbl = "linear-gradient(135deg,#fdcb6e 0%,#ff7043 100%)", "#7c3626", "ATENÇÃO"
                else:             cf, ct, lbl = "linear-gradient(135deg,#ef5350 0%,#c62828 100%)", "white",   "EVITAR"

                st.markdown(f"""
                <div style='background:{cf};padding:1.5rem;border-radius:12px;margin-bottom:1.5rem;'>
                    <div style='text-align:center;'>
                        <h1 style='margin:0;color:{ct};font-size:4rem;font-weight:900;'>{score:.0f}%</h1>
                        <p style='margin:0.5rem 0 0 0;color:{ct};font-size:1.5rem;font-weight:600;'>{lbl}</p>
                    </div>
                </div>""", unsafe_allow_html=True)

                if 'BRAPI' in fonte:
                    st.info(f"📡 **Fonte:** {fonte} | Ticker: **{t_fonte}**")
                else:
                    st.success(f"📡 **Fonte:** {fonte} | Ticker US: **{t_fonte}**")

                fc1, fc2, fc3 = st.columns(3)
                with fc1:
                    st.markdown("### 📈 Valuation")
                    st.metric("P/E Ratio", f"{fund_data['pe_ratio']:.2f}" if fund_data.get('pe_ratio') else "N/A")
                    if fund_data.get('market_cap'):
                        mcap_b = fund_data['market_cap'] / 1e9
                        st.metric("Market Cap", f"${mcap_b/1000:.2f}T" if mcap_b >= 1000 else f"${mcap_b:.1f}B")
                    else:
                        st.metric("Market Cap", "N/A")

                with fc2:
                    st.markdown("### 💰 Rentabilidade")
                    st.metric("Dividend Yield",
                              f"{fund_data['dividend_yield']*100:.2f}%" if fund_data.get('dividend_yield') else "N/A")
                    if fund_data.get('revenue_growth'):
                        g = fund_data['revenue_growth'] * 100
                        st.metric("Crescimento Receita", f"{g:+.1f}%")
                    elif fund_data.get('volume_b3'):
                        st.metric("Volume B3", f"{fund_data['volume_b3']:,.0f}")
                    else:
                        st.metric("Crescimento Receita", "N/A")

                with fc3:
                    st.markdown("### 🎯 Info")
                    rec = fund_data.get('recomendacao')
                    if rec and rec != 'N/A':
                        rec_map = {
                            'strong_buy':('🟢 COMPRA FORTE','green'),'buy':('🟢 Compra','green'),
                            'hold':('🟡 Manter','orange'),'sell':('🔴 Venda','red'),
                            'strong_sell':('🔴 VENDA FORTE','red'),
                        }
                        rt, rc = rec_map.get(rec, (rec.upper(),'gray'))
                        st.markdown("**Analistas:**")
                        st.markdown(f"<h3 style='color:{rc};margin:0;'>{rt}</h3>", unsafe_allow_html=True)
                    if fund_data.get('setor') and fund_data['setor'] != 'N/A':
                        st.markdown(f"**Setor:** {fund_data['setor']}")

                st.markdown("---")
                st.markdown("### 📋 Detalhamento da Pontuação")
                detalhes     = fund_data.get('detalhes', {})
                dados_tabela = []
                if 'fonte' in detalhes and 'BRAPI' in detalhes['fonte'].get('valor', ''):
                    for k, lbl_d, max_pts in [('market_cap','Market Cap','/20'),('volume','Volume B3','/10')]:
                        d = detalhes.get(k, {})
                        if d.get('valor'):
                            v = d['valor']
                            vs = f"${v/1e9:.1f}B" if k=='market_cap' else f"{v:,.0f}"
                            dados_tabela.append({'Métrica':lbl_d,'Valor':vs,
                                                 'Pontos':f"{d['pontos']:+d}{max_pts}",'Avaliação':d.get('criterio','')})
                else:
                    campos = [
                        ('pe_ratio',      'P/E Ratio',            '/15', lambda v: f"{v:.2f}"),
                        ('dividend_yield','Dividend Yield',       '/10', lambda v: f"{v*100:.2f}%"),
                        ('revenue_growth','Crescimento Receita',  '/15', lambda v: f"{v*100:+.1f}%"),
                        ('recomendacao',  'Recomendação',         '/10', lambda v: v.replace('_',' ').title()),
                        ('market_cap',    'Market Cap',           '/10',
                         lambda v: f"${v/1e12:.2f}T" if v>=1e12 else f"${v/1e9:.1f}B"),
                    ]
                    for campo, lbl_d, max_pts, fmt in campos:
                        d = detalhes.get(campo, {})
                        if d.get('valor') is not None:
                            dados_tabela.append({'Métrica':lbl_d,'Valor':fmt(d['valor']),
                                                 'Pontos':f"{d['pontos']:+d}{max_pts}",'Avaliação':d.get('criterio','')})
                if dados_tabela:
                    st.dataframe(pd.DataFrame(dados_tabela), use_container_width=True, hide_index=True)
                    st.caption(f"**Score Total:** {score:.0f}/100 (Base: 50 + Bônus/Penalidades)")
            else:
                st.warning(f"⚠️ Não foi possível obter dados fundamentalistas para {ticker}")
                st.info(f"Ticker US mapeado: `{mapear_ticker_us(ticker)}`")

            # ── Machine Learning ──────────────────────────────────────────────
            st.markdown("---")
            try:
                df_ml    = df_calc.xs(ticker, axis=1, level=1).dropna()
                res_ml   = prever_preco_ml(df_ml, ticker, dias_previsao=5)
            except Exception:
                res_ml = {'erro': 'Não foi possível obter dados para o modelo.'}
            renderizar_painel_ml(res_ml, ticker, row['Empresa'], dias_previsao=5)

            # ── Notícias ──────────────────────────────────────────────────────
            st.markdown("---")
            st.markdown('<h3 class="section-header">📰 Últimas Notícias</h3>', unsafe_allow_html=True)

            ticker_us_news    = mapear_ticker_us(ticker)
            empresa_nome_news = ticker_us_news
            setor_news        = ''
            if fund_data:
                empresa_nome_news = fund_data.get('nome', ticker_us_news) or ticker_us_news
                setor_news        = fund_data.get('setor', '') or ''

            nc1, nc2 = st.columns([3, 1])
            with nc1:
                st.markdown(
                    f"Buscando notícias para **{empresa_nome_news}** (`{ticker_us_news}`)"
                    + (f" — Setor: *{setor_news}*" if setor_news else "")
                )
            with nc2:
                st.button("🔄 Atualizar notícias", key=f"btn_news_{ticker}")

            with st.spinner("Buscando e traduzindo notícias..."):
                noticias_lista = buscar_noticias_com_traducao(ticker_us_news)

            if noticias_lista:
                fontes_enc = list(dict.fromkeys(n['fonte'] for n in noticias_lista))
                st.caption(f"✅ {len(noticias_lista)} notícias | Fontes: {', '.join(fontes_enc)} | 🌐 PT-BR")
                nn1, nn2 = st.columns(2)
                metade   = (len(noticias_lista) + 1) // 2
                with nn1:
                    for n in noticias_lista[:metade]:
                        st.markdown(_renderizar_card_noticia(n), unsafe_allow_html=True)
                with nn2:
                    for n in noticias_lista[metade:]:
                        st.markdown(_renderizar_card_noticia(n), unsafe_allow_html=True)

                st.markdown("---")
                st.markdown("**🔗 Ver mais nas fontes:**")
                lc = st.columns(4)
                with lc[0]: st.markdown(f"[📊 Yahoo Finance](https://finance.yahoo.com/quote/{ticker_us_news}/news/)")
                with lc[1]: st.markdown(f"[📈 Seeking Alpha](https://seekingalpha.com/symbol/{ticker_us_news}/news)")
                with lc[2]: st.markdown(f"[🔍 Finviz](https://finviz.com/quote.ashx?t={ticker_us_news})")
                with lc[3]: st.markdown(f"[🧙 GuruFocus](https://www.gurufocus.com/news/{ticker_us_news})")
            else:
                st.warning(f"⚠️ Sem notícias para **{empresa_nome_news}**. Acesse diretamente:")
                lc2 = st.columns(4)
                with lc2[0]: st.markdown(f"[📊 Yahoo](https://finance.yahoo.com/quote/{ticker_us_news}/news/)")
                with lc2[1]: st.markdown(f"[📈 SeekingAlpha](https://seekingalpha.com/symbol/{ticker_us_news}/news)")
                with lc2[2]: st.markdown(f"[🔍 Finviz](https://finviz.com/quote.ashx?t={ticker_us_news})")
                with lc2[3]: st.markdown(f"[🧙 GuruFocus](https://www.gurufocus.com/news/{ticker_us_news})")

        else:
            st.markdown("""
            <div style='background:linear-gradient(135deg,#e0e7ff 0%,#c7d2fe 100%);
                        padding:2rem;border-radius:8px;text-align:center;margin:2rem 0;'>
                <p style='margin:0;color:#3730a3;font-size:1.1rem;font-weight:500;'>
                    👆 Selecione uma BDR na tabela para ver a análise completa
                </p>
            </div>""", unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style='background:linear-gradient(135deg,#ffeaa7 0%,#fdcb6e 100%);
                    padding:2rem;border-radius:8px;text-align:center;'>
            <h3 style='margin:0;color:#7c3626;'>📊 Nenhuma oportunidade detectada</h3>
            <p style='margin:0.5rem 0 0 0;color:#7c3626;'>Aguarde ou ajuste os filtros</p>
        </div>""", unsafe_allow_html=True)

# ── Rodapé ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style='text-align:center;padding:2rem 0;color:#64748b;'>
    <p style='margin:0;font-size:0.9rem;'>
        <strong>Monitor BDR - Swing Trade Pro</strong> | Python · yFinance · Streamlit
    </p>
    <p style='margin:0.5rem 0 0 0;font-size:0.8rem;'>
        ⚠️ Apenas para fins educacionais. Não constitui recomendação de investimento.
    </p>
</div>""", unsafe_allow_html=True)
