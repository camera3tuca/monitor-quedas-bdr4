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

PERIODO = "1y"  # Mantido por compatibilidade, mas o período agora é dinâmico
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
        dt = datetime.strptime(pub_raw, '%a, %d %b %Y %H:%M:%S %z')
        return dt.strftime('%d/%m/%Y %H:%M')
    except Exception:
        return pub_raw

def _traduzir_com_mymemory(textos):
    """
    Traduz lista de strings de inglês para português via API pública MyMemory.
    Gratuita, sem chave. Faz uma chamada por item (limite 500 chars cada).
    Retorna a lista original em caso de falha.
    """
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
    """
    Agrega notícias de múltiplas fontes, deduplica e traduz títulos
    e descrições para português via API Claude.
    """
    todas = []
    todas += _buscar_yahoo_rss(ticker_us, max_noticias=8)
    if len(todas) < 4:
        todas += _buscar_gurufocus_rss(ticker_us, max_noticias=6)
    if len(todas) < 6:
        todas += _buscar_seekingalpha_rss(ticker_us, max_noticias=6)
    if len(todas) < 4:
        todas += _buscar_finviz(ticker_us, max_noticias=6)

    # Deduplica por título
    vistos, unicas = set(), []
    for n in todas:
        chave = n['titulo'].lower()[:60]
        if chave not in vistos:
            vistos.add(chave)
            unicas.append(n)
    unicas = unicas[:12]

    if not unicas:
        return []

    # Traduz títulos em bloco
    titulos_orig = [n['titulo'] for n in unicas]
    titulos_trad = _traduzir_com_mymemory(titulos_orig)
    for n, t in zip(unicas, titulos_trad):
        n['titulo'] = t

    # Traduz descrições não-vazias em bloco
    idx_desc = [(i, n['descricao']) for i, n in enumerate(unicas) if n.get('descricao')]
    if idx_desc:
        indices, descs = zip(*idx_desc)
        descs_trad = _traduzir_com_mymemory(list(descs))
        for i, d in zip(indices, descs_trad):
            unicas[i]['descricao'] = d

    return unicas

def _renderizar_card_noticia(noticia):
    """Renderiza card de notícia em HTML."""
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
    """
    Treina um modelo de Regressão Linear com features enriquecidas e
    prevê os próximos `dias_previsao` dias de forma iterativa.

    Features: Close, EMA20, EMA50, RSI14, retorno diário, volatilidade 10d.
    Target  : preço de fechamento do dia seguinte (shift -1).
    """
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

        # --- Features adicionais ---
        df['Retorno']  = df['Close'].pct_change()          # retorno diário
        df['Volatil']  = df['Close'].pct_change().rolling(10).std()  # vol. 10d
        df['EMA_Dist'] = (df['Close'] - df['EMA20']) / df['EMA20']  # distância EMA20

        # Target: preço do próximo dia
        df['Target'] = df['Close'].shift(-1)
        df = df.dropna()

        feature_cols = ['Close', 'EMA20', 'RSI14', 'Retorno', 'Volatil', 'EMA_Dist']
        if 'EMA50' in df.columns:
            feature_cols.append('EMA50')

        X = df[feature_cols].values
        y = df['Target'].values

        # Normalização
        scaler_X = MinMaxScaler()
        scaler_y = MinMaxScaler()
        X_sc = scaler_X.fit_transform(X)
        y_sc = scaler_y.fit_transform(y.reshape(-1, 1)).ravel()

        # Split temporal 80/20
        split    = int(len(X_sc) * 0.8)
        X_train  = X_sc[:split];  y_train = y_sc[:split]
        X_test   = X_sc[split:];  y_test  = y_sc[split:]

        modelo = LinearRegression()
        modelo.fit(X_train, y_train)
        confianca = max(0.0, float(modelo.score(X_test, y_test)))

        # --- Estado inicial para previsão iterativa ---
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

            # Atualiza estado para próximo passo
            ret_cur    = (preco_prev - preco_cur) / preco_cur if preco_cur else 0
            vol_cur    = vol_cur * 0.9 + abs(ret_cur) * 0.1   # suavização exponencial
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
    """
    Renderiza o painel de previsão ML dentro de um st.expander.
    Totalmente isolado — não interfere em nenhuma outra seção.
    """
    with st.expander("🤖 Previsão por Inteligência Artificial (Machine Learning)", expanded=False):

        if resultado_ml.get('erro'):
            st.warning(f"⚠️ {resultado_ml['erro']}")
            return

        direcao   = resultado_ml['direcao']
        variacao  = resultado_ml['variacao_pct']
        confianca = resultado_ml['confianca']
        previsoes = resultado_ml['previsoes']
        ult_preco = resultado_ml['ultimo_preco']

        # --- Cabeçalho explicativo ---
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

        # --- Cards de resumo ---
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

        # --- Gráfico ---
        todos_precos = [ult_preco] + previsoes
        todos_labels = ["Hoje"] + [f"D+{i+1}" for i in range(dias_previsao)]
        cor_linha = "#16a34a" if direcao == "ALTA" else "#dc2626" if direcao == "BAIXA" else "#d97706"

        # Escala Y centrada nos preços previstos (±2% de margem)
        y_min = min(todos_precos) * 0.985
        y_max = max(todos_precos) * 1.015
        # Garante altura mínima visível mesmo em variação quase zero
        if (y_max - y_min) < ult_preco * 0.01:
            y_min = ult_preco * 0.992
            y_max = ult_preco * 1.008

        fig, ax = plt.subplots(figsize=(7, 3.2))
        fig.patch.set_facecolor('#f8fafc')
        ax.set_facecolor('#f8fafc')

        xs = list(range(len(todos_precos)))

        # Área preenchida só na faixa relevante
        ax.fill_between(xs, todos_precos, y_min,
                        alpha=0.18, color=cor_linha)
        # Linha principal
        ax.plot(xs, todos_precos,
                color=cor_linha, linewidth=2.5,
                marker='o', markersize=6,
                markerfacecolor='white',
                markeredgecolor=cor_linha, markeredgewidth=2,
                zorder=3)
        # Ponto "Hoje" destacado
        ax.scatter([0], [ult_preco], color='#6366f1', s=110, zorder=5)
        # Linha de referência do preço atual
        ax.axhline(ult_preco, color='#94a3b8',
                   linestyle='--', linewidth=1, alpha=0.5)

        # Anotações com offset dinâmico para evitar sobreposição
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

        ax.set_ylim(y_min, y_max + margem * 0.35)   # espaço extra no topo para labels
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

        # --- Legenda de confiança ---
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
# Referência: https://hw-br.online/education/triple-screen-strategy-3-steps-to-make-profit/
# =============================================================================

def analisar_triple_screen(df_ticker):
    """
    Avalia a Estratégia Triple Screen de Alexander Elder nos dados diários da BDR.
    """
    try:
        close  = df_ticker['Close'].dropna()
        volume = df_ticker['Volume'].dropna()

        if len(close) < 30:
            return None

        # ── TELA 1: EMA13 + MACD(12,26,9) — identifica a MARÉ ───────────────────────
        ema13 = close.ewm(span=13, adjust=False).mean()
        ema13_slope = ema13.iloc[-1] - ema13.iloc[-3]  # variação em 3 dias

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

        # ── TELA 2: EFI(2) — identifica a ONDA (correção dentro da tendência) ────────
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
    """
    Renderiza o painel Triple Screen dentro de um st.expander.
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
    'A1FL34': 'Aflac',
    # Adicionei os primeiros itens para manter o dicionário base
    # (Por limitação de visualização, se você tiver mais empresas mapeadas, pode adicionar o dicionário completo aqui)
    'AAPL34': 'AAPL',
    'AMZO34': 'AMZN',
    'MSFT34': 'MSFT',
    'NVDC34': 'NVDA',
    'TSLA34': 'TSLA',
    'GOGL34': 'GOOGL',
    'GOGL35': 'GOOG',
    'M1TA34': 'META',
}

def mapear_ticker_us(ticker_bdr):
    """
    Mapeia BDR para o ticker US da empresa mãe.
    """
    if ticker_bdr in BDR_TO_US_MAP:
        return BDR_TO_US_MAP[ticker_bdr]
    stripped = ticker_bdr.rstrip('0123456789')
    return stripped

def calcular_score_fundamentalista(info):
    """
    Calcula score 0-100 baseado em métricas fundamentalistas
    """
    score = 50  
    detalhes = {
        'pe_ratio': {'valor': None, 'pontos': 0, 'criterio': ''},
        'dividend_yield': {'valor': None, 'pontos': 0, 'criterio': ''},
        'revenue_growth': {'valor': None, 'pontos': 0, 'criterio': ''},
        'recomendacao': {'valor': None, 'pontos': 0, 'criterio': ''},
        'market_cap': {'valor': None, 'pontos': 0, 'criterio': ''},
    }
    
    try:
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
            detalhes['market_cap']['pontos'] = 20
            detalhes['market_cap']['criterio'] = 'Large Cap (>$100B)'
            score += 20
        elif mcap_b > 10:
            detalhes['market_cap']['pontos'] = 10
            detalhes['market_cap']['criterio'] = 'Mid Cap (>$10B)'
            score += 10
        else:
            detalhes['market_cap']['criterio'] = 'Small Cap (<$10B)'
    
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
                info['marketCap']   = getattr(r, 'mkt_cap', None)
                info['sector']      = getattr(r, 'sector', None)
                info['industry']    = getattr(r, 'industry', None)
                info['symbol']      = ticker_us
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

        try:
            rec = obb.equity.estimates.consensus(symbol=ticker_us, provider="fmp")
            if rec and rec.results:
                cons = rec.results[0]
                raw = str(getattr(cons, 'consensus', '') or '').lower().replace(' ', '_')
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
    'AAPL34': 'Apple Inc.',
    'AMZO34': 'Amazon.com, Inc.',
    'MSFT34': 'Microsoft Corp',
    # Adicione as demais empresas do seu dicionário original aqui se quiser manter todas
}

@st.cache_data(ttl=3600, show_spinner=False)
def buscar_dados_fundamentalistas(ticker_bdr):
    ticker_us = mapear_ticker_us(ticker_bdr)

    def _score_from_yf_info(info, fonte_label, ticker_label):
        if not info or len(info) < 5:
            return None
        if not any([
            info.get('marketCap'),
            info.get('trailingPE'),
            info.get('forwardPE'),
            info.get('revenueGrowth'),
        ]):
            return None

        score = 50
        det = {}

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

        dy = info.get('dividendYield')
        if dy and isinstance(dy, (int, float)):
            det['dividend_yield'] = {'valor': dy, 'pontos': 0, 'criterio': ''}
            if dy > 0.04:   score += 10; det['dividend_yield'].update(pontos=10, criterio='Excelente (>4%)')
            elif dy > 0.02: score +=  5; det['dividend_yield'].update(pontos=5,  criterio='Bom (>2%)')
            else:                        det['dividend_yield']['criterio'] = 'Baixo (<2%)'
        else:
            det['dividend_yield'] = {'valor': None, 'pontos': 0, 'criterio': ''}

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

    try:
        nome_empresa = NOMES_BDRS.get(ticker_bdr, '')
        nome_limpo = nome_empresa
        for sufixo in [' ADR', ' ADS', ' Ordinary Shares', ' Class A', ' Class B',
                       ' Class C', ' A Shares', ' B Shares']:
            nome_limpo = nome_limpo.replace(sufixo, '')
        nome_limpo = nome_limpo.strip()

        if nome_limpo:
            try:
                resultado_busca = yf.Search(nome_limpo, max_results=5)
                quotes = resultado_busca.quotes if hasattr(resultado_busca, 'quotes') else []
                tickers_encontrados = []
                for q in quotes:
                    tipo = q.get('quoteType', '')
                    exchange = q.get('exchange', '')
                    symbol = q.get('symbol', '')
                    if tipo in ('EQUITY',) and '.' not in symbol and exchange in (
                        'NMS', 'NYQ', 'NGM', 'NCM', 'ASE', 'PCX', 'BTS', 'NAS', 'NYSE', 'NASDAQ'
                    ):
                        tickers_encontrados.append(symbol)

                for t in tickers_encontrados[:3]:  
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

    try:
        info_obb = buscar_dados_openbb(ticker_us)
        resultado = _score_from_yf_info(info_obb, f'OpenBB / FMP — {ticker_us}', ticker_us)
        if resultado:
            return resultado
    except Exception:
        pass

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


# --- FUNÇÕES ALTERADAS (PASSO 1) ---

@st.cache_data(ttl=1800)
def buscar_dados(tickers, interval="1d", period="1y"):
    """
    Busca os dados no Yahoo Finance aceitando intervalo (timeframe) e período dinâmicos.
    """
    if not tickers: return pd.DataFrame()
    sa_tickers = [f"{t}.SA" for t in tickers]
    try:
        df = yf.download(sa_tickers, period=period, interval=interval, auto_adjust=True, progress=False, timeout=60)
        if df.empty: return pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = pd.MultiIndex.from_tuples([(c[0], c[1].replace(".SA", "")) for c in df.columns])
        return df.dropna(axis=1, how='all')
    except Exception: 
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def obter_nomes_yfinance(tickers):
    mapa_nomes = {}
    total = len(tickers)
    
    if total > 0:
        progresso_nomes = st.progress(0, text="Buscando nomes das empresas...")
        
        for i, ticker in enumerate(tickers):
            try:
                if i % 5 == 0:
                    progresso_nomes.progress(min((i + 1) / total, 1.0), 
                                            text=f"Buscando nomes... {i+1}/{total}")
                
                ticker_yf = yf.Ticker(f"{ticker}.SA")
                info = ticker_yf.info
                
                nome = (info.get('longName') or 
                       info.get('shortName') or 
                       ticker)
                
                mapa_nomes[ticker] = nome
            except:
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
            
            delta = close.diff()
            ganho = delta.clip(lower=0).rolling(14).mean()
            perda = -delta.clip(upper=0).rolling(14).mean()
            rs = ganho / perda
            df_calc[('RSI14', ticker)] = 100 - (100 / (1 + rs))

            lowest_low = low.rolling(window=14).min()
            highest_high = high.rolling(window=14).max()
            stoch_k = 100 * ((close - lowest_low) / (highest_high - lowest_low))
            df_calc[('Stoch_K', ticker)] = stoch_k

            df_calc[('EMA20', ticker)] = close.ewm(span=20).mean()
            df_calc[('EMA50', ticker)] = close.ewm(span=50).mean()
            df_calc[('EMA200', ticker)] = close.ewm(span=200).mean()
            sma = close.rolling(20).mean()
            std = close.rolling(20).std()
            df_calc[('BB_Lower', ticker)] = sma - (std * 2)
            df_calc[('BB_Upper', ticker)] = sma + (std * 2)

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
    explicacoes = []  
    
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

            queda_dia = ((preco - preco_ant) / preco_ant) * 100
            gap = ((preco_open - preco_ant) / preco_ant) * 100
            
            if queda_dia >= 0: continue 

            sinais, score, classificacao, explicacoes = gerar_sinal(last, df_ticker)
            
            rsi = last.get('RSI14', 50)
            stoch = last.get('Stoch_K', 50)
            is_index = ((100 - rsi) + (100 - stoch)) / 2

            try:
                n = min(20, len(df_ticker))
                vol_serie = df_ticker['Volume'].tail(n)
                vol_medio = vol_serie.mean()
                if pd.isna(vol_medio): vol_medio = 0

                n_gaps = 0
                for i in range(1, min(n + 1, len(df_ticker))):
                    c_ant = df_ticker['Close'].iloc[-i-1]
                    o_at  = df_ticker['Open'].iloc[-i]
                    if c_ant > 0 and abs((o_at - c_ant) / c_ant) * 100 > 1:
                        n_gaps += 1

                consist = sum(1 for v in vol_serie if pd.notna(v) and v >= vol_medio * 0.8) / n if n > 0 else 0

                liq = 0
                if   vol_medio > 500000: liq += 40
                elif vol_medio > 100000: liq += 35
                elif vol_medio >  50000: liq += 30
                elif vol_medio >  10000: liq += 25
                elif vol_medio >   5000: liq += 20
                elif vol_medio >   1000: liq += 15
                elif vol_medio >    100: liq += 10
                else:                    liq += 5

                if   n_gaps == 0: liq += 30
                elif n_gaps <= 2: liq += 25
                elif n_gaps <= 5: liq += 20
                elif n_gaps <= 8: liq += 15
                elif n_gaps <=12: liq += 10
                else:             liq += 5

                if   consist >= 0.75: liq += 30
                elif consist >= 0.50: liq += 20
                elif consist >= 0.25: liq += 10
                else:                 liq += 5

                ranking_liq = max(0, min(10, round(liq / 10)))
            except Exception:
                ranking_liq = 1

            nome_completo = mapa_nomes.get(ticker, ticker)
            
            if nome_completo == ticker:
                nome_curto = ticker
            else:
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

# --- FUNÇÃO ALTERADA (PASSO 3) ---

def plotar_grafico(df_ticker, ticker, empresa, rsi, is_val, tipo_grafico="Linha"):
    import matplotlib.dates as mdates

    colunas_necessarias = ['Close', 'Open', 'High', 'Low', 'Volume',
                           'EMA20', 'RSI14', 'Stoch_K', 'BB_Lower', 'BB_Upper']
    colunas_opcionais   = ['EMA50', 'EMA200', 'MACD_Hist']
    colunas_presentes   = [c for c in colunas_necessarias + colunas_opcionais
                           if c in df_ticker.columns]
    df = df_ticker[colunas_presentes].copy()

    df = df.dropna(subset=['Close', 'EMA20'])
    df = df.sort_index()   

    close  = df['Close']
    ema20  = df['EMA20']
    ema50  = df['EMA50']  if 'EMA50'  in df.columns else None
    ema200 = df['EMA200'] if 'EMA200' in df.columns else None
    datas  = df.index     

    fig, axes = plt.subplots(3, 1, figsize=(10, 8), sharex=True,
                             gridspec_kw={'height_ratios': [3, 1, 1]})

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

    ax1 = axes[0]

    if 'BB_Lower' in df.columns and 'BB_Upper' in df.columns:
        ax1.fill_between(datas, df['BB_Lower'], df['BB_Upper'],
                         alpha=0.07, color='#607d8b', zorder=0)

    for nivel, preco_fib in fib_levels.items():
        cor = fib_colors[nivel]
        ax1.axhline(preco_fib, color=cor, linestyle='--', linewidth=0.9,
                    alpha=0.55, zorder=1)
        ax1.text(datas[-1], preco_fib, f' Fib {nivel}',
                 fontsize=7.5, color=cor, va='center',
                 bbox=dict(boxstyle='round,pad=0.2', facecolor='white',
                           edgecolor=cor, alpha=0.75))

    ax1.axhspan(fib_levels['61.8%'] * 0.99, fib_levels['61.8%'] * 1.01,
                alpha=0.12, color='#2ecc71', zorder=0, label='Zona de Ouro')

    # EMAs — plotadas na camada de baixo para não sobrepor os preços
    ax1.plot(datas, ema20,  label='EMA20',  color='#2962FF', linewidth=1.4, alpha=0.9, zorder=3)
    if ema50 is not None:
        ema50_alinhada = ema50.reindex(datas)
        ax1.plot(datas, ema50_alinhada, label='EMA50', color='#FF6D00', linewidth=1.4, alpha=0.85, zorder=3)
    if ema200 is not None:
        ema200_alinhada = ema200.reindex(datas)
        ax1.plot(datas, ema200_alinhada, label='EMA200', color='#00695C', linewidth=1.8, alpha=0.8, zorder=3)

    # --- Lógica de Renderização: Linha vs Candles ---
    if tipo_grafico == "Candles" and all(col in df.columns for col in ['Open', 'High', 'Low', 'Close']):
        largura_base = 0.8
        if len(datas) > 1:
            diff_segundos = (datas[1] - datas[0]).total_seconds()
            largura_base = (diff_segundos / (24 * 3600)) * 0.7 
            
        up = df[df['Close'] >= df['Open']]
        down = df[df['Close'] < df['Open']]

        ax1.vlines(up.index, up['Low'], up['High'], color='#2ecc71', linewidth=1.5, zorder=4)
        ax1.vlines(down.index, down['Low'], down['High'], color='#e74c3c', linewidth=1.5, zorder=4)

        ax1.bar(up.index, up['Close'] - up['Open'], bottom=up['Open'], color='#2ecc71', width=largura_base, zorder=5)
        ax1.bar(down.index, down['Open'] - down['Close'], bottom=down['Close'], color='#e74c3c', width=largura_base, zorder=5)
    else:
        ax1.plot(datas, close, label='Close', color='#1a1a2e', linewidth=2.2, zorder=5)
        ax1.scatter([datas[-1]], [close.iloc[-1]], color='#e74c3c', s=40, zorder=6)

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

    nivel_mais_proximo = min(fib_levels, key=lambda n: abs(ult_close - fib_levels[n]))

    ax1.set_title(
        f'{ticker} - {empresa} | I.S.: {is_val:.0f} | {status} | Próx. Fib: {nivel_mais_proximo}',
        fontweight='bold', fontsize=10, pad=6)
    ax1.legend(loc='upper left', fontsize=7, framealpha=0.92, ncol=3)
    ax1.grid(True, alpha=0.18, zorder=0)
    ax1.set_ylabel('Preço (R$)', fontsize=9)

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

    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%b/%y'))
    ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=30, ha='right', fontsize=8)
    ax3.set_xlabel('Data', fontsize=9)

    plt.tight_layout()
    return fig

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
    cores = {
        '🌟': ('#1b5e20', 'white'),  
        '✅': ('#2e7d32', 'white'),   
        '⚖️': ('#fdd835', 'black'),   
        '⚠️': ('#ff7043', 'white'),   
        '🔴': ('#c62828', 'white'),   
        '—': ('#e0e0e0', 'black'),   
    }
    bg, fg = cores.get(val, ('#e0e0e0', 'black'))
    return (f'background-color: {bg}; color: {fg}; '
            f'font-weight: 900; font-size: 1.2em; text-align: center;')


# --- LAYOUT DO APP ---

st.markdown("""
<style>
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
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #667eea;
    }
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
    .stCheckbox {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    }
    .section-header {
        color: #667eea;
        font-size: 1.5rem;
        font-weight: 600;
        margin-top: 2rem;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #667eea;
    }
    .dataframe {
        border-radius: 8px;
        overflow: hidden;
    }
    .stAlert {
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

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

col_info1, col_info2, col_info3 = st.columns(3)
with col_info1:
    st.markdown("**📈 Estratégia:** Reversão em Sobrevenda")
with col_info2:
    st.markdown("**🎯 Foco:** BDRs em Queda com Potencial")
with col_info3:
    st.markdown("**⏱️ Ferramenta:** Analisador Multi-Timeframe")

st.markdown("---")

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
    """)

# --- NOVAS CONFIGURAÇÕES DE GRÁFICO E TIMEFRAME (PASSO 2) ---
st.markdown('<h3 class="section-header">⚙️ Configurações da Análise</h3>', unsafe_allow_html=True)
col_cfg1, col_cfg2 = st.columns(2)

with col_cfg1:
    timeframe_opcoes = {
        "5 Minutos": {"interval": "5m", "period": "60d"}, 
        "60 Minutos": {"interval": "1h", "period": "730d"}, 
        "Diário": {"interval": "1d", "period": "1y"},
        "Semanal": {"interval": "1wk", "period": "2y"},
        "Mensal": {"interval": "1mo", "period": "5y"}
    }
    tf_selecionado = st.selectbox("⏳ Timeframe", list(timeframe_opcoes.keys()), index=2)

with col_cfg2:
    tipo_grafico = st.radio("📈 Tipo de Gráfico", ["Linha", "Candles"], horizontal=True)

intervalo_tf = timeframe_opcoes[tf_selecionado]["interval"]
periodo_tf = timeframe_opcoes[tf_selecionado]["period"]

st.markdown("---")

if st.button("🔄 Atualizar Análise", type="primary"):
    with st.spinner(f"Conectando à API e baixando dados ({tf_selecionado})..."):
        lista_bdrs = list(NOMES_BDRS.keys())
        
        df = buscar_dados(lista_bdrs, interval=intervalo_tf, period=periodo_tf)
        
        if df.empty:
            st.error("Erro ao carregar dados. Se o Yahoo tiver bloqueado, aguarde alguns minutos.")
            st.stop()
        
    with st.spinner("Calculando indicadores técnicos..."):
        df_calc = calcular_indicadores(df)
        
    with st.spinner("Analisando oportunidades..."):
        oportunidades = analisar_oportunidades(df_calc, NOMES_BDRS)
        
        if oportunidades:
            st.session_state['oportunidades'] = oportunidades
            st.session_state['df_calc'] = df_calc

if 'oportunidades' in st.session_state and 'df_calc' in st.session_state:
    oportunidades = st.session_state['oportunidades']
    df_calc = st.session_state['df_calc']
    
    df_res = pd.DataFrame(oportunidades)
    df_res = df_res.sort_values(by='Queda_Dia', ascending=True)
    
    st.success(f"✅ {len(oportunidades)} oportunidades detectadas!")
    
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

    st.markdown("**💧 Liquidez mínima:**")
    ranking_min_liq = st.slider(
        "0 = sem filtro  |  10 = máxima exigência",
        min_value=0, max_value=10, value=0, step=1,
        help="Filtra BDRs pelo ranking de liquidez 0-10. Quanto maior, menor o risco de gaps e volume baixo."
    )
    
    if filtrar_ema20 or filtrar_ema50 or filtrar_ema200 or ranking_min_liq > 0:
        df_res_filtrado = []
        contadores = {'ema20': 0, 'ema50': 0, 'ema200': 0, 'sem_dados': 0}
        
        for opp in oportunidades:
            ticker = opp['Ticker']
            try:
                df_ticker = df_calc.xs(ticker, axis=1, level=1).dropna()
                
                tam = len(df_ticker)
                if tam < 20:
                    contadores['sem_dados'] += 1
                    continue
                
                ultimo_close = df_ticker['Close'].iloc[-1]
                
                passa_filtro = True
                
                if filtrar_ema20:
                    if 'EMA20' in df_ticker.columns and tam >= 20:
                        ultima_ema20 = df_ticker['EMA20'].iloc[-1]
                        if pd.notna(ultima_ema20) and ultimo_close > ultima_ema20:
                            contadores['ema20'] += 1
                        else:
                            passa_filtro = False
                    else:
                        passa_filtro = False
                
                if filtrar_ema50 and passa_filtro:
                    if 'EMA50' in df_ticker.columns and tam >= 50:
                        ultima_ema50 = df_ticker['EMA50'].iloc[-1]
                        if pd.notna(ultima_ema50) and ultimo_close > ultima_ema50:
                            contadores['ema50'] += 1
                        else:
                            passa_filtro = False
                    else:
                        passa_filtro = False
                
                if filtrar_ema200 and passa_filtro:
                    if 'EMA200' in df_ticker.columns and tam >= 50:
                        ultima_ema200 = df_ticker['EMA200'].iloc[-1]
                        if pd.notna(ultima_ema200) and ultimo_close > ultima_ema200:
                            contadores['ema200'] += 1
                        else:
                            passa_filtro = False
                    else:
                        passa_filtro = False

                if ranking_min_liq > 0 and passa_filtro:
                    if opp.get('Liquidez', 0) < ranking_min_liq:
                        passa_filtro = False
                
                if passa_filtro:
                    df_res_filtrado.append(opp)
                    
            except Exception as e:
                contadores['sem_dados'] += 1
                continue
        
        if df_res_filtrado:
            df_res = pd.DataFrame(df_res_filtrado)
            df_res = df_res.sort_values(by='Queda_Dia', ascending=True)
            
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
            df_res = pd.DataFrame()  
    
    if not df_res.empty:
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
        
        if evento.selection and evento.selection.rows:
            st.markdown("---")
            linha_selecionada = evento.selection.rows[0]
            row = df_res.iloc[linha_selecionada]
            ticker = row['Ticker']
            
            st.markdown(f'<h3 class="section-header">📈 Análise Técnica: {ticker} - {row["Empresa"]}</h3>', unsafe_allow_html=True)
            
            try:
                df_ticker = df_calc.xs(ticker, axis=1, level=1).dropna()
                
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    # --- CHAMADA DO GRÁFICO ALTERADA (PASSO 4) ---
                    fig = plotar_grafico(df_ticker, ticker, row['Empresa'], row['RSI14'], row['IS'], tipo_grafico)
                    st.pyplot(fig)
                
                with col2:
                    potencial = row['Potencial']
                    
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
                    
                    st.markdown("""
                    <div style='background: #e0e7ff; padding: 0.75rem; border-radius: 6px; margin-top: 1rem;'>
                        <p style='margin: 0; font-weight: 600; color: #3730a3; font-size: 0.9rem;'>
                            📋 Sinais Detectados
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
                    st.markdown(f"<p style='font-size: 0.85rem; color: #475569;'>{row['Sinais']}</p>", unsafe_allow_html=True)
                    
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

            st.markdown("---")
            try:
                df_ticker_ts = df_calc.xs(ticker, axis=1, level=1).dropna()
                resultado_ts = analisar_triple_screen(df_ticker_ts)
            except Exception:
                resultado_ts = None
            renderizar_triple_screen(resultado_ts, ticker, row['Empresa'])

            st.markdown("---")
            st.markdown('<h3 class="section-header">📊 Análise Fundamentalista</h3>', unsafe_allow_html=True)
            
            with st.spinner(f"Buscando dados fundamentalistas de {ticker}..."):
                fund_data = buscar_dados_fundamentalistas(ticker)
            
            if fund_data:
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
                
                if 'BRAPI' in fonte:
                    st.info(f"📡 **Fonte:** {fonte} | Ticker: **{ticker_fonte}**\n\n⚠️ *Dados limitados disponíveis para esta BDR. Score baseado em Market Cap e Volume na B3.*")
                else:
                    st.success(f"📡 **Fonte:** {fonte} | Ticker US: **{ticker_fonte}**")
                
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
                
                st.markdown("---")
                st.markdown("### 📋 Detalhamento da Pontuação")
                
                detalhes = fund_data.get('detalhes', {})
                
                dados_tabela = []
                
                if 'fonte' in detalhes and 'BRAPI' in detalhes['fonte'].get('valor', ''):
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
                """)

            st.markdown("---")
            try:
                df_ticker_ml = df_calc.xs(ticker, axis=1, level=1).dropna()
                resultado_ml = prever_preco_ml(df_ticker_ml, ticker, dias_previsao=5)
            except Exception:
                resultado_ml = {'erro': 'Não foi possível obter os dados para o modelo.'}
            renderizar_painel_ml(resultado_ml, ticker, row['Empresa'], dias_previsao=5)

            st.markdown("---")
            st.markdown('<h3 class="section-header">📰 Últimas Notícias da Empresa</h3>', unsafe_allow_html=True)

            ticker_us_news = mapear_ticker_us(ticker)
            empresa_nome_news = ticker_us_news
            setor_news = ''
            if fund_data:
                empresa_nome_news = fund_data.get('nome', ticker_us_news) or ticker_us_news
                setor_news        = fund_data.get('setor', '') or ''

            info_cols = st.columns([3, 1])
            with info_cols[0]:
                st.markdown(
                    f"Buscando notícias para **{empresa_nome_news}** (`{ticker_us_news}`)"
                    + (f" — Setor: *{setor_news}*" if setor_news else "")
                )
            with info_cols[1]:
                st.button("🔄 Atualizar notícias", key=f"btn_news_{ticker}")

            with st.spinner("Buscando e traduzindo notícias..."):
                noticias_lista = buscar_noticias_com_traducao(ticker_us_news)

            if noticias_lista:
                fontes_encontradas = list(dict.fromkeys(n['fonte'] for n in noticias_lista))
                st.caption(
                    f"✅ {len(noticias_lista)} notícias encontradas | "
                    f"Fontes: {', '.join(fontes_encontradas)} | 🌐 Traduzidas para português"
                )
                col_n1, col_n2 = st.columns(2)
                metade = (len(noticias_lista) + 1) // 2
                with col_n1:
                    for noticia in noticias_lista[:metade]:
                        st.markdown(_renderizar_card_noticia(noticia), unsafe_allow_html=True)
                with col_n2:
                    for noticia in noticias_lista[metade:]:
                        st.markdown(_renderizar_card_noticia(noticia), unsafe_allow_html=True)

                st.markdown("---")
                st.markdown("**🔗 Ver mais notícias diretamente nas fontes:**")
                lc = st.columns(4)
                with lc[0]:
                    st.markdown(f"[📊 Yahoo Finance](https://finance.yahoo.com/quote/{ticker_us_news}/news/)")
                with lc[1]:
                    st.markdown(f"[📈 Seeking Alpha](https://seekingalpha.com/symbol/{ticker_us_news}/news)")
                with lc[2]:
                    st.markdown(f"[🔍 Finviz](https://finviz.com/quote.ashx?t={ticker_us_news})")
                with lc[3]:
                    st.markdown(f"[🧙 GuruFocus](https://www.gurufocus.com/news/{ticker_us_news})")
            else:
                st.warning(
                    f"⚠️ Não foi possível buscar notícias para **{empresa_nome_news}** "
                    f"(`{ticker_us_news}`). Acesse diretamente:"
                )
                lc2 = st.columns(4)
                with lc2[0]:
                    st.markdown(f"[📊 Yahoo Finance](https://finance.yahoo.com/quote/{ticker_us_news}/news/)")
                with lc2[1]:
                    st.markdown(f"[📈 Seeking Alpha](https://seekingalpha.com/symbol/{ticker_us_news}/news)")
                with lc2[2]:
                    st.markdown(f"[🔍 Finviz](https://finviz.com/quote.ashx?t={ticker_us_news})")
                with lc2[3]:
                    st.markdown(f"[🧙 GuruFocus](https://www.gurufocus.com/news/{ticker_us_news})")

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
