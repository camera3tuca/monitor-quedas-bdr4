import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import requests
from datetime import datetime
import pytz
import warnings
import xml.etree.ElementTree as ET
import html as html_lib
import re

st.set_page_config(page_title="Monitor BDRs - Swing Trade", page_icon="📉", layout="wide")
warnings.filterwarnings('ignore')
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

PERIODO      = "1y"
BRAPI_TOKEN  = "iExnKM1xcbQcYL3cNPhPQ3"
FMP_API_KEY  = "tBsRam74Ac6bZRWS3C8HY83C6not17Uh"

# =============================================================================
# NOTÍCIAS
# =============================================================================
def _limpar_html(t):
    if not t: return ""
    t = re.sub(r'<[^>]+>', '', t)
    return html_lib.unescape(t).strip()

def _formatar_data(pub_raw):
    try:
        return datetime.strptime(pub_raw, '%a, %d %b %Y %H:%M:%S %z').strftime('%d/%m/%Y %H:%M')
    except: return pub_raw

def _traduzir_com_mymemory(textos):
    out = []
    for texto in textos:
        if not texto or not texto.strip(): out.append(texto); continue
        try:
            r = requests.get("https://api.mymemory.translated.net/get",
                             params={"q": texto[:500], "langpair": "en|pt-br"}, timeout=6)
            if r.status_code == 200:
                t = r.json().get("responseData", {}).get("translatedText", "")
                out.append(t if t and t.upper() != texto.upper() else texto)
            else: out.append(texto)
        except: out.append(texto)
    return out

def _buscar_yahoo_rss(ticker_us, max_n=8):
    hdrs = {'User-Agent': 'Mozilla/5.0'}
    for url in [f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker_us}&region=US&lang=en-US",
                f"https://finance.yahoo.com/rss/headline?s={ticker_us}"]:
        try:
            r = requests.get(url, headers=hdrs, timeout=8)
            if r.status_code != 200: continue
            root = ET.fromstring(r.content); ch = root.find('channel')
            if ch is None: continue
            out = []
            for item in ch.findall('item')[:max_n]:
                t = _limpar_html(item.findtext('title',''))
                if not t: continue
                out.append({'titulo':t,'link':item.findtext('link',''),
                            'data':_formatar_data(item.findtext('pubDate','')),
                            'descricao':_limpar_html(item.findtext('description',''))[:280],
                            'fonte':'Yahoo Finance'})
            if out: return out
        except: continue
    return []

def _buscar_gurufocus_rss(ticker_us, max_n=6):
    hdrs = {'User-Agent': 'Mozilla/5.0'}
    try:
        r = requests.get(f"https://www.gurufocus.com/news/rss/{ticker_us}", headers=hdrs, timeout=8)
        if r.status_code != 200: return []
        root = ET.fromstring(r.content); ch = root.find('channel')
        if ch is None: return []
        out = []
        for item in ch.findall('item')[:max_n]:
            t = _limpar_html(item.findtext('title',''))
            if not t: continue
            out.append({'titulo':t,'link':item.findtext('link',''),
                        'data':_formatar_data(item.findtext('pubDate','')),
                        'descricao':_limpar_html(item.findtext('description',''))[:280],
                        'fonte':'GuruFocus'})
        return out
    except: return []

def buscar_noticias_com_traducao(ticker_us):
    todas = _buscar_yahoo_rss(ticker_us)
    if len(todas) < 4: todas += _buscar_gurufocus_rss(ticker_us)
    vistos, unicas = set(), []
    for n in todas:
        k = n['titulo'].lower()[:60]
        if k not in vistos: vistos.add(k); unicas.append(n)
    unicas = unicas[:12]
    if not unicas: return []
    tits = _traduzir_com_mymemory([n['titulo'] for n in unicas])
    for n, t in zip(unicas, tits): n['titulo'] = t
    idx = [(i, n['descricao']) for i, n in enumerate(unicas) if n.get('descricao')]
    if idx:
        ids, ds = zip(*idx)
        dts = _traduzir_com_mymemory(list(ds))
        for i, d in zip(ids, dts): unicas[i]['descricao'] = d
    return unicas

def _renderizar_card_noticia(n):
    cores = {
        'Yahoo Finance': ('#eff6ff','#1d4ed8','#dbeafe'),
        'GuruFocus':     ('#fefce8','#854d0e','#fef9c3'),
    }
    bg, cf, bb = cores.get(n.get('fonte',''), ('#f8fafc','#475569','#e2e8f0'))
    desc_html = f"<p style='margin:.4rem 0 0;font-size:.82rem;color:#64748b;line-height:1.4'>{n['descricao']}</p>" if n.get('descricao') else ""
    return f"""<div style='background:{bg};border:1px solid {bb};border-radius:10px;padding:1rem 1.1rem;margin-bottom:.75rem;'>
<div style='display:flex;justify-content:space-between;align-items:flex-start;gap:.5rem;'>
<a href="{n['link']}" target="_blank" style='font-size:.92rem;font-weight:600;color:#1e293b;text-decoration:none;flex:1;'>{n['titulo']}</a>
<span style='background:{bb};color:{cf};font-size:.7rem;font-weight:700;padding:.15rem .55rem;border-radius:999px;white-space:nowrap;'>{n.get('fonte','')}</span>
</div>{desc_html}
<p style='margin:.5rem 0 0;font-size:.75rem;color:#94a3b8;'>🕐 {n.get('data','')}</p></div>"""

# =============================================================================
# MACHINE LEARNING
# =============================================================================
def prever_preco_ml(df_ticker, ticker, dias_previsao=5):
    try:
        from sklearn.linear_model import LinearRegression
        from sklearn.preprocessing import MinMaxScaler
        df = df_ticker.copy()
        for col in ['Close','EMA20','RSI14']:
            if col not in df.columns: return {'erro': f'Coluna {col} ausente.'}
        cols = ['Close','EMA20','RSI14'] + (['EMA50'] if 'EMA50' in df.columns else [])
        df = df[cols].dropna()
        if len(df) < 60: return {'erro': 'Dados insuficientes (mín. 60 dias).'}
        df['Retorno'] = df['Close'].pct_change()
        df['Volatil'] = df['Close'].pct_change().rolling(10).std()
        df['EMA_Dist'] = (df['Close']-df['EMA20'])/df['EMA20']
        df['Target'] = df['Close'].shift(-1)
        df = df.dropna()
        fcols = ['Close','EMA20','RSI14','Retorno','Volatil','EMA_Dist'] + (['EMA50'] if 'EMA50' in df.columns else [])
        X = df[fcols].values; y = df['Target'].values
        sX = MinMaxScaler(); sy = MinMaxScaler()
        Xs = sX.fit_transform(X); ys = sy.fit_transform(y.reshape(-1,1)).ravel()
        sp = int(len(Xs)*.8)
        m = LinearRegression().fit(Xs[:sp], ys[:sp])
        confianca = max(0.0, float(m.score(Xs[sp:], ys[sp:])))
        u = df.iloc[-1]
        pc=float(u['Close']); e20=float(u['EMA20']); rsi=float(u['RSI14'])
        ret=float(u['Retorno']); vol=float(u['Volatil'])
        e50=float(u['EMA50']) if 'EMA50' in df.columns else e20
        a20=2/(20+1); a50=2/(50+1); prevs=[]
        for _ in range(dias_previsao):
            ed=(pc-e20)/e20 if e20 else 0
            rf=[pc,e20,rsi,ret,vol,ed]+([e50] if 'EMA50' in df.columns else [])
            pp=float(sy.inverse_transform(m.predict(sX.transform([rf])).reshape(-1,1))[0][0])
            prevs.append(round(pp,2))
            ret=(pp-pc)/pc if pc else 0; vol=vol*.9+abs(ret)*.1
            e20=a20*pp+(1-a20)*e20; e50=a50*pp+(1-a50)*e50
            d=pp-pc; rsi=min(max(rsi+(max(d,0)-max(-d,0))/(pc+1e-9)*30,0),100); pc=pp
        var=((prevs[-1]-float(df.iloc[-1]['Close']))/float(df.iloc[-1]['Close']))*100
        return {'erro':None,'previsoes':prevs,'direcao':'ALTA' if var>1.5 else 'BAIXA' if var<-1.5 else 'LATERAL',
                'variacao_pct':round(var,2),'confianca':round(confianca*100,1),'ultimo_preco':round(float(df.iloc[-1]['Close']),2)}
    except ImportError: return {'erro': 'scikit-learn não instalado.'}
    except Exception as e: return {'erro': f'Erro: {e}'}

def renderizar_painel_ml(res, ticker, empresa, dias_previsao=5):
    with st.expander("🤖 Previsão por Machine Learning", expanded=False):
        if res.get('erro'): st.warning(f"⚠️ {res['erro']}"); return
        direcao=res['direcao']; var=res['variacao_pct']; conf=res['confianca']
        prevs=res['previsoes']; ultp=res['ultimo_preco']
        st.markdown("""<div style='background:linear-gradient(135deg,#1e1b4b,#312e81);padding:1rem;border-radius:10px;margin-bottom:1rem;'>
<p style='margin:0;color:#c7d2fe;font-size:.82rem;'>🧠 <strong style='color:#a5b4fc;'>Regressão Linear</strong> treinada com Close, EMA20, EMA50, RSI14, Retorno e Volatilidade.
<br>⚠️ <strong style='color:#fbbf24;'>Aviso:</strong> Estimativa estatística — use como critério auxiliar.</p></div>""", unsafe_allow_html=True)
        cfg={'ALTA':("#d4fc79","#96e6a1","#14532d","🚀","ALTA PREVISTA"),
             'BAIXA':("#fca5a5","#ef4444","#7f1d1d","📉","BAIXA PREVISTA"),
             'LATERAL':("#fde047","#fbbf24","#78350f","➡️","LATERAL PREVISTA")}
        b1,b2,ct,ico,lbl=cfg[direcao]
        c1,c2,c3=st.columns(3)
        with c1: st.markdown(f"<div style='background:linear-gradient(135deg,{b1},{b2});padding:1.2rem;border-radius:10px;text-align:center;height:110px;display:flex;flex-direction:column;justify-content:center;'><div style='font-size:2rem;'>{ico}</div><div style='font-weight:800;color:{ct};'>{lbl}</div><div style='font-size:.78rem;color:{ct};'>próx. {dias_previsao} dias</div></div>",unsafe_allow_html=True)
        with c2:
            cc="#15803d" if conf>=60 else "#b45309" if conf>=40 else "#b91c1c"
            nv="Boa" if conf>=60 else "Moderada" if conf>=40 else "Baixa"
            st.markdown(f"<div style='background:#f8fafc;border:2px solid #e2e8f0;padding:1.2rem;border-radius:10px;text-align:center;height:110px;display:flex;flex-direction:column;justify-content:center;'><div style='font-size:1.9rem;font-weight:800;color:{cc};'>{conf:.0f}%</div><div style='font-size:.8rem;color:#64748b;'>Confiança ({nv})</div></div>",unsafe_allow_html=True)
        with c3:
            sv="+" if var>=0 else ""; cv="#15803d" if var>1.5 else "#b91c1c" if var<-1.5 else "#b45309"
            st.markdown(f"<div style='background:#f8fafc;border:2px solid #e2e8f0;padding:1.2rem;border-radius:10px;text-align:center;height:110px;display:flex;flex-direction:column;justify-content:center;'><div style='font-size:1.9rem;font-weight:800;color:{cv};'>{sv}{var:.2f}%</div><div style='font-size:.8rem;color:#64748b;'>Variação D0→D+{dias_previsao}</div></div>",unsafe_allow_html=True)
        st.markdown("<div style='height:1rem'></div>",unsafe_allow_html=True)
        todos=[ultp]+prevs; lbls=["Hoje"]+[f"D+{i+1}" for i in range(dias_previsao)]
        cl="#16a34a" if direcao=="ALTA" else "#dc2626" if direcao=="BAIXA" else "#d97706"
        ym=min(todos)*.985; yx=max(todos)*1.015
        if (yx-ym)<ultp*.01: ym=ultp*.992; yx=ultp*1.008
        fig,ax=plt.subplots(figsize=(7,3.2)); fig.patch.set_facecolor('#f8fafc'); ax.set_facecolor('#f8fafc')
        xs=list(range(len(todos))); mg=yx-ym
        ax.fill_between(xs,todos,ym,alpha=.18,color=cl)
        ax.plot(xs,todos,color=cl,linewidth=2.5,marker='o',markersize=6,markerfacecolor='white',markeredgecolor=cl,markeredgewidth=2,zorder=3)
        ax.scatter([0],[ultp],color='#6366f1',s=110,zorder=5)
        ax.axhline(ultp,color='#94a3b8',linestyle='--',linewidth=1,alpha=.5)
        for i,p in enumerate(todos): ax.annotate(f'R${p:.2f}',xy=(i,p),xytext=(0,mg*.12),textcoords='offset points',ha='center',va='bottom',fontsize=7.5,color='#1e293b',fontweight='600')
        ax.set_ylim(ym,yx+mg*.35); ax.set_xticks(xs); ax.set_xticklabels(lbls,fontsize=8.5,color='#475569')
        ax.set_ylabel('Preço (R$)',fontsize=8,color='#64748b')
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v,_: f'R${v:.2f}'))
        ax.tick_params(axis='y',labelsize=7.5,colors='#64748b')
        for s in ['top','right']: ax.spines[s].set_visible(False)
        ax.spines['left'].set_color('#e2e8f0'); ax.spines['bottom'].set_color('#e2e8f0')
        ax.set_title(f'Previsão ML — {ticker} ({empresa})',fontsize=9,color='#334155',pad=10)
        plt.tight_layout(); st.pyplot(fig); plt.close(fig)
        st.markdown("**📋 Preços Previstos:**")
        cols_p=st.columns(dias_previsao)
        for i,(col,p) in enumerate(zip(cols_p,prevs)):
            dp=((p-ultp)/ultp)*100; sp="+"; cd="#15803d" if dp>0 else "#dc2626" if dp<0 else "#78350f"
            with col: st.markdown(f"<div style='background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:.65rem .4rem;text-align:center;'><div style='font-size:.7rem;color:#94a3b8;font-weight:700;'>D+{i+1}</div><div style='font-size:.95rem;font-weight:800;color:#1e293b;'>R${p:.2f}</div><div style='font-size:.73rem;font-weight:600;color:{cd};'>{'+' if dp>=0 else ''}{dp:.1f}%</div></div>",unsafe_allow_html=True)

# =============================================================================
# TRIPLE SCREEN
# =============================================================================
def analisar_triple_screen(df_ticker):
    try:
        close=df_ticker['Close'].dropna(); volume=df_ticker['Volume'].dropna()
        if len(close)<30: return None
        ema13=close.ewm(span=13,adjust=False).mean()
        slope=ema13.iloc[-1]-ema13.iloc[-3]
        ema12=close.ewm(span=12,adjust=False).mean(); ema26=close.ewm(span=26,adjust=False).mean()
        macd=ema12-ema26; sig=macd.ewm(span=9,adjust=False).mean(); hist=macd-sig
        mv=hist.iloc[-1]; ms=hist.iloc[-1]-hist.iloc[-2]; ev=ema13.iloc[-1]; pu=close.iloc[-1]
        pct=((pu-ev)/ev)*100
        if (slope>0) and (mv>0 or ms>0): s1,e1="ALTA","🟢"; d1=f"EMA13 ascendente (+{slope:+.2f}), MACD positivo. Preço {abs(pct):.1f}% acima da EMA13. MARÉ de alta."
        elif (slope<0) and (mv<0 or ms<0): s1,e1="BAIXA","🔴"; d1=f"EMA13 descendente ({slope:+.2f}), MACD negativo. Preço {abs(pct):.1f}% abaixo da EMA13. MARÉ de baixa."
        else: s1,e1="NEUTRO","🟡"; d1=f"EMA13 sem direção clara (slope:{slope:+.2f}). Aguarde definição."
        ic=close.index.intersection(volume.index)
        efi=((close.loc[ic].diff()*volume.loc[ic]).ewm(span=2,adjust=False).mean())
        e2v=efi.iloc[-1]; e2s=efi.std(); lp=e2s*.5; ln=-e2s*.5
        if e2v<ln: s2,e2="SOBREVENDA","🟢"; d2=f"EFI(2)={e2v:,.0f} — sobrevenda. Em uptrend: momento de entrada."
        elif e2v>lp: s2,e2="SOBRECOMPRA","🔴"; d2=f"EFI(2)={e2v:,.0f} — sobrecompra. Em downtrend: momento de saída."
        else: s2,e2="NEUTRO","🟡"; d2=f"EFI(2)={e2v:,.0f} — zona neutra. Aguarde extremos."
        pa=close.iloc[-1]; mx=df_ticker['High'].iloc[-5:].max(); mn=df_ticker['Low'].iloc[-5:].min()
        if s1=="ALTA" and s2=="SOBREVENDA": s3,e3="COMPRA","🚀"; d3=f"✅ Setup de COMPRA!\n• Buy Stop: R${mx:.2f}\n• Stop-Loss: R${mn:.2f}"
        elif s1=="BAIXA" and s2=="SOBRECOMPRA": s3,e3="VENDA","📉"; d3=f"⚠️ Setup de VENDA!\n• Sell Stop: R${mn:.2f}\n• Stop-Loss: R${mx:.2f}"
        else: s3,e3="AGUARDAR","⏳"; d3="Setup incompleto. Aguarde alinhamento das telas 1 e 2."
        forca=sum([s1=="ALTA", s2=="SOBREVENDA", s3=="COMPRA"])
        return {'tela1':{'status':s1,'emoji':e1,'valor':round(slope,4),'desc':d1},
                'tela2':{'status':s2,'emoji':e2,'valor':round(e2v,0),'desc':d2},
                'tela3':{'status':s3,'emoji':e3,'desc':d3},
                'veredicto':s3,'forca':forca,'preco_atual':round(pa,2),
                'serie_close':close.iloc[-60:],'serie_macd':ema13.iloc[-60:],'serie_efi2':efi.iloc[-60:],
                'limiar_pos':lp,'limiar_neg':ln,'maxima_rec':round(mx,2),'minima_rec':round(mn,2)}
    except: return None

def renderizar_triple_screen(res, ticker, empresa):
    with st.expander("🖥️ Estratégia Triple Screen — Alexander Elder", expanded=False):
        if res is None: st.warning("⚠️ Dados insuficientes."); return
        ver=res['veredicto']; forca=res['forca']
        t1=res['tela1']; t2=res['tela2']; t3=res['tela3']
        st.markdown("""<div style='background:linear-gradient(135deg,#0f2027,#203a43,#2c5364);padding:1rem;border-radius:10px;margin-bottom:1rem;'>
<p style='margin:0;color:#cfd8dc;font-size:.83rem;'>🌊 <b style='color:#80deea;'>1ª Tela (EMA13+MACD):</b> Tendência dominante &nbsp;|&nbsp;
🌀 <b style='color:#80deea;'>2ª Tela (EFI2):</b> Correções/repiques &nbsp;|&nbsp;
🎯 <b style='color:#80deea;'>3ª Tela:</b> Buy/Sell Stop na ação do preço</p></div>""",unsafe_allow_html=True)
        cv={'COMPRA':("#d4edda","#155724","#28a745","🚀","SETUP DE COMPRA"),
            'VENDA':("#f8d7da","#721c24","#dc3545","📉","SETUP DE VENDA"),
            'AGUARDAR':("#fff3cd","#856404","#ffc107","⏳","AGUARDAR")}
        bv,tv,brv,iv,lv=cv[ver]
        st.markdown(f"<div style='background:{bv};border:2px solid {brv};border-radius:12px;padding:1rem 1.4rem;margin-bottom:1rem;'><div style='font-size:1.2rem;font-weight:800;color:{tv};'>{iv} {lv}</div><div style='font-size:.82rem;color:{tv};'>Força: {'⭐'*forca}{'☆'*(3-forca)} ({forca}/3) | {ticker} — {empresa}</div></div>",unsafe_allow_html=True)
        css={'ALTA':("#e8f5e9","#1b5e20","#43a047"),'BAIXA':("#ffebee","#b71c1c","#e53935"),
             'NEUTRO':("#fffde7","#f57f17","#fbc02d"),'SOBREVENDA':("#e8f5e9","#1b5e20","#43a047"),
             'SOBRECOMPRA':("#ffebee","#b71c1c","#e53935"),'COMPRA':("#e8f5e9","#1b5e20","#43a047"),
             'VENDA':("#ffebee","#b71c1c","#e53935"),'AGUARDAR':("#fffde7","#f57f17","#fbc02d")}
        sc=res['serie_close']; sm=res['serie_macd']; se=res['serie_efi2']
        lp=res['limiar_pos']; ln=res['limiar_neg']; mx=res['maxima_rec']; mn=res['minima_rec']; pa=res['preco_atual']
        c1,c2,c3=st.columns(3)
        for col,tela,num,nome,sub in [(c1,t1,"1ª","Maré","EMA13+MACD"),(c2,t2,"2ª","Onda","EFI(2)"),(c3,t3,"3ª","Execução","Stop")]:
            bg,tx,br=css.get(tela['status'],("#f5f5f5","#333","#999"))
            vl=""
            if 'valor' in tela:
                v=tela['valor']; vf=f"{int(v):,}".replace(",",".") if abs(v)>=1000 else f"{v:+.4f}"
                vl=f"<div style='font-size:.74rem;color:{tx};font-family:monospace;'>{vf}</div>"
            with col:
                st.markdown(f"<div style='background:{bg};border:1.5px solid {br};border-radius:10px 10px 0 0;padding:.75rem .9rem .5rem;'><div style='font-size:.68rem;font-weight:700;color:{br};text-transform:uppercase;'>{num} TELA — {nome}</div><div style='font-size:.65rem;color:{tx};margin-bottom:.4rem;'>{sub}</div><div style='display:flex;align-items:center;gap:.4rem;'><span style='font-size:1.3rem;'>{tela['emoji']}</span><span style='font-size:.9rem;font-weight:800;color:{tx};'>{tela['status']}</span></div>{vl}</div>",unsafe_allow_html=True)
                fig_m,ax_m=plt.subplots(figsize=(3.2,1.6)); fig_m.patch.set_facecolor(bg); ax_m.set_facecolor(bg)
                if num=="1ª":
                    xs=range(len(sc)); ax_m.plot(xs,sc.values,color='#607d8b',linewidth=1,alpha=.6)
                    ax_m.plot(xs,sm.values,color=br,linewidth=2)
                    ax_m.fill_between(xs,sc.values,sm.values,where=(sc.values>=sm.values),alpha=.15,color='#43a047',interpolate=True)
                    ax_m.fill_between(xs,sc.values,sm.values,where=(sc.values<sm.values),alpha=.15,color='#e53935',interpolate=True)
                    ax_m.set_title("EMA13",fontsize=7,color=tx,pad=3)
                elif num=="2ª":
                    xs=range(len(se)); vals=se.values; cb=[br if v>=0 else '#e53935' for v in vals]
                    ax_m.bar(xs,vals,color=cb,alpha=.7,width=1)
                    ax_m.axhline(lp,color='#e53935',linewidth=.9,linestyle='--',alpha=.8)
                    ax_m.axhline(ln,color='#43a047',linewidth=.9,linestyle='--',alpha=.8)
                    ax_m.axhline(0,color='#90a4ae',linewidth=.7); ax_m.set_title("EFI(2)",fontsize=7,color=tx,pad=3)
                else:
                    c20=sc.iloc[-20:]; xs=range(len(c20))
                    cl='#43a047' if t3['status']=='COMPRA' else '#e53935' if t3['status']=='VENDA' else '#f57f17'
                    ax_m.plot(xs,c20.values,color=cl,linewidth=1.5)
                    ax_m.axhline(mx,color='#43a047',linewidth=1,linestyle='--',alpha=.9)
                    ax_m.axhline(mn,color='#e53935',linewidth=1,linestyle='--',alpha=.9)
                    ax_m.set_title("Preço+Stop",fontsize=7,color=tx,pad=3)
                for sp in ax_m.spines.values(): sp.set_visible(False)
                ax_m.set_xticks([]); ax_m.tick_params(axis='y',labelsize=6,colors=tx,length=0)
                ax_m.yaxis.set_major_formatter(plt.FuncFormatter(lambda v,_: f'{v/1e6:.1f}M' if abs(v)>=1e6 else f'{v/1e3:.0f}K' if abs(v)>=1e3 else f'{v:.2f}'))
                plt.tight_layout(pad=.3); st.pyplot(fig_m,use_container_width=True); plt.close(fig_m)
                st.markdown(f"<div style='background:{bg};border:1.5px solid {br};border-top:none;border-radius:0 0 10px 10px;height:6px;margin-top:-4px;'></div>",unsafe_allow_html=True)
        st.markdown("<div style='height:.8rem'></div>",unsafe_allow_html=True)
        for tela,num,ico,titulo in [(t1,"1ª","🌊","Maré (EMA13+MACD)"),(t2,"2ª","🌀","Onda (EFI2)"),(t3,"3ª","🎯","Execução")]:
            bg,tx,br=css.get(tela['status'],("#f8fafc","#334155","#cbd5e1"))
            st.markdown(f"<div style='background:{bg};border-left:4px solid {br};border-radius:0 8px 8px 0;padding:.8rem 1rem;margin-bottom:.6rem;'><div style='font-weight:700;font-size:.88rem;color:{tx};margin-bottom:.35rem;'>{ico} {num} Tela — {titulo}</div><div style='font-size:.82rem;color:{tx};line-height:1.55;white-space:pre-wrap;'>{tela['desc']}</div></div>",unsafe_allow_html=True)

# =============================================================================
# MAPEAMENTO BDR → US
# =============================================================================
BDR_TO_US_MAP = {
    'AAPL34':'AAPL','AMZO34':'AMZN','MSFT34':'MSFT','NVDC34':'NVDA','GOGL34':'GOOGL',
    'GOGL35':'GOOG','FBOK34':'META','TSLA34':'TSLA','AVGO34':'AVGO','LILY34':'LLY',
    'JPMC34':'JPM','VISA34':'V','MSCD34':'MA','NFLX34':'NFLX','ABTT34':'ABT',
    'JNJB34':'JNJ','UNHH34':'UNH','PFIZ34':'PFE','ABBV34':'ABBV','MRCK34':'MRK',
    'AMGN34':'AMGN','GILD34':'GILD','BIIB34':'BIIB','REGN34':'REGN','BMYB34':'BMY',
    'CHVX34':'CVX','EXXO34':'XOM','COPH34':'COP','SLBG34':'SLB','HALI34':'HAL',
    'CSCO34':'CSCO','INTU34':'INTU','ADBE34':'ADBE','ORCL34':'ORCL','QCOM34':'QCOM',
    'ITLC34':'INTC','A1MD34':'AMD','A1MT34':'AMAT','K1LA34':'KLAC','L1RC34':'LRCX',
    'N1XP34':'NXPI','MUTC34':'MU','TSMC34':'TSM','ASML34':'ASML','STMN34':'STM',
    'HOME34':'HD','LOWC34':'LOW','WALM34':'WMT','COWC34':'COST','MCDC34':'MCD',
    'SBUB34':'SBUX','PGCO34':'PG','COCA34':'KO','PEPB34':'PEP','COLG34':'CL',
    'PHMO34':'PM','MOOO34':'MO','KMBB34':'KMB','ULEV34':'UL','MDLZ34':'MDLZ',
    'KHCB34':'KHC','TSNF34':'TSN','DEEC34':'DE','CATP34':'CAT','BOEI34':'BA',
    'GDBR34':'GD','NOCG34':'NOC','RYTT34':'RTX','GEOO34':'GE','G2EV34':'GEV',
    'UPAC34':'UNP','CPRL34':'CP','CNIC34':'CNI','CSXC34':'CSX','FDXB34':'FDX',
    'BOAC34':'BAC','WFCO34':'WFC','CTGP34':'C','MSBR34':'MS','GSGI34':'GS',
    'AXPB34':'AXP','SCHW34':'SCHW','BONY34':'BK','USBC34':'USB','PNCS34':'PNC',
    'CAON34':'COF','BERK34':'BRK-B','BLAK34':'BLK','SPGI34':'SPGI','MCOR34':'MCO',
    'IBKR34':'IBKR','PYPL34':'PYPL','SSFO34':'CRM','N1OW34':'NOW','W1MC34':'WM',
    'R1SG34':'RSG','F1IS34':'FI','P1GR34':'PGR','TRVC34':'TRV','C1BL34':'CB',
    'A1FL34':'AFL','A1SU34':'AIZ','MELI34':'MELI','PAGS34':'PAGS','STOC34':'STNE',
    'ROXO34':'NU','XPBR31':'XP','INBR32':'INTR','V2TX34':'VTEX','JBSS32':'JBSS',
    'ATTB34':'T','VERZ34':'VZ','NOKI34':'NOK','E1RI34':'ERIC','SNEC34':'SONY',
    'HOND34':'HMC','TMCO34':'TM','ASML34':'ASML','SAPP34':'SAP','UBSG34':'UBS',
    'DBAG34':'DB','B1CS34':'BCS','H1SB34':'HSBC','B1PP34':'BP','E1QN34':'EQNR',
    'CHVX34':'CVX','ABUD34':'BUD','DEOP34':'DEO','ULEV34':'UL','PHGN34':'PHG',
    'NEXT34':'NEE','D1LR34':'DLR','EQIX34':'EQIX','T1OW34':'AMT','S1BA34':'SBAC',
    'A1VB34':'AVB','M1AA34':'MAA','I2NV34':'INVH','S2UI34':'SUI','P1LD34':'PLD',
    'R1IN34':'O','O2HI34':'OHI','N2LY34':'NLY','G1AM34':'GLPI','B2PW34':'MPW',
    'T2DH34':'TDOC','R1KU34':'ROKU','S1PO34':'SPOT','A1PP34':'APP','P2LT34':'PLTR',
    'C2RW34':'CRWD','P2AN34':'PANW','F1TN34':'FTNT','N2ET34':'NET','O1KT34':'OKTA',
    'Z2SC34':'ZS','D1DG34':'DDOG','S2NW34':'SNOW','M1DB34':'MDB','U2ST34':'U',
    'R2BL34':'RBLX','D2AS34':'DASH','AIRB34':'ABNB','U1BE34':'UBER','DUOL34':'DUOL',
    'T2TD34':'TTD','C2OI34':'COIN','M2ST34':'MSTR','RGTI34':'RGTI','QUBT34':'QUBT',
    'BIDU34':'BIDU','BABA34':'BABA','JDCO34':'JD','NETE34':'NTES','B1IL34':'BILI',
    'P1DD34':'PDD','K2CG34':'KC','I1QY34':'IQ','T1AL34':'TAL','E1DU34':'EDU',
    'RIOT34':'RIO','N1EM34':'NEM','FCXO34':'FCX','G1FI34':'GFI','S1BS34':'SBSW',
    'A1LB34':'ALB','S2GM34':'SGML','MOSC34':'MOS','TXSA34':'TX','Y2PF34':'YPF',
    'E1CO34':'EC','GPRK34':'GPRK','AURA33':'ORA','B1NT34':'BNTX','M1RN34':'MRNA',
    'C2RS34':'CRSP','B1MR34':'BMRN','BHVN34':'BHVN','I1LM34':'ILMN','N2VC34':'NVCR',
    'N1BI34':'NBIX','I1NC34':'INCY','F2IC34':'FICO','V1RS34':'VRSK','M1SC34':'MSCI',
    'I1QV34':'IQV','R1OP34':'ROP','G1AR34':'IT','W2ST34':'WST','I1SR34':'ISRG',
    'D1EX34':'DXCM','R1MD34':'RMD','E1WL34':'EW','H1UM34':'HUM','E1LV34':'ELV',
    'C1IC34':'CI','CVSH34':'CVS','H1CA34':'HCA','U1HS34':'UHS','MDTC34':'MDT',
    'TMOS34':'TMO','D1LR34':'DLR','I1RM34':'IRM','V1RT34':'VRT','E1TN34':'ETN',
    'DHER34':'DHR','A2XO34':'AXON','G1WW34':'GWW','P1AC34':'PCAR','U1RI34':'URI',
    'O1DF34':'ODFL','L1HX34':'LHX','H1II34':'HII','V1MC34':'VMC','E2AG34':'EXP',
    'S1NP34':'SNPS','C1DN34':'CDNS','A1NE34':'ANET','S1HW34':'SHW','N1DA34':'NDAQ',
    'A1MP34':'AMP','G1PI34':'GPN','ACNB34':'ACN','M1MC34':'MMC','R1EL34':'RELX',
    'SPGI34':'SPGI','L1YV34':'LYV','V1SA34':'V','W1MG34':'WMG','N1VO34':'NVO',
    'N1VS34':'NVS','A1ZN34':'AZN','G1SK34':'GSK','B1TI34':'BTI','R1DY34':'RDY',
    'I1FO34':'INFY','ICLR34':'ICLR','H1DB34':'HDB','N1WG34':'NWG','L1YG34':'LYG',
    'BCSA34':'SAN','K1BF34':'KB','S1KM34':'SKM','K1TC34':'KT','T1LK34':'TLK',
    'ARMT34':'MT','ARNC34':'HWM','E2NP34':'ENPH','FSLR34':'FSLR','S2ED34':'SEDG',
    'Q2SC34':'QS','KMIC34':'KMI','W1MB34':'WMB','S1RE34':'SRE','T1SO34':'SO',
    'NEXT34':'NEE','A1WK34':'AWK','N1IS34':'NI','P1NW34':'PNW','P1PL34':'PPL',
    'A1EP34':'AEP','C1MS34':'CMS','N1RG34':'NRG','V1ST34':'VST','N1GG34':'NGG',
    'W1EL34':'WELL','V1TA34':'VTR','R1EG34':'REG','G1MI34':'GIS','K1MX34':'KMX',
    'ROST34':'ROST','ORLY34':'ORLY','T1SC34':'TSCO','C1MG34':'CMG','D2PZ34':'DPZ',
    'YUMR34':'YUM','MCDC34':'MCD','H1AS34':'HAS','EAIN34':'EA','EBAY34':'EBAY',
    'GPSI34':'GPS','MACY34':'M','W2YF34':'W','A1KA34':'AKAM','WUNI34':'WU',
    'FDMO34':'F','GMCO34':'GM','HOND34':'HMC','BOEI34':'BA','U1AL34':'UAL',
    'AALL34':'AAL','S1OU34':'LUV','N1CL34':'NCLH','C1CL34':'CCL','R1YA34':'RYAAY',
    'T1MU34':'TMUS','T1AM34':'TEAM','H2UB34':'HUBS','G2DD34':'GDDY','SSFO34':'CRM',
    'T1WL34':'TWLO','R2NG34':'RNG','Z1OM34':'ZM','U2PS34':'UPST','F2IV34':'FIVN',
    'S2HO34':'SHOP','P2ST34':'PSTG','A1PO34':'APO','L2PL34':'LPLA','I1AC34':'IAC',
    'B2UR34':'BURL','S2FM34':'SFM','F2NV34':'FNV','C1CJ34':'CCJ','N1EM34':'NEM',
    'W1DC34':'WDC','S1TX34':'STX','M2RV34':'MRVL','L2SC34':'LSCC','O2NS34':'ON',
    'M2PR34':'MPWR','A1PH34':'APH','T2ER34':'TER','G1LW34':'GLW','K1SG34':'KEYS',
    'M1SI34':'MSI','I1EX34':'IEX','M1CH34':'MCHP','A1DI34':'ADI','I1PG34':'IPGP',
    'B2RK34':'BRKR','S2YN34':'SYNA','I2NG34':'INGR','L1WH34':'LW','S1WK34':'SWK',
    'V1RS34':'VRSK','T2RM34':'TRMB','P2TC34':'PTC','T2YL34':'TYL','G1RM34':'GRMN',
    'W1HR34':'WHR','C1HR34':'CHRW','P1AY34':'PAYX','FASL34':'FAST','S1NA34':'SNA',
    'V1MC34':'VMC','R1HI34':'RHI','C2AC34':'CACI','B2AH34':'BAH','M2KS34':'MKSI',
    'D2OX34':'DOX','I1PC34':'IP','D1OW34':'DOW','E1MN34':'EMN','D2AR34':'DAR',
    'DDNB34':'DD','C1AG34':'CAG','M1NS34':'MNST','C1OO34':'COO','L1EN34':'LEN',
    'M1HK34':'MHK','P1RG34':'PRGO','KHCB34':'KHC','C1AH34':'CAH','M1CK34':'MCK',
    'D1VN34':'DVN','E1OG34':'EOG','F1AN34':'FANG','OXYP34':'OXY','A1ES34':'AES',
    'F1MC34':'FMC','A1LK34':'ALK','P1SX34':'PSX','VLOE34':'VLO','STZB34':'STZ',
    'D2NL34':'DNLI','B2YN34':'BYND','B2MB34':'BMBL','CLOV34':'CLOV','Q2SC34':'QS',
    'S2EA34':'SE','U2PW34':'UPWK','R2PD34':'RPD','F2RS34':'FRSH','E2ST34':'ESTC',
    'E2TS34':'ETSY','B2HI34':'BILL','B2LN34':'BL','P2EG34':'PEGA','P2AT34':'PATH',
    'C2OU34':'COUR','I1QY34':'IQ','R2BL34':'RBLX','F1SL34':'FSLY','N2TN34':'NTNX',
    'T2DH34':'TDOC','D2OC34':'DOCS','Z1BR34':'ZBRA','Z2LL34':'Z','LBRD34':'LBRDA',
    'CHCM34':'CHTR','W1BD34':'WBD','PSKY34':'PARA','WABC34':'WAB','HPQB34':'HPQ',
    'H1PE34':'HPE','ELCI34':'EL','DVAI34':'DVA','C1NC34':'CNC','C1BR34':'CBRE',
    'A1IV34':'AIV','V1NO34':'VNO','S1LG34':'SLG','H2TA34':'HR','BOXP34':'BXP',
    'O2HI34':'OHI','G1AM34':'GLPI','S2TA34':'STAG','R1EG34':'REG','D1OC34':'DOCU',
    'B1RF34':'BR','A1MP34':'AMP','M1TC34':'MTCH','T1TW34':'TTWO','T1RI34':'TRIP',
    'W1BO34':'WB','M1UF34':'MUFG','NMRH34':'NMR','TAKP34':'TAK','K1SS34':'KSS',
    'H1RL34':'HRL','L1NC34':'LNC','L1MN34':'LUMN','V1OD34':'VOD','A1RG34':'ARGX',
    'EXPB31':'EXPN','PRXB31':'PRX','EVTC31':'EVTC','GROP31':'GRO',
}

def mapear_ticker_us(t):
    return BDR_TO_US_MAP.get(t, t.rstrip('0123456789'))

def buscar_dados_brapi(ticker_bdr):
    try:
        r=requests.get(f"https://brapi.dev/api/quote/{ticker_bdr}?token={BRAPI_TOKEN}",timeout=10)
        if r.status_code!=200: return None
        d=r.json()
        if 'results' not in d or not d['results']: return None
        res=d['results'][0]
        return {'preco':res.get('regularMarketPrice'),'variacao':res.get('regularMarketChangePercent'),
                'volume':res.get('regularMarketVolume'),'market_cap':res.get('marketCap'),
                'setor':res.get('sector','N/A'),'nome':res.get('longName',ticker_bdr)}
    except: return None

def calcular_score_brapi(d):
    score=50; det={'fonte':{'valor':'BRAPI (B3)','pontos':0,'criterio':'Dados da BDR na B3'},
                   'market_cap':{'valor':None,'pontos':0,'criterio':''},'volume':{'valor':None,'pontos':0,'criterio':''}}
    mc=d.get('market_cap')
    if mc:
        det['market_cap']['valor']=mc; mb=mc/1e9
        if mb>100: det['market_cap'].update(pontos=20,criterio='Large Cap (>$100B)'); score+=20
        elif mb>10: det['market_cap'].update(pontos=10,criterio='Mid Cap (>$10B)'); score+=10
        else: det['market_cap']['criterio']='Small Cap'
    vol=d.get('volume')
    if vol:
        det['volume']['valor']=vol
        if vol>1000000: det['volume'].update(pontos=10,criterio='Alta liquidez'); score+=10
        elif vol>100000: det['volume'].update(pontos=5,criterio='Boa liquidez'); score+=5
        else: det['volume']['criterio']='Baixa liquidez'
    return max(0,min(100,score)),det

def buscar_dados_openbb(ticker_us):
    try:
        from openbb import obb
        try: obb.user.credentials.fmp_api_key=FMP_API_KEY
        except: pass
        info={}
        try:
            p=obb.equity.profile(symbol=ticker_us,provider="fmp")
            if p and p.results: r=p.results[0]; info['marketCap']=getattr(r,'mkt_cap',None); info['sector']=getattr(r,'sector',None)
        except: pass
        try:
            m=obb.equity.fundamental.metrics(symbol=ticker_us,provider="fmp")
            if m and m.results: r=m.results[0]; info['trailingPE']=getattr(r,'pe_ratio',None); info['dividendYield']=getattr(r,'dividend_yield',None); info['revenueGrowth']=getattr(r,'revenue_growth',None)
        except: pass
        if info.get('marketCap') or info.get('trailingPE'): return info
    except: pass
    return None

NOMES_BDRS = {
    'AAPL34':'Apple Inc.','AMZO34':'Amazon.com','MSFT34':'Microsoft','NVDC34':'NVIDIA',
    'GOGL34':'Alphabet (GOOGL)','GOGL35':'Alphabet (GOOG)','FBOK34':'Meta Platforms',
    'TSLA34':'Tesla','AVGO34':'Broadcom','LILY34':'Eli Lilly','JPMC34':'JPMorgan Chase',
    'VISA34':'Visa','MSCD34':'Mastercard','NFLX34':'Netflix','ABTT34':'Abbott Laboratories',
    'JNJB34':'Johnson & Johnson','UNHH34':'UnitedHealth Group','PFIZ34':'Pfizer',
    'ABBV34':'AbbVie','MRCK34':'Merck','AMGN34':'Amgen','GILD34':'Gilead Sciences',
    'BIIB34':'Biogen','REGN34':'Regeneron','BMYB34':'Bristol-Myers Squibb','CHVX34':'Chevron',
    'EXXO34':'Exxon Mobil','COPH34':'ConocoPhillips','SLBG34':'SLB','HALI34':'Halliburton',
    'CSCO34':'Cisco Systems','INTU34':'Intuit','ADBE34':'Adobe','ORCL34':'Oracle',
    'QCOM34':'Qualcomm','ITLC34':'Intel','A1MD34':'AMD','A1MT34':'Applied Materials',
    'K1LA34':'KLA Corporation','L1RC34':'Lam Research','MUTC34':'Micron Technology',
    'TSMC34':'TSMC ADR','ASML34':'ASML ADR','HOME34':'Home Depot','LOWC34':"Lowe's",
    'WALM34':'Walmart','COWC34':'Costco','MCDC34':"McDonald's",'SBUB34':'Starbucks',
    'PGCO34':'Procter & Gamble','COCA34':'Coca-Cola','PEPB34':'PepsiCo',
    'COLG34':'Colgate-Palmolive','PHMO34':'Philip Morris','MOOO34':'Altria Group',
    'KMBB34':'Kimberly-Clark','MDLZ34':'Mondelez','KHCB34':'Kraft Heinz',
    'DEEC34':'Deere & Co','CATP34':'Caterpillar','BOEI34':'Boeing','GDBR34':'General Dynamics',
    'NOCG34':'Northrop Grumman','RYTT34':'RTX','GEOO34':'GE Aerospace','G2EV34':'GE Vernova',
    'UPAC34':'Union Pacific','CPRL34':'Canadian Pacific','CNIC34':'Canadian National Railway',
    'CSXC34':'CSX Corporation','FDXB34':'FedEx','BOAC34':'Bank of America',
    'WFCO34':'Wells Fargo','CTGP34':'Citigroup','MSBR34':'Morgan Stanley',
    'GSGI34':'Goldman Sachs','AXPB34':'American Express','SCHW34':'Charles Schwab',
    'BONY34':'Bank of New York Mellon','USBC34':'U.S. Bancorp','PNCS34':'PNC Financial',
    'CAON34':'Capital One','BERK34':'Berkshire Hathaway B','BLAK34':'BlackRock',
    'SPGI34':'S&P Global','MCOR34':"Moody's",'IBKR34':'Interactive Brokers',
    'PYPL34':'PayPal','SSFO34':'Salesforce','N1OW34':'ServiceNow','MELI34':'MercadoLibre',
    'PAGS34':'PagSeguro','STOC34':'StoneCo','ROXO34':'Nu Holdings','XPBR31':'XP Inc.',
    'INBR32':'Inter & Co.','V2TX34':'VTEX','JBSS32':'JBS N.V.','ATTB34':'AT&T',
    'VERZ34':'Verizon','NOKI34':'Nokia','E1RI34':'Ericsson ADR','SNEC34':'Sony ADR',
    'HOND34':'Honda ADR','TMCO34':'Toyota ADR','SAPP34':'SAP ADR','UBSG34':'UBS ADR',
    'DBAG34':'Deutsche Bank','B1CS34':'Barclays ADR','H1SB34':'HSBC','B1PP34':'BP PLC',
    'E1QN34':'Equinor ADR','ABUD34':'Anheuser-Busch InBev ADR','DEOP34':'Diageo ADR',
    'ULEV34':'Unilever ADR','PHGN34':'Philips ADR','NEXT34':'NextEra Energy',
    'D1LR34':'Digital Realty','EQIX34':'Equinix','T1OW34':'American Tower',
    'S1BA34':'SBA Communications','P1LD34':'Prologis','R1IN34':'Realty Income',
    'T2DH34':'Teladoc Health','R1KU34':'Roku','S1PO34':'Spotify','A1PP34':'AppLovin',
    'P2LT34':'Palantir','C2RW34':'CrowdStrike','P2AN34':'Palo Alto Networks',
    'F1TN34':'Fortinet','N2ET34':'Cloudflare','O1KT34':'Okta','Z2SC34':'Zscaler',
    'D1DG34':'Datadog','S2NW34':'Snowflake','M1DB34':'MongoDB','U2ST34':'Unity Software',
    'R2BL34':'Roblox','D2AS34':'DoorDash','AIRB34':'Airbnb','U1BE34':'Uber Technologies',
    'DUOL34':'Duolingo','T2TD34':'Trade Desk','C2OI34':'Coinbase','M2ST34':'Strategy Inc',
    'RGTI34':'Rigetti Computing','QUBT34':'Quantum Computing','BIDU34':'Baidu ADR',
    'BABA34':'Alibaba ADR','JDCO34':'JD.com ADR','NETE34':'Netease ADR','B1IL34':'Bilibili ADR',
    'P1DD34':'PDD Holdings ADR','N1EM34':'Newmont','FCXO34':'Freeport-McMoRan',
    'A1LB34':'Albemarle','S2GM34':'Sigma Lithium','MOSC34':'Mosaic','Y2PF34':'YPF SA',
    'E2NP34':'Enphase Energy','FSLR34':'First Solar','B1NT34':'BioNTech ADR',
    'M1RN34':'Moderna','C2RS34':'CRISPR Therapeutics','I1LM34':'Illumina',
    'I1SR34':'Intuitive Surgical','R1MD34':'ResMed','D1EX34':'DexCom',
    'E1LV34':'Elevance Health','C1IC34':'Cigna Group','CVSH34':'CVS Health',
    'H1CA34':'HCA Healthcare','UNHH34':'UnitedHealth Group','MDTC34':'Medtronic',
    'TMOS34':'Thermo Fisher Scientific','DHER34':'Danaher','A2XO34':'Axon Enterprise',
    'S1NP34':'Synopsys','C1DN34':'Cadence Design','A1NE34':'Arista Networks',
    'N1DA34':'Nasdaq Inc','M1MC34':'Marsh & McLennan','ACNB34':'Accenture',
    'T1MU34':'T-Mobile US','T1AM34':'Atlassian','H2UB34':'HubSpot','G2DD34':'GoDaddy',
    'T1WL34':'Twilio','R2NG34':'RingCentral','Z1OM34':'Zoom','U2PS34':'Upstart',
    'S2HO34':'Shopify','NIKE34':'Nike','ROST34':'Ross Stores','ORLY34':"O'Reilly Automotive",
    'T1SC34':'Tractor Supply','C1MG34':'Chipotle Mexican Grill','EBAY34':'eBay',
    'EAIN34':'Electronic Arts','U1AL34':'United Airlines','AALL34':'American Airlines',
    'S1OU34':'Southwest Airlines','N1CL34':'Norwegian Cruise Line','C1CL34':'Carnival',
    'DISB34':'Walt Disney','CHCM34':'Charter Communications','W1BD34':'Warner Bros. Discovery',
    'HPQB34':'HP Inc','H1PE34':'Hewlett Packard Enterprise','ELCI34':'Estee Lauder',
    'WUNI34':'Western Union','G1WW34':'W.W. Grainger','FASL34':'Fastenal',
    'P1AC34':'PACCAR','U1RI34':'United Rentals','O1DF34':'Old Dominion Freight',
    'L1HX34':'L3Harris Technologies','H1II34':'Huntington Ingalls','V1MC34':'Vulcan Materials',
    'F2IC34':'Fair Isaac (FICO)','V1RS34':'Verisk Analytics','M1SC34':'MSCI Inc',
    'I1QV34':'IQVIA Holdings','R1OP34':'Roper Technologies','G1AR34':'Gartner',
    'G1RM34':'Garmin','I1EX34':'IDEX Corporation','M1SI34':'Motorola Solutions',
    'K1SG34':'Keysight Technologies','A1DI34':'Analog Devices','A1MT34':'Applied Materials',
    'T2ER34':'Teradyne','G1LW34':'Corning','S1WK34':'Stanley Black & Decker',
    'KMIC34':'Kinder Morgan','W1MB34':'Williams Companies','S1RE34':'Sempra',
    'T1SO34':'Southern Company','A1WK34':'American Water Works',
}

@st.cache_data(ttl=3600, show_spinner=False)
def buscar_dados_fundamentalistas(ticker_bdr):
    ticker_us=mapear_ticker_us(ticker_bdr)
    def _score(info, fonte, tl):
        if not info or len(info)<5: return None
        if not any([info.get('marketCap'),info.get('trailingPE'),info.get('forwardPE'),info.get('revenueGrowth')]): return None
        score=50; det={}
        pe=info.get('trailingPE') or info.get('forwardPE')
        if pe and isinstance(pe,(int,float)):
            det['pe_ratio']={'valor':round(pe,2),'pontos':0,'criterio':''}
            if 10<=pe<=25: score+=15; det['pe_ratio'].update(pontos=15,criterio='Ótimo (10-25)')
            elif 5<=pe<=35: score+=10; det['pe_ratio'].update(pontos=10,criterio='Bom')
            elif pe<5: score+=5; det['pe_ratio'].update(pontos=5,criterio='Baixo (<5)')
            elif pe>50: score-=10; det['pe_ratio'].update(pontos=-10,criterio='Muito alto (>50)')
            else: det['pe_ratio']['criterio']='Regular'
        else: det['pe_ratio']={'valor':None,'pontos':0,'criterio':''}
        dy=info.get('dividendYield')
        if dy and isinstance(dy,(int,float)):
            det['dividend_yield']={'valor':dy,'pontos':0,'criterio':''}
            if dy>0.04: score+=10; det['dividend_yield'].update(pontos=10,criterio='Excelente (>4%)')
            elif dy>0.02: score+=5; det['dividend_yield'].update(pontos=5,criterio='Bom (>2%)')
            else: det['dividend_yield']['criterio']='Baixo (<2%)'
        else: det['dividend_yield']={'valor':None,'pontos':0,'criterio':''}
        rg=info.get('revenueGrowth')
        if rg and isinstance(rg,(int,float)):
            det['revenue_growth']={'valor':rg,'pontos':0,'criterio':''}
            if rg>0.20: score+=15; det['revenue_growth'].update(pontos=15,criterio='Excelente (>20%)')
            elif rg>0.10: score+=10; det['revenue_growth'].update(pontos=10,criterio='Muito bom (>10%)')
            elif rg>0.05: score+=5; det['revenue_growth'].update(pontos=5,criterio='Bom (>5%)')
            elif rg<-0.10: score-=10; det['revenue_growth'].update(pontos=-10,criterio='Negativo')
            else: det['revenue_growth']['criterio']='Estável'
        else: det['revenue_growth']={'valor':None,'pontos':0,'criterio':''}
        rec=info.get('recommendationKey','')
        pr={'strong_buy':10,'buy':5,'hold':0,'sell':-5,'strong_sell':-10}
        cr={'strong_buy':'Compra Forte','buy':'Compra','hold':'Manter','sell':'Venda','strong_sell':'Venda Forte'}
        score+=pr.get(rec,0); det['recomendacao']={'valor':rec,'pontos':pr.get(rec,0),'criterio':cr.get(rec,rec.replace('_',' ').title() if rec else '')}
        mc=info.get('marketCap')
        if mc and isinstance(mc,(int,float)):
            det['market_cap']={'valor':mc,'pontos':0,'criterio':''}
            if mc>1e12: score+=10; det['market_cap'].update(pontos=10,criterio='Mega Cap (>$1T)')
            elif mc>100e9: score+=5; det['market_cap'].update(pontos=5,criterio='Large Cap (>$100B)')
            elif mc>10e9: det['market_cap']['criterio']='Mid Cap (>$10B)'
            else: det['market_cap']['criterio']='Small Cap (<$10B)'
        else: det['market_cap']={'valor':None,'pontos':0,'criterio':''}
        score=max(0,min(100,score))
        return {'fonte':fonte,'ticker_fonte':tl,'score':score,'detalhes':det,
                'pe_ratio':det['pe_ratio']['valor'],'dividend_yield':det['dividend_yield']['valor'],
                'market_cap':det['market_cap']['valor'],'revenue_growth':det['revenue_growth']['valor'],
                'recomendacao':det['recomendacao']['valor'],'setor':info.get('sector','N/A')}

    # 1. Yahoo via nome
    try:
        nome=NOMES_BDRS.get(ticker_bdr,'')
        for sfx in [' ADR',' ADS',' ADR A',' ADR B',' Class A',' Class B']: nome=nome.replace(sfx,'')
        nome=nome.strip()
        if nome:
            rb=yf.Search(nome,max_results=5)
            qs=rb.quotes if hasattr(rb,'quotes') else []
            for q in qs:
                if q.get('quoteType')=='EQUITY' and '.' not in q.get('symbol','') and q.get('exchange','') in ('NMS','NYQ','NGM','NCM','ASE','PCX','NAS'):
                    try:
                        info=yf.Ticker(q['symbol']).info
                        r=_score(info,f"Yahoo Finance — {q['symbol']}",q['symbol'])
                        if r: return r
                    except: continue
    except: pass

    # 2. Yahoo direto pelo ticker US
    try:
        for t in ([ticker_us,ticker_us.replace('-','.')] if '-' in ticker_us else [ticker_us]):
            try:
                info=yf.Ticker(t).info; r=_score(info,f"Yahoo Finance — {t}",t)
                if r: return r
            except: continue
    except: pass

    # 3. OpenBB
    try:
        info=buscar_dados_openbb(ticker_us); r=_score(info,f"OpenBB/FMP — {ticker_us}",ticker_us)
        if r: return r
    except: pass

    # 4. BRAPI
    try:
        d=buscar_dados_brapi(ticker_bdr)
        if d:
            s,det=calcular_score_brapi(d)
            return {'fonte':'BRAPI (BDR na B3)','ticker_fonte':ticker_bdr,'score':s,'detalhes':det,
                    'pe_ratio':None,'dividend_yield':None,'market_cap':d.get('market_cap'),
                    'revenue_growth':None,'recomendacao':None,'setor':d.get('setor','N/A'),'volume_b3':d.get('volume')}
    except: pass
    return None

# =============================================================================
# DADOS E INDICADORES
# =============================================================================
@st.cache_data(ttl=1800)
def buscar_dados(tickers):
    if not tickers: return pd.DataFrame()
    sa=[f"{t}.SA" for t in tickers]
    try:
        df=yf.download(sa,period=PERIODO,auto_adjust=True,progress=False,timeout=60)
        if df.empty: return pd.DataFrame()
        if isinstance(df.columns,pd.MultiIndex):
            df.columns=pd.MultiIndex.from_tuples([(c[0],c[1].replace(".SA","")) for c in df.columns])
        return df.dropna(axis=1,how='all')
    except: return pd.DataFrame()

def calcular_indicadores(df):
    dc=df.copy(); tickers=dc.columns.get_level_values(1).unique()
    prog=st.progress(0); tot=len(tickers)
    for i,t in enumerate(tickers):
        prog.progress((i+1)/tot)
        try:
            c=dc[('Close',t)]; h=dc[('High',t)]; l=dc[('Low',t)]
            d=c.diff(); g=d.clip(lower=0).rolling(14).mean(); p=-d.clip(upper=0).rolling(14).mean()
            dc[('RSI14',t)]=100-(100/(1+g/p))
            ll=l.rolling(14).min(); hh=h.rolling(14).max()
            dc[('Stoch_K',t)]=100*((c-ll)/(hh-ll))
            dc[('EMA20',t)]=c.ewm(span=20).mean(); dc[('EMA50',t)]=c.ewm(span=50).mean()
            dc[('EMA200',t)]=c.ewm(span=200).mean()
            sm=c.rolling(20).mean(); st=c.rolling(20).std()
            dc[('BB_Lower',t)]=sm-st*2; dc[('BB_Upper',t)]=sm+st*2
            e12=c.ewm(span=12).mean(); e26=c.ewm(span=26).mean()
            macd=e12-e26; sig=macd.ewm(span=9).mean(); dc[('MACD_Hist',t)]=macd-sig
        except: continue
    prog.empty(); return dc

def calcular_fibonacci(df_t):
    try:
        if len(df_t)<50: return None
        h=df_t['High'].max(); l=df_t['Low'].min(); d=h-l
        return {'61.8%':l+(d*.618)}
    except: return None

def gerar_sinal(row, df_t):
    sinais=[]; score=0; exp=[]
    def cls(s): return "Muito Alta" if s>=4 else "Alta" if s>=2 else "Média" if s>=1 else "Baixa"
    try:
        c=row.get('Close'); rsi=row.get('RSI14'); stoch=row.get('Stoch_K')
        mh=row.get('MACD_Hist'); bbl=row.get('BB_Lower')
        if pd.notna(rsi):
            if rsi<30: sinais.append("RSI Oversold"); score+=3; exp.append(f"📉 RSI {rsi:.1f} (<30): Forte sobrevenda")
            elif rsi<40: sinais.append("RSI Baixo"); score+=1; exp.append(f"📊 RSI {rsi:.1f} (<40): Sobrevenda moderada")
        if pd.notna(stoch) and stoch<20: sinais.append("Stoch. Fundo"); score+=2; exp.append(f"📉 Estocástico {stoch:.1f} (<20): Muito sobrevendido")
        if pd.notna(mh) and mh>0: sinais.append("MACD Virando"); score+=1; exp.append("🔄 MACD positivo: Momentum de alta")
        if pd.notna(c) and pd.notna(bbl):
            if c<bbl: sinais.append("Abaixo BB"); score+=2; exp.append("⚠️ Abaixo da Banda de Bollinger: Sobrevenda extrema")
            elif c<bbl*1.02: sinais.append("Suporte BB"); score+=1; exp.append("🎯 Próximo da Banda Inferior: Zona de suporte")
        fib=calcular_fibonacci(df_t)
        if fib and c and (fib['61.8%']*.99<=c<=fib['61.8%']*1.01): sinais.append("Fibo 61.8%"); score+=2; exp.append("⭐ Na Zona de Ouro Fibonacci (61.8%)!")
        return sinais,score,cls(score),exp
    except: return [],[],0,"Indefinida",[]

def analisar_oportunidades(df_calc, mapa_nomes):
    res=[]; tickers=df_calc.columns.get_level_values(1).unique()
    for t in tickers:
        try:
            df_t=df_calc.xs(t,axis=1,level=1).dropna()
            if len(df_t)<50: continue
            last=df_t.iloc[-1]; ant=df_t.iloc[-2]
            p=last.get('Close'); pa=ant.get('Close'); po=last.get('Open'); vol=last.get('Volume')
            if pd.isna(p) or pd.isna(pa): continue
            qd=((p-pa)/pa)*100; gap=((po-pa)/pa)*100
            if qd>=0: continue
            sinais,score,cls,exp=gerar_sinal(last,df_t)
            rsi=last.get('RSI14',50); stoch=last.get('Stoch_K',50)
            is_idx=((100-rsi)+(100-stoch))/2
            try:
                n=min(20,len(df_t)); vs=df_t['Volume'].tail(n); vm=vs.mean()
                if pd.isna(vm): vm=0
                ng=sum(1 for i in range(1,min(n+1,len(df_t))) if df_t['Close'].iloc[-i-1]>0 and abs((df_t['Open'].iloc[-i]-df_t['Close'].iloc[-i-1])/df_t['Close'].iloc[-i-1])*100>1)
                co=sum(1 for v in vs if pd.notna(v) and v>=vm*.8)/n if n>0 else 0
                liq=0
                if vm>500000: liq+=40
                elif vm>100000: liq+=35
                elif vm>50000: liq+=30
                elif vm>10000: liq+=25
                elif vm>5000: liq+=20
                elif vm>1000: liq+=15
                elif vm>100: liq+=10
                else: liq+=5
                if ng==0: liq+=30
                elif ng<=2: liq+=25
                elif ng<=5: liq+=20
                elif ng<=8: liq+=15
                elif ng<=12: liq+=10
                else: liq+=5
                if co>=0.75: liq+=30
                elif co>=0.50: liq+=20
                elif co>=0.25: liq+=10
                else: liq+=5
                rliq=max(0,min(10,round(liq/10)))
            except: rliq=1
            nc=mapa_nomes.get(t,t)
            if nc==t: ns=t
            else:
                il=['INC','CORP','LTD','S.A.','GMBH','PLC','GROUP','HOLDINGS','CO','LLC']
                pv=[w for w in nc.split() if w.upper().replace('.','').replace(',','') not in il]
                ns=" ".join(pv[:2]).replace(',','').title() if pv else nc
            res.append({'Ticker':t,'Empresa':ns,'Preco':p,'Volume':vol,'Queda_Dia':qd,'Gap':gap,
                        'IS':is_idx,'RSI14':rsi,'Stoch':stoch,'Potencial':cls,'Score':score,
                        'Sinais':", ".join(sinais) if sinais else "-",'Explicacoes':exp,'Liquidez':int(rliq)})
        except: continue
    return res

# =============================================================================
# ★★★ GRÁFICO AVANÇADO ★★★
# =============================================================================
@st.cache_data(ttl=900, show_spinner=False)
def _buscar_ohlcv_tf(ticker_bdr: str, timeframe: str):
    cfg={"60min":dict(period="60d",interval="60m"),"Diário":dict(period="1y",interval="1d"),
         "Semanal":dict(period="3y",interval="1wk"),"Mensal":dict(period="10y",interval="1mo")}
    c=cfg.get(timeframe,cfg["Diário"])
    try:
        df=yf.download(f"{ticker_bdr}.SA",period=c["period"],interval=c["interval"],
                       auto_adjust=True,progress=False,timeout=30)
        if df.empty: return pd.DataFrame()
        if isinstance(df.columns,pd.MultiIndex): df.columns=df.columns.get_level_values(0)
        return df[["Open","High","Low","Close","Volume"]].dropna()
    except: return pd.DataFrame()

def _calc_ind(df):
    d=df.copy(); c=d["Close"]
    d["EMA20"]=c.ewm(span=20,adjust=False).mean()
    d["EMA50"]=c.ewm(span=50,adjust=False).mean()
    d["EMA200"]=c.ewm(span=200,adjust=False).mean()
    dlt=c.diff(); g=dlt.clip(lower=0).rolling(14).mean(); l=(-dlt.clip(upper=0)).rolling(14).mean()
    d["RSI14"]=100-100/(1+g/l.replace(0,np.nan))
    lo=d["Low"].rolling(14).min(); hi=d["High"].rolling(14).max()
    d["Stoch_K"]=100*(c-lo)/(hi-lo).replace(0,np.nan)
    sm=c.rolling(20).mean(); st=c.rolling(20).std()
    d["BB_Lower"]=sm-2*st; d["BB_Upper"]=sm+2*st
    return d

def _plotar_avancado(df_diario, ticker, empresa, rsi_val, is_val,
                     timeframe="Diário", n_candles=120, tipo="Linha"):
    df_raw=_buscar_ohlcv_tf(ticker, timeframe)
    if df_raw.empty:
        cols=[c for c in ["Open","High","Low","Close","Volume"] if c in df_diario.columns]
        df_raw=df_diario[cols].copy()
    df=_calc_ind(df_raw).dropna(subset=["Close","EMA20"]).sort_index()
    df=df.iloc[-min(n_candles,len(df)):]
    c=df["Close"]; dt=df.index
    e20=df["EMA20"]; e50=df.get("EMA50"); e200=df.get("EMA200")

    # Fibonacci
    fib_l={"0%":df["High"].max(),"23.6%":df["High"].max()-df["High"].max()*.236,
            "38.2%":df["High"].max()-(df["High"].max()-df["Low"].min())*.382,
            "50%":df["High"].max()-(df["High"].max()-df["Low"].min())*.5,
            "61.8%":df["High"].max()-(df["High"].max()-df["Low"].min())*.618,
            "78.6%":df["High"].max()-(df["High"].max()-df["Low"].min())*.786,
            "100%":df["Low"].min()}
    fc={"0%":"#FF5252","23.6%":"#FF7043","38.2%":"#FFCA28",
        "50%":"#42A5F5","61.8%":"#66BB6A","78.6%":"#26C6DA","100%":"#AB47BC"}

    BG="#0d1117"; PNL="#161b22"; GR="#21262d"; TX="#c9d1d9"; UP="#3fb950"; DN="#f85149"
    plt.rcParams.update({"text.color":TX,"axes.labelcolor":TX,"xtick.color":TX,"ytick.color":TX})

    fig,axes=plt.subplots(3,1,figsize=(12,9),sharex=True,
                          gridspec_kw={"height_ratios":[4,1,1]},facecolor=BG)
    for ax in axes:
        ax.set_facecolor(PNL); ax.grid(True,color=GR,linewidth=.6,zorder=0)
        for sp in ax.spines.values(): sp.set_edgecolor(GR)
    ax1,ax2,ax3=axes

    # Bollinger
    if "BB_Lower" in df.columns:
        ax1.fill_between(dt,df["BB_Lower"],df["BB_Upper"],alpha=.07,color="#607d8b")
        ax1.plot(dt,df["BB_Upper"],color="#607d8b",linewidth=.6,alpha=.4)
        ax1.plot(dt,df["BB_Lower"],color="#607d8b",linewidth=.6,alpha=.4)

    # Fibonacci
    uc=float(c.iloc[-1])
    npm=min(fib_l,key=lambda n: abs(uc-fib_l[n]))
    for nv,pf in fib_l.items():
        ax1.axhline(pf,color=fc[nv],linestyle="--",linewidth=.8,alpha=.45)
        ax1.text(dt[-1],pf,f" {nv}",fontsize=6.5,color=fc[nv],va="center",
                 bbox=dict(boxstyle="round,pad=.15",facecolor=BG,edgecolor=fc[nv],alpha=.75))
    ax1.axhspan(fib_l["61.8%"]*.99,fib_l["61.8%"]*1.01,alpha=.08,color=UP)

    use_c=(tipo=="Candles" and "Open" in df.columns)
    if use_c:
        xs=np.arange(len(df)); op=df["Open"].values; hi=df["High"].values
        lo=df["Low"].values; cl=c.values; cols=[UP if cv>=ov else DN for cv,ov in zip(cl,op)]
        for i,(h,l,col) in enumerate(zip(hi,lo,cols)): ax1.plot([i,i],[l,h],color=col,linewidth=.9,alpha=.85)
        for i,(o,cv,col) in enumerate(zip(op,cl,cols)):
            bot=min(o,cv); ht=max(abs(cv-o),uc*.001)
            ax1.bar(i,ht,bottom=bot,width=.6,color=col,alpha=.9)
        step=max(1,len(df)//8)
        ax1.set_xticks(xs[::step])
        ax1.set_xticklabels([str(d)[:10] for d in dt[::step]],rotation=30,ha="right",fontsize=7.5)
        ax1.plot(xs,e20.values,label="EMA20",color="#2979FF",linewidth=1.3,alpha=.9)
        if e50 is not None: ax1.plot(xs,e50.values,label="EMA50",color="#FF6D00",linewidth=1.3,alpha=.85)
        if e200 is not None: ax1.plot(xs,e200.values,label="EMA200",color="#00E676",linewidth=1.7,alpha=.8)
        ax1.scatter([xs[-1]],[uc],color=DN,s=50,zorder=6)
        ax2x=xs; ax3x=xs
        rsi_s=df["RSI14"].values if "RSI14" in df.columns else None
        st_s=df["Stoch_K"].values if "Stoch_K" in df.columns else None
    else:
        ax1.plot(dt,e20,label="EMA20",color="#2979FF",linewidth=1.3,alpha=.9)
        if e50 is not None: ax1.plot(dt,e50,label="EMA50",color="#FF6D00",linewidth=1.3,alpha=.85)
        if e200 is not None: ax1.plot(dt,e200,label="EMA200",color="#00E676",linewidth=1.7,alpha=.8)
        ax1.fill_between(dt,c,c.min()*.99,alpha=.10,color="#2979FF")
        ax1.plot(dt,c,label="Close",color="#e6edf3",linewidth=1.9,zorder=5)
        ax1.scatter([dt[-1]],[uc],color=DN,s=50,zorder=6)
        ax3.xaxis.set_major_formatter(mdates.DateFormatter("%b/%y"))
        ax3.xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.setp(ax3.xaxis.get_majorticklabels(),rotation=30,ha="right",fontsize=7.5)
        ax2x=dt; ax3x=dt
        rsi_s=df["RSI14"] if "RSI14" in df.columns else None
        st_s=df["Stoch_K"] if "Stoch_K" in df.columns else None

    # Tendência
    v20=float(e20.iloc[-1])
    v50=float(e50.iloc[-1]) if e50 is not None else None
    v200=float(e200.iloc[-1]) if e200 is not None else None
    if v50 and v200:
        if uc>v20>v50>v200: status="🟢 Tendência Forte de Alta"
        elif uc>v20 and uc>v50 and uc>v200: status="🟢 Acima das 3 EMAs"
        elif uc<v20 and uc<v50 and uc<v200: status="🔴 Abaixo das 3 EMAs"
        else: status="🟡 Tendência Mista"
    else: status="🟢 Acima EMA20" if uc>v20 else "🔴 Abaixo EMA20"

    tl="🕯️ Candles" if use_c else "📈 Linha"
    ax1.set_title(f"{ticker} — {empresa}  |  {timeframe}  |  {tl}  |  I.S.:{is_val:.0f}  |  {status}  |  Fib:{npm}",
                  color=TX,fontweight="bold",fontsize=9.5,pad=8)
    ax1.legend(loc="upper left",fontsize=7.5,framealpha=.25,facecolor=BG,edgecolor=GR,ncol=4)
    ax1.set_ylabel("Preço (R$)",fontsize=8.5,color=TX)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda v,_: f"R${v:.2f}"))

    # RSI
    if rsi_s is not None:
        ra=np.array(rsi_s) if not isinstance(rsi_s,np.ndarray) else rsi_s
        ax2.plot(ax2x,ra,color="#FF6F00",linewidth=1.4,label="RSI14")
        ax2.axhline(30,color=DN,linestyle="--",linewidth=.9,alpha=.7)
        ax2.axhline(70,color=UP,linestyle="--",linewidth=.9,alpha=.7)
        ax2.fill_between(ax2x,0,30,alpha=.12,color=DN)
        ax2.fill_between(ax2x,70,100,alpha=.12,color=UP)
    ax2.set_ylabel("RSI 14",fontsize=8,color=TX); ax2.set_ylim(0,100)
    ax2.legend(loc="upper right",fontsize=7,framealpha=.25,facecolor=BG,edgecolor=GR)

    # Estocástico
    if st_s is not None:
        sa=np.array(st_s) if not isinstance(st_s,np.ndarray) else st_s
        ax3.plot(ax3x,sa,color="#9C27B0",linewidth=1.4,label="Stoch %K")
        ax3.axhline(20,color=DN,linestyle="--",linewidth=.9,alpha=.7)
        ax3.axhline(80,color=UP,linestyle="--",linewidth=.9,alpha=.7)
        ax3.fill_between(ax3x,0,20,alpha=.12,color=DN)
        ax3.fill_between(ax3x,80,100,alpha=.12,color=UP)
    ax3.set_ylabel("Stoch %K",fontsize=8,color=TX); ax3.set_ylim(0,100)
    ax3.legend(loc="upper right",fontsize=7,framealpha=.25,facecolor=BG,edgecolor=GR)
    ax3.set_xlabel("Data",fontsize=8.5,color=TX)
    plt.tight_layout(); return fig


def renderizar_grafico_com_controles(df_ticker, ticker, empresa, rsi_val, is_val):
    """Controles de timeframe / zoom / tipo + gráfico avançado dark."""
    st.markdown("""<div style='background:#161b22;border:1px solid #30363d;border-radius:8px;
padding:.6rem 1rem;margin-bottom:.5rem;'><span style='color:#8b949e;font-size:.8rem;'>⚙️ Controles do Gráfico</span></div>""",
                unsafe_allow_html=True)

    c1,c2,c3,c4=st.columns([3,3,1,1])
    with c1:
        tf=st.radio("⏱ Timeframe",["60min","Diário","Semanal","Mensal"],
                    index=1,horizontal=True,key=f"tf_{ticker}")
    with c2:
        tipo=st.radio("📊 Tipo",["Linha","Candles"],index=0,horizontal=True,key=f"tipo_{ticker}")
    with c3:
        st.markdown("<div style='height:1.6rem'></div>",unsafe_allow_html=True)
        if st.button("🔍 +",key=f"zi_{ticker}",help="Zoom In — menos candles"):
            st.session_state[f"zoom_{ticker}"]=max(20,st.session_state.get(f"zoom_{ticker}",120)-20)
    with c4:
        st.markdown("<div style='height:1.6rem'></div>",unsafe_allow_html=True)
        if st.button("🔎 −",key=f"zo_{ticker}",help="Zoom Out — mais candles"):
            st.session_state[f"zoom_{ticker}"]=min(500,st.session_state.get(f"zoom_{ticker}",120)+40)

    n=st.slider("Janela de visualização (candles/barras)",min_value=20,max_value=500,
                value=st.session_state.get(f"zoom_{ticker}",120),step=10,key=f"slider_{ticker}")
    st.session_state[f"zoom_{ticker}"]=n

    with st.spinner(f"Carregando {tf}..."):
        fig=_plotar_avancado(df_ticker,ticker,empresa,rsi_val,is_val,
                             timeframe=tf,n_candles=n,tipo=tipo)
    st.pyplot(fig,use_container_width=True); plt.close(fig)

# =============================================================================
# ESTILOS
# =============================================================================
def estilizar_is(v):
    if v>=75: return 'background-color:#d32f2f;color:white;font-weight:bold'
    elif v>=60: return 'background-color:#ffa726;color:black'
    return 'color:#888'

def estilizar_potencial(v):
    m={'Muito Alta':'background-color:#2e7d32;color:white;font-weight:bold',
       'Alta':'background-color:#66bb6a;color:black;font-weight:bold',
       'Média':'background-color:#ffa726;color:black',
       'Baixa':'background-color:#e0e0e0;color:black'}
    return m.get(v,'')

def estilizar_liquidez(v):
    p={0:('#7f0000','white'),1:('#c62828','white'),2:('#ef5350','white'),3:('#ff7043','white'),
       4:('#ffa726','black'),5:('#fdd835','black'),6:('#d4e157','black'),7:('#9ccc65','black'),
       8:('#66bb6a','black'),9:('#2e7d32','white'),10:('#1b5e20','white')}
    try: v=int(v)
    except: v=0
    bg,fg=p.get(v,('#9e9e9e','white'))
    return f'background-color:{bg};color:{fg};font-weight:900;font-size:1.1em;text-align:center;'

# =============================================================================
# LAYOUT
# =============================================================================
st.markdown("""
<style>
.main-header{background:linear-gradient(135deg,#667eea,#764ba2);padding:2rem;border-radius:10px;
             margin-bottom:2rem;box-shadow:0 4px 6px rgba(0,0,0,.1);}
.main-title{color:white;font-size:2.5rem;font-weight:700;margin:0;text-align:center;}
.main-subtitle{color:rgba(255,255,255,.9);font-size:1.1rem;text-align:center;margin-top:.5rem;}
.section-header{color:#667eea;font-size:1.5rem;font-weight:600;margin-top:2rem;margin-bottom:1rem;
                padding-bottom:.5rem;border-bottom:2px solid #667eea;}
.stButton>button{width:100%;background:linear-gradient(135deg,#667eea,#764ba2);color:white;
                 font-weight:600;border:none;padding:.75rem 2rem;border-radius:8px;transition:all .3s;}
.stButton>button:hover{transform:translateY(-2px);box-shadow:0 4px 12px rgba(102,126,234,.4);}
</style>""", unsafe_allow_html=True)

fuso=pytz.timezone('America/Sao_Paulo'); agora=datetime.now(fuso)
dh=agora.strftime("%d/%m/%Y às %H:%M:%S")
dias_pt={'Monday':'Segunda-feira','Tuesday':'Terça-feira','Wednesday':'Quarta-feira',
         'Thursday':'Quinta-feira','Friday':'Sexta-feira','Saturday':'Sábado','Sunday':'Domingo'}
ds=dias_pt.get(agora.strftime("%A"),agora.strftime("%A"))

st.markdown(f"""<div class="main-header">
<h1 class="main-title">📊 Monitor BDR - Swing Trade Pro</h1>
<p class="main-subtitle">Análise Técnica Avançada | Rastreamento de Oportunidades em Tempo Real</p>
<p style="color:rgba(255,255,255,.8);font-size:.9rem;text-align:center;margin-top:.5rem;">
🕐 {ds}, {dh} (Horário de Brasília)</p></div>""", unsafe_allow_html=True)

c1,c2,c3=st.columns(3)
with c1: st.markdown("**📈 Estratégia:** Reversão em Sobrevenda")
with c2: st.markdown("**🎯 Foco:** BDRs em Queda com Potencial")
with c3: st.markdown("**⏱️ Timeframe:** Múltiplos | Diário padrão")
st.markdown("---")

with st.expander("📚 Guia dos Indicadores e Timeframes", expanded=False):
    st.markdown("""
### 🎯 Índice de Sobrevenda (I.S.)
- **75-100**: 🔴 Muito sobrevendido | **60-75**: 🟠 Moderado | **< 60**: ⚪ Normal

### ⏱️ Timeframes do Gráfico
| Timeframe | Período | Intervalo | Uso |
|-----------|---------|-----------|-----|
| 60min | 60 dias | 1 hora | Day/swing curto prazo |
| Diário | 1 ano | 1 dia | Swing trade (padrão) |
| Semanal | 3 anos | 1 semana | Tendência médio prazo |
| Mensal | 10 anos | 1 mês | Tendência longo prazo |

### 🕯️ Candles vs Linha
- **Candles**: Exibe Open/High/Low/Close — ideal para padrões de reversão
- **Linha**: Somente fechamento — mais limpo para tendências

### 🔍 Zoom In/Out
- **🔍 +** reduz a janela (foco nos candles mais recentes)
- **🔎 −** amplia a janela (visão histórica maior)
- O slider permite ajuste preciso de 20 a 500 candles

### 🌟 Fibonacci 61.8% → Zona de Ouro (maior probabilidade de reversão)
    """)
st.markdown("---")

if st.button("🔄 Atualizar Análise", type="primary"):
    with st.spinner("Baixando dados..."):
        lista_bdrs=list(NOMES_BDRS.keys())
        df=buscar_dados(lista_bdrs)
        if df.empty: st.error("Erro ao carregar dados. Tente novamente em alguns minutos."); st.stop()
    with st.spinner("Calculando indicadores..."):
        df_calc=calcular_indicadores(df)
    with st.spinner("Analisando oportunidades..."):
        oportunidades=analisar_oportunidades(df_calc,NOMES_BDRS)
        if oportunidades:
            st.session_state['oportunidades']=oportunidades
            st.session_state['df_calc']=df_calc

if 'oportunidades' in st.session_state and 'df_calc' in st.session_state:
    oportunidades=st.session_state['oportunidades']
    df_calc=st.session_state['df_calc']
    df_res=pd.DataFrame(oportunidades).sort_values('Queda_Dia')
    st.success(f"✅ {len(oportunidades)} oportunidades detectadas!")

    st.markdown('<h3 class="section-header">🎯 Filtros</h3>', unsafe_allow_html=True)
    cf1,cf2,cf3=st.columns(3)
    with cf1: f20=st.checkbox("📈 Acima da EMA20",value=False)
    with cf2: f50=st.checkbox("📊 Acima da EMA50",value=False)
    with cf3: f200=st.checkbox("📉 Acima da EMA200",value=False)
    st.markdown("**💧 Liquidez mínima:**")
    liq_min=st.slider("0=sem filtro | 10=máxima exigência",min_value=0,max_value=10,value=0,step=1)

    if f20 or f50 or f200 or liq_min>0:
        filtrado=[]
        for opp in oportunidades:
            t=opp['Ticker']
            try:
                dft=df_calc.xs(t,axis=1,level=1).dropna()
                if len(dft)<20: continue
                uc=dft['Close'].iloc[-1]; ok=True
                if f20: ok=ok and 'EMA20' in dft.columns and pd.notna(dft['EMA20'].iloc[-1]) and uc>dft['EMA20'].iloc[-1]
                if f50 and len(dft)>=50: ok=ok and 'EMA50' in dft.columns and pd.notna(dft['EMA50'].iloc[-1]) and uc>dft['EMA50'].iloc[-1]
                elif f50: ok=False
                if f200 and len(dft)>=50: ok=ok and 'EMA200' in dft.columns and pd.notna(dft['EMA200'].iloc[-1]) and uc>dft['EMA200'].iloc[-1]
                elif f200: ok=False
                if liq_min>0 and opp.get('Liquidez',0)<liq_min: ok=False
                if ok: filtrado.append(opp)
            except: continue
        if filtrado:
            df_res=pd.DataFrame(filtrado).sort_values('Queda_Dia')
            st.success(f"✅ {len(df_res)} BDRs após filtros")
        else:
            st.warning("⚠️ Nenhuma BDR passou em todos os filtros."); df_res=pd.DataFrame()

    if not df_res.empty:
        st.markdown('<h3 class="section-header">📊 Oportunidades</h3>', unsafe_allow_html=True)
        evento=st.dataframe(
            df_res.style.map(estilizar_potencial,subset=['Potencial'])
                       .map(estilizar_is,subset=['IS'])
                       .map(estilizar_liquidez,subset=['Liquidez'])
            .format({'Preco':'R$ {:.2f}','Volume':'{:,.0f}','Queda_Dia':'{:.2f}%',
                     'Gap':'{:.2f}%','IS':'{:.0f}','RSI14':'{:.0f}','Stoch':'{:.0f}','Liquidez':'{:.0f}'}),
            column_order=("Ticker","Empresa","Liquidez","Preco","Queda_Dia","IS","Volume","Gap","Potencial","Score","Sinais"),
            column_config={
                "Empresa":st.column_config.TextColumn("Empresa",width="medium"),
                "Liquidez":st.column_config.NumberColumn("💧 Liq.",width="small"),
                "IS":st.column_config.NumberColumn("I.S."),
                "Score":st.column_config.ProgressColumn("Força",format="%d",min_value=0,max_value=10),
                "Sinais":st.column_config.TextColumn("Sinais",width="large"),
            },
            use_container_width=True,hide_index=True,on_select="rerun",selection_mode="single-row"
        )

        if evento.selection and evento.selection.rows:
            st.markdown("---")
            row=df_res.iloc[evento.selection.rows[0]]
            ticker=row['Ticker']
            st.markdown(f'<h3 class="section-header">📈 Análise: {ticker} — {row["Empresa"]}</h3>',
                        unsafe_allow_html=True)
            try:
                df_ticker=df_calc.xs(ticker,axis=1,level=1).dropna()
                col1,col2=st.columns([3,1])
                with col1:
                    # ★ GRÁFICO AVANÇADO ★
                    renderizar_grafico_com_controles(df_ticker,ticker,row['Empresa'],row['RSI14'],row['IS'])
                with col2:
                    pt=row['Potencial']
                    if "Alta" in pt: cbg="linear-gradient(135deg,#d4fc79,#96e6a1)"; ct="#166534"; ic="🟢"
                    elif "Média" in pt: cbg="linear-gradient(135deg,#ffeaa7,#fdcb6e)"; ct="#7c3626"; ic="🟡"
                    else: cbg="linear-gradient(135deg,#dfe6e9,#b2bec3)"; ct="#2d3436"; ic="⚪"
                    st.markdown(f"<div style='background:{cbg};padding:1rem;border-radius:8px;margin-bottom:1rem;'><h2 style='margin:0;color:{ct};text-align:center;'>{ic} {pt}</h2></div>",unsafe_allow_html=True)
                    st.metric("💰 Preço",f"R$ {row['Preco']:.2f}")
                    st.metric("📉 Queda",f"{row['Queda_Dia']:.2f}%",delta_color="inverse")
                    st.metric("🎯 I.S.",f"{row['IS']:.0f}/100")
                    if row['Gap']<-1: st.metric("⚡ Gap",f"{row['Gap']:.2f}%",delta_color="inverse")
                    st.markdown(f"**⭐ Score:** {row['Score']}/10  \n**📊 Volume:** {row['Volume']:,.0f}")
                    st.markdown("<div style='background:#e0e7ff;padding:.75rem;border-radius:6px;margin-top:1rem;'><p style='margin:0;font-weight:600;color:#3730a3;font-size:.9rem;'>📋 Sinais</p></div>",unsafe_allow_html=True)
                    st.markdown(f"<p style='font-size:.85rem;color:#475569;'>{row['Sinais']}</p>",unsafe_allow_html=True)
                    if row.get('Explicacoes'):
                        st.markdown("<div style='background:#fef3c7;padding:.75rem;border-radius:6px;margin-top:1rem;'><p style='margin:0;font-weight:600;color:#92400e;font-size:.9rem;'>💡 O que significa?</p></div>",unsafe_allow_html=True)
                        for e in row['Explicacoes']: st.markdown(f"<p style='font-size:.82rem;color:#92400e;margin:.3rem 0;'>• {e}</p>",unsafe_allow_html=True)
            except Exception as e: st.error(f"❌ Erro ao carregar gráfico: {e}")

            # Triple Screen
            st.markdown("---")
            try:
                ts_res=analisar_triple_screen(df_calc.xs(ticker,axis=1,level=1).dropna())
            except: ts_res=None
            renderizar_triple_screen(ts_res,ticker,row['Empresa'])

            # Fundamentalista
            st.markdown("---")
            st.markdown('<h3 class="section-header">📊 Análise Fundamentalista</h3>',unsafe_allow_html=True)
            with st.spinner(f"Buscando dados de {ticker}..."):
                fd=buscar_dados_fundamentalistas(ticker)
            if fd:
                sc=fd['score']
                if sc>=80: cf="#d4fc79"; ct="#166534"; lb="EXCELENTE"
                elif sc>=65: cf="#a7f3d0"; ct="#065f46"; lb="BOM"
                elif sc>=50: cf="#fde047"; ct="#92400e"; lb="NEUTRO"
                elif sc>=35: cf="#fdcb6e"; ct="#7c3626"; lb="ATENÇÃO"
                else: cf="#ef5350"; ct="white"; lb="EVITAR"
                st.markdown(f"<div style='background:{cf};padding:1.5rem;border-radius:12px;margin-bottom:1.5rem;text-align:center;'><h1 style='margin:0;color:{ct};font-size:4rem;font-weight:900;'>{sc:.0f}%</h1><p style='margin:.5rem 0 0;color:{ct};font-size:1.5rem;font-weight:600;'>{lb}</p></div>",unsafe_allow_html=True)
                if 'BRAPI' in fd.get('fonte',''): st.info(f"📡 **Fonte:** {fd['fonte']} | **{fd.get('ticker_fonte',ticker)}**")
                else: st.success(f"📡 **Fonte:** {fd['fonte']} | Ticker US: **{fd.get('ticker_fonte',ticker)}**")
                mc1,mc2,mc3=st.columns(3)
                with mc1:
                    st.markdown("### 📈 Valuation")
                    st.metric("P/E Ratio",f"{fd['pe_ratio']:.2f}" if fd.get('pe_ratio') else "N/A")
                    mc=fd.get('market_cap')
                    st.metric("Market Cap",f"${mc/1e12:.2f}T" if mc and mc>1e12 else f"${mc/1e9:.1f}B" if mc else "N/A")
                with mc2:
                    st.markdown("### 💰 Rentabilidade")
                    dy=fd.get('dividend_yield')
                    st.metric("Dividend Yield",f"{dy*100:.2f}%" if dy else "N/A")
                    rg=fd.get('revenue_growth')
                    if rg: st.metric("Crescimento Receita",f"{rg*100:+.1f}%")
                    elif fd.get('volume_b3'): st.metric("Volume B3",f"{fd['volume_b3']:,.0f}")
                    else: st.metric("Crescimento Receita","N/A")
                with mc3:
                    st.markdown("### 🎯 Info")
                    rec=fd.get('recomendacao')
                    if rec:
                        rm={'strong_buy':('🟢 COMPRA FORTE','green'),'buy':('🟢 Compra','green'),
                            'hold':('🟡 Manter','orange'),'sell':('🔴 Venda','red'),'strong_sell':('🔴 VENDA FORTE','red')}
                        rt,rc=rm.get(rec,(rec.upper(),'gray'))
                        st.markdown(f"**Analistas:** <span style='color:{rc};font-weight:800;'>{rt}</span>",unsafe_allow_html=True)
                    st.markdown(f"**Setor:** {fd.get('setor','N/A')}")
            else:
                st.warning(f"⚠️ Dados fundamentalistas não disponíveis para {ticker}")

            # ML
            st.markdown("---")
            try:
                ml_res=prever_preco_ml(df_calc.xs(ticker,axis=1,level=1).dropna(),ticker,5)
            except: ml_res={'erro':'Não foi possível obter os dados.'}
            renderizar_painel_ml(ml_res,ticker,row['Empresa'],5)

            # Notícias
            st.markdown("---")
            st.markdown('<h3 class="section-header">📰 Últimas Notícias</h3>',unsafe_allow_html=True)
            tus=mapear_ticker_us(ticker)
            st.markdown(f"Buscando notícias para **{NOMES_BDRS.get(ticker,tus)}** (`{tus}`)")
            with st.spinner("Buscando e traduzindo..."):
                news=buscar_noticias_com_traducao(tus)
            if news:
                fontes=list(dict.fromkeys(n['fonte'] for n in news))
                st.caption(f"✅ {len(news)} notícias | {', '.join(fontes)} | 🌐 Traduzidas")
                nc1,nc2=st.columns(2); m=(len(news)+1)//2
                with nc1:
                    for n in news[:m]: st.markdown(_renderizar_card_noticia(n),unsafe_allow_html=True)
                with nc2:
                    for n in news[m:]: st.markdown(_renderizar_card_noticia(n),unsafe_allow_html=True)
                st.markdown("---"); lc=st.columns(4)
                with lc[0]: st.markdown(f"[📊 Yahoo](https://finance.yahoo.com/quote/{tus}/news/)")
                with lc[1]: st.markdown(f"[📈 Seeking Alpha](https://seekingalpha.com/symbol/{tus}/news)")
                with lc[2]: st.markdown(f"[🔍 Finviz](https://finviz.com/quote.ashx?t={tus})")
                with lc[3]: st.markdown(f"[🧙 GuruFocus](https://www.gurufocus.com/news/{tus})")
            else:
                st.warning(f"⚠️ Sem notícias para {tus}. Acesse diretamente as fontes acima.")
        else:
            st.markdown("""<div style='background:linear-gradient(135deg,#e0e7ff,#c7d2fe);padding:2rem;border-radius:8px;text-align:center;margin:2rem 0;'>
<p style='margin:0;color:#3730a3;font-size:1.1rem;font-weight:500;'>👆 Selecione uma BDR na tabela para ver a análise completa</p></div>""",unsafe_allow_html=True)
    else:
        st.markdown("""<div style='background:linear-gradient(135deg,#ffeaa7,#fdcb6e);padding:2rem;border-radius:8px;text-align:center;'>
<h3 style='margin:0;color:#7c3626;'>📊 Nenhuma oportunidade detectada</h3>
<p style='margin:.5rem 0 0;color:#7c3626;'>Ajuste os filtros ou aguarde novas oportunidades</p></div>""",unsafe_allow_html=True)

st.markdown("---")
st.markdown("""<div style='text-align:center;padding:2rem 0;color:#64748b;'>
<p style='margin:0;font-size:.9rem;'><strong>Monitor BDR - Swing Trade Pro</strong> | Python · yFinance · Streamlit</p>
<p style='margin:.5rem 0 0;font-size:.8rem;'>⚠️ Apenas para fins educacionais. Não constitui recomendação de investimento.</p>
</div>""", unsafe_allow_html=True)
