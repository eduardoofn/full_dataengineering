import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# ── CONEXÃO ──────────────────────────────────────────────────────────────────
engine_dm = create_engine(os.getenv("DASHBOARD_DATABASE_URL"))


def carregar_dados() -> pd.DataFrame:
    df = pd.read_sql("""
        SELECT id, ano, mes, qtde_vendida, valor_total_real, valor_total_esperado
        FROM public.vendas_ano_mes_eduardo
        ORDER BY ano, mes
    """, engine_dm)
    df["desvio"]    = df["valor_total_real"] - df["valor_total_esperado"]
    df["pct_ating"] = (df["valor_total_real"] / df["valor_total_esperado"] * 100).round(1)
    df["nome_mes"]  = df["mes"].map({
        1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
        7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
    })
    return df


def carregar_localidade() -> pd.DataFrame:
    df = pd.read_sql("""
        SELECT ano, sigla_estado, estado, cidade,
               qtde_vendida, valor_total_real, valor_total_esperado, pct_atingimento
        FROM public.vendas_localidade_eduardo
    """, engine_dm)
    return df


DF     = carregar_dados()
DF_LOC = carregar_localidade()

# ── PALETA PREMIUM ─────────────────────────────────────────────────────────────
BG       = "#080810"
SURFACE  = "#0D0D1C"
CARD     = "#111128"
CARD2    = "#13132E"
BORDER   = "#1E1E40"
BORDER2  = "#252550"
ORANGE   = "#FF6B2B"
ORANGE2  = "#FF9A5C"
ORANGE_G = "linear-gradient(135deg, #FF6B2B 0%, #FF9A5C 100%)"
WHITE    = "#F2F2FF"
MUTED    = "#6B6B9A"
MUTED2   = "#9090B8"
GREEN    = "#00E5A0"
GREEN2   = "#00C87A"
RED      = "#FF4566"
YELLOW   = "#FFD166"
BLUE     = "#4D7CFF"


# ── CSS GLOBAL ────────────────────────────────────────────────────────────────
GLOBAL_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800;900&family=Space+Mono:wght@400;700&display=swap');

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

body {
    background: #080810;
    background-image:
        radial-gradient(ellipse 80% 60% at 20% -10%, rgba(255,107,43,0.07) 0%, transparent 60%),
        radial-gradient(ellipse 60% 50% at 80% 110%, rgba(77,124,255,0.05) 0%, transparent 60%);
    background-attachment: fixed;
    font-family: 'Outfit', sans-serif;
    color: #F2F2FF;
    min-height: 100vh;
}

/* Scrollbar */
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: #080810; }
::-webkit-scrollbar-thumb { background: #FF6B2B; border-radius: 4px; }

/* Animações de entrada */
@keyframes fadeUp {
    from { opacity: 0; transform: translateY(20px); }
    to   { opacity: 1; transform: translateY(0); }
}
@keyframes fadeIn {
    from { opacity: 0; }
    to   { opacity: 1; }
}
@keyframes shimmer {
    0%   { background-position: -200% center; }
    100% { background-position:  200% center; }
}
@keyframes pulse {
    0%, 100% { opacity: 1; }
    50%       { opacity: 0.6; }
}

.kpi-card {
    animation: fadeUp 0.5s ease both;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}
.kpi-card:hover {
    transform: translateY(-3px);
    box-shadow: 0 12px 40px rgba(255,107,43,0.15) !important;
}
.kpi-card:nth-child(1) { animation-delay: 0.05s; }
.kpi-card:nth-child(2) { animation-delay: 0.10s; }
.kpi-card:nth-child(3) { animation-delay: 0.15s; }
.kpi-card:nth-child(4) { animation-delay: 0.20s; }
.kpi-card:nth-child(5) { animation-delay: 0.25s; }

.graph-card {
    animation: fadeIn 0.6s ease both;
    animation-delay: 0.3s;
    transition: border-color 0.2s ease;
}
.graph-card:hover { border-color: #252550 !important; }

/* Dropdown Dash override */
.Select-control {
    background: #111128 !important;
    border-color: #FF6B2B !important;
    border-radius: 24px !important;
}
.Select-menu-outer {
    background: #111128 !important;
    border-color: #1E1E40 !important;
    border-radius: 12px !important;
}
.Select-option { color: #F2F2FF !important; }
.Select-option:hover, .Select-option.is-focused {
    background: rgba(255,107,43,0.15) !important;
}
.Select-value-label { color: #F2F2FF !important; font-weight: 700 !important; }
.Select-arrow { border-top-color: #FF6B2B !important; }

/* Tag de badge */
.badge-live {
    display: inline-flex; align-items: center; gap: 5px;
    background: rgba(0,229,160,0.1); border: 1px solid rgba(0,229,160,0.3);
    border-radius: 20px; padding: 3px 10px;
    font-size: 10px; font-weight: 700; letter-spacing: 1px;
    text-transform: uppercase; color: #00E5A0;
}
.badge-live::before {
    content: '';
    width: 6px; height: 6px; border-radius: 50%;
    background: #00E5A0;
    animation: pulse 1.5s ease-in-out infinite;
}

/* Divisor decorativo */
.section-label {
    font-size: 9px; font-weight: 700; letter-spacing: 2.5px;
    text-transform: uppercase; color: #6B6B9A;
    display: flex; align-items: center; gap: 12px;
    margin-bottom: 12px;
}
.section-label::after {
    content: ''; flex: 1; height: 1px;
    background: linear-gradient(90deg, #1E1E40 0%, transparent 100%);
}
"""


# ── HELPERS ───────────────────────────────────────────────────────────────────
def fmt_brl(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"R$ {v/1_000_000:.2f}M"
    if abs(v) >= 1_000:
        return f"R$ {v/1_000:.1f}K"
    return f"R$ {v:,.2f}"


def plot_layout(height=260) -> dict:
    return dict(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="'Outfit', sans-serif", color=MUTED2, size=11),
        margin=dict(l=8, r=8, t=12, b=8),
        xaxis=dict(
            showgrid=False, zeroline=False,
            linecolor=BORDER2, linewidth=1,
            tickfont=dict(size=11, color=MUTED),
        ),
        yaxis=dict(
            gridcolor=BORDER, gridwidth=1,
            zeroline=False, linecolor=BORDER2,
            tickfont=dict(size=11, color=MUTED),
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)", orientation="h",
            y=1.18, x=0, font=dict(size=11, color=MUTED2),
        ),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor=CARD2, bordercolor=BORDER2,
            font=dict(color=WHITE, size=12, family="'Outfit', sans-serif"),
        ),
    )


def kpi_card(titulo, valor, sub, sub_cor, accent=ORANGE, icon=""):
    return html.Div(className="kpi-card", style={
        "background": f"linear-gradient(145deg, {CARD} 0%, {CARD2} 100%)",
        "border": f"1px solid {BORDER}",
        "borderRadius": "16px",
        "padding": "20px 18px 16px",
        "position": "relative",
        "overflow": "hidden",
    }, children=[
        # Brilho decorativo no canto
        html.Div(style={
            "position": "absolute", "top": "-30px", "right": "-30px",
            "width": "80px", "height": "80px", "borderRadius": "50%",
            "background": f"radial-gradient(circle, {accent}22 0%, transparent 70%)",
            "pointerEvents": "none",
        }),
        # Linha de acento no topo
        html.Div(style={
            "position": "absolute", "top": 0, "left": "20px", "right": "20px",
            "height": "2px", "borderRadius": "0 0 4px 4px",
            "background": f"linear-gradient(90deg, transparent, {accent}, transparent)",
        }),
        # Ícone + título
        html.Div(style={"display": "flex", "alignItems": "center", "gap": "8px", "marginBottom": "12px"}, children=[
            html.Span(icon, style={"fontSize": "14px"}) if icon else None,
            html.Span(titulo, style={
                "fontSize": "9px", "fontWeight": "700", "letterSpacing": "1.5px",
                "textTransform": "uppercase", "color": MUTED,
            }),
        ]),
        # Valor principal
        html.Div(valor, style={
            "fontFamily": "'Outfit', sans-serif",
            "fontSize": "24px", "fontWeight": "800",
            "color": WHITE, "lineHeight": "1.1",
            "letterSpacing": "-0.5px",
        }),
        # Subtítulo
        html.Div(style={
            "display": "flex", "alignItems": "center", "gap": "5px",
            "marginTop": "8px",
        }, children=[
            html.Div(style={
                "width": "6px", "height": "6px", "borderRadius": "50%",
                "background": sub_cor, "flexShrink": "0",
            }),
            html.Span(sub, style={
                "fontSize": "11px", "fontWeight": "500", "color": sub_cor,
            }),
        ]),
    ])


def graph_card(title, graph_id, badge=None):
    return html.Div(className="graph-card", style={
        "background": f"linear-gradient(160deg, {CARD} 0%, {CARD2} 100%)",
        "border": f"1px solid {BORDER}",
        "borderRadius": "16px",
        "padding": "22px 20px 16px",
        "position": "relative",
        "overflow": "hidden",
    }, children=[
        # Reflexo decorativo
        html.Div(style={
            "position": "absolute", "bottom": "-40px", "right": "-40px",
            "width": "120px", "height": "120px", "borderRadius": "50%",
            "background": "radial-gradient(circle, rgba(255,107,43,0.04) 0%, transparent 70%)",
            "pointerEvents": "none",
        }),
        # Header do card
        html.Div(style={
            "display": "flex", "alignItems": "center",
            "justifyContent": "space-between", "marginBottom": "16px",
        }, children=[
            html.Div(title, style={
                "fontSize": "9px", "fontWeight": "700", "letterSpacing": "1.8px",
                "textTransform": "uppercase", "color": MUTED,
            }),
            html.Div(badge, style={
                "fontSize": "9px", "fontWeight": "700", "letterSpacing": "1px",
                "textTransform": "uppercase", "color": MUTED,
                "background": BORDER, "borderRadius": "6px", "padding": "3px 8px",
            }) if badge else None,
        ]),
        dcc.Graph(id=graph_id, config={"displayModeBar": False}),
    ])


# ── APP ───────────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    title="Painel de Vendas · DC",
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800;900&family=Space+Mono:wght@400;700&display=swap"
    ],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

app.index_string = f"""
<!DOCTYPE html>
<html>
<head>
    {{%metas%}}
    <title>{{%title%}}</title>
    {{%favicon%}}
    {{%css%}}
    <style>{GLOBAL_CSS}</style>
</head>
<body>
    {{%app_entry%}}
    <footer>{{%config%}}{{%scripts%}}{{%renderer%}}</footer>
</body>
</html>
"""

app.layout = html.Div(style={
    "backgroundColor": BG, "minHeight": "100vh",
    "fontFamily": "'Outfit', sans-serif", "color": WHITE,
}, children=[

    # ── HEADER ────────────────────────────────────────────────────────────────
    html.Div(style={
        "background": f"linear-gradient(180deg, {SURFACE} 0%, rgba(13,13,28,0.95) 100%)",
        "backdropFilter": "blur(20px)",
        "borderBottom": f"1px solid {BORDER}",
        "padding": "0 36px",
        "height": "64px",
        "display": "flex",
        "alignItems": "center",
        "justifyContent": "space-between",
        "position": "sticky", "top": "0", "zIndex": "100",
    }, children=[

        # Logo + título
        html.Div(style={"display": "flex", "alignItems": "center", "gap": "16px"}, children=[
            html.Div(style={
                "width": "40px", "height": "40px",
                "background": ORANGE_G,
                "borderRadius": "10px",
                "display": "flex", "alignItems": "center", "justifyContent": "center",
                "fontFamily": "'Space Mono', monospace",
                "fontWeight": "700", "fontSize": "14px", "color": "#fff",
                "boxShadow": f"0 0 20px {ORANGE}44",
            }, children="DC"),
            html.Div(children=[
                html.Div(style={"display": "flex", "alignItems": "center", "gap": "10px"}, children=[
                    html.Span("Painel de Vendas", style={
                        "fontFamily": "'Outfit', sans-serif",
                        "fontWeight": "800", "fontSize": "17px", "color": WHITE,
                        "letterSpacing": "-0.3px",
                    }),
                    html.Span(className="badge-live", children="ao vivo"),
                ]),
                html.Div("Digital College · Data Mart", style={
                    "fontSize": "10px", "color": MUTED, "fontWeight": "400",
                    "letterSpacing": "0.3px",
                }),
            ]),
        ]),

        # Filtro de ano
        html.Div(style={"display": "flex", "alignItems": "center", "gap": "10px"}, children=[
            html.Span("Período", style={
                "fontSize": "9px", "fontWeight": "700",
                "letterSpacing": "1.5px", "textTransform": "uppercase", "color": MUTED,
            }),
            dcc.Dropdown(
                id="filtro-ano",
                options=[], value=None, clearable=False,
                style={
                    "width": "100px", "fontSize": "14px",
                    "fontFamily": "'Outfit', sans-serif", "fontWeight": "700",
                    "backgroundColor": CARD, "color": WHITE,
                    "border": f"1px solid {ORANGE}",
                    "borderRadius": "24px",
                },
            ),
        ]),
    ]),

    # ── CORPO ─────────────────────────────────────────────────────────────────
    html.Div(style={"padding": "28px 36px 40px"}, children=[

        # Label seção KPIs
        html.Div(className="section-label", children="Indicadores do Ano"),

        # 5 CARDS KPI
        html.Div(id="cards-kpi", style={
            "display": "grid",
            "gridTemplateColumns": "repeat(5, 1fr)",
            "gap": "12px",
            "marginBottom": "28px",
        }),

        # Label seção gráficos temporais
        html.Div(className="section-label", children="Evolução Temporal"),

        # LINHA 1: Receita mensal (2/3) + Atingimento (1/3)
        html.Div(style={
            "display": "grid", "gridTemplateColumns": "2fr 1fr",
            "gap": "12px", "marginBottom": "12px",
        }, children=[
            graph_card("Receita Real vs Esperada por Mês", "g-receita", badge="Barras"),
            graph_card("% Atingimento da Meta",            "g-ating",   badge="Mensal"),
        ]),

        # LINHA 2: Qtde vendida (1/2) + Desvio (1/2)
        html.Div(style={
            "display": "grid", "gridTemplateColumns": "1fr 1fr",
            "gap": "12px", "marginBottom": "28px",
        }, children=[
            graph_card("Quantidade Vendida por Mês", "g-qtde",   badge="Área"),
            graph_card("Desvio Real − Esperado",     "g-desvio", badge="Variação"),
        ]),

        # Label seção geolocalização
        html.Div(className="section-label", children="Análise Geográfica"),

        # LINHA 3: Estado + Cidade
        html.Div(style={
            "display": "grid", "gridTemplateColumns": "1fr 1fr",
            "gap": "12px",
        }, children=[
            graph_card("Receita Real por Estado · Top 10",       "g-estado", badge="Ranking"),
            graph_card("Receita Real vs Meta · Top 20 Cidades",  "g-cidade", badge="Comparativo"),
        ]),

        # Footer
        html.Div(style={
            "marginTop": "40px", "paddingTop": "20px",
            "borderTop": f"1px solid {BORDER}",
            "display": "flex", "justifyContent": "space-between", "alignItems": "center",
        }, children=[
            html.Span("Pipeline ETL · PostgreSQL → Redshift → HDFS → Data Mart", style={
                "fontSize": "10px", "color": MUTED, "fontFamily": "'Space Mono', monospace",
            }),
            html.Span("Eduardo · Digital College", style={
                "fontSize": "10px", "color": MUTED, "fontFamily": "'Space Mono', monospace",
            }),
        ]),
    ]),
])


# ── CALLBACKS ─────────────────────────────────────────────────────────────────

@app.callback(
    Output("filtro-ano", "options"),
    Output("filtro-ano", "value"),
    Input("filtro-ano", "id"),
)
def popular_anos(_):
    anos = sorted(DF["ano"].unique(), reverse=True)
    opts = [{"label": str(a), "value": a} for a in anos]
    return opts, anos[0]


@app.callback(
    Output("cards-kpi", "children"),
    Output("g-receita",  "figure"),
    Output("g-ating",    "figure"),
    Output("g-qtde",     "figure"),
    Output("g-desvio",   "figure"),
    Output("g-estado",   "figure"),
    Output("g-cidade",   "figure"),
    Input("filtro-ano",  "value"),
)
def atualizar(ano):
    if ano is None:
        raise dash.exceptions.PreventUpdate

    df     = DF[DF["ano"] == ano].copy()
    df_loc = DF_LOC[DF_LOC["ano"] == ano].copy()

    # ── KPIs ─────────────────────────────────────────────────────────────────
    rec_real  = df["valor_total_real"].sum()
    rec_esp   = df["valor_total_esperado"].sum()
    qtde_tot  = df["qtde_vendida"].sum()
    desvio_t  = rec_real - rec_esp
    pct_geral = rec_real / rec_esp * 100 if rec_esp else 0
    ticket    = rec_real / qtde_tot if qtde_tot else 0
    idx_best  = df["valor_total_real"].idxmax()
    melhor_m  = df.loc[idx_best, "nome_mes"]
    melhor_v  = df.loc[idx_best, "valor_total_real"]

    cards = [
        kpi_card("Receita Real", fmt_brl(rec_real),
                 f"{'▲' if desvio_t >= 0 else '▼'} {fmt_brl(abs(desvio_t))} vs meta",
                 GREEN if desvio_t >= 0 else RED,
                 accent=GREEN if desvio_t >= 0 else RED, icon="💰"),
        kpi_card("Meta Esperada", fmt_brl(rec_esp),
                 "acumulado anual", MUTED2, icon="🎯"),
        kpi_card("Qtde Vendida", f"{int(qtde_tot):,}".replace(",", "."),
                 f"ticket médio {fmt_brl(ticket)}", MUTED2, icon="📦"),
        kpi_card("Atingimento", f"{pct_geral:.1f}%",
                 "acima da meta" if pct_geral >= 100 else "abaixo da meta",
                 GREEN if pct_geral >= 100 else RED,
                 accent=GREEN if pct_geral >= 100 else RED, icon="📊"),
        kpi_card("Melhor Mês", melhor_m,
                 fmt_brl(melhor_v), ORANGE2, accent=ORANGE2, icon="🏆"),
    ]

    meses = df["nome_mes"].tolist()

    # ── G1: Receita Real vs Esperada ─────────────────────────────────────────
    fig_rec = go.Figure(layout=go.Layout(**plot_layout()))
    fig_rec.add_trace(go.Bar(
        x=meses, y=df["valor_total_esperado"], name="Esperado",
        marker_color=BORDER2, marker_line_width=0,
        hovertemplate="R$ %{y:,.0f}<extra>Esperado</extra>",
    ))
    fig_rec.add_trace(go.Bar(
        x=meses, y=df["valor_total_real"], name="Real",
        marker=dict(
            color=df["valor_total_real"],
            colorscale=[[0, ORANGE], [1, ORANGE2]],
            showscale=False,
        ),
        marker_line_width=0,
        hovertemplate="R$ %{y:,.0f}<extra>Real</extra>",
    ))
    fig_rec.update_layout(
        barmode="group", bargap=0.25, bargroupgap=0.05,
        yaxis=dict(tickformat=",.0f"),
    )

    # ── G2: % Atingimento ────────────────────────────────────────────────────
    cores_at = [GREEN if v >= 100 else YELLOW if v >= 85 else RED for v in df["pct_ating"]]
    fig_at = go.Figure(layout=go.Layout(**plot_layout()))
    fig_at.add_trace(go.Bar(
        x=meses, y=df["pct_ating"],
        marker_color=cores_at, marker_line_width=0,
        showlegend=False,
        text=[f"{v:.0f}%" for v in df["pct_ating"]],
        textposition="outside",
        textfont=dict(size=10, color=MUTED2, family="'Outfit', sans-serif"),
        hovertemplate="%{y:.1f}%<extra></extra>",
    ))
    fig_at.add_hline(
        y=100, line_dash="dot", line_color=MUTED, line_width=1,
        annotation_text="100%",
        annotation_font_size=9, annotation_font_color=MUTED,
    )
    fig_at.update_layout(yaxis=dict(ticksuffix="%"))

    # ── G3: Qtde vendida ──────────────────────────────────────────────────────
    fig_qt = go.Figure(layout=go.Layout(**plot_layout()))
    fig_qt.add_trace(go.Scatter(
        x=meses, y=df["qtde_vendida"],
        mode="lines+markers", name="Qtde",
        line=dict(color=ORANGE, width=2.5, shape="spline", smoothing=0.8),
        marker=dict(
            size=8, color=ORANGE2,
            line=dict(color=CARD, width=2.5),
        ),
        fill="tozeroy",
        fillcolor="rgba(255,107,43,0.08)",
        hovertemplate="%{y:,}<extra>Qtde vendida</extra>",
    ))
    fig_qt.update_layout(yaxis=dict(tickformat=","))

    # ── G4: Desvio ────────────────────────────────────────────────────────────
    cores_dev = [GREEN if v >= 0 else RED for v in df["desvio"]]
    fig_dev = go.Figure(layout=go.Layout(**plot_layout()))
    fig_dev.add_trace(go.Bar(
        x=meses, y=df["desvio"],
        marker_color=cores_dev, marker_line_width=0,
        marker_opacity=0.85,
        showlegend=False,
        text=[fmt_brl(v) for v in df["desvio"]],
        textposition="outside",
        textfont=dict(size=9, color=MUTED2, family="'Outfit', sans-serif"),
        hovertemplate="R$ %{y:,.0f}<extra>Desvio</extra>",
    ))
    fig_dev.add_hline(y=0, line_color=BORDER2, line_width=1.5)

    # ── G5: Ranking por Estado ────────────────────────────────────────────────
    if df_loc.empty:
        fig_est = go.Figure(layout=go.Layout(**plot_layout(height=300)))
        fig_est.add_annotation(
            text="Sem dados de localidade para este ano",
            xref="paper", yref="paper", x=0.5, y=0.5,
            showarrow=False, font=dict(color=MUTED, size=13),
        )
    else:
        top_estados = (
            df_loc.groupby("sigla_estado", as_index=False)
            .agg(
                valor_total_real=("valor_total_real", "sum"),
                valor_total_esperado=("valor_total_esperado", "sum"),
            )
            .nlargest(10, "valor_total_real")
            .sort_values("valor_total_real")
        )
        top_estados["pct"] = (
            top_estados["valor_total_real"] / top_estados["valor_total_esperado"] * 100
        ).round(1)

        # Gradiente de cores por valor
        n = len(top_estados)
        cores_est = [
            f"rgba(255,107,43,{0.45 + 0.55 * i / max(n-1, 1):.2f})"
            for i in range(n)
        ]

        fig_est = go.Figure(layout=go.Layout(**plot_layout(height=300)))
        fig_est.update_layout(
            xaxis=dict(
                showgrid=True, gridcolor=BORDER,
                tickformat=",.0f", tickfont=dict(size=10, color=MUTED),
            ),
            yaxis=dict(
                showgrid=False, tickfont=dict(size=12, color=WHITE, family="'Outfit', sans-serif"),
            ),
            margin=dict(l=20, r=70, t=12, b=8),
        )
        fig_est.add_trace(go.Bar(
            y=top_estados["sigla_estado"],
            x=top_estados["valor_total_real"],
            orientation="h",
            marker_color=cores_est,
            marker_line_width=0,
            showlegend=False,
            text=[
                f"  {fmt_brl(v)}  {p:.0f}%"
                for v, p in zip(top_estados["valor_total_real"], top_estados["pct"])
            ],
            textposition="outside",
            textfont=dict(size=9, color=MUTED2, family="'Outfit', sans-serif"),
            hovertemplate="%{y}: R$ %{x:,.0f}<extra>Receita Real</extra>",
        ))

    # ── G6: Atingimento por Cidade Top 20 ────────────────────────────────────
    if df_loc.empty:
        fig_cid = go.Figure(layout=go.Layout(**plot_layout(height=300)))
        fig_cid.add_annotation(
            text="Sem dados de localidade para este ano",
            xref="paper", yref="paper", x=0.5, y=0.5,
            showarrow=False, font=dict(color=MUTED, size=13),
        )
    else:
        top_cidades = (
            df_loc.groupby("cidade", as_index=False)
            .agg(
                valor_total_real=("valor_total_real", "sum"),
                valor_total_esperado=("valor_total_esperado", "sum"),
            )
            .nlargest(20, "valor_total_real")
            .sort_values("valor_total_real", ascending=False)
        )

        fig_cid = go.Figure(layout=go.Layout(**plot_layout(height=300)))
        fig_cid.add_trace(go.Bar(
            x=top_cidades["cidade"],
            y=top_cidades["valor_total_esperado"],
            name="Esperado",
            marker_color=BORDER2,
            marker_line_width=0,
            hovertemplate="R$ %{y:,.0f}<extra>Esperado</extra>",
        ))
        fig_cid.add_trace(go.Bar(
            x=top_cidades["cidade"],
            y=top_cidades["valor_total_real"],
            name="Real",
            marker=dict(
                color=ORANGE,
                opacity=0.9,
            ),
            marker_line_width=0,
            hovertemplate="R$ %{y:,.0f}<extra>Real</extra>",
        ))
        fig_cid.update_layout(
            barmode="group", bargap=0.18, bargroupgap=0.06,
            xaxis=dict(
                tickangle=-40,
                tickfont=dict(size=9, color=MUTED),
            ),
            yaxis=dict(tickformat=",.0f"),
        )
        fig_cid.add_hline(
            y=top_cidades["valor_total_esperado"].mean(),
            line_dash="dot", line_color=MUTED, line_width=1,
            annotation_text="média meta",
            annotation_font_size=9, annotation_font_color=MUTED,
        )

    return cards, fig_rec, fig_at, fig_qt, fig_dev, fig_est, fig_cid


# ── RUN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(
        debug=os.getenv("DASH_DEBUG", "false").lower() == "true",
        host=os.getenv("DASH_HOST", "0.0.0.0"),
        port=int(os.getenv("DASH_PORT", "8050")),
    )