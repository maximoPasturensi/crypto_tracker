import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go
import plotly.express as px

# 1. Configuración de página (Look & Feel)
st.set_page_config(page_title="Crypto Engine Pro", layout="wide", initial_sidebar_state="expanded")

# Inyectamos un poco de CSS para que las métricas resalten
st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 24px; color: #00ffcc; }
    .main { background-color: #0e1117; }
    </style>
    """, unsafe_allow_html=True)

# 2. Conexión a la base de datos
def get_data():
    conn = sqlite3.connect('crypto_data.db')
    # Traemos todo el historial
    df = pd.read_sql("SELECT * FROM prices ORDER BY timestamp ASC", conn)
    conn.close()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

df = get_data()

# --- SIDEBAR ---
st.sidebar.title("🛠️ Panel de Control")
if not df.empty:
    available_coins = df['id'].unique()
    selected_coin = st.sidebar.selectbox("Selecciona una Cripto para detalle:", available_coins)
    
    # Filtro de volatilidad (usando la columna nueva que creamos en el ETL)
    st.sidebar.subheader("Filtros de Datos")
    show_high_vol = st.sidebar.checkbox("Mostrar solo volatilidad Alta")

# --- CUERPO PRINCIPAL ---
st.title("📊 Crypto Data Engineering Dashboard")
st.markdown(f"Última actualización: `{df['timestamp'].max()}`")

if not df.empty:
    # Filtramos datos para la moneda seleccionada
    coin_df = df[df['id'] == selected_coin]
    
    # 3. KPIs Superiores
    last_entry = coin_df.iloc[-1]
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric("Precio Actual", f"${last_entry['current_price']:,.2f}")
    col2.metric("Variación 24h", f"{last_entry['price_change_percentage_24h']:.2f}%")
    col3.metric("Volatilidad", last_entry['volatility_label'])
    col4.metric("Volumen Total", f"${last_entry['total_volume']:,.0f}")

    st.divider()

    # 4. GRÁFICO DE VELAS (Candlestick) - Nivel Senior
    # Como solo tenemos puntos de precio por tiempo, simulamos las velas 
    # (En un proyecto real usarías Open, High, Low, Close)
    st.subheader(f"📈 Análisis Técnico: {selected_coin.upper()}")
    
    fig = go.Figure(data=[go.Candlestick(
        x=coin_df['timestamp'],
        open=coin_df['current_price'], # Simplificado para este ejemplo
        high=coin_df['current_price'] * 1.002, 
        low=coin_df['current_price'] * 0.998,
        close=coin_df['current_price'],
        name="Market Price"
    )])
    
    fig.update_layout(template="plotly_dark", xaxis_rangeslider_visible=False, height=450)
    st.plotly_chart(fig, use_container_width=True)

    # 5. COMPARATIVA DE MERCADO
    st.subheader("🚀 Comparativa de Market Cap")
    latest_all = df.sort_values('timestamp').groupby('id').last().reset_index()
    fig_bar = px.bar(latest_all, x='id', y='market_cap', color='volatility_label', 
                     title="Capitalización por Moneda y Riesgo",
                     template="plotly_dark")
    st.plotly_chart(fig_bar, use_container_width=True)

else:
    st.error("No se encontraron datos. Por favor, ejecutá 'python3 etl_script.py' primero.")

st.sidebar.info("Proyecto desarrollado con Python, SQL y Streamlit.")