import requests
import sqlite3
import pandas as pd
import logging
from datetime import datetime
import time # Importante para reintentos

# --- 1. CONFIGURACIÓN DE LOGS (Auditoría) ---
# En producción, no usas 'print()'. Usas logging para saber qué pasó y cuándo falló.
# Esto crea un archivo 'crypto_etl.log' que podés revisar si algo sale mal en la nube.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crypto_etl.log"), # Guarda en archivo
        logging.StreamHandler() # Muestra en consola
    ]
)
logger = logging.getLogger(__name__)

# --- 2. CONFIGURACIÓN DE LA API Y DB ---
# Definimos constantes para que sea fácil cambiarlas después (ej: si agregas más monedas).
DB_NAME = 'crypto_data.db'
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
COINS_TO_TRACK = 'bitcoin,ethereum,solana,binancecoin,cardano'

class CryptoETL:
    """Clase profesional para gestionar el pipeline de datos de Criptomonedas."""

    def __init__(self, db_name=DB_NAME):
        self.db_name = db_name

    # --- 3. EXTRACCIÓN CON REINTENTOS (Robustez) ---
    def extract(self, retries=3):
        """Obtiene datos de la API de CoinGecko con lógica de reintentos."""
        params = {
            'vs_currency': 'usd',
            'ids': COINS_TO_TRACK,
            'order': 'market_cap_desc'
        }
        
        for attempt in range(retries):
            try:
                logger.info(f"Iniciando extracción (Intento {attempt+1}/{retries})...")
                # Ponemos un timeout corto (5 seg) para que el script no se cuelgue si la API está lenta.
                response = requests.get(API_URL, params=params, timeout=5)
                
                # Esto lanza un error si el código HTTP es 4xx o 5xx.
                response.raise_for_status() 
                
                logger.info("Extracción exitosa.")
                return response.json()
            
            except requests.exceptions.RequestException as e:
                logger.warning(f"Error en el intento {attempt+1}: {e}")
                if attempt < retries - 1:
                    logger.info("Esperando antes de reintentar...")
                    # Esperamos un poco más en cada intento (exponential backoff simple).
                    time.sleep(2 * (attempt + 1)) 
                else:
                    logger.error("Se agotaron los reintentos de extracción.")
                    return None

    # --- 4. TRANSFORMACIÓN CON LÓGICA DE NEGOCIO (Análisis) ---
    def transform(self, raw_data):
        """Limpia y enriquece los datos crudos."""
        if not raw_data: 
            return None
        
        try:
            logger.info("Iniciando transformación de datos...")
            # Seleccionamos solo las columnas útiles.
            df = pd.DataFrame(raw_data)[[
                'id', 'symbol', 'current_price', 'market_cap', 
                'total_volume', 'price_change_percentage_24h'
            ]]
            
            # Agregamos la marca de tiempo exacta de la ingesta.
            df['timestamp'] = datetime.now()
            
            # --- AGREGAMOS VALOR (Análisis) ---
            # Creamos una métrica de volatilidad rápida basada en el cambio de 24h.
            def classify_volatility(change):
                if change > 5: return 'High Positive'
                if change < -5: return 'High Negative'
                return 'Moderate'
                
            df['volatility_label'] = df['price_change_percentage_24h'].apply(classify_volatility)
            
            logger.info(f"Transformación completada ({len(df)} registros procesados).")
            return df
            
        except Exception as e:
            logger.error(f"Error crítico durante la transformación: {e}")
            return None

    # --- 5. CARGA SEGURA EN SQL (Integridad) ---
    def load(self, df):
        """Guarda el DataFrame en la base de datos SQLite."""
        if df is None or df.empty:
            logger.warning("No hay datos para cargar.")
            return

        try:
            logger.info(f"Iniciando carga en la base de datos {self.db_name}...")
            # Usamos el context manager 'with' para asegurar que la conexión se cierre.
            with sqlite3.connect(self.db_name) as conn:
                # 'append' para guardar el historial.
                df.to_sql('prices', conn, if_exists='append', index=False)
            
            logger.info("Carga exitosa. Datos persistidos.")
            
        except Exception as e:
            logger.error(f"Error crítico durante la carga en SQL: {e}")

# --- 6. PUNTO DE ENTRADA PRINCIPAL ---
if __name__ == "__main__":
    etl = CryptoETL()
    
    # Ejecutamos el pipeline completo
    data_cruda = etl.extract()
    if data_cruda:
        data_procesada = etl.transform(data_cruda)
        etl.load(data_procesada)