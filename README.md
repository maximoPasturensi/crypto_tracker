# 🚀 Crypto Data Pipeline & Dashboard

Pipeline de datos automatizado que extrae, transforma y visualiza precios de criptomonedas en tiempo real.

## 🛠️ Stack Tecnológico
- **Lenguaje:** Python (Pandas, Requests, Plotly)
- **Base de Datos:** SQL (SQLite)
- **Infraestructura:** GitHub Actions (CI/CD)
- **Visualización:** Streamlit Cloud

## ⚙️ Arquitectura del Proyecto
1. **Extraction:** Script ETL que consume la API de CoinGecko con lógica de reintentos.
2. **Transformation:** Limpieza de datos y cálculo de etiquetas de volatilidad.
3. **Loading:** Persistencia en base de datos SQL local.
4. **Automation:** Workflow de GitHub Actions que ejecuta el pipeline cada hora.

---
Desarrollado por Maximo Pasturensi - Estudiante de Analista Programador & Data Engineer.
