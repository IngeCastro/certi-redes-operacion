import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect
import gc
import time # Para el sistema de reintentos

# =====================================================================
# 🎚️ SWITCH DE ENTORNO (PRODUCCIÓN vs PRUEBAS)
# =====================================================================
# Cambie esto a True cuando esté desarrollando en su PC para no dañar la BD real.
# CAMBIELO A False ANTES DE SINCRONIZAR A GITHUB (Para que la web use la real).
MODO_PRUEBA = False 
# =====================================================================

@st.cache_resource
def init_connection():
    try:
        uri = st.secrets["SUPABASE_URI"]
        if uri.startswith("postgres://"):
            uri = uri.replace("postgres://", "postgresql://", 1)
        if "sslmode" not in uri:
            uri += "?sslmode=require"
            
        connect_args = {'options': '-c statement_timeout=120000'}
        
        # NUEVO: Agregamos pool_pre_ping y pool_recycle como "despertadores" de conexión
        return create_engine(
            uri, 
            pool_size=10, 
            max_overflow=20, 
            connect_args=connect_args,
            pool_pre_ping=True,  # Verifica si la conexión está viva antes de usarla
            pool_recycle=1800    # Recicla las conexiones viejas cada 30 minutos
        )
    except Exception as e:
        st.error(f"⚠️ Error fatal de conexión a la Base de Datos: {e}")
        st.stop()

engine = init_connection()

# --- Modificador de Nombres de Tabla ---
def obtener_nombre_tabla(nombre_original):
    """Si está en MODO_PRUEBA, le agrega el sufijo '_test' a las tablas."""
    if MODO_PRUEBA:
        return f"{nombre_original}_test"
    return nombre_original

@st.cache_data(ttl=600)
def cargar_tabla(nombre_tabla):
    tabla_real = obtener_nombre_tabla(nombre_tabla)
    try:
        inspector = inspect(engine)
        if not inspector.has_table(tabla_real):
            return pd.DataFrame() 
        
        df = pd.read_sql_table(tabla_real, engine)
        df.columns = df.columns.astype(str).str.strip().str.lower()
        return df.astype(str)
    except Exception as e:
        # NUEVO: Ahora mostrará si hay un error real en vez de ocultarlo y fingir que está vacía
        print(f"❌ Error interno al cargar {tabla_real}: {e}")
        st.error(f"⚠️ Error al conectar con la Nube. La tabla '{tabla_real}' no respondió. Intente recargar la página (F5).")
        return pd.DataFrame()

def guardar_tabla(df, nombre_tabla, reintentos=3):
    tabla_real = obtener_nombre_tabla(nombre_tabla)
    for intento in range(reintentos):
        try:
            if df.empty:
                df.to_sql(tabla_real, engine, if_exists='replace', index=False)
                return
                
            df.head(0).to_sql(tabla_real, engine, if_exists='replace', index=False)
            
            chunk_size = 1500
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                chunk.to_sql(tabla_real, engine, if_exists='append', index=False)
                
            st.cache_data.clear()
            return 
            
        except Exception as e:
            error_str = str(e).lower()
            if "timeout" in error_str or "canceling statement" in error_str or "lock" in error_str:
                if intento < reintentos - 1:
                    print(f"⏳ La Base de Datos está ocupada. Reintentando en 3 segundos... (Intento {intento + 1} de {reintentos})")
                    time.sleep(3)
                    continue
            raise e