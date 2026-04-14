import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect
import gc
import time # Para el sistema de reintentos

@st.cache_resource
def init_connection():
    try:
        uri = st.secrets["SUPABASE_URI"]
        if uri.startswith("postgres://"):
            uri = uri.replace("postgres://", "postgresql://", 1)
        if "sslmode" not in uri:
            uri += "?sslmode=require"
            
        # NUEVO: Ampliamos el tiempo de espera a 2 minutos (120000 ms) para evitar cortes
        connect_args = {'options': '-c statement_timeout=120000'}
        
        return create_engine(uri, pool_size=10, max_overflow=20, connect_args=connect_args)
    except Exception as e:
        st.error(f"⚠️ Error fatal de conexión a la Base de Datos: {e}")
        st.stop()

engine = init_connection()

@st.cache_data(ttl=600)
def cargar_tabla(nombre_tabla):
    try:
        # RADAR: Revisa si la tabla exacta existe ANTES de buscarla y estrellarse
        inspector = inspect(engine)
        if not inspector.has_table(nombre_tabla):
            return pd.DataFrame() # Si no existe, devuelve vacío en paz
        
        df = pd.read_sql_table(nombre_tabla, engine)
        df.columns = df.columns.astype(str).str.strip().str.lower()
        return df.astype(str)
    except Exception:
        return pd.DataFrame()

def guardar_tabla(df, nombre_tabla, reintentos=3):
    """Guarda la tabla con un sistema de reintentos en caso de bloqueos de tráfico en la nube"""
    for intento in range(reintentos):
        try:
            if df.empty:
                df.to_sql(nombre_tabla, engine, if_exists='replace', index=False)
                return
                
            # Borra y recrea la estructura primero
            df.head(0).to_sql(nombre_tabla, engine, if_exists='replace', index=False)
            
            # Sube los datos por bloques (chunks) para no asfixiar la memoria
            chunk_size = 1500
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                chunk.to_sql(nombre_tabla, engine, if_exists='append', index=False)
                
            st.cache_data.clear()
            return # Si termina todo bien, sale de la función
            
        except Exception as e:
            error_str = str(e).lower()
            # Si el error es por "Timeout" o "bloqueo", esperamos y reintentamos
            if "timeout" in error_str or "canceling statement" in error_str or "lock" in error_str:
                if intento < reintentos - 1: # Si aún nos quedan intentos
                    print(f"⏳ La Base de Datos está ocupada. Reintentando en 3 segundos... (Intento {intento + 1} de {reintentos})")
                    time.sleep(3)
                    continue
            # Si es otro error grave, o se acabaron los intentos, lo mostramos
            raise e