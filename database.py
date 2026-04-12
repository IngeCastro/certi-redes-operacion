import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect
import gc

@st.cache_resource
def init_connection():
    try:
        uri = st.secrets["SUPABASE_URI"]
        if uri.startswith("postgres://"):
            uri = uri.replace("postgres://", "postgresql://", 1)
        if "sslmode" not in uri:
            uri += "?sslmode=require"
        return create_engine(uri, pool_size=10, max_overflow=20)
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
            return pd.DataFrame() # Si no existe (o hay error de mayúsculas), devuelve vacío en paz
        
        df = pd.read_sql_table(nombre_tabla, engine)
        df.columns = df.columns.astype(str).str.strip().str.lower()
        return df.astype(str)
    except Exception:
        return pd.DataFrame()

def guardar_tabla(df, nombre_tabla):
    try:
        if df.empty:
            df.to_sql(nombre_tabla, engine, if_exists='replace', index=False)
            return
        df.head(0).to_sql(nombre_tabla, engine, if_exists='replace', index=False)
        chunk_size = 1500
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            chunk.to_sql(nombre_tabla, engine, if_exists='append', index=False)
        st.cache_data.clear()
    except Exception as e:
        raise e