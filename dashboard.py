import streamlit as st
import pandas as pd
from PIL import Image
import io
import plotly.express as px  
import datetime
import gc 

# IMPORTAMOS NUESTROS NUEVOS MÓDULOS
from database import cargar_tabla, guardar_tabla
from whatsapp_module import enviar_mensajes_agenda

# ==========================================
# 0. CONSTANTES Y CONFIGURACIÓN
# ==========================================
st.set_page_config(layout="wide", page_title="Panel Certi-Redes", page_icon="✅")
pd.set_option("styler.render.max_elements", 5000000)

TABLA_BASE = 'base_general'
TABLA_HISTORIAL = 'historial_certiredes'
TABLA_INSPECTORES = 'directorio_inspectores'

# ==========================================
# 1. MOTOR DE PROCESAMIENTO (Bases de datos)
# ==========================================
def normalizar_columnas(df):
    df.columns = df.columns.astype(str).str.strip().str.lower()
    mapeo = {
        'orden': 'orden', 'contrato': 'contrato', 'nombre': 'nombre', 
        'dirección': 'direccion', 'direccion': 'direccion', 'telefono': 'telefono',
        'fecha programación': 'fecha_programacion', 'fecha programacion': 'fecha_programacion',
        'jornada': 'jornada', 'tipo orden': 'tipo_orden', 'tipo trabajo': 'tipo_trabajo', 
        'fecha asignación': 'fecha_asignacion', 'fecha asignacion': 'fecha_asignacion', 
        '# vne': 'num_vne', 'consumo': 'consumo', 'meses': 'meses',
        'cabecera': 'municipio', 'cabeceras': 'municipio',
        'nombre técnico': 'inspector', 'estado gestión': 'estado_ejecucion',
        'codigo tecnico': 'codigo_tecnico', 'código técnico': 'codigo_tecnico'
    }
    df.rename(columns=mapeo, inplace=True)
    return df.loc[:, ~df.columns.duplicated()]

def procesar_nuevas_bases(archivos_subidos):
    try:
        nuevos_registros = []
        for archivo in archivos_subidos:
            if archivo.name.lower().endswith('.csv'):
                contenido = archivo.getvalue().decode('utf-8-sig', errors='replace')
                sep = ';' if ';' in contenido.split('\n')[0] else ','
                df_temp = pd.read_csv(io.StringIO(contenido), sep=sep, low_memory=False)
                columnas_test = df_temp.columns.astype(str).str.strip().str.lower()
                if 'orden' not in columnas_test and 'contrato' not in columnas_test:
                    archivo.seek(0)
                    df_temp = pd.read_csv(io.StringIO(contenido), header=4, sep=sep, low_memory=False)
            else:
                df_temp = pd.read_excel(archivo, sheet_name='Coordinación', header=4, engine='openpyxl')
                
            df_temp = normalizar_columnas(df_temp)
            if 'orden' in df_temp.columns and 'contrato' in df_temp.columns:
                df_temp['orden'] = df_temp['orden'].astype(str).str.replace('.0', '', regex=False).str.strip()
                df_temp['contrato'] = df_temp['contrato'].astype(str).str.replace('.0', '', regex=False).str.strip()
                # Filtro Cazafantasmas
                df_temp = df_temp[~df_temp['orden'].isin(['nan', 'None', '', 'NaT', '<NA>', 'null'])]
                nuevos_registros.append(df_temp)
            del df_temp 
            gc.collect()

        if nuevos_registros:
            df_nuevos = pd.concat(nuevos_registros, ignore_index=True)
            df_hist = cargar_tabla(TABLA_HISTORIAL)
            if not df_hist.empty and 'orden' in df_hist.columns:
                df_nuevos = df_nuevos[~df_nuevos['orden'].isin(df_hist['orden'].astype(str).tolist())]
            
            if df_nuevos.empty: return "Las órdenes cargadas ya están cerradas/archivadas."

            if 'fecha_programacion' in df_nuevos.columns:
                df_nuevos['fecha_prog_limpia'] = pd.to_datetime(df_nuevos['fecha_programacion'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
            if 'fecha_asignacion' in df_nuevos.columns:
                df_nuevos['fecha_asignacion'] = pd.to_datetime(df_nuevos['fecha_asignacion'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
                
            for col in ['estado_whatsapp', 'estado_ejecucion', 'num_vne', 'municipio', 'estado_visita', 'codigo_tecnico']:
                if col not in df_nuevos.columns:
                    df_nuevos[col] = 0 if col == 'num_vne' else 'SIN DEFINIR' if col in ['municipio', 'codigo_tecnico'] else 'Pendiente' if col != 'estado_visita' else '⏳ Esperando'
            
            if 'meses' in df_nuevos.columns: df_nuevos['meses'] = df_nuevos['meses'].astype(str).str.replace('.0', '', regex=False).str.strip()
                    
            df_base = cargar_tabla(TABLA_BASE)
            if not df_base.empty and 'orden' in df_base.columns:
                df_nuevos = df_nuevos.set_index('orden')
                df_base_index = df_base.set_index('orden')
                cols_exist = [c for c in ['estado_whatsapp', 'estado_ejecucion', 'num_vne', 'estado_visita'] if c in df_base_index.columns and c in df_nuevos.columns]
                
                df_nuevos[cols_exist] = df_nuevos[cols_exist].astype(str)
                df_base_index[cols_exist] = df_base_index[cols_exist].astype(str).replace(['None', 'nan', '<NA>'], 'Pendiente')
                df_nuevos.update(df_base_index[cols_exist])
                
                df_consolidado = pd.concat([df_base[~df_base['orden'].isin(df_nuevos.reset_index()['orden'])], df_nuevos.reset_index()], ignore_index=True)
            else:
                df_consolidado = df_nuevos
                
            guardar_tabla(df_consolidado, TABLA_BASE)
            return True
        return False
    except Exception as e:
        return f"Error fatal: {str(e)}"

# ==========================================
# 2. INTERFAZ GRÁFICA (UI)
# ==========================================
def centrar_df(df_o_styler):
    if isinstance(df_o_styler, pd.DataFrame):
        styler = df_o_styler.fillna('').astype(str).style
    else:
        styler = df_o_styler.format(lambda x: str(x) if pd.notnull(x) else '')
    return styler.set_properties(**{'text-align': 'center !important'}).set_table_styles([
        {'selector': 'th', 'props': [('text-align', 'center !important')]},
        {'selector': 'td', 'props': [('text-align', 'center !important')]}
    ])

with st.sidebar:
    try: st.image(Image.open('Logo_CertiRedes_Transparente.png'), width=200)
    except: st.write("### CERTI-REDES")
    st.markdown("🟢 **Nube Activa**")
    st.markdown("---")
    
    st.markdown("### 📥 1. ALIMENTAR BASE GENERAL")
    archivos_bases = st.file_uploader("Bases diarias (.csv, .xlsx)", accept_multiple_files=True)
    if archivos_bases and st.button("🚀 Procesar", use_container_width=True):
        with st.spinner("Limpiando y subiendo..."):
            res = procesar_nuevas_bases(archivos_bases)
            if res is True:
                st.success("¡Base actualizada!")
                st.rerun()
            else:
                st.error(res)

df_activa = cargar_tabla(TABLA_BASE)
st.title("🚀 Panel Certi-Redes (Cloud)")

if df_activa.empty:
    st.warning("⚠️ La base de datos está vacía. Cargue archivos.")
else:
    t_wa, t_op, t_ans, t_hist, t_insp = st.tabs(["💬 WhatsApp", "📊 Monitor", "⏱️ ANS", "📦 Historial", "⚙️ Inspectores"])

    with t_wa:
        st.write("### 📅 Agenda y Envíos Twilio")
        f_str = st.date_input("Fecha de Programación:").strftime('%Y-%m-%d')
        
        if 'fecha_prog_limpia' in df_activa.columns:
            df_dia = df_activa[df_activa['fecha_prog_limpia'] == f_str]
            if not df_dia.empty:
                st.metric("Total Programados", len(df_dia))
                if st.button("📤 Enviar Mensajes a la Agenda", type="primary"):
                    with st.spinner("Conectando con Twilio..."):
                        exito, msj = enviar_mensajes_agenda(df_dia)
                        if exito:
                            st.success(msj)
                            st.rerun()
                        else:
                            st.error(msj)
                st.dataframe(centrar_df(df_dia), use_container_width=True)
            else:
                st.info("Sin agenda para este día.")

    with t_op:
        st.write("### 📊 Monitor Operativo")
        st.dataframe(centrar_df(df_activa), use_container_width=True)

    with t_ans:
        st.write("### ⏱️ Control ANS")
        if 'fecha_asignacion' in df_activa.columns:
            st.dataframe(centrar_df(df_activa[['orden', 'fecha_asignacion', 'tipo_orden', 'estado_ejecucion']]), use_container_width=True)

    with t_hist:
        st.write("### 📦 Historial")
        df_h = cargar_tabla(TABLA_HISTORIAL)
        if not df_h.empty: st.dataframe(centrar_df(df_h), use_container_width=True)

    with t_insp:
        st.write("### ⚙️ Directorio de Inspectores")
        df_insp = cargar_tabla(TABLA_INSPECTORES)
        c1, c2 = st.columns([1, 2])
        with c1:
            with st.form("f_insp"):
                f_cod = st.text_input("Código Técnico (Ej: 321)")
                f_ced, f_nom, f_cel = st.text_input("Cédula"), st.text_input("Nombre"), st.text_input("Celular (Sin +57)")
                if st.form_submit_button("Guardar"):
                    nuevo = pd.DataFrame([{'codigo_tecnico': f_cod.strip(), 'cedula': f_ced.strip(), 'nombre': f_nom.strip(), 'celular': f_cel.strip()}])
                    if not df_insp.empty:
                        df_final = pd.concat([df_insp[df_insp['codigo_tecnico'] != f_cod.strip()], nuevo], ignore_index=True)
                    else: df_final = nuevo
                    guardar_tabla(df_final, TABLA_INSPECTORES)
                    st.rerun()
        with c2:
            if not df_insp.empty: st.dataframe(centrar_df(df_insp), use_container_width=True)