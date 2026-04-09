import streamlit as st
import pandas as pd
from PIL import Image
import io
import plotly.express as px  
import json
from twilio.rest import Client
import datetime
import os

# ==========================================
# 1. CONFIGURACIÓN MAESTRA Y CREDENCIALES
# ==========================================
st.set_page_config(layout="wide", page_title="Panel Certi-Redes", page_icon="✅")

# Credenciales de Twilio
ACCOUNT_SID = 'ACe411b7d301357600771550712214d873'
AUTH_TOKEN = 'dbd33bde262bb08538309c92676c697a' 
CONTENT_SID = 'HX8a0789521437fb76f489c025a2be5513' # SID V11 Aprobado
NUMERO_TWILIO = 'whatsapp:+15559416718' 

# Memoria de estado
if 'filtro_rapido_tipo' not in st.session_state:
    st.session_state.filtro_rapido_tipo = None
if 'filtro_rapido_valor' not in st.session_state:
    st.session_state.filtro_rapido_valor = None
if 'ultimo_archivo_ejec' not in st.session_state:
    st.session_state.ultimo_archivo_ejec = None

def aplicar_filtro_rapido(tipo, valor):
    st.session_state.filtro_rapido_tipo = tipo
    st.session_state.filtro_rapido_valor = valor

# ==========================================
# 2. CARGA DINÁMICA DE AGENDA (MODO NUBE)
# ==========================================
with st.sidebar:
    try:
        st.image(Image.open('Logo_CertiRedes_Transparente.png'), width=200)
    except:
        st.write("### CERTI-REDES S.A.S")
    
    st.markdown("---")
    st.markdown("### 📂 CARGAR BASE DE DATOS")
    subida_agenda = st.file_uploader("Subir Agenda (.xlsm / .xlsx)", type=["xlsm", "xlsx"])

# Lógica de carga: Prioriza el archivo subido, si no, usa el de GitHub
df_agenda = pd.DataFrame()
try:
    fuente_agenda = subida_agenda if subida_agenda is not None else 'agenda.xlsm'
    df_agenda = pd.read_excel(fuente_agenda, sheet_name='base', engine='openpyxl')
    df_agenda.columns = df_agenda.columns.str.strip().str.lower()
    df_agenda['contrato'] = df_agenda['contrato'].astype(str).str.split('.').str[0].str.strip()
    # Limpieza robusta de fecha
    df_agenda['fecha_dt'] = pd.to_datetime(df_agenda['fecha'], dayfirst=True, errors='coerce')
    df_agenda['fecha_limpia'] = df_agenda['fecha_dt'].dt.strftime('%d/%m/%Y')
    
    # Motor de Auditoría de Jornada
    df_agenda['hora'] = df_agenda['hora'].fillna('Sin hora').astype(str).str.strip().str.upper()
    def sacar_jornada(h):
        if 'AM' in h or 'A.M' in h: return 'AM'
        if 'PM' in h or 'P.M' in h: return 'PM'
        try:
            hr = int(str(h).split(':')[0])
            return 'PM' if (hr >= 12 and hr != 24) else 'AM'
        except: return 'Sin Jornada'
    df_agenda['jornada'] = df_agenda['hora'].apply(sacar_jornada)

    # Filtro de contratos ya archivados
    if os.path.exists('contratos_archivados.csv'):
        df_archivados = pd.read_csv('contratos_archivados.csv', dtype=str)
        df_agenda = df_agenda[~df_agenda['contrato'].isin(df_archivados['contrato'])]
except Exception as e:
    st.sidebar.error(f"Error al cargar agenda: {e}")

# ==========================================
# 3. LEER RESPUESTAS (LOG WHATSAPP)
# ==========================================
logs = []
if os.path.exists('log_certiredes.txt'):
    try:
        with open('log_certiredes.txt', 'r', encoding='latin-1', errors='ignore') as f:
            for linea in f:
                if " - Contrato: " in linea:
                    parte_fecha_estado, resto = linea.split(" - Contrato: ")
                    contrato_extraido, _ = resto.split(" - Inspector: ")
                    estado_crudo = parte_fecha_estado.split("] ")[1].strip()
                    fecha_hora = parte_fecha_estado.split("] ")[0].replace("[", "")
                    estado_final = "CONFIRMÓ" if "CONFIRM" in estado_crudo.upper() else "CANCELÓ"
                    logs.append({"fecha_respuesta": fecha_hora, "estado_visita": estado_final, "contrato": contrato_extraido.strip()})
    except: pass

df_logs = pd.DataFrame(logs)
if df_logs.empty:
    df_logs = pd.DataFrame(columns=["fecha_respuesta", "estado_visita", "contrato"])
else:
    df_logs = df_logs.drop_duplicates(subset=['contrato'], keep='last')

# Cruce inicial
if not df_agenda.empty:
    df_dashboard = pd.merge(df_agenda, df_logs, on='contrato', how='left')
    df_dashboard['estado_visita'] = df_dashboard['estado_visita'].fillna('⏳ Esperando')
    df_dashboard['fecha_respuesta'] = df_dashboard['fecha_respuesta'].fillna('-')
else:
    df_dashboard = pd.DataFrame()

# ==========================================
# 4. GESTIÓN DE EJECUCIÓN CAMPO
# ==========================================
with st.sidebar:
    st.markdown("---")
    st.markdown("### 🛠️ EJECUCIÓN CAMPO")
    archivo_ejecucion = st.file_uploader("Subir reporte GoDoWorks", type=["csv", "xlsx"])
    archivo_bd_ejecucion = 'bd_ejecucion.csv'
    
    if archivo_ejecucion is not None:
        file_id = archivo_ejecucion.name + str(archivo_ejecucion.size)
        if st.session_state.ultimo_archivo_ejec != file_id:
            try:
                if archivo_ejecucion.name.lower().endswith('.csv'):
                    contenido = archivo_ejecucion.getvalue().decode('utf-8-sig', errors='replace')
                    sep = ';' if ';' in contenido.split('\n')[0] else ','
                    df_ejec = pd.read_csv(io.StringIO(contenido), sep=sep, dtype=str)
                else:
                    df_ejec = pd.read_excel(archivo_ejecucion, dtype=str)
                
                df_ejec.columns = df_ejec.columns.str.strip().str.upper()
                if 'CONTRATO' in df_ejec.columns and 'ESTADO' in df_ejec.columns:
                    df_ejec['CONTRATO'] = df_ejec['CONTRATO'].astype(str).str.split('.').str[0].str.strip()
                    
                    def mapear_ejecucion(e):
                        e = str(e).strip().upper()
                        if e in ["CERTIFICADO", "NO CERTIFICADO", "EJECUTADA(CUMPLE)"]: return "✅ Cumplidas"
                        if e == "VISITA NO EFECTIVA": return "❌ No efectiva"
                        return "! Pendiente"
                    
                    df_ejec['estado_final'] = df_ejec['ESTADO'].apply(mapear_ejecucion)
                    df_nueva = df_ejec[['CONTRATO', 'ESTADO', 'estado_final']]
                    
                    if os.path.exists(archivo_bd_ejecucion):
                        df_hist = pd.read_csv(archivo_bd_ejecucion, dtype=str)
                        df_act = pd.concat([df_hist, df_nueva]).drop_duplicates(subset=['CONTRATO'], keep='last')
                    else: df_act = df_nueva
                    
                    df_act.to_csv(archivo_bd_ejecucion, index=False, encoding='utf-8-sig')
                    st.session_state.ultimo_archivo_ejec = file_id
            except Exception as e: st.error(f"Error GoDoWorks: {e}")

if not df_dashboard.empty and os.path.exists(archivo_bd_ejecucion):
    df_bd = pd.read_csv(archivo_bd_ejecucion, dtype=str)
    df_dashboard = pd.merge(df_dashboard, df_bd[['CONTRATO', 'estado_final']], left_on='contrato', right_on='CONTRATO', how='left')
    df_dashboard['estado_ejecucion'] = df_dashboard['estado_final'].fillna('! Pendiente')
elif not df_dashboard.empty:
    df_dashboard['estado_ejecucion'] = '! Pendiente'

# ==========================================
# 5. UI PRINCIPAL Y FILTROS
# ==========================================
st.title("🚀 Control de Operación en Tiempo Real")

if not df_dashboard.empty:
    # Filtros superiores
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: muni = st.selectbox("📍 Municipio", ["Todos"] + list(df_dashboard['ciudad'].unique()))
    with c2: insp = st.selectbox("👷 Inspector", ["Todos"] + list(df_dashboard['inspector'].unique()))
    with c3: est_w = st.selectbox("🚦 Respuesta WA", ["Todos", "CONFIRMÓ", "CANCELÓ", "⏳ Esperando"])
    with c4: est_e = st.selectbox("🛠️ Ejecución", ["Todos", "✅ Cumplidas", "❌ No efectiva", "! Pendiente"])
    with c5: jor = st.selectbox("⏰ Jornada", ["Todas", "AM", "PM", "Sin Jornada"])

    # Aplicación de filtros
    df_filtrado = df_dashboard.copy()
    if muni != "Todos": df_filtrado = df_filtrado[df_filtrado['ciudad'] == muni]
    if insp != "Todos": df_filtrado = df_filtrado[df_filtrado['inspector'] == insp]
    if est_w != "Todos": df_filtrado = df_filtrado[df_filtrado['estado_visita'] == est_w]
    if est_e != "Todos": df_filtrado = df_filtrado[df_filtrado['estado_ejecucion'] == est_e]
    if jor != "Todas": df_filtrado = df_filtrado[df_filtrado['jornada'] == jor]

    if st.session_state.filtro_rapido_tipo == 'whatsapp':
        df_filtrado = df_filtrado[df_filtrado['estado_visita'] == st.session_state.filtro_rapido_valor]
    elif st.session_state.filtro_rapido_tipo == 'ejecucion':
        df_filtrado = df_filtrado[df_filtrado['estado_ejecucion'] == st.session_state.filtro_rapido_valor]

    # --- SECCIÓN A: DETALLE ---
    st.markdown("---")
    col_tA, col_bA = st.columns([8, 2])
    with col_tA: st.write(f"#### 📋 Detalle de Órdenes ({len(df_filtrado)} registros)")
    
    cols_v = ['jornada', 'hora', 'fecha_respuesta', 'nombre', 'estado_visita', 'estado_ejecucion', 'ciudad', 'contrato', 'direccion', 'inspector']
    df_exp = df_filtrado[cols_v].copy()
    df_exp.columns = ['Jornada', 'Hora Prog.', 'Fecha Resp.', 'Cliente', 'WhatsApp', 'Ejecución', 'Ciudad', 'Contrato', 'Dirección', 'Inspector']

    # Auditoría de Retrasos Visual
    h_act = datetime.datetime.now().hour
    def eval_retraso(row):
        if row['Jornada'] == 'AM' and h_act >= 12 and 'Pendiente' in row['Ejecución']: return 'AM 🚨 RETRASO'
        return row['Jornada']
    df_exp['Jornada'] = df_exp.apply(eval_retraso, axis=1)

    with col_bA:
        buf = io.BytesIO()
        df_exp.to_excel(buf, index=False)
        st.download_button("📥 Descargar Excel", buf.getvalue(), "detalle.xlsx", use_container_width=True)

    def color_filas(row):
        est = [''] * len(row)
        if 'RETRASO' in str(row['Jornada']): est[row.index.get_loc('Jornada')] = 'background-color: #b71c1c; color: white;'
        if 'Cumplidas' in str(row['Ejecución']): est[row.index.get_loc('Ejecución')] = 'background-color: #e8f5e9; color: #2e7d32; font-weight: bold;'
        if 'CONFIRM' in str(row['WhatsApp']).upper(): est[row.index.get_loc('WhatsApp')] = 'color: #2e7d32; font-weight: bold;'
        return est

    st.dataframe(df_exp.style.apply(color_filas, axis=1), use_container_width=True, hide_index=True)

    # --- SECCIÓN B: RESUMEN INSPECTOR ---
    st.markdown("---")
    st.subheader("👷 Resumen por Inspector")
    res = df_filtrado.groupby(['inspector', 'estado_ejecucion']).size().unstack(fill_value=0)
    for c in ['! Pendiente', '✅ Cumplidas', '❌ No efectiva']:
        if c not in res.columns: res[c] = 0
    res['TOTAL'] = res['! Pendiente'] + res['✅ Cumplidas'] + res['❌ No efectiva']
    res = res[['! Pendiente', '✅ Cumplidas', '❌ No efectiva', 'TOTAL']].reset_index()
    res.columns = ['Inspector', 'Pendientes', 'Cumplidas', 'No Efectivas', 'Total']
    
    st.dataframe(res, use_container_width=True, hide_index=True)

else:
    st.info("👋 ¡Bienvenido! Por favor carga el archivo **agenda.xlsm** en el panel de la izquierda para comenzar.")

# --- BOTÓN DE ARCHIVADO (CORTE) ---
if not df_dashboard.empty:
    with st.sidebar:
        st.markdown("---")
        if st.button("📦 Finalizar y Archivar", type="primary", use_container_width=True):
            df_cerrar = df_dashboard[df_dashboard['estado_ejecucion'].isin(['✅ Cumplidas', '❌ No efectiva'])]
            if not df_cerrar.empty:
                nuevos = df_cerrar[['contrato']]
                if os.path.exists('contratos_archivados.csv'): nuevos.to_csv('contratos_archivados.csv', mode='a', header=False, index=False)
                else: nuevos.to_csv('contratos_archivados.csv', index=False)
                st.success("Corte realizado. Refresca la página.")
                st.rerun()