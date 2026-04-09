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
# 0. CONFIGURACIÓN DE REGLAS ANS (TIEMPOS)
# ==========================================
REGLAS_ANS = {
    'REVISION PERIODICA': 48, # horas
    'CERTIFICACION': 72,
    'RECONEXION': 24,
    'SUSPENSION': 24,
    'POR DEFECTO': 48
}

# ==========================================
# 1. CONFIGURACIÓN MAESTRA
# ==========================================
st.set_page_config(layout="wide", page_title="Panel Certi-Redes", page_icon="✅")

# Credenciales de Twilio
ACCOUNT_SID = 'ACe411b7d301357600771550712214d873'
AUTH_TOKEN = 'dbd33bde262bb08538309c92676c697a' 
CONTENT_SID = 'HX8a0789521437fb76f489c025a2be5513'
NUMERO_TWILIO = 'whatsapp:+15559416718' 

if 'ultimo_archivo_ejec' not in st.session_state:
    st.session_state.ultimo_archivo_ejec = None

# Memoria de estado para los botones laterales
if 'filtro_rapido_tipo' not in st.session_state:
    st.session_state.filtro_rapido_tipo = None
if 'filtro_rapido_valor' not in st.session_state:
    st.session_state.filtro_rapido_valor = None

def aplicar_filtro_rapido(tipo, valor):
    st.session_state.filtro_rapido_tipo = tipo
    st.session_state.filtro_rapido_valor = valor

# ==========================================
# 2. CARGA DE ARCHIVOS (AGENDA Y GODO)
# ==========================================
with st.sidebar:
    try:
        st.image(Image.open('Logo_CertiRedes_Transparente.png'), width=200)
    except:
        st.write("### CERTI-REDES S.A.S")
    
    st.markdown("---")
    st.markdown("### 📂 1. CARGAR AGENDA")
    subida_agenda = st.file_uploader("Subir Agenda (.xlsm / .xlsx)", type=["xlsm", "xlsx"])

    st.markdown("---")
    st.markdown("### 🛠️ 2. ACTUALIZAR GODO")
    subida_godo = st.file_uploader("Subir reporte GoDoWorks", type=["csv", "xlsx"], key="godo_asig")

# --- PROCESAR AGENDA ---
df_agenda = pd.DataFrame()
try:
    fuente_agenda = subida_agenda if subida_agenda is not None else 'agenda.xlsm'
    df_agenda = pd.read_excel(fuente_agenda, sheet_name='base', engine='openpyxl')
    df_agenda.columns = df_agenda.columns.str.strip().str.lower()
    df_agenda['contrato'] = df_agenda['contrato'].astype(str).str.split('.').str[0].str.strip()
    
    df_agenda['fecha_dt'] = pd.to_datetime(df_agenda['fecha'], dayfirst=True, errors='coerce')
    df_agenda['fecha_prog'] = df_agenda['fecha_dt'].dt.strftime('%d/%m/%Y')
    
    col_asig = 'fecha asignacion' if 'fecha asignacion' in df_agenda.columns else 'fecha'
    df_agenda['fecha_asig_dt'] = pd.to_datetime(df_agenda[col_asig], dayfirst=True, errors='coerce')
    
    df_agenda['hora'] = df_agenda['hora'].fillna('Sin hora').astype(str).str.strip().str.upper()
    def sacar_jornada(h):
        if 'AM' in h or 'A.M' in h: return 'AM'
        if 'PM' in h or 'P.M' in h: return 'PM'
        try:
            hr = int(str(h).split(':')[0])
            return 'PM' if (hr >= 12 and hr != 24) else 'AM'
        except: return 'Sin Jornada'
    df_agenda['jornada'] = df_agenda['hora'].apply(sacar_jornada)

    if os.path.exists('contratos_archivados.csv'):
        df_archivados = pd.read_csv('contratos_archivados.csv', dtype=str)
        df_agenda = df_agenda[~df_agenda['contrato'].isin(df_archivados['contrato'])]
except Exception as e:
    st.sidebar.error(f"Esperando carga de Agenda...")

# --- PROCESAR GODO (EJECUCIÓN) ---
archivo_bd_ejecucion = 'bd_ejecucion.csv'
if subida_godo is not None:
    file_id = subida_godo.name + str(subida_godo.size)
    if st.session_state.ultimo_archivo_ejec != file_id:
        try:
            if subida_godo.name.lower().endswith('.csv'):
                contenido = subida_godo.getvalue().decode('utf-8-sig', errors='replace')
                sep = ';' if ';' in contenido.split('\n')[0] else ','
                df_ejec = pd.read_csv(io.StringIO(contenido), sep=sep, dtype=str)
            else:
                df_ejec = pd.read_excel(subida_godo, dtype=str)
            
            df_ejec.columns = df_ejec.columns.str.strip().str.upper()
            if 'CONTRATO' in df_ejec.columns and 'ESTADO' in df_ejec.columns:
                df_ejec['CONTRATO'] = df_ejec['CONTRATO'].astype(str).str.split('.').str[0].str.strip()
                
                # Función mejorada para atrapar espacios extra en el estado de GoDoWorks
                def mapear_ejecucion(e):
                    e = str(e).strip().upper()
                    if e in ["CERTIFICADO", "NO CERTIFICADO", "EJECUTADA(CUMPLE)", "EJECUTADA (CUMPLE)", "EJECUTADA"]: return "✅ Cumplidas"
                    if e in ["VISITA NO EFECTIVA", "NO EFECTIVA"]: return "❌ No efectiva"
                    return "! Pendiente"
                
                df_ejec['estado_final'] = df_ejec['ESTADO'].apply(mapear_ejecucion)
                df_nueva = df_ejec[['CONTRATO', 'ESTADO', 'estado_final']]
                
                if os.path.exists(archivo_bd_ejecucion):
                    df_hist = pd.read_csv(archivo_bd_ejecucion, dtype=str)
                    df_act = pd.concat([df_hist, df_nueva]).drop_duplicates(subset=['CONTRATO'], keep='last')
                else: df_act = df_nueva
                
                df_act.to_csv(archivo_bd_ejecucion, index=False, encoding='utf-8-sig')
                st.session_state.ultimo_archivo_ejec = file_id
        except Exception as e: st.error(f"Error procesando GoDoWorks: {e}")

# --- LOGS WHATSAPP ---
logs = []
if os.path.exists('log_certiredes.txt'):
    try:
        with open('log_certiredes.txt', 'r', encoding='latin-1', errors='ignore') as f:
            for linea in f:
                if " - Contrato: " in linea:
                    p1, resto = linea.split(" - Contrato: ")
                    contrato_ex, _ = resto.split(" - Inspector: ")
                    logs.append({"estado_visita": "CONFIRMÓ" if "CONFIRM" in p1.upper() else "CANCELÓ", "contrato": contrato_ex.strip()})
    except: pass
df_logs = pd.DataFrame(logs).drop_duplicates(subset=['contrato'], keep='last') if logs else pd.DataFrame(columns=["estado_visita", "contrato"])

# ==========================================
# 3. CRUCE FINAL Y BOTONES LATERALES
# ==========================================
if not df_agenda.empty:
    df_dashboard = pd.merge(df_agenda, df_logs, on='contrato', how='left')
    df_dashboard['estado_visita'] = df_dashboard['estado_visita'].fillna('⏳ Esperando')
    
    if os.path.exists(archivo_bd_ejecucion):
        df_bd = pd.read_csv(archivo_bd_ejecucion, dtype=str)
        df_dashboard = pd.merge(df_dashboard, df_bd[['CONTRATO', 'estado_final']], left_on='contrato', right_on='CONTRATO', how='left')
        df_dashboard['estado_ejecucion'] = df_dashboard['estado_final'].fillna('! Pendiente')
    else:
        df_dashboard['estado_ejecucion'] = '! Pendiente'
        
    # BOTONES DE FILTRO RÁPIDO
    with st.sidebar:
        st.markdown("---")
        st.markdown("### 💬 3. RESUMEN WHATSAPP")
        c_conf = len(df_dashboard[df_dashboard['estado_visita'] == 'CONFIRMÓ'])
        c_canc = len(df_dashboard[df_dashboard['estado_visita'] == 'CANCELÓ'])
        c_all = len(df_dashboard)

        st.button(f"✅ Confirmados: {c_conf}", on_click=aplicar_filtro_rapido, args=('whatsapp', 'CONFIRMÓ'), use_container_width=True)
        st.button(f"❌ Cancelados: {c_canc}", on_click=aplicar_filtro_rapido, args=('whatsapp', 'CANCELÓ'), use_container_width=True)
        st.button(f"📋 Ver Todos: {c_all}", on_click=aplicar_filtro_rapido, args=(None, None), use_container_width=True)

        st.markdown("---")
        st.markdown("### 🛠️ 4. RESUMEN EJECUCIÓN")
        e_cump = len(df_dashboard[df_dashboard['estado_ejecucion'] == '✅ Cumplidas'])
        e_noef = len(df_dashboard[df_dashboard['estado_ejecucion'] == '❌ No efectiva'])
        e_pend = len(df_dashboard[df_dashboard['estado_ejecucion'] == '! Pendiente'])

        st.button(f"✅ Cumplidas: {e_cump}", on_click=aplicar_filtro_rapido, args=('ejecucion', '✅ Cumplidas'), use_container_width=True)
        st.button(f"❌ No Efectivas: {e_noef}", on_click=aplicar_filtro_rapido, args=('ejecucion', '❌ No efectiva'), use_container_width=True)
        st.button(f"❗ Pendientes: {e_pend}", on_click=aplicar_filtro_rapido, args=('ejecucion', '! Pendiente'), use_container_width=True)
else:
    df_dashboard = pd.DataFrame()

# ==========================================
# 4. INTERFAZ DE PESTAÑAS (TABS)
# ==========================================
st.title("🚀 Certi-Redes: Control Integral de Operación")

tab1, tab2 = st.tabs(["📊 Operación Diaria", "⏱️ Auditoría de Tiempos (ANS)"])

# ------------------------------------------
# TAB 1: OPERACIÓN DIARIA
# ------------------------------------------
with tab1:
    if not df_dashboard.empty:
        c1, c2, c3, c4 = st.columns(4)
        with c1: muni = st.selectbox("📍 Municipio", ["Todos"] + list(df_dashboard['ciudad'].unique()), key="muni_op")
        with c2: insp = st.selectbox("👷 Inspector", ["Todos"] + list(df_dashboard['inspector'].unique()), key="insp_op")
        with c3: est_e = st.selectbox("🛠️ Ejecución", ["Todos", "✅ Cumplidas", "❌ No efectiva", "! Pendiente"], key="ejec_op")
        with c4: jor = st.selectbox("⏰ Jornada", ["Todas", "AM", "PM"], key="jor_op")

        df_fil = df_dashboard.copy()
        
        # Filtros de listas desplegables
        if muni != "Todos": df_fil = df_fil[df_fil['ciudad'] == muni]
        if insp != "Todos": df_fil = df_fil[df_fil['inspector'] == insp]
        if est_e != "Todos": df_fil = df_fil[df_fil['estado_ejecucion'] == est_e]
        if jor != "Todas": df_fil = df_fil[df_fil['jornada'] == jor]

        # Aplicar Filtros Rápidos
        if st.session_state.filtro_rapido_tipo == 'whatsapp':
            df_fil = df_fil[df_fil['estado_visita'] == st.session_state.filtro_rapido_valor]
            st.info(f"💡 Mostrando solo: **{st.session_state.filtro_rapido_valor}** (WhatsApp). Haga clic en '📋 Ver Todos' en la barra lateral para quitar el filtro.")
        elif st.session_state.filtro_rapido_tipo == 'ejecucion':
            df_fil = df_fil[df_fil['estado_ejecucion'] == st.session_state.filtro_rapido_valor]
            st.info(f"💡 Mostrando solo: **{st.session_state.filtro_rapido_valor}** (Ejecución). Haga clic en '📋 Ver Todos' en la barra lateral para quitar el filtro.")

        st.markdown("---")
        st.write(f"#### 📋 Detalle de Órdenes ({len(df_fil)} registros)")
        
        cols_op = ['fecha_prog', 'jornada', 'hora', 'nombre', 'estado_visita', 'estado_ejecucion', 'contrato', 'inspector']
        df_op_disp = df_fil[cols_op].copy()
        df_op_disp.columns = ['F. Agenda', 'Jornada', 'Hora', 'Cliente', 'WhatsApp', 'Ejecución', 'Contrato', 'Inspector']

        def style_op(row):
            styles = [''] * len(row)
            if 'Cumplidas' in str(row['Ejecución']): styles[row.index.get_loc('Ejecución')] = 'background-color: #e8f5e9; color: #2e7d32; font-weight: bold;'
            if 'CONFIRM' in str(row['WhatsApp']).upper(): styles[row.index.get_loc('WhatsApp')] = 'color: #2e7d32; font-weight: bold;'
            return styles

        # ESTA ES LA LÍNEA QUE SE CORRIGIÓ: de .map a .apply
        st.dataframe(df_op_disp.style.apply(style_op, axis=1), use_container_width=True, hide_index=True)
    else:
        st.info("Cargue la agenda en la izquierda.")

# ------------------------------------------
# TAB 2: CONTROL DE ANS (TIEMPOS)
# ------------------------------------------
with tab2:
    if not df_dashboard.empty:
        st.write("### ⏱️ Control de Acuerdos de Nivel de Servicio")
        
        df_ans = df_dashboard[df_dashboard['estado_ejecucion'] == '! Pendiente'].copy()
        
        def calcular_ans(row):
            tipo = str(row.get('tipo', 'POR DEFECTO')).upper()
            horas_limite = REGLAS_ANS.get(tipo, REGLAS_ANS['POR DEFECTO'])
            fecha_asig = row['fecha_asig_dt']
            
            if pd.isnull(fecha_asig): return "N/A", "Sin Fecha", 0
            
            vencimiento = fecha_asig + datetime.timedelta(hours=horas_limite)
            ahora = datetime.datetime.now()
            diferencia = vencimiento - ahora
            horas_restantes = diferencia.total_seconds() / 3600
            
            if horas_restantes < 0:
                return "🔴 VENCIDO", f"Venció hace {abs(int(horas_restantes))}h", horas_restantes
            elif horas_restantes < 24:
                return "🟡 POR VENCER", f"Vence en {int(horas_restantes)}h", horas_restantes
            else:
                return "🟢 A TIEMPO", f"{int(horas_restantes)}h restantes", horas_restantes

        if not df_ans.empty:
            ans_results = df_ans.apply(calcular_ans, axis=1)
            df_ans['Estado ANS'] = [r[0] for r in ans_results]
            df_ans['Tiempo Restante'] = [r[1] for r in ans_results]
            df_ans['horas_num'] = [r[2] for r in ans_results]

            m1, m2, m3 = st.columns(3)
            m1.metric("Total Pendientes", len(df_ans))
            m2.metric("Vencidos 🔴", len(df_ans[df_ans['Estado ANS'] == "🔴 VENCIDO"]))
            m3.metric("Críticos (24h) 🟡", len(df_ans[df_ans['Estado ANS'] == "🟡 POR VENCER"]))

            st.write("#### 🕵️ Auditoría Detallada de Incumplimientos")
            cols_ans = ['Estado ANS', 'Tiempo Restante', 'fecha_asig_dt', 'contrato', 'inspector', 'ciudad']
            df_ans_disp = df_ans.sort_values('horas_num')[cols_ans]
            df_ans_disp.columns = ['Estado', 'Reloj ANS', 'F. Asignación', 'Contrato', 'Inspector', 'Ciudad']
            
            def style_ans(val):
                if 'VENCIDO' in str(val): return 'background-color: #ffebee; color: #c62828; font-weight: bold;'
                if 'POR VENCER' in str(val): return 'background-color: #fffde7; color: #f57f17; font-weight: bold;'
                if 'A TIEMPO' in str(val): return 'background-color: #e8f5e9; color: #2e7d32;'
                return ''

            st.dataframe(df_ans_disp.style.map(style_ans, subset=['Estado']), use_container_width=True, hide_index=True)
            
            fig_ans = px.bar(df_ans, x="inspector", color="Estado ANS", title="Cumplimiento de ANS por Inspector",
                            color_discrete_map={"🔴 VENCIDO": "#e74c3c", "🟡 POR VENCER": "#f1c40f", "🟢 A TIEMPO": "#2ecc71"})
            st.plotly_chart(fig_ans, use_container_width=True)
        else:
            st.success("🎉 ¡No hay órdenes pendientes! Todos los ANS están al día.")
    else:
        st.info("Cargue la agenda para ver los indicadores de tiempo.")

# ==========================================
# 5. ARCHIVADO (CORTE FINAL)
# ==========================================
if not df_dashboard.empty:
    with st.sidebar:
        st.markdown("---")
        if st.button("📦 Finalizar y Archivar", type="primary", use_container_width=True):
            df_cerrar = df_dashboard[df_dashboard['estado_ejecucion'].isin(['✅ Cumplidas', '❌ No efectiva'])]
            if not df_cerrar.empty:
                nuevos = df_cerrar[['contrato']]
                if os.path.exists('contratos_archivados.csv'): nuevos.to_csv('contratos_archivados.csv', mode='a', header=False, index=False)
                else: nuevos.to_csv('contratos_archivados.csv', index=False)
                st.success("Corte realizado con éxito.")
                st.rerun()