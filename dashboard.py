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

# Memoria para filtros interactivos y control de archivos
if 'filtro_rapido_tipo' not in st.session_state:
    st.session_state.filtro_rapido_tipo = None
if 'filtro_rapido_valor' not in st.session_state:
    st.session_state.filtro_rapido_valor = None
if 'ultimo_archivo' not in st.session_state:
    st.session_state.ultimo_archivo = None

def aplicar_filtro_rapido(tipo, valor):
    st.session_state.filtro_rapido_tipo = tipo
    st.session_state.filtro_rapido_valor = valor

# ==========================================
# 2. CARGA DE DATOS, AUDITORÍA DE TIEMPOS Y ARCHIVADOS
# ==========================================
df_agenda = pd.read_excel('agenda.xlsm', sheet_name='base', engine='openpyxl')
df_agenda.columns = df_agenda.columns.str.strip().str.lower()
df_agenda['contrato'] = df_agenda['contrato'].astype(str).str.split('.').str[0].str.strip()
df_agenda['fecha_limpia'] = pd.to_datetime(df_agenda['fecha'], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')

# ✨ NUEVO: MOTOR DE AUDITORÍA DE JORNADA
df_agenda['hora'] = df_agenda['hora'].fillna('Sin hora').astype(str).str.strip().str.upper()

def sacar_jornada(h):
    if 'AM' in h or 'A.M' in h: return 'AM'
    if 'PM' in h or 'P.M' in h: return 'PM'
    try:
        hr = int(str(h).split(':')[0])
        return 'PM' if (hr >= 12 and hr != 24) else 'AM'
    except:
        return 'Sin Jornada'

df_agenda['jornada'] = df_agenda['hora'].apply(sacar_jornada)

# FILTRO DE CORTE DE CICLO
if os.path.exists('contratos_archivados.csv'):
    df_archivados = pd.read_csv('contratos_archivados.csv', dtype=str)
    df_agenda = df_agenda[~df_agenda['contrato'].isin(df_archivados['contrato'])]

# ==========================================
# 3. LEER RESPUESTAS (LOG WHATSAPP) + BLINDAJE
# ==========================================
logs = []
try:
    with open('log_certiredes.txt', 'r', encoding='latin-1', errors='ignore') as f:
        for linea in f:
            if " - Contrato: " in linea:
                parte_fecha_estado, resto = linea.split(" - Contrato: ")
                contrato_extraido, inspector = resto.split(" - Inspector: ")
                estado_crudo = parte_fecha_estado.split("] ")[1].strip()
                fecha_hora = parte_fecha_estado.split("] ")[0].replace("[", "")
                
                estado_final = "CONFIRMÓ" if "CONFIRM" in estado_crudo.upper() else "CANCELÓ"
                
                logs.append({
                    "fecha_respuesta": fecha_hora, 
                    "estado_visita": estado_final, 
                    "contrato": contrato_extraido.strip()
                })
except FileNotFoundError:
    pass

df_logs = pd.DataFrame(logs)

if df_logs.empty:
    df_logs = pd.DataFrame(columns=["fecha_respuesta", "estado_visita", "contrato"])
else:
    df_logs = df_logs.drop_duplicates(subset=['contrato'], keep='last')

# --- CRUCE 1: AGENDA + LOG WHATSAPP ---
df_dashboard = pd.merge(df_agenda, df_logs, on='contrato', how='left')
df_dashboard['estado_visita'] = df_dashboard['estado_visita'].fillna('⏳ Esperando')
df_dashboard['fecha_respuesta'] = df_dashboard['fecha_respuesta'].fillna('-')

# ==========================================
# 4. BARRA LATERAL (GESTIÓN Y ARCHIVO)
# ==========================================
with st.sidebar:
    try:
        st.image(Image.open('Logo_CertiRedes_Transparente.png'), width=200)
    except:
        st.write("### CERTI-REDES S.A.S")
        
    st.markdown("---")
    
    # --- LANZADOR DE MENSAJES ---
    with st.expander("🚀 ENVIAR AGENDA POR WHATSAPP", expanded=False):
        fecha_envio = st.date_input("Seleccione la fecha de programación:")
        
        if st.button("ENVIAR A INSPECTORES", type="primary", use_container_width=True):
            fecha_filtro = fecha_envio.strftime('%d/%m/%Y')
            citas_del_dia = df_agenda[df_agenda['fecha_limpia'] == fecha_filtro]
            
            if citas_del_dia.empty:
                st.warning(f"⚠️ No hay citas para el {fecha_filtro} en el Excel.")
            else:
                with st.spinner(f'Enviando {len(citas_del_dia)} mensajes...'):
                    cliente_tw = Client(ACCOUNT_SID, AUTH_TOKEN)
                    enviados = 0
                    errores = 0
                    
                    for indice, fila in citas_del_dia.iterrows():
                        try:
                            variables_json = {
                                "1": str(fila['contrato']).strip(), "2": str(fila['nombre']).strip(),
                                "3": str(fila['direccion']).strip(), "4": str(fila['barrio']).strip(),
                                "5": fecha_filtro, "6": str(fila['hora']).strip(), "7": str(fila['ciudad']).strip()
                            }
                            tel_crudo = str(fila['telefono']).split('.')[0].strip()
                            tel_final = f"whatsapp:+{tel_crudo}" if not tel_crudo.startswith('+') else f"whatsapp:{tel_crudo}"
                            
                            cliente_tw.messages.create(
                                from_=NUMERO_TWILIO, to=tel_final,
                                content_sid=CONTENT_SID, content_variables=json.dumps(variables_json)
                            )
                            enviados += 1
                        except Exception as e:
                            errores += 1
                    
                st.success(f"✅ Proceso terminado: {enviados} enviados, {errores} errores.")

    st.markdown("---")
    
    # --- BLOQUE WHATSAPP ---
    st.markdown("### 💬 RESUMEN WHATSAPP")
    confirmados = len(df_dashboard[df_dashboard['estado_visita'] == 'CONFIRMÓ'])
    cancelados = len(df_dashboard[df_dashboard['estado_visita'] == 'CANCELÓ'])
    
    st.button(f"✅ Confirmados: {confirmados}", on_click=aplicar_filtro_rapido, args=('whatsapp', 'CONFIRMÓ'), use_container_width=True)
    st.button(f"❌ Cancelados: {cancelados}", on_click=aplicar_filtro_rapido, args=('whatsapp', 'CANCELÓ'), use_container_width=True)
    st.button(f"📋 Ver Todos: {len(df_dashboard)}", on_click=aplicar_filtro_rapido, args=(None, None), use_container_width=True)
    
    st.markdown("---")
    
    # --- BLOQUE EJECUCIÓN CAMPO ---
    st.markdown("### 🛠️ EJECUCIÓN CAMPO")
    
    df_dashboard['estado_ejecucion'] = '! Pendiente'
    archivo_ejecucion = st.file_uploader("Subir reporte de GoDoWorks", type=["csv", "xlsx", "xls"])
    archivo_bd_ejecucion = 'bd_ejecucion.csv'
    
    if archivo_ejecucion is not None:
        file_id = archivo_ejecucion.name + str(archivo_ejecucion.size)
        
        # Solo procesa si es nuevo
        if st.session_state.ultimo_archivo != file_id:
            try:
                if archivo_ejecucion.name.lower().endswith('.csv'):
                    contenido_crudo = archivo_ejecucion.getvalue().decode('utf-8-sig', errors='replace')
                    separador = ';' if ';' in contenido_crudo.split('\n')[0] else ','
                    df_ejecucion = pd.read_csv(io.StringIO(contenido_crudo), sep=separador, dtype=str)
                else:
                    df_ejecucion = pd.read_excel(archivo_ejecucion, dtype=str)

                df_ejecucion.columns = df_ejecucion.columns.str.strip().str.upper()
                
                if 'CONTRATO' in df_ejecucion.columns and 'ESTADO' in df_ejecucion.columns:
                    df_ejecucion['CONTRATO'] = df_ejecucion['CONTRATO'].astype(str).str.split('.').str[0].str.strip()
                    
                    def asignar_prioridad(e):
                        e = str(e).strip().upper()
                        if e == "CERTIFICADO": return 1
                        if e == "NO CERTIFICADO": return 2
                        if e == "EJECUTADA(CUMPLE)": return 2 
                        if e == "VISITA NO EFECTIVA": return 3
                        return 4 

                    df_ejecucion['prioridad_peso'] = df_ejecucion['ESTADO'].apply(asignar_prioridad)
                    df_ejecucion = df_ejecucion.sort_values(by=['CONTRATO', 'prioridad_peso'], ascending=[True, True])
                    df_ejecucion = df_ejecucion.drop_duplicates(subset=['CONTRATO'], keep='first')

                    def mapear_ejecucion(e):
                        e = str(e).strip().upper()
                        if e in ["CERTIFICADO", "NO CERTIFICADO", "EJECUTADA(CUMPLE)"]: return "✅ Cumplidas"
                        if e == "VISITA NO EFECTIVA": return "❌ No efectiva"
                        return "! Pendiente"
                    
                    df_ejecucion['estado_final'] = df_ejecucion['ESTADO'].apply(mapear_ejecucion)
                    
                    df_nueva = df_ejecucion[['CONTRATO', 'ESTADO', 'estado_final']]
                    if os.path.exists(archivo_bd_ejecucion):
                        df_historico = pd.read_csv(archivo_bd_ejecucion, dtype=str)
                        df_bd_actualizada = pd.concat([df_historico, df_nueva])
                        df_bd_actualizada = df_bd_actualizada.drop_duplicates(subset=['CONTRATO'], keep='last')
                    else:
                        df_bd_actualizada = df_nueva
                        
                    df_bd_actualizada.to_csv(archivo_bd_ejecucion, index=False, encoding='utf-8-sig')
                    st.session_state.ultimo_archivo = file_id 
                    
            except Exception as e:
                st.error(f"Error procesando archivo: {e}")

    # LECTURA PERSISTENTE
    if os.path.exists(archivo_bd_ejecucion):
        df_bd = pd.read_csv(archivo_bd_ejecucion, dtype=str)
        df_dashboard = pd.merge(df_dashboard, df_bd[['CONTRATO', 'estado_final']], left_on='contrato', right_on='CONTRATO', how='left')
        df_dashboard['estado_ejecucion'] = df_dashboard['estado_final'].fillna('! Pendiente')

    ejec_ok = len(df_dashboard[df_dashboard['estado_ejecucion'] == '✅ Cumplidas'])
    ejec_no = len(df_dashboard[df_dashboard['estado_ejecucion'] == '❌ No efectiva'])
    ejec_pend = len(df_dashboard[df_dashboard['estado_ejecucion'] == '! Pendiente'])
    
    st.button(f"✅ Cumplidas: {ejec_ok}", on_click=aplicar_filtro_rapido, args=('ejecucion', '✅ Cumplidas'), use_container_width=True)
    st.button(f"❌ No Efectivas: {ejec_no}", on_click=aplicar_filtro_rapido, args=('ejecucion', '❌ No efectiva'), use_container_width=True)
    st.button(f"❗ Pendientes: {ejec_pend}", on_click=aplicar_filtro_rapido, args=('ejecucion', '! Pendiente'), use_container_width=True)

    st.markdown("---")
    
    # --- BOTÓN DE CORTE DE CICLO ---
    st.markdown("### 📦 CORTE DE JORNADA")
    st.info("Oculta las órdenes finalizadas y las guarda en el historial.")
    
    if st.button("Finalizar y Archivar", type="primary", use_container_width=True):
        df_cerrados = df_dashboard[df_dashboard['estado_ejecucion'].isin(['✅ Cumplidas', '❌ No efectiva'])]
        
        if not df_cerrados.empty:
            if os.path.exists('historial_ejecucion.csv'):
                df_cerrados.to_csv('historial_ejecucion.csv', mode='a', header=False, index=False, encoding='utf-8-sig')
            else:
                df_cerrados.to_csv('historial_ejecucion.csv', index=False, encoding='utf-8-sig')
                
            nuevos_arch = df_cerrados[['contrato']].copy()
            if os.path.exists('contratos_archivados.csv'):
                nuevos_arch.to_csv('contratos_archivados.csv', mode='a', header=False, index=False)
            else:
                nuevos_arch.to_csv('contratos_archivados.csv', index=False)
                
            try:
                contratos_a_borrar = nuevos_arch['contrato'].tolist()
                if os.path.exists('log_certiredes.txt'):
                    with open('log_certiredes.txt', 'r', encoding='latin-1', errors='ignore') as f:
                        lineas = f.readlines()
                    with open('log_certiredes.txt', 'w', encoding='latin-1') as f:
                        for linea in lineas:
                            if " - Contrato: " in linea:
                                c = linea.split(" - Contrato: ")[1].split(" - Inspector: ")[0].strip()
                                if c not in contratos_a_borrar: f.write(linea)
                            else: f.write(linea)
                                
                if os.path.exists(archivo_bd_ejecucion):
                    df_bd_clean = pd.read_csv(archivo_bd_ejecucion, dtype=str)
                    df_bd_clean = df_bd_clean[~df_bd_clean['CONTRATO'].isin(contratos_a_borrar)]
                    df_bd_clean.to_csv(archivo_bd_ejecucion, index=False, encoding='utf-8-sig')
            except Exception: pass
                
            st.success(f"✅ Se archivaron {len(df_cerrados)} órdenes.")
            try: st.rerun()
            except AttributeError: st.experimental_rerun()
        else:
            st.warning("⚠️ No hay órdenes finalizadas en la vista actual para archivar.")

# ==========================================
# 5. UI PRINCIPAL Y ORDEN VISUAL
# ==========================================
st.title("🚀 Control de Operación en Tiempo Real")

st.markdown("""
    <style>
        [data-testid="stHeader"] { font-size: 16px !important; font-weight: bold !important; color: #31333F !important; text-align: center !important; }
        div[data-testid="stTable"] td { text-align: center !important; }
        div[data-testid="stTable"] th { text-align: center !important; }
    </style>
""", unsafe_allow_html=True)

# --- FILTROS EN CASCADA (AHORA 5 FILTROS INCLUYENDO JORNADA) ---
df_filtrado = df_dashboard.copy()
c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    muni = st.selectbox("📍 Municipio", ["Todos"] + list(df_dashboard['ciudad'].unique()))
    if muni != "Todos": df_filtrado = df_filtrado[df_filtrado['ciudad'] == muni]

with c2:
    insp = st.selectbox("👷 Inspector", ["Todos"] + list(df_filtrado['inspector'].unique()))
    if insp != "Todos": df_filtrado = df_filtrado[df_filtrado['inspector'] == insp]

with c3:
    est_w = st.selectbox("🚦 Respuesta WA", ["Todos", "CONFIRMÓ", "CANCELÓ", "⏳ Esperando"])
    if est_w != "Todos": df_filtrado = df_filtrado[df_filtrado['estado_visita'] == est_w]

with c4:
    est_e = st.selectbox("🛠️ Ejecución", ["Todos", "✅ Cumplidas", "❌ No efectiva", "! Pendiente"])
    if est_e != "Todos": df_filtrado = df_filtrado[df_filtrado['estado_ejecucion'] == est_e]

with c5:
    jor = st.selectbox("⏰ Jornada", ["Todas", "AM", "PM", "Sin Jornada"])
    if jor != "Todas": df_filtrado = df_filtrado[df_filtrado['jornada'] == jor]

if st.session_state.filtro_rapido_tipo == 'whatsapp':
    df_filtrado = df_filtrado[df_filtrado['estado_visita'] == st.session_state.filtro_rapido_valor]
elif st.session_state.filtro_rapido_tipo == 'ejecucion':
    df_filtrado = df_filtrado[df_filtrado['estado_ejecucion'] == st.session_state.filtro_rapido_valor]


# --- SECCIÓN A: DETALLE DE ÓRDENES ---
st.markdown("---")
if st.session_state.filtro_rapido_tipo:
    st.info(f"🔍 **Filtro activo:** Mostrando **{st.session_state.filtro_rapido_valor}**. Restablezca en 'Ver Todos' a la izquierda.")

col_tit_A, col_btn_A = st.columns([8, 2])
with col_tit_A:
    st.write(f"#### 📋 Detalle de Órdenes ({len(df_filtrado)} registros)")

# Agregamos jornada y hora a las columnas visibles
cols_detalles = ['jornada', 'hora', 'fecha_respuesta', 'nombre', 'estado_visita', 'estado_ejecucion', 'ciudad', 'contrato', 'direccion', 'inspector']
df_mostrar = df_filtrado[cols_detalles].copy()
df_mostrar.columns = ['Jornada', 'Hora Prog.', 'Fecha Resp.', 'Nombre Cliente', 'Respuesta WA', 'Ejecución', 'Ciudad', 'Contrato', 'Dirección', 'Inspector']

# ⏰ EVALUACIÓN INTELIGENTE DE RETRASOS
hora_actual = datetime.datetime.now().hour
es_tarde_para_am = hora_actual >= 12  # Si son las 12 PM o más, ya es tarde para la mañana

def evaluar_retraso(row):
    jor = str(row['Jornada'])
    est = str(row['Ejecución'])
    if jor == 'AM' and es_tarde_para_am and 'Pendiente' in est:
        return 'AM 🚨 RETRASO' # El texto cambia para que se note en el Excel descargado
    return jor

df_mostrar['Jornada'] = df_mostrar.apply(evaluar_retraso, axis=1)

with col_btn_A:
    buffer_detalles = io.BytesIO()
    df_mostrar.to_excel(buffer_detalles, index=False, engine='openpyxl')
    st.download_button(
        label="📥 Descargar Excel", 
        data=buffer_detalles.getvalue(), 
        file_name='detalle_ordenes_con_retrasos.xlsx', 
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
        use_container_width=True
    )

# Función de diseño avanzado por filas
def estilo_dataframe(row):
    estilos = [''] * len(row)
    idx_wa = row.index.get_loc('Respuesta WA')
    idx_ejec = row.index.get_loc('Ejecución')
    idx_jor = row.index.get_loc('Jornada')
    
    val_wa = str(row['Respuesta WA']).upper()
    if 'CONFIRM' in val_wa: estilos[idx_wa] = 'color: #2e7d32; font-weight: bold;'
    elif 'CANCEL' in val_wa: estilos[idx_wa] = 'color: #b71c1c; font-weight: bold;'
    
    val_ejec = str(row['Ejecución'])
    if 'Cumplidas' in val_ejec: estilos[idx_ejec] = 'background-color: #e8f5e9; color: #2e7d32; font-weight: bold;'
    elif 'No efectiva' in val_ejec: estilos[idx_ejec] = 'background-color: #ffebee; color: #c62828; font-weight: bold;'
    elif 'Pendiente' in val_ejec: estilos[idx_ejec] = 'background-color: #fffde7; color: #f57f17; font-weight: bold;'
    
    # Pinta la alerta visual en el panel
    val_jor = str(row['Jornada'])
    if 'RETRASO' in val_jor: estilos[idx_jor] = 'background-color: #b71c1c; color: white; font-weight: 900;'
    elif val_jor == 'AM': estilos[idx_jor] = 'color: #f57f17; font-weight: bold;'
    elif val_jor == 'PM': estilos[idx_jor] = 'color: #1565c0; font-weight: bold;'
        
    return estilos

styled_df_mostrar = df_mostrar.style.apply(estilo_dataframe, axis=1)
st.dataframe(styled_df_mostrar, use_container_width=True, hide_index=True)


# --- SECCIÓN B: RESUMEN POR INSPECTOR ---
st.markdown("---")

col_tit_B, col_btn_B = st.columns([8, 2])
with col_tit_B:
    st.subheader("👷 Resumen de Avance por Inspector")

df_resumen = df_filtrado.groupby(['inspector', 'estado_ejecucion']).size().unstack(fill_value=0)

for col in ['! Pendiente', '✅ Cumplidas', '❌ No efectiva']:
    if col not in df_resumen.columns:
        df_resumen[col] = 0

df_resumen['TOTAL'] = df_resumen['! Pendiente'] + df_resumen['✅ Cumplidas'] + df_resumen['❌ No efectiva']
df_resumen = df_resumen.sort_values(by='TOTAL', ascending=False).reset_index()

df_resumen = df_resumen[['inspector', '! Pendiente', '✅ Cumplidas', '❌ No efectiva', 'TOTAL']]
df_resumen.columns = ['Nombre del Inspector', 'Pendientes ⏳', 'Cumplidas ✅', 'No Efectivas ❌', 'Total Carga']

with col_btn_B:
    buffer_resumen = io.BytesIO()
    df_resumen.to_excel(buffer_resumen, index=False, engine='openpyxl')
    st.download_button(
        label="📥 Descargar Excel", 
        data=buffer_resumen.getvalue(), 
        file_name='resumen_inspectores.xlsx', 
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
        use_container_width=True
    )

def estilo_resumen(s):
    if s.name == 'Pendientes ⏳':
        return ['background-color: rgba(255, 235, 59, 0.2); color: #856404; font-weight: bold; text-align: center;'] * len(s)
    elif s.name == 'Cumplidas ✅':
        return ['background-color: rgba(76, 175, 80, 0.15); color: #1b5e20; font-weight: bold; text-align: center;'] * len(s)
    elif s.name == 'No Efectivas ❌':
        return ['background-color: rgba(244, 67, 54, 0.15); color: #b71c1c; font-weight: bold; text-align: center;'] * len(s)
    elif s.name == 'Total Carga':
        return ['background-color: #f5f5f5; color: #424242; font-weight: 900; text-align: center;'] * len(s)
    elif s.name == 'Nombre del Inspector':
        return ['font-weight: 600; text-align: left;'] * len(s)
    return [''] * len(s)

styled_df_resumen = df_resumen.style.apply(estilo_resumen, axis=0)
st.dataframe(styled_df_resumen, use_container_width=True, hide_index=True)


# --- SECCIÓN C: GRÁFICOS DE ANÁLISIS ---
st.markdown("---")
st.subheader("📊 Análisis Visual de Efectividad")
g1, g2 = st.columns([1, 2])

with g1:
    st.write("#### Efectividad General")
    fig_pie = px.pie(df_filtrado, names='estado_ejecucion', hole=0.4,
                     color='estado_ejecucion',
                     color_discrete_map={'✅ Cumplidas':'#2ecc71', '❌ No efectiva':'#e74c3c', '! Pendiente':'#f1c40f'})
    st.plotly_chart(fig_pie, use_container_width=True)

with g2:
    st.write("#### Avance por Municipio")
    fig_bar = px.bar(df_filtrado, x='ciudad', color='estado_ejecucion', barmode='group',
                     color_discrete_map={'✅ Cumplidas':'#2ecc71', '❌ No efectiva':'#e74c3c', '! Pendiente':'#f1c40f'})
    st.plotly_chart(fig_bar, use_container_width=True)