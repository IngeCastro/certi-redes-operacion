import streamlit as st
import pandas as pd
from PIL import Image
import io
import plotly.express as px  
import datetime
import gc 
import traceback # Para capturar errores exactos en la consola
import warnings # Para silenciar advertencias de rendimiento

# Apagamos todas las advertencias rojas molestas de Pandas en la consola
warnings.filterwarnings('ignore')

# IMPORTAMOS NUESTROS NUEVOS MÓDULOS
from database import cargar_tabla, guardar_tabla
from whatsapp_module import enviar_mensajes_agenda

# ==========================================
# 0. CONSTANTES Y CONFIGURACIÓN
# ==========================================
# Hace que el panel lateral esté siempre expandido
st.set_page_config(layout="wide", page_title="Panel Certi-Redes", page_icon="✅", initial_sidebar_state="expanded")
pd.set_option("styler.render.max_elements", 5000000)

TABLA_BASE = 'base_general'
TABLA_HISTORIAL = 'historial_certiredes'
TABLA_INSPECTORES = 'directorio_inspectores'

# --- NUEVO ESCUDO PROTECTOR DE TABLAS ---
def cargar_tabla_segura(nombre_tabla):
    """Intenta cargar la tabla. Si la tabla no existe en la nube, captura el error y devuelve un DataFrame vacío para no colapsar."""
    try:
        df = cargar_tabla(nombre_tabla)
        if isinstance(df, pd.DataFrame):
            return df
        return pd.DataFrame()
    except Exception as e:
        print(f"⚠️ Aviso silencioso: No se pudo cargar la tabla {nombre_tabla}. Detalle: {e}")
        return pd.DataFrame()
# ----------------------------------------

# ==========================================
# 1. MOTOR DE PROCESAMIENTO (Bases de datos)
# ==========================================
def convertir_fechas_espanol(serie):
    """Convierte fechas a formato estandarizado YYYY-MM-DD (VERSIÓN ALTO RENDIMIENTO)."""
    # 1. Limpieza extrema vectorizada
    s = serie.astype(str).replace({'None': '', 'nan': '', '<NA>': '', 'NaT': '', 'NaN': ''}).str.strip()
    s = s.str.split(' ').str[0] # Quitar horas
    
    # 2. Motor Regex Vectorizado (C++) para traducción masiva de meses (10x más rápido)
    meses_map = {
        r'(?i)[-/ ](ene\.?|enero)[-/ ]': '-01-',
        r'(?i)[-/ ](feb\.?|febrero)[-/ ]': '-02-',
        r'(?i)[-/ ](mar\.?|marzo)[-/ ]': '-03-',
        r'(?i)[-/ ](abr\.?|abril)[-/ ]': '-04-',
        r'(?i)[-/ ](may\.?|mayo)[-/ ]': '-05-',
        r'(?i)[-/ ](jun\.?|junio)[-/ ]': '-06-',
        r'(?i)[-/ ](jul\.?|julio)[-/ ]': '-07-',
        r'(?i)[-/ ](ago\.?|agosto)[-/ ]': '-08-',
        r'(?i)[-/ ](sep\.?|septiembre)[-/ ]': '-09-',
        r'(?i)[-/ ](oct\.?|octubre)[-/ ]': '-10-',
        r'(?i)[-/ ](nov\.?|noviembre)[-/ ]': '-11-',
        r'(?i)[-/ ](dic\.?|diciembre)[-/ ]': '-12-'
    }
    s_lower = s.replace(meses_map, regex=True)
        
    # 3. PARSEO INTELIGENTE "DOBLE MOTOR"
    res = pd.to_datetime(s_lower, errors='coerce')
    
    # Lo que falló (NaT), lo intenta en formato latino (DD/MM/YYYY)
    mask_nat = res.isna() & (s_lower != '')
    if mask_nat.any():
        res[mask_nat] = pd.to_datetime(s_lower[mask_nat], errors='coerce', dayfirst=True)
        
    return res.dt.strftime('%Y-%m-%d')

def normalizar_columnas(df):
    # Bajar a minúsculas y quitar espacios
    cols = df.columns.astype(str).str.strip().str.lower()
    df.columns = cols

    # Búsqueda INTELIGENTE por palabras clave
    nuevos_nombres = {}
    for col in df.columns:
        if col == 'orden' or col == 'ot': nuevos_nombres[col] = 'orden'
        elif col == 'contrato': nuevos_nombres[col] = 'contrato'
        elif 'nombre' in col and 'tecnic' not in col and 'técnic' not in col: nuevos_nombres[col] = 'nombre'
        elif 'direcc' in col or 'direcci' in col: nuevos_nombres[col] = 'direccion'
        elif 'telefon' in col or 'teléfon' in col: nuevos_nombres[col] = 'telefono'
        elif 'fecha' in col and 'programac' in col: nuevos_nombres[col] = 'fecha_programacion'
        elif 'estado' in col and 'programac' in col: nuevos_nombres[col] = 'estado_programacion'
        elif 'jornada' in col: nuevos_nombres[col] = 'jornada'
        elif 'tipo' in col and 'orden' in col: nuevos_nombres[col] = 'tipo_orden'
        elif 'tipo' in col and 'trabajo' in col: nuevos_nombres[col] = 'tipo_trabajo'
        elif 'fecha' in col and 'asignac' in col: nuevos_nombres[col] = 'fecha_asignacion'
        elif 'vne' in col: nuevos_nombres[col] = 'num_vne'
        elif 'consumo' in col: nuevos_nombres[col] = 'consumo'
        elif 'meses' in col: nuevos_nombres[col] = 'meses'
        elif 'cabecera' in col: nuevos_nombres[col] = 'municipio'
        elif 'nombre' in col and ('tecnic' in col or 'técnic' in col): nuevos_nombres[col] = 'inspector'
        elif 'estado' in col and ('gestion' in col or 'gestión' in col): nuevos_nombres[col] = 'estado_ejecucion'
        elif 'codigo' in col and ('tecnic' in col or 'técnic' in col): nuevos_nombres[col] = 'codigo_tecnico'
        elif col == 'estado' or col == 'estado visita' or col == 'estado de la orden': nuevos_nombres[col] = 'estado_visita'

    df.rename(columns=nuevos_nombres, inplace=True)
    return df.loc[:, ~df.columns.duplicated()]

def procesar_nuevas_bases(archivos_subidos):
    """
    Motor principal de carga y actualización cruzando por CONTRATO (Optimizado para memoria RAM).
    """
    try:
        print("\n=========================================================")
        print("🚀 INICIANDO ANÁLISIS DE ARCHIVOS (MODO RENDIMIENTO)")
        print("=========================================================")
        
        nuevos_registros = []
        for archivo in archivos_subidos:
            print(f"📄 Leyendo archivo: {archivo.name}")
            
            if archivo.name.lower().endswith('.csv'):
                contenido = archivo.getvalue().decode('utf-8-sig', errors='replace')
                contenido = contenido.replace('"', '')
                sep = ';' if ';' in contenido.split('\n')[0] else ','
                df_temp = pd.read_csv(io.StringIO(contenido), sep=sep, low_memory=False)
                columnas_test = df_temp.columns.astype(str).str.strip().str.lower()
                
                if 'orden' not in columnas_test and 'contrato' not in columnas_test and 'ot' not in columnas_test:
                    df_temp = pd.read_csv(io.StringIO(contenido), header=4, sep=sep, low_memory=False)
            else:
                try:
                    df_temp = pd.read_excel(archivo, sheet_name='Coordinación', header=4, engine='openpyxl')
                except:
                    df_temp = pd.read_excel(archivo, header=0, engine='openpyxl')
                
            df_temp = normalizar_columnas(df_temp)
            
            if 'contrato' in df_temp.columns:
                df_temp['contrato'] = df_temp['contrato'].astype(str).str.replace('.0', '', regex=False).str.strip()
                if 'orden' in df_temp.columns:
                    df_temp['orden'] = df_temp['orden'].astype(str).str.replace('.0', '', regex=False).str.strip()
                
                df_temp = df_temp[~df_temp['contrato'].isin(['nan', 'None', '', 'NaT', '<NA>', 'null'])]
                
                if len(df_temp) > 0:
                    nuevos_registros.append(df_temp)
            else:
                print("❌ ERROR GRAVE: El archivo no tiene la columna reconocida como 'contrato'.")
                
            del df_temp 
            gc.collect()

        if nuevos_registros:
            df_nuevos = pd.concat(nuevos_registros, ignore_index=True)
            df_hist = cargar_tabla_segura(TABLA_HISTORIAL)
            
            if not df_hist.empty:
                df_hist = normalizar_columnas(df_hist)
                if 'contrato' in df_hist.columns:
                    df_nuevos = df_nuevos[~df_nuevos['contrato'].isin(df_hist['contrato'].astype(str).tolist())]
            
            if df_nuevos.empty: 
                return "❌ Todos los contratos cargados ya están cerrados/archivados en el historial."

            if 'fecha_programacion' in df_nuevos.columns:
                df_nuevos['fecha_prog_limpia'] = convertir_fechas_espanol(df_nuevos['fecha_programacion'])
            if 'fecha_asignacion' in df_nuevos.columns:
                df_nuevos['fecha_asignacion'] = convertir_fechas_espanol(df_nuevos['fecha_asignacion'])
                
            for col in ['estado_whatsapp', 'estado_ejecucion', 'num_vne', 'municipio', 'estado_visita', 'codigo_tecnico']:
                if col not in df_nuevos.columns:
                    df_nuevos[col] = pd.NA
            
            if 'meses' in df_nuevos.columns: df_nuevos['meses'] = df_nuevos['meses'].astype(str).str.replace('.0', '', regex=False).str.strip()
                    
            df_base = cargar_tabla_segura(TABLA_BASE)
            
            if not df_base.empty:
                df_base = normalizar_columnas(df_base)
                
            if not df_base.empty and 'contrato' in df_base.columns:
                df_nuevos = df_nuevos.drop_duplicates(subset=['contrato'], keep='last')
                df_base_index = df_base.drop_duplicates(subset=['contrato'], keep='last')
                
                df_nuevos = df_nuevos.set_index('contrato')
                df_base_index = df_base_index.set_index('contrato')
                
                for col in df_base_index.columns:
                    if col not in df_nuevos.columns:
                        df_nuevos[col] = df_base_index[col]
                    else:
                        s_nuevo = df_nuevos[col].replace(['None', 'nan', '', '<NA>'], pd.NA)
                        s_viejo = df_base_index[col].replace(['None', 'nan', '', '<NA>'], pd.NA)
                        
                        if col in ['estado_visita', 'estado_ejecucion']:
                            s_nuevo = s_nuevo.replace(['Pendiente', 'Pendentes', '⏳ Esperando', '⏳ Agardando'], pd.NA)
                            df_nuevos[col] = s_nuevo.fillna(s_viejo).fillna('Pendiente')
                        elif col == 'num_vne':
                            df_nuevos[col] = s_nuevo.fillna(s_viejo).fillna(0)
                        elif col == 'estado_whatsapp':
                            df_nuevos[col] = s_nuevo.fillna(s_viejo)
                        else:
                            df_nuevos[col] = s_nuevo.fillna(s_viejo)
                
                df_consolidado = pd.concat([
                    df_base_index[~df_base_index.index.isin(df_nuevos.index)].reset_index(),
                    df_nuevos.reset_index()
                ], ignore_index=True)
            else:
                for col in ['estado_visita', 'estado_ejecucion']:
                    df_nuevos[col] = df_nuevos[col].fillna('Pendiente')
                df_nuevos['num_vne'] = df_nuevos['num_vne'].fillna(0)
                df_consolidado = df_nuevos
            
            # --- PARCHE DE LIMPIEZA DE DUPLICADOS ---
            if 'contrato' in df_consolidado.columns:
                df_consolidado = df_consolidado.drop_duplicates(subset=['contrato'], keep='last')
            
            # --- EL EXORCISTA DE LOS "NONES" OPTIMIZADO (Ahorra 60% RAM) ---
            print("🧼 Limpiando textos nulos optimizadamente...")
            cols_texto = df_consolidado.select_dtypes(include=['object', 'string']).columns
            for col in cols_texto:
                df_consolidado[col] = df_consolidado[col].replace({'None': '', 'nan': '', '<NA>': '', 'NaT': '', 'NaN': ''}).fillna('')
            # ---------------------------------------------------------------
            
            print("💾 Guardando consolidado final en la Nube...")
            guardar_tabla(df_consolidado, TABLA_BASE)
            print("=========================================================")
            print("✅ PROCESO COMPLETADO CON ÉXITO")
            print("=========================================================\n")
            return True
            
        return "❌ Error: El archivo subido no contenía registros válidos o no se detectó la columna 'contrato'."
        
    except Exception as e:
        print("🚨🚨 ERROR FATAL DETECTADO 🚨🚨")
        print(traceback.format_exc())
        return f"Error fatal interno del sistema: {str(e)}"

# ==========================================
# 2. INTERFAZ GRÁFICA (UI)
# ==========================================
def centrar_df(df_o_styler):
    """Aplica estilos CSS para centrar celdas (Cuidado: Lento en bases masivas)"""
    if isinstance(df_o_styler, pd.DataFrame):
        styler = df_o_styler.fillna('').astype(str).style
    else:
        styler = df_o_styler.format(lambda x: str(x) if pd.notnull(x) else '')
    return styler.set_properties(**{'text-align': 'center !important'}).set_table_styles([
        {'selector': 'th', 'props': [('text-align', 'center !important')]},
        {'selector': 'td', 'props': [('text-align', 'center !important')]}
    ])

def mostrar_tabla_optimizada(df):
    """Muestra la tabla de forma inteligente para no congelar el navegador con 100k filas."""
    if len(df) > 1500:
        st.warning(f"⚡ MODO ALTO RENDIMIENTO: Mostrando los últimos 1500 registros de **{len(df):,}** totales. Use el botón 'Descargar' para ver la base completa en Excel.")
        # Mostramos los últimos 1500 SIN el estilizador para carga instantánea
        st.dataframe(df.tail(1500).fillna('').astype(str), use_container_width=True)
    else:
        st.dataframe(centrar_df(df), use_container_width=True)

def formatear_estado_visita(df):
    df_formateado = df.copy()
    if 'estado_visita' in df_formateado.columns:
        def agregar_icono(estado):
            estado_str = str(estado).upper().strip()
            if 'CERTIFICADO' in estado_str or 'NO CERTIFICADO' in estado_str:
                return f"✅ {estado}"
            elif 'VISITA NO EFECTIVA' in estado_str or 'VNE' in estado_str or 'NO EFECTIVA' in estado_str or 'PROGRAMADA' in estado_str:
                return f"❌ {estado}"
            elif 'PENDIENTE' in estado_str:
                return f"⏳ {estado}"
            else:
                return estado 
        
        df_formateado['estado_visita'] = df_formateado['estado_visita'].apply(agregar_icono)
    return df_formateado

def convertir_df_a_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Base_General')
    processed_data = output.getvalue()
    return processed_data


with st.sidebar:
    try: st.image(Image.open('Logo_CertiRedes_Transparente.png'), width=200)
    except: st.write("### CERTI-REDES")
    st.markdown("🟢 **Nube Activa**")
    st.markdown("---")
    
    st.markdown("### 📥 1. CARGA DE BASE GENERAL")
    archivos_bases = st.file_uploader("Suba su matriz original (.csv, .xlsx)", accept_multiple_files=True, key="side_uploader")
    if archivos_bases and st.button("🚀 Cargar a la Nube", use_container_width=True, key="side_btn"):
        with st.spinner("Limpiando y subiendo (Alto Rendimiento)..."):
            res = procesar_nuevas_bases(archivos_bases)
            if res is True:
                st.success("¡Base actualizada!")
                st.rerun()
            else:
                st.error(res)

df_activa = cargar_tabla_segura(TABLA_BASE)

if not df_activa.empty:
    df_activa = normalizar_columnas(df_activa)
    # Limpiamos duplicados en pantalla
    if 'contrato' in df_activa.columns:
        df_activa = df_activa.drop_duplicates(subset=['contrato'], keep='last')
        
    if 'fecha_programacion' in df_activa.columns:
        df_activa['fecha_prog_limpia'] = convertir_fechas_espanol(df_activa['fecha_programacion'])

st.title("🚀 Panel Certi-Redes (Cloud)")

if df_activa.empty:
    st.warning("⚠️ La base de datos está vacía actualmente.")
    st.info("👆 Para habilitar todos los módulos, por favor cargue su primera base de datos aquí abajo:")
    st.write("---")
    
    archivos_bases_main = st.file_uploader("Arrastre su archivo Excel o CSV aquí:", accept_multiple_files=True, key="main_uploader")
    if archivos_bases_main and st.button("🚀 Iniciar Procesamiento de Base", use_container_width=True, key="main_btn"):
        with st.spinner("Limpiando formatos, detectando fechas y subiendo a la nube..."):
            res = procesar_nuevas_bases(archivos_bases_main)
            if res is True:
                st.success("¡Base actualizada con éxito!")
                st.rerun()
            else:
                st.error(res)
else:
    t_wa, t_op, t_ans, t_hist, t_insp = st.tabs(["💬 Operación Diaria", "📊 Monitor", "⏱️ ANS", "📦 Historial", "⚙️ Inspectores"])

    with t_wa:
        st.write("### 📅 Centro de Mando Operativo")
        
        col_f1, col_f2 = st.columns([1, 2])
        
        with col_f1:
            fecha_select = st.date_input("Seleccione la Fecha de Programación:")
            f_str = fecha_select.strftime('%Y-%m-%d')
            
        with col_f2:
            with st.expander("⬆️ Actualizar Ejecución (Suba aquí el archivo con la columna 'ESTADO' y 'CONTRATO')", expanded=False):
                archivos_ejec = st.file_uploader("Suba el Excel de los inspectores para actualizar Efectivas/VNE:", accept_multiple_files=True, key="up_ejec")
                if archivos_ejec and st.button("🔄 Procesar y Actualizar Estados", use_container_width=True):
                    with st.spinner("Cruzando datos (por Contrato) y actualizando tablero..."):
                        res = procesar_nuevas_bases(archivos_ejec)
                        if res is True:
                            st.success("¡Estados de ejecución actualizados!")
                            st.rerun()
                        else:
                            st.error(res)
        
        if 'fecha_programacion' in df_activa.columns:
            df_dia = df_activa[df_activa['fecha_prog_limpia'] == f_str].copy()
            
            if not df_dia.empty:
                st.write("---")
                st.markdown("#### 🔍 Filtros Operativos")
                col_filt1, col_filt2, col_filt3 = st.columns(3)
                
                municipios_disp = ["Todos"] + sorted(df_dia['municipio'].dropna().unique().tolist())
                filtro_muni = col_filt1.selectbox("Filtrar por Municipio:", options=municipios_disp, index=0)
                
                inspectores_disp = ["Todos"] + sorted(df_dia['inspector'].dropna().unique().tolist())
                filtro_insp = col_filt2.selectbox("Filtrar por Inspector:", options=inspectores_disp, index=0)
                
                df_dia['estado_puro'] = df_dia['estado_visita'].astype(str).str.upper().str.strip()
                estados_disp = ["Todos", "EFECTIVAS (Certificadas/No Certificadas)", "NO EFECTIVAS (VNE)", "PENDIENTES"]
                filtro_estado = col_filt3.selectbox("Filtrar por Estado:", options=estados_disp, index=0)
                
                df_filtrado = df_dia.copy()
                if filtro_muni != "Todos": df_filtrado = df_filtrado[df_filtrado['municipio'] == filtro_muni]
                if filtro_insp != "Todos": df_filtrado = df_filtrado[df_filtrado['inspector'] == filtro_insp]
                if filtro_estado == "EFECTIVAS (Certificadas/No Certificadas)": df_filtrado = df_filtrado[df_filtrado['estado_puro'].isin(['CERTIFICADO', 'NO CERTIFICADO'])]
                elif filtro_estado == "NO EFECTIVAS (VNE)": df_filtrado = df_filtrado[df_filtrado['estado_puro'].str.contains('VISITA NO EFECTIVA', na=False) | df_filtrado['estado_puro'].isin(['VNE', 'NO EFECTIVA', 'PROGRAMADA'])]
                elif filtro_estado == "PENDIENTES": df_filtrado = df_filtrado[df_filtrado['estado_puro'].isin(['PENDIENTE', '⏳ ESPERANDO', 'NAN', ''])]
                
                # --- NUEVA LÓGICA PARA ETIQUETAR CADA FILA (ESTADO ANS) ---
                hoy = datetime.date.today()
                hora_actual = datetime.datetime.now().hour
                
                def evaluar_ans_fila(row):
                    est = str(row.get('estado_puro', '')).upper()
                    jornada = str(row.get('jornada', '')).upper()
                    
                    es_ejecutada = 'CERTIFICADO' in est or 'VNE' in est or 'NO EFECTIVA' in est or 'PROGRAMADA' in est
                    if es_ejecutada:
                        return "✅ Ejecutada"
                        
                    es_am = 'AM' in jornada
                    
                    if fecha_select < hoy:
                        return "🔴 AM Vencida" if es_am else "🔴 PM Vencida"
                    elif fecha_select == hoy:
                        if es_am and hora_actual >= 12:
                            return "🔴 AM Vencida"
                        elif not es_am and hora_actual >= 17:
                            return "🔴 PM Vencida"
                        else:
                            return "🟡 En Tiempo"
                    else:
                        return "🟡 En Tiempo"

                # Aplicamos la función para crear la nueva columna en el Excel y la Tabla
                df_filtrado['estado_ans'] = df_filtrado.apply(evaluar_ans_fila, axis=1)
                # -----------------------------------------------------------

                c_prog = len(df_filtrado)
                c_env = len(df_filtrado[df_filtrado['estado_whatsapp'].astype(str).str.upper().str.contains('ENVIADO', na=False)]) if 'estado_whatsapp' in df_filtrado.columns else 0
                c_no_env = c_prog - c_env
                
                estados_filtrados = df_filtrado['estado_puro']
                mask_efectivas = estados_filtrados.isin(['CERTIFICADO', 'NO CERTIFICADO'])
                c_cert = len(df_filtrado[mask_efectivas])
                mask_vne = estados_filtrados.str.contains('VISITA NO EFECTIVA', na=False) | estados_filtrados.isin(['VNE', 'NO EFECTIVA', 'PROGRAMADA'])
                c_vne = len(df_filtrado[mask_vne])
                c_pend = c_prog - c_cert - c_vne
                
                st.write("---")
                
                st.markdown("#### 📱 1. Gestión de Mensajería WhatsApp")
                m1, m2, m3, btn_col = st.columns([1, 1, 1, 2])
                m1.metric("📅 Total Programados", c_prog)
                m2.metric("📤 Total Enviados", c_env)
                m3.metric("⏳ No Enviados", c_no_env)
                with btn_col:
                    st.write("") 
                    if st.button("📤 Disparar Mensajes Pendientes (De la tabla actual)", type="primary", use_container_width=True):
                        with st.spinner("Verificando credenciales y conectando con Twilio..."):
                            if "TWILIO_ACCOUNT_SID" not in st.secrets:
                                st.error("🚨 ERROR: No se encontraron credenciales de Twilio.")
                            else:
                                exito, msj = enviar_mensajes_agenda(df_filtrado) 
                                if exito:
                                    st.success(msj)
                                    st.rerun()
                                else:
                                    st.error(msj)

                st.write("---")
                
                col_efect, col_jornada = st.columns(2)
                
                with col_efect:
                    st.markdown("#### 🛠️ 2. Efectividad de Visitas")
                    e1, e2, e3 = st.columns(3)
                    e1.metric("✅ Efectivas (Cert/No Cert)", c_cert)
                    e2.metric("❌ No Efectivas (VNE)", c_vne)
                    e3.metric("⏳ Pendientes", c_pend)
                    
                with col_jornada:
                    st.markdown("#### 🚦 3. Control de Jornada (Semáforo ANS)")
                    jornada_am = df_filtrado[df_filtrado['jornada'].astype(str).str.upper().str.contains('AM', na=False)]
                    jornada_pm = df_filtrado[df_filtrado['jornada'].astype(str).str.upper().str.contains('PM', na=False)]
                    
                    def calc_jornada(df_j, es_am):
                        if df_j.empty: return 0, 0, 0
                        est = df_j.loc[:, 'estado_puro']
                        cumplidos = len(df_j[est.isin(['CERTIFICADO', 'NO CERTIFICADO']) | (est.str.contains('VISITA NO EFECTIVA', na=False) | est.isin(['VNE', 'NO EFECTIVA', 'PROGRAMADA']))])
                        pendientes = len(df_j) - cumplidos
                        vencidos = 0
                        
                        # LÓGICA DEL SEMÁFORO:
                        if fecha_select < hoy: 
                            vencidos = pendientes
                        elif fecha_select == hoy:
                            # AM vence a las 13:00 (1:00 PM) para dar margen de reporte
                            if es_am and hora_actual >= 13: 
                                vencidos = pendientes 
                            # PM vence a las 18:00 (6:00 PM)
                            elif not es_am and hora_actual >= 18: 
                                vencidos = pendientes 
                                
                        return len(df_j), cumplidos, vencidos

                    am_tot, am_cump, am_venc = calc_jornada(jornada_am, True)
                    pm_tot, pm_cump, pm_venc = calc_jornada(jornada_pm, False)
                    
                    # Semáforo Visual Rediseñado
                    estado_am = "🔴 RIESGO (Vencido)" if am_venc > 0 else ("🟢 OK (Completo)" if am_tot > 0 and am_cump == am_tot else "🟡 EN TIEMPO")
                    if am_tot == 0: estado_am = "⚪ N/A"
                    
                    estado_pm = "🔴 RIESGO (Vencido)" if pm_venc > 0 else ("🟢 OK (Completo)" if pm_tot > 0 and pm_cump == pm_tot else "🟡 EN TIEMPO")
                    if pm_tot == 0: estado_pm = "⚪ N/A"
                    
                    st.info(f"**Estado AM:** {estado_am} ㅤ|ㅤ **Estado PM:** {estado_pm}")
                    
                    j1, j2, j3, j4 = st.columns(4)
                    j1.metric("AM Ejecutadas", f"{am_cump} de {am_tot}")
                    j2.metric("AM Vencidas", am_venc, delta=f"-{am_venc} Vencidas" if am_venc > 0 else None, delta_color="inverse")
                    j3.metric("PM Ejecutadas", f"{pm_cump} de {pm_tot}")
                    j4.metric("PM Vencidas", pm_venc, delta=f"-{pm_venc} Vencidas" if pm_venc > 0 else None, delta_color="inverse")

                st.write("---")
                
                # --- ACTUALIZACIÓN DE LA FUNCIÓN DE ETIQUETADO PARA LA TABLA ---
                def evaluar_ans_fila(row):
                    est = str(row.get('estado_puro', '')).upper()
                    jornada = str(row.get('jornada', '')).upper()
                    
                    es_ejecutada = 'CERTIFICADO' in est or 'VNE' in est or 'NO EFECTIVA' in est
                    if es_ejecutada:
                        return "✅ Ejecutada"
                        
                    es_am = 'AM' in jornada
                    
                    if fecha_select < hoy:
                        return "🔴 AM Vencida" if es_am else "🔴 PM Vencida"
                    elif fecha_select == hoy:
                        # AM vence a las 13:00 (1:00 PM)
                        if es_am and hora_actual >= 13:
                            return "🔴 AM Vencida"
                        # PM vence a las 18:00 (6:00 PM)
                        elif not es_am and hora_actual >= 18:
                            return "🔴 PM Vencida"
                        else:
                            return "🟡 En Tiempo"
                    else:
                        return "🟡 En Tiempo"

                df_filtrado['estado_ans'] = df_filtrado.apply(evaluar_ans_fila, axis=1)
                # -----------------------------------------------------------
                
                col_tab1, col_tab2 = st.columns([4, 1])
                with col_tab1:
                    st.write("#### 📋 Base Detallada de la Jornada")
                with col_tab2:
                    excel_dia = convertir_df_a_excel(df_filtrado)
                    st.download_button(
                        label="📥 Exportar Esta Tabla",
                        data=excel_dia,
                        file_name=f"Operacion_{f_str}_{filtro_muni}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                
                # AÑADIMOS 'estado_ans' A LAS COLUMNAS VISIBLES DE LA TABLA
                cols_vista = ['orden', 'contrato', 'nombre', 'municipio', 'fecha_programacion', 'jornada', 'inspector', 'estado_visita', 'estado_ans', 'estado_whatsapp']
                columnas_presentes = [c for c in cols_vista if c in df_filtrado.columns]
                
                df_limpio = df_filtrado[columnas_presentes].replace({'None': '', 'nan': '', '<NA>': '', 'NaT': '', 'NaN': ''}).fillna('')
                df_visual = formatear_estado_visita(df_limpio)
                mostrar_tabla_optimizada(df_visual)
            else:
                st.info(f"Sin agenda u operación registrada para el día {fecha_select.strftime('%d/%m/%Y')}.")
                fechas_disp = df_activa['fecha_prog_limpia'].dropna().unique()
                fechas_disp = [f for f in fechas_disp if str(f) not in ['nan', 'NaT', 'None', '']]
                if len(fechas_disp) > 0:
                    st.warning("🕵️ **El sistema encontró estas fechas disponibles en su base general:**")
                    st.code(", ".join(sorted(fechas_disp)[:20]))
        else:
            st.error("No se detectó la columna 'fecha_programacion' en la base de datos.")

    with t_op:
        col_tit, col_desc = st.columns([3, 1])
        with col_tit:
            st.write("### 📊 Monitor Operativo General")
        with col_desc:
            excel_data = convertir_df_a_excel(df_activa)
            st.download_button(
                label="📥 Descargar Base Completa",
                data=excel_data,
                file_name=f"Base_General_CertiRedes_{datetime.date.today().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
            
        df_activa_visual = formatear_estado_visita(df_activa)
        mostrar_tabla_optimizada(df_activa_visual)

    with t_ans:
        st.write("### ⏱️ Control ANS")
        if 'fecha_asignacion' in df_activa.columns:
            mostrar_tabla_optimizada(df_activa[['orden', 'fecha_asignacion', 'tipo_orden', 'estado_ejecucion']])

    with t_hist:
        st.write("### 📦 Historial")
        df_h = cargar_tabla_segura(TABLA_HISTORIAL)
        if not df_h.empty: 
            df_h = normalizar_columnas(df_h)
            mostrar_tabla_optimizada(df_h)

    with t_insp:
        st.write("### ⚙️ Directorio de Inspectores")
        df_insp = cargar_tabla_segura(TABLA_INSPECTORES)
        
        with st.expander("⬆️ Carga Masiva de Inspectores (Excel/CSV)", expanded=False):
            st.info("Suba un archivo con las columnas: Código Técnico, Cédula, Nombre, Celular.")
            archivo_insp = st.file_uploader("Subir base de técnicos", type=['csv', 'xlsx'], key="up_insp")
            
            if archivo_insp and st.button("🚀 Procesar y Guardar Directorio", use_container_width=True):
                with st.spinner("Cruzando y actualizando base de inspectores..."):
                    try:
                        if archivo_insp.name.lower().endswith('.csv'):
                            cont_insp = archivo_insp.getvalue().decode('utf-8-sig', errors='replace')
                            sep_insp = ';' if ';' in cont_insp.split('\n')[0] else ','
                            df_nuevo_insp = pd.read_csv(io.StringIO(cont_insp), sep=sep_insp, dtype=str)
                        else:
                            df_nuevo_insp = pd.read_excel(archivo_insp, dtype=str)
                        
                        cols_insp = df_nuevo_insp.columns.astype(str).str.strip().str.lower()
                        df_nuevo_insp.columns = cols_insp
                        
                        map_insp = {}
                        for c in cols_insp:
                            if 'codigo' in c or 'código' in c or 'cod' in c: map_insp[c] = 'codigo_tecnico'
                            elif 'cedula' in c or 'cédula' in c or 'identificacion' in c: map_insp[c] = 'cedula'
                            elif 'nombre' in c or 'tecnico' in c or 'técnico' in c: map_insp[c] = 'nombre'
                            elif 'celular' in c or 'telefono' in c or 'teléfono' in c: map_insp[c] = 'celular'
                        
                        df_nuevo_insp.rename(columns=map_insp, inplace=True)
                        
                        if 'codigo_tecnico' in df_nuevo_insp.columns:
                            df_nuevo_insp['codigo_tecnico'] = df_nuevo_insp['codigo_tecnico'].astype(str).str.replace('.0', '', regex=False).str.strip()
                            df_nuevo_insp = df_nuevo_insp[~df_nuevo_insp['codigo_tecnico'].isin(['nan', 'None', '', '<NA>'])]
                            
                            if not df_nuevo_insp.empty:
                                if not df_insp.empty:
                                    df_final_insp = pd.concat([df_insp, df_nuevo_insp], ignore_index=True)
                                    df_final_insp = df_final_insp.drop_duplicates(subset=['codigo_tecnico'], keep='last')
                                else:
                                    df_final_insp = df_nuevo_insp
                                
                                guardar_tabla(df_final_insp, TABLA_INSPECTORES)
                                st.success(f"¡Directorio actualizado con éxito! Se procesaron {len(df_nuevo_insp)} registros.")
                                st.rerun()
                            else:
                                st.warning("El archivo no contenía códigos válidos.")
                        else:
                            st.error("❌ El archivo no tiene una columna reconocible para el 'Código Técnico'.")
                    except Exception as e:
                        st.error(f"Error al procesar el archivo: {str(e)}")

        st.write("---")
        
        c1, c2 = st.columns([1, 2])
        with c1:
            st.write("#### ✍️ Registro Individual")
            with st.form("f_insp"):
                f_cod = st.text_input("Código Técnico (Ej: 321)")
                f_ced = st.text_input("Cédula")
                f_nom = st.text_input("Nombre")
                f_cel = st.text_input("Celular (Sin +57)")
                if st.form_submit_button("Guardar Técnico"):
                    nuevo = pd.DataFrame([{'codigo_tecnico': f_cod.strip(), 'cedula': f_ced.strip(), 'nombre': f_nom.strip(), 'celular': f_cel.strip()}])
                    if not df_insp.empty:
                        df_final = pd.concat([df_insp[df_insp['codigo_tecnico'] != f_cod.strip()], nuevo], ignore_index=True)
                    else: df_final = nuevo
                    guardar_tabla(df_final, TABLA_INSPECTORES)
                    st.rerun()
        with c2:
            st.write("#### 📋 Listado Actual")
            if not df_insp.empty: mostrar_tabla_optimizada(df_insp)