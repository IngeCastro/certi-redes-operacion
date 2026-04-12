import streamlit as st
import pandas as pd
from PIL import Image
import io
import plotly.express as px  
import datetime
import gc 
import traceback # Para capturar errores exactos en consola

# IMPORTAMOS NUESTROS NUEVOS MÓDULOS
from database import cargar_tabla, guardar_tabla
from whatsapp_module import enviar_mensajes_agenda

# ==========================================
# 0. CONSTANTES Y CONFIGURACIÓN
# ==========================================
# Fuerza el panel lateral a estar siempre expandido
st.set_page_config(layout="wide", page_title="Panel Certi-Redes", page_icon="✅", initial_sidebar_state="expanded")
pd.set_option("styler.render.max_elements", 5000000)

TABLA_BASE = 'base_general'
TABLA_HISTORIAL = 'historial_certiredes'
TABLA_INSPECTORES = 'directorio_inspectores'

# --- NUEVO ESCUDO PROTECTOR DE TABLAS ---
def cargar_tabla_segura(nombre_tabla):
    """Intenta cargar la tabla. Si la tabla no existe en la nube, atrapa el error y devuelve un DataFrame vacío para no colapsar."""
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
    """Convierte fechas con meses en texto español (ej: 12-abr-26) a formato YYYY-MM-DD."""
    s = serie.astype(str).str.lower().str.replace('00:00:00', '', regex=False).str.strip()
    reemplazos = [
        ('ene.', '01'), ('ene', '01'), ('enero', '01'),
        ('feb.', '02'), ('feb', '02'), ('febrero', '02'),
        ('mar.', '03'), ('mar', '03'), ('marzo', '03'),
        ('abr.', '04'), ('abr', '04'), ('abril', '04'),
        ('may.', '05'), ('may', '05'), ('mayo', '05'),
        ('jun.', '06'), ('jun', '06'), ('junio', '06'),
        ('jul.', '07'), ('jul', '07'), ('julio', '07'),
        ('ago.', '08'), ('ago', '08'), ('agosto', '08'),
        ('sep.', '09'), ('sep', '09'), ('septiembre', '09'),
        ('oct.', '10'), ('oct', '10'), ('octubre', '10'),
        ('nov.', '11'), ('nov', '11'), ('noviembre', '11'),
        ('dic.', '12'), ('dic', '12'), ('diciembre', '12')
    ]
    for texto, num in reemplazos:
        s = s.str.replace(f'-{texto}-', f'-{num}-', regex=False)
        s = s.str.replace(f'/{texto}/', f'/{num}/', regex=False)
        s = s.str.replace(f' {texto} ', f'-{num}-', regex=False)
    return pd.to_datetime(s, dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')

def normalizar_columnas(df):
    # 1. Bajar a minúsculas y quitar espacios extra a los lados
    cols = df.columns.astype(str).str.strip().str.lower()
    df.columns = cols

    # 2. Búsqueda INTELIGENTE por palabras clave (ignora tildes, rombos y espacios raros)
    nuevos_nombres = {}
    for col in df.columns:
        if col == 'orden': nuevos_nombres[col] = 'orden'
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

    df.rename(columns=nuevos_nombres, inplace=True)
    return df.loc[:, ~df.columns.duplicated()]

def procesar_nuevas_bases(archivos_subidos):
    """
    Motor principal. Se agregaron 'prints' para Monitoreo en Consola.
    """
    try:
        print("\n=========================================================")
        print("🚀 INICIANDO ANÁLISIS DE ARCHIVOS (MONITOR DE DIAGNÓSTICO)")
        print("=========================================================")
        
        nuevos_registros = []
        for archivo in archivos_subidos:
            print(f"📄 Leyendo archivo: {archivo.name}")
            
            if archivo.name.lower().endswith('.csv'):
                contenido = archivo.getvalue().decode('utf-8-sig', errors='replace')
                sep = ';' if ';' in contenido.split('\n')[0] else ','
                print(f"🔍 Detectado CSV. Separador: '{sep}'")
                df_temp = pd.read_csv(io.StringIO(contenido), sep=sep, low_memory=False)
                columnas_test = df_temp.columns.astype(str).str.strip().str.lower()
                if 'orden' not in columnas_test and 'contrato' not in columnas_test:
                    print("⚠️ 'orden' no encontrada en fila 0. Saltando 4 filas (Formato Excel)...")
                    archivo.seek(0)
                    df_temp = pd.read_csv(io.StringIO(contenido), header=4, sep=sep, low_memory=False)
            else:
                print("🔍 Detectado Excel. Leyendo pestaña 'Coordinación' desde fila 4...")
                df_temp = pd.read_excel(archivo, sheet_name='Coordinación', header=4, engine='openpyxl')
                
            print(f"✅ Lectura inicial completada. Filas encontradas: {len(df_temp)}")
            
            df_temp = normalizar_columnas(df_temp)
            print(f"📋 Columnas normalizadas detectadas: {list(df_temp.columns)}")
            
            if 'orden' in df_temp.columns and 'contrato' in df_temp.columns:
                print("🟢 ¡Éxito! Columnas 'orden' y 'contrato' encontradas.")
                df_temp['orden'] = df_temp['orden'].astype(str).str.replace('.0', '', regex=False).str.strip()
                df_temp['contrato'] = df_temp['contrato'].astype(str).str.replace('.0', '', regex=False).str.strip()
                
                # Filtro Cazafantasmas
                filas_antes = len(df_temp)
                df_temp = df_temp[~df_temp['orden'].isin(['nan', 'None', '', 'NaT', '<NA>', 'null'])]
                filas_despues = len(df_temp)
                print(f"👻 Fantasmas eliminados: {filas_antes - filas_despues}. Registros válidos: {filas_despues}")
                
                if filas_despues > 0:
                    nuevos_registros.append(df_temp)
                else:
                    print("⚠️ El archivo quedó vacío tras eliminar los fantasmas (órdenes nulas).")
            else:
                print("❌ ERROR GRAVE: El archivo no tiene columnas reconocidas como 'orden' o 'contrato'.")
                
            del df_temp 
            gc.collect()

        if nuevos_registros:
            print("🔗 Consolidando registros válidos...")
            df_nuevos = pd.concat(nuevos_registros, ignore_index=True)
            
            print("☁️ Verificando historial en la Nube...")
            df_hist = cargar_tabla_segura(TABLA_HISTORIAL)
            
            # --- PARCHE 1: Normalizar base histórica al descargar ---
            if not df_hist.empty:
                df_hist = normalizar_columnas(df_hist)
            # --------------------------------------------------------
                
            if not df_hist.empty and 'orden' in df_hist.columns:
                filas_antes_hist = len(df_nuevos)
                df_nuevos = df_nuevos[~df_nuevos['orden'].isin(df_hist['orden'].astype(str).tolist())]
                print(f"📦 Órdenes repetidas del historial rechazadas: {filas_antes_hist - len(df_nuevos)}")
            
            if df_nuevos.empty: 
                print("🏁 Proceso detenido: Todas las órdenes ya están archivadas.")
                return "❌ Todas las órdenes cargadas ya están cerradas/archivadas en el historial."

            # PARCHE: Aplicamos la nueva función traductora al cargar los datos
            print("🗓️ Procesando y limpiando formatos de fechas...")
            if 'fecha_programacion' in df_nuevos.columns:
                df_nuevos['fecha_prog_limpia'] = convertir_fechas_espanol(df_nuevos['fecha_programacion'])
            if 'fecha_asignacion' in df_nuevos.columns:
                df_nuevos['fecha_asignacion'] = convertir_fechas_espanol(df_nuevos['fecha_asignacion'])
                
            for col in ['estado_whatsapp', 'estado_ejecucion', 'num_vne', 'municipio', 'estado_visita', 'codigo_tecnico']:
                if col not in df_nuevos.columns:
                    df_nuevos[col] = 0 if col == 'num_vne' else 'SIN DEFINIR' if col in ['municipio', 'codigo_tecnico'] else 'Pendiente' if col != 'estado_visita' else '⏳ Esperando'
            
            if 'meses' in df_nuevos.columns: df_nuevos['meses'] = df_nuevos['meses'].astype(str).str.replace('.0', '', regex=False).str.strip()
                    
            print("☁️ Descargando Base General activa...")
            df_base = cargar_tabla_segura(TABLA_BASE)
            
            # --- PARCHE 2: Normalizar base activa al descargar ---
            if not df_base.empty:
                df_base = normalizar_columnas(df_base)
            # -----------------------------------------------------
                
            if not df_base.empty and 'orden' in df_base.columns:
                print("🔄 Cruzando datos nuevos con los existentes en la nube...")
                df_nuevos = df_nuevos.set_index('orden')
                df_base_index = df_base.set_index('orden')
                cols_exist = [c for c in ['estado_whatsapp', 'estado_ejecucion', 'num_vne', 'estado_visita'] if c in df_base_index.columns and c in df_nuevos.columns]
                
                df_nuevos[cols_exist] = df_nuevos[cols_exist].astype(str)
                df_base_index[cols_exist] = df_base_index[cols_exist].astype(str).replace(['None', 'nan', '<NA>'], 'Pendiente')
                df_nuevos.update(df_base_index[cols_exist])
                
                df_consolidado = pd.concat([df_base[~df_base['orden'].isin(df_nuevos.reset_index()['orden'])], df_nuevos.reset_index()], ignore_index=True)
            else:
                df_consolidado = df_nuevos
                
            print("💾 Guardando consolidado final en la Nube...")
            guardar_tabla(df_consolidado, TABLA_BASE)
            print("=========================================================")
            print("✅ PROCESO COMPLETADO CON ÉXITO")
            print("=========================================================\n")
            return True
            
        print("❌ Proceso detenido: No se generó ningún registro nuevo o las columnas estaban mal escritas.")
        return "❌ Error: El archivo subido no contenía registros válidos o no se detectaron las columnas 'Orden' y 'Contrato'."
        
    except Exception as e:
        print("🚨🚨 ERROR FATAL DETECTADO 🚨🚨")
        print(traceback.format_exc())
        print("=========================================================")
        return f"Error fatal interno del sistema: {str(e)}"

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
    archivos_bases = st.file_uploader("Bases diarias (.csv, .xlsx)", accept_multiple_files=True, key="side_uploader")
    if archivos_bases and st.button("🚀 Procesar", use_container_width=True, key="side_btn"):
        with st.spinner("Limpiando y subiendo... Mire la consola/logs para ver los detalles."):
            res = procesar_nuevas_bases(archivos_bases)
            if res is True:
                st.success("¡Base actualizada!")
                st.rerun()
            else:
                st.error(res)

df_activa = cargar_tabla_segura(TABLA_BASE)

# --- PARCHE 3: NORMALIZAR LA BASE PARA LA PANTALLA ---
if not df_activa.empty:
    df_activa = normalizar_columnas(df_activa)
# -------------------------------------------------------

st.title("🚀 Panel Certi-Redes (Cloud)")

# =========================================================================
# NUEVA MEJORA DE INTERFAZ: Si la base está vacía, pone el cargador al medio
# =========================================================================
if df_activa.empty:
    st.warning("⚠️ La base de datos está vacía actualmente.")
    st.info("👆 Para habilitar todos los módulos, por favor cargue su primera base de datos aquí abajo:")
    st.write("---")
    
    archivos_bases_main = st.file_uploader("Arrastre su archivo Excel o CSV aquí:", accept_multiple_files=True, key="main_uploader")
    if archivos_bases_main and st.button("🚀 Iniciar Procesamiento de Base", use_container_width=True, key="main_btn"):
        with st.spinner("Limpiando formatos, detectando fechas y subiendo a la nube... Mire la consola/logs para ver detalles."):
            res = procesar_nuevas_bases(archivos_bases_main)
            if res is True:
                st.success("¡Base actualizada con éxito!")
                st.rerun()
            else:
                st.error(res)
else:
    # Si la base tiene datos, muestra los módulos normales
    t_wa, t_op, t_ans, t_hist, t_insp = st.tabs(["💬 WhatsApp", "📊 Monitor", "⏱️ ANS", "📦 Historial", "⚙️ Inspectores"])

    with t_wa:
        st.write("### 📅 Generador de Agenda y Mensajería Automática")
        
        col_f1, col_f2 = st.columns([1, 3])
        with col_f1:
            fecha_select = st.date_input("Seleccione la Fecha de Programación:")
            f_str = fecha_select.strftime('%Y-%m-%d')
        
        if 'fecha_programacion' in df_activa.columns:
            df_activa['fecha_prog_limpia'] = convertir_fechas_espanol(df_activa['fecha_programacion'])
            df_dia = df_activa[df_activa['fecha_prog_limpia'] == f_str]
            
            if not df_dia.empty:
                c_prog = len(df_dia)
                # AQUÍ ESTÁ LA CORRECCIÓN DEL CONTADOR: Parámetro na=False para ignorar celdas vacías/flotantes
                c_env = len(df_dia[df_dia['estado_whatsapp'].astype(str).str.upper().str.contains('ENVIADO', na=False)]) if 'estado_whatsapp' in df_dia.columns else 0
                
                st.write("#### 📊 Resumen de Mensajería del Día")
                m1, m2 = st.columns(2)
                m1.metric("📅 Total Programados", c_prog)
                m2.metric("📤 Total Enviados", c_env)
                
                col_btn, _ = st.columns([1, 3])
                with col_btn:
                    if st.button("📤 Enviar Mensajes a la Agenda del Día", type="primary", use_container_width=True):
                        with st.spinner("Verificando credenciales y conectando con Twilio..."):
                            # VALIDACIÓN PREVIA DE SECRETOS EN LA INTERFAZ
                            if "TWILIO_ACCOUNT_SID" not in st.secrets:
                                st.error("🚨 ERROR CRÍTICO: No se encontraron las credenciales de Twilio en los 'Secrets' de la nube.")
                                st.info("👉 Vaya a Streamlit Cloud -> Settings -> Secrets y asegúrese de que TWILIO_ACCOUNT_SID esté escrito correctamente.")
                            else:
                                exito, msj = enviar_mensajes_agenda(df_dia)
                                if exito:
                                    st.success(msj)
                                    st.rerun()
                                else:
                                    st.error(msj)
                
                st.write("#### 📋 Detalle de la Agenda (Con Códigos y Celulares cruzados)")
                cols_vista = ['orden', 'contrato', 'nombre', 'direccion', 'telefono', 'municipio', 'fecha_programacion', 'jornada', 'inspector', 'codigo_tecnico', 'estado_whatsapp']
                columnas_presentes = [c for c in cols_vista if c in df_dia.columns]
                st.dataframe(centrar_df(df_dia[columnas_presentes]), use_container_width=True)
            else:
                st.info(f"Sin agenda para el día {fecha_select.strftime('%d/%m/%Y')}.")
                fechas_disp = df_activa['fecha_prog_limpia'].dropna().unique()
                fechas_disp = [f for f in fechas_disp if str(f) not in ['nan', 'NaT', 'None', '']]
                if len(fechas_disp) > 0:
                    st.warning("🕵️ **El sistema encontró estas fechas disponibles en su base general:**")
                    st.code(", ".join(sorted(fechas_disp)[:20]))
        else:
            st.error("No se detectó la columna 'fecha_programacion' en la base de datos.")

    with t_op:
        st.write("### 📊 Monitor Operativo")
        st.dataframe(centrar_df(df_activa), use_container_width=True)

    with t_ans:
        st.write("### ⏱️ Control ANS")
        if 'fecha_asignacion' in df_activa.columns:
            st.dataframe(centrar_df(df_activa[['orden', 'fecha_asignacion', 'tipo_orden', 'estado_ejecucion']]), use_container_width=True)

    with t_hist:
        st.write("### 📦 Historial")
        df_h = cargar_tabla_segura(TABLA_HISTORIAL)
        if not df_h.empty: 
            df_h = normalizar_columnas(df_h)
            st.dataframe(centrar_df(df_h), use_container_width=True)

    with t_insp:
        st.write("### ⚙️ Directorio de Inspectores")
        df_insp = cargar_tabla_segura(TABLA_INSPECTORES)
        c1, c2 = st.columns([1, 2])
        with c1:
            with st.form("f_insp"):
                f_cod = st.text_input("Código Técnico (Ej: 321)")
                f_ced = st.text_input("Cédula")
                f_nom = st.text_input("Nombre")
                f_cel = st.text_input("Celular (Sin +57)")
                if st.form_submit_button("Guardar"):
                    nuevo = pd.DataFrame([{'codigo_tecnico': f_cod.strip(), 'cedula': f_ced.strip(), 'nombre': f_nom.strip(), 'celular': f_cel.strip()}])
                    if not df_insp.empty:
                        df_final = pd.concat([df_insp[df_insp['codigo_tecnico'] != f_cod.strip()], nuevo], ignore_index=True)
                    else: df_final = nuevo
                    guardar_tabla(df_final, TABLA_INSPECTORES)
                    st.rerun()
        with c2:
            if not df_insp.empty: st.dataframe(centrar_df(df_insp), use_container_width=True)