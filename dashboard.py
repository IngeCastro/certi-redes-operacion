import streamlit as st
import pandas as pd
from PIL import Image
import io
import plotly.express as px  
import datetime
from sqlalchemy import create_engine
import os
import gc # Limpiador de memoria RAM
import time # Controlador de tiempos para la subida a la nube

# ==========================================
# 0. CONSTANTES Y CONFIGURACIÓN
# ==========================================
st.set_page_config(layout="wide", page_title="Panel Certi-Redes", page_icon="✅")

# Ampliar el límite de celdas a 5 millones para Pandas Styler
pd.set_option("styler.render.max_elements", 5000000)

# ==========================================
# 1. CONEXIÓN A BASE DE DATOS SUPABASE
# ==========================================
@st.cache_resource
def init_connection():
    try:
        # Se conecta usando el secreto guardado en su bóveda .streamlit/secrets.toml
        uri = st.secrets["SUPABASE_URI"]
        if "sslmode" not in uri:
            uri += "?sslmode=require"
        # Ajustamos el pool para que sea más resistente a archivos grandes
        return create_engine(uri, pool_size=10, max_overflow=20)
    except Exception as e:
        st.error(f"⚠️ Error fatal de conexión a la Base de Datos: {e}")
        st.stop()

engine = init_connection()

# Funciones maestras de Lectura/Escritura a la Nube
@st.cache_data(ttl=600) # Guarda los datos por 10 minutos
def cargar_tabla(nombre_tabla):
    """Descarga la tabla desde Supabase a la memoria."""
    try:
        df = pd.read_sql_table(nombre_tabla, engine)
        # VITAL: Pasamos todo a minúsculas para evitar KeyErrors por diferencias entre SQL y Pandas
        df.columns = df.columns.astype(str).str.strip().str.lower()
        return df.astype(str) 
    except ValueError:
        # Si da ValueError es porque la tabla aún no existe
        return pd.DataFrame()
    except Exception as e:
        print(f"Error al leer la base de datos ({nombre_tabla}): {e}")
        return pd.DataFrame()

def guardar_tabla(df, nombre_tabla):
    """Sube la tabla a Supabase con control de ancho de banda para evitar WebSocketClosedError."""
    try:
        print(f"-> Subiendo {len(df)} filas a la nube...")
        if df.empty:
            df.to_sql(nombre_tabla, engine, if_exists='replace', index=False)
            return
            
        # 1. Creamos la estructura de la tabla vacía
        df.head(0).to_sql(nombre_tabla, engine, if_exists='replace', index=False)
        
        # 2. Subimos los datos por goteo para no saturar el internet local
        chunk_size = 500
        progreso = st.progress(0, text="Iniciando transmisión segura a la nube...")
        
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            chunk.to_sql(nombre_tabla, engine, if_exists='append', index=False)
            
            # Actualizamos la barra de progreso
            avance = min((i + len(chunk)) / len(df), 1.0)
            progreso.progress(avance, text=f"☁️ Subiendo bloque {i//chunk_size + 1}... ({i+len(chunk)}/{len(df)} filas)")
            
            # Micro-pausa vital para que Streamlit mantenga viva la conexión y no arroje StreamClosedError
            time.sleep(0.5) 
            
        print("-> ¡Subida a la nube exitosa!")
        st.cache_data.clear() # Limpiamos caché para ver los datos nuevos inmediatamente
    except Exception as e:
        print(f"❌ Error crítico al guardar en la nube: {e}")
        raise e

# Nombres de las tablas en la nube
TABLA_BASE = 'base_general'
TABLA_HISTORIAL = 'historial_certiredes'

# ==========================================
# 2. FUNCIONES DEL MOTOR DE DATOS
# ==========================================

def normalizar_columnas(df):
    """Limpia y estandariza los nombres de las columnas para evitar errores de espacios."""
    df.columns = df.columns.astype(str).str.strip().str.lower()
    
    # Unificamos los nombres de su Excel con lo que pide el Dashboard
    mapeo = {
        'orden': 'orden', 'contrato': 'contrato', 'nombre': 'nombre', 
        'dirección': 'direccion', 'direccion': 'direccion', 'telefono': 'telefono',
        'fecha programación': 'fecha_programacion', 'fecha programacion': 'fecha_programacion',
        'jornada': 'jornada', 
        'tipo orden': 'tipo_orden', 
        'tipo trabajo': 'tipo_trabajo', 
        'fecha asignación': 'fecha_asignacion', 'fecha asignacion': 'fecha_asignacion', 
        '# vne': 'num_vne', 'consumo': 'consumo', 'meses': 'meses',
        'cabecera': 'municipio', 'cabeceras': 'municipio',
        'nombre técnico': 'inspector', 'estado gestión': 'estado_ejecucion'
    }
    df.rename(columns=mapeo, inplace=True)
    
    # Eliminamos copias de columnas para seguridad
    df = df.loc[:, ~df.columns.duplicated()]
    return df

def procesar_nuevas_bases(archivos_subidos):
    """Lee los Excels/CSV, rechaza órdenes del historial y actualiza la base en la Nube."""
    try:
        print("\n=== INICIANDO PROCESO DE CARGA Y DETECCIÓN ===")
        nuevos_registros = []
        
        for archivo in archivos_subidos:
            print(f"Paso 1: Leyendo el archivo '{archivo.name}'...")
            
            # LÓGICA ROBUSTA PARA CSV (Auto-detecta separadores y encabezados)
            if archivo.name.lower().endswith('.csv'):
                contenido = archivo.getvalue().decode('utf-8-sig', errors='replace')
                sep = ';' if ';' in contenido.split('\n')[0] else ','
                
                # Intentamos lectura estándar (fila 0 como encabezado)
                df_temp = pd.read_csv(io.StringIO(contenido), sep=sep, low_memory=False)
                
                # Si no encuentra las columnas, intentamos saltando 4 líneas (formato Excel bruto)
                columnas_test = df_temp.columns.astype(str).str.strip().str.lower()
                if 'orden' not in columnas_test and 'contrato' not in columnas_test:
                    archivo.seek(0)
                    df_temp = pd.read_csv(io.StringIO(contenido), header=4, sep=sep, low_memory=False)
            else:
                if archivo.size > 50 * 1024 * 1024:
                    st.warning(f"⚠️ El archivo '{archivo.name}' es muy pesado. Recomendamos guardarlo como '.csv' si el proceso falla.")
                df_temp = pd.read_excel(archivo, sheet_name='Coordinación', header=4, engine='openpyxl')
                
            print(f"-> Archivo leído. Filas encontradas: {len(df_temp)}")
            
            print("Paso 2: Normalizando nombres de columnas...")
            df_temp = normalizar_columnas(df_temp)
            
            if 'orden' in df_temp.columns and 'contrato' in df_temp.columns:
                print("-> Limpiando columnas clave (Orden y Contrato)...")
                df_temp['orden'] = df_temp['orden'].astype(str).str.replace('.0', '', regex=False).str.strip()
                df_temp['contrato'] = df_temp['contrato'].astype(str).str.replace('.0', '', regex=False).str.strip()
                df_temp = df_temp[df_temp['orden'] != 'nan'] 
                nuevos_registros.append(df_temp)
            else:
                print("❌ ADVERTENCIA: El archivo no tiene columnas 'Orden' o 'Contrato'. Revise el formato.")
                
            del df_temp 
            gc.collect()

        if nuevos_registros:
            print("Paso 3: Consolidando los archivos subidos...")
            df_nuevos = pd.concat(nuevos_registros, ignore_index=True)
            
            print("Paso 4: Verificando historial en la nube para rechazar repetidas...")
            df_hist = cargar_tabla(TABLA_HISTORIAL)
            if not df_hist.empty and 'orden' in df_hist.columns:
                ordenes_cerradas = df_hist['orden'].astype(str).tolist()
                df_nuevos = df_nuevos[~df_nuevos['orden'].isin(ordenes_cerradas)]
            
            if df_nuevos.empty:
                print("-> Fin: Las bases cargadas solo contenían órdenes ya cerradas.")
                return "Las bases cargadas solo contenían órdenes que ya fueron cerradas y archivadas anteriormente."

            print("Paso 5: Dando formato a las fechas...")
            if 'fecha_programacion' in df_nuevos.columns:
                df_nuevos['fecha_prog_limpia'] = pd.to_datetime(df_nuevos['fecha_programacion'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
            if 'fecha_asignacion' in df_nuevos.columns:
                df_nuevos['fecha_asignacion'] = pd.to_datetime(df_nuevos['fecha_asignacion'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
                
            print("Paso 6: Asegurando columnas operativas...")
            for col in ['estado_whatsapp', 'estado_ejecucion', 'num_vne', 'municipio', 'estado_visita']:
                if col not in df_nuevos.columns:
                    if col == 'num_vne': df_nuevos[col] = 0
                    elif col == 'municipio': df_nuevos[col] = 'SIN DEFINIR'
                    elif col == 'estado_ejecucion': df_nuevos[col] = 'Pendiente'
                    elif col == 'estado_visita': df_nuevos[col] = '⏳ Esperando'
                    else: df_nuevos[col] = 'Pendiente'
            
            if 'meses' in df_nuevos.columns:
                df_nuevos['meses'] = df_nuevos['meses'].astype(str).str.replace('.0', '', regex=False).str.strip()
                    
            print("Paso 7: Mezclando datos nuevos con la base activa en la Nube...")
            df_base = cargar_tabla(TABLA_BASE)
            if not df_base.empty and 'orden' in df_base.columns:
                df_nuevos = df_nuevos.set_index('orden')
                df_base_index = df_base.set_index('orden')
                
                columnas_protegidas = ['estado_whatsapp', 'estado_ejecucion', 'num_vne', 'estado_visita']
                cols_existentes = [c for c in columnas_protegidas if c in df_base_index.columns and c in df_nuevos.columns]
                
                df_nuevos.update(df_base_index[cols_existentes])
                df_nuevos = df_nuevos.reset_index()
                
                df_base = df_base[~df_base['orden'].isin(df_nuevos['orden'])]
                df_consolidado = pd.concat([df_base, df_nuevos], ignore_index=True)
            else:
                df_consolidado = df_nuevos
                
            print("Paso 8: Iniciando la transmisión final a Supabase...")
            guardar_tabla(df_consolidado, TABLA_BASE)
            print("=== PROCESO TERMINADO CON ÉXITO ===")
            return True
        return False
        
    except Exception as e:
        mensaje_error = f"Tipo de error: {type(e).__name__} | Mensaje: {str(e)}"
        print("\n" + "="*40)
        print("🚨 ¡ALERTA DE FALLO CRÍTICO EN EL MOTOR! 🚨")
        print(mensaje_error)
        print("="*40 + "\n")
        return f"Error fatal detectado por el sistema: {mensaje_error}"

def procesar_godoworks(archivo_godo):
    """Procesa el CSV de GoDoWorks, archiva cumplidas y suma VNE en la Nube."""
    try:
        if archivo_godo.name.lower().endswith('.csv'):
            contenido = archivo_godo.getvalue().decode('utf-8-sig', errors='replace')
            sep = ';' if ';' in contenido.split('\n')[0] else ','
            df_godo = pd.read_csv(io.StringIO(contenido), sep=sep, dtype=str)
        else:
            df_godo = pd.read_excel(archivo_godo, dtype=str)
            
        df_godo.columns = df_godo.columns.astype(str).str.strip().str.upper()
        col_contrato = next((col for col in df_godo.columns if 'CONTRATO' in col), None)
        col_estado = next((col for col in df_godo.columns if 'ESTADO' in col), None)

        if not col_contrato or not col_estado:
            st.error("El archivo GoDoWorks no tiene columnas CONTRATO o ESTADO.")
            return False

        df_godo[col_contrato] = df_godo[col_contrato].astype(str).str.replace('.0', '', regex=False).str.strip()
        
        df_base = cargar_tabla(TABLA_BASE)
        if df_base.empty or 'contrato' not in df_base.columns:
            st.warning("No hay una Base General activa en la nube para actualizar.")
            return False
            
        df_historial = cargar_tabla(TABLA_HISTORIAL)
        
        ordenes_a_archivar = []
        
        for _, row_godo in df_godo.iterrows():
            contrato = row_godo[col_contrato]
            estado = str(row_godo[col_estado]).strip().upper()
            
            mask = df_base['contrato'] == contrato
            if mask.any():
                if any(val in estado for val in ["CERTIFICADO", "NO CERTIFICADO", "EJECUTADA"]): 
                    ordenes_a_archivar.extend(df_base.loc[mask, 'orden'].tolist())
                elif any(val in estado for val in ["VISITA NO EFECTIVA", "NO EFECTIVA"]):
                    df_base.loc[mask, 'num_vne'] = pd.to_numeric(df_base.loc[mask, 'num_vne'], errors='coerce').fillna(0) + 1
                    df_base.loc[mask, 'estado_ejecucion'] = '❌ No efectiva'

        if ordenes_a_archivar:
            df_cumplidas = df_base[df_base['orden'].isin(ordenes_a_archivar)].copy()
            df_cumplidas['estado_ejecucion'] = '✅ Cumplida (Archivada)'
            df_historial = pd.concat([df_historial, df_cumplidas], ignore_index=True)
            guardar_tabla(df_historial, TABLA_HISTORIAL)
            
            df_base = df_base[~df_base['orden'].isin(ordenes_a_archivar)]

        guardar_tabla(df_base, TABLA_BASE)
        return True
    except Exception as e:
        st.error(f"Error procesando GoDoWorks: {e}")
        return False

# ==========================================
# 3. BARRA LATERAL (MENÚ DE CARGA)
# ==========================================
with st.sidebar:
    try: st.image(Image.open('Logo_CertiRedes_Transparente.png'), width=200)
    except: st.write("### CERTI-REDES S.A.S")
    st.markdown("🟢 **Conectado a la Nube (Supabase)**")
    
    st.markdown("---")
    st.markdown("### 📥 1. ALIMENTAR BASE GENERAL")
    st.info("Suba sus bases diarias. El sistema rechazará automáticamente las órdenes que ya son Efectivas en la Nube.")
    archivos_bases = st.file_uploader("Seleccionar Bases (.xlsm/.xlsx/.csv)", type=["xlsm", "xlsx", "csv"], accept_multiple_files=True)
    if archivos_bases and st.button("🚀 Procesar Bases de Datos", use_container_width=True):
        with st.spinner("Iniciando motor de datos. Por favor, no recargue la página..."):
            resultado = procesar_nuevas_bases(archivos_bases)
            if resultado is True:
                st.success("¡Base General actualizada en la Nube correctamente!")
                st.rerun()
            elif isinstance(resultado, str):
                st.error(resultado)

    st.markdown("---")
    st.markdown("### 🛠️ 2. ACTUALIZAR EJECUCIÓN (GoDoWorks)")
    st.info("Sube el archivo de GoDoWorks. Las órdenes 'Cumplidas' irán al Historial y las 'No Efectivas' sumarán VNE.")
    archivo_godo = st.file_uploader("Subir reporte GoDoWorks", type=["csv", "xlsx"])
    if archivo_godo and st.button("🔄 Ejecutar Cruce Automático", use_container_width=True):
        with st.spinner("Sincronizando estados en la Nube..."):
            if procesar_godoworks(archivo_godo):
                st.success("¡Cruce realizado! Órdenes actualizadas y/o archivadas en la Nube.")
                st.rerun()

# ==========================================
# 4. LECTURA DE LA NUBE PARA VISUALIZACIÓN
# ==========================================
df_activa = cargar_tabla(TABLA_BASE)

# Reparación automática en memoria por si faltan columnas
if not df_activa.empty:
    if 'consumo' not in df_activa.columns: df_activa['consumo'] = 'N/A'
    if 'meses' not in df_activa.columns: df_activa['meses'] = 'N/A'
    if 'tipo_orden' not in df_activa.columns: df_activa['tipo_orden'] = 'POR DEFECTO'
    if 'municipio' not in df_activa.columns: df_activa['municipio'] = 'SIN DEFINIR'
    if 'estado_ejecucion' not in df_activa.columns: df_activa['estado_ejecucion'] = 'Pendiente'
    if 'estado_visita' not in df_activa.columns: df_activa['estado_visita'] = '⏳ Esperando'

# ==========================================
# 5. INTERFAZ PRINCIPAL (PESTAÑAS)
# ==========================================

def centrar_df(df_o_styler):
    if isinstance(df_o_styler, pd.DataFrame):
        df_str = df_o_styler.fillna('').astype(str)
        styler = df_str.style
    else:
        styler = df_o_styler
        styler = styler.format(lambda x: str(x) if pd.notnull(x) else '')
            
    return styler.set_properties(**{'text-align': 'center !important'}).set_table_styles([
        {'selector': 'th', 'props': [('text-align', 'center !important')]},
        {'selector': 'td', 'props': [('text-align', 'center !important')]}
    ])

st.title("🚀 Panel de Control Operativo - Certi-Redes (Cloud)")

if df_activa.empty:
    st.warning("⚠️ La Base General en la Nube está vacía. Por favor, cargue los archivos de Excel en el panel lateral para crearla.")
else:
    tab_wa, tab_op, tab_ans, tab_hist = st.tabs([
        "💬 1. Módulo WhatsApp (Agenda)", 
        "📊 2. Monitor Operativo", 
        "⏱️ 3. Auditoría de Tiempos (ANS)",
        "📦 4. Historial Archivadas"
    ])

    # ------------------------------------------
    # TAB 1: MÓDULO WHATSAPP Y AGENDA
    # ------------------------------------------
    with tab_wa:
        st.write("### 📅 Generador de Agenda y Mensajería")
        
        col_f1, col_f2 = st.columns([1, 3])
        with col_f1:
            fecha_select = st.date_input("Seleccione la Fecha de Programación:")
            fecha_str = fecha_select.strftime('%Y-%m-%d')
            
        if 'fecha_prog_limpia' in df_activa.columns:
            df_agenda_dia = df_activa[df_activa['fecha_prog_limpia'] == fecha_str].copy()
            
            if df_agenda_dia.empty:
                st.info(f"No hay órdenes programadas para el {fecha_select.strftime('%d/%m/%Y')}.")
            else:
                c_prog = len(df_agenda_dia)
                c_env = len(df_agenda_dia[df_agenda_dia['estado_whatsapp'].astype(str).str.upper().str.contains('ENVIADO')])
                c_conf = len(df_agenda_dia[df_agenda_dia['estado_visita'].astype(str).str.upper().str.contains('CONFIRMADO')])
                c_canc = len(df_agenda_dia[df_agenda_dia['estado_visita'].astype(str).str.upper().str.contains('CANCELADO')])

                st.write("#### 📊 Resumen de Mensajería del Día")
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("📅 Total Programados", c_prog)
                m2.metric("📤 Total Enviados", c_env)
                m3.metric("✅ Confirmados", c_conf)
                m4.metric("❌ Cancelados", c_canc)

                st.markdown("---")
                
                col_btn, _ = st.columns([1, 3])
                with col_btn:
                    if st.button("📤 Enviar Mensajes a la Agenda del Día", type="primary", use_container_width=True):
                        ordenes_dia = df_agenda_dia['orden'].tolist()
                        df_temp = cargar_tabla(TABLA_BASE)
                        df_temp.loc[df_temp['orden'].isin(ordenes_dia), 'estado_whatsapp'] = '✅ MSJ ENVIADO'
                        guardar_tabla(df_temp, TABLA_BASE)
                        st.success("Toda la agenda del día ha sido marcada como 'Enviada' en la Nube.")
                        st.rerun()
                
                st.write("#### 📋 Detalle de la Agenda")
                cols_vista = ['orden', 'contrato', 'nombre', 'direccion', 'telefono', 'jornada', 'num_vne', 'estado_whatsapp', 'estado_visita']
                columnas_presentes = [c for c in cols_vista if c in df_agenda_dia.columns]
                
                st.dataframe(centrar_df(df_agenda_dia[columnas_presentes]), use_container_width=True)
                
        else:
            st.error("La columna de Fecha de Programación no fue encontrada en la base.")

    # ------------------------------------------
    # TAB 2: MONITOR OPERATIVO GENERAL
    # ------------------------------------------
    with tab_op:
        st.write("### 📊 Monitor de Base Activa General")
        
        activos_count = df_activa['consumo'].astype(str).str.upper().str.contains('ACTIVO', na=False).sum()
        suspendidos_count = df_activa['consumo'].astype(str).str.upper().str.contains('SUSPENDIDO', na=False).sum()
        meses_60_count = (df_activa['meses'].astype(str).str.replace('.0', '', regex=False).str.strip() == '60').sum()

        col_izq_op, col_der_op = st.columns([1, 1.2])
        
        with col_izq_op:
            st.write("#### 📈 Métricas Generales")
            c1, c2 = st.columns(2)
            c1.metric("📋 Total Órdenes Activas", len(df_activa))
            c2.metric("⏳ Esperando Ejecución", len(df_activa[df_activa['estado_ejecucion'] == 'Pendiente']))
            
            c3, c4 = st.columns(2)
            c3.metric("🟢 Serv. Activos", activos_count)
            c4.metric("🔴 Serv. Suspendidos", suspendidos_count)
            
            st.metric("📅 60 Meses (Total)", meses_60_count)

        with col_der_op:
            st.write("#### 📋 Resúmenes Operativos")
            tab_res_tipo, tab_res_muni = st.tabs(["🛠️ Por Tipo de Trabajo", "📍 Por Municipio"])
            
            with tab_res_tipo:
                resumen_op = df_activa.groupby(['tipo_orden', 'estado_ejecucion']).size().unstack(fill_value=0).reset_index()
                for col in ['Pendiente', '❌ No efectiva', '✅ Cumplida (Archivada)']:
                    if col not in resumen_op.columns: resumen_op[col] = 0
                resumen_op['TOTAL'] = resumen_op.iloc[:, 1:].sum(axis=1)
                resumen_op.rename(columns={'tipo_orden': 'Tipo Trabajo'}, inplace=True)
                resumen_op = resumen_op.sort_values(by='TOTAL', ascending=False)
                resumen_op.set_index('Tipo Trabajo', inplace=True)
                
                st.table(centrar_df(resumen_op))
            
            with tab_res_muni:
                resumen_muni = df_activa.groupby(['municipio', 'estado_ejecucion']).size().unstack(fill_value=0).reset_index()
                for col in ['Pendiente', '❌ No efectiva', '✅ Cumplida (Archivada)']:
                    if col not in resumen_muni.columns: resumen_muni[col] = 0
                resumen_muni['TOTAL'] = resumen_muni.iloc[:, 1:].sum(axis=1)
                resumen_muni.rename(columns={'municipio': 'Municipio'}, inplace=True)
                resumen_muni = resumen_muni.sort_values(by='TOTAL', ascending=False)
                resumen_muni.set_index('Municipio', inplace=True)
                
                st.table(centrar_df(resumen_muni))

        st.markdown("---")
        st.write("#### 🗃️ Detalle de Base Activa Completa")
        st.dataframe(centrar_df(df_activa), use_container_width=True)

    # ------------------------------------------
    # TAB 3: AUDITORÍA DE TIEMPOS (ANS)
    # ------------------------------------------
    with tab_ans:
        st.write("### ⏱️ Control de Acuerdos de Nivel de Servicio (Pendientes)")
        
        if 'fecha_asignacion' in df_activa.columns and 'tipo_orden' in df_activa.columns:
            df_ans = df_activa[df_activa['estado_ejecucion'] != '✅ Cumplida (Archivada)'].copy()
            
            mask_masivas = df_ans['tipo_orden'].astype(str).str.upper().str.contains('10444|MASIVA', regex=True, na=False)
            df_ans = df_ans[~mask_masivas]
            
            df_ans['fecha_asig_dt'] = pd.to_datetime(df_ans['fecha_asignacion'], errors='coerce')
            
            if 'num_vne' in df_ans.columns:
                df_ans['num_vne'] = pd.to_numeric(df_ans['num_vne'], errors='coerce').fillna(0).astype(int)
            else:
                df_ans['num_vne'] = 0
                
            c_61 = df_ans['tipo_orden'].astype(str).str.upper().str.contains('61', na=False).sum()
            c_63 = df_ans['tipo_orden'].astype(str).str.upper().str.contains('63', na=False).sum()
            c_64 = df_ans['tipo_orden'].astype(str).str.upper().str.contains('64', na=False).sum()
            
            mask_tipos_obj = df_ans['tipo_orden'].astype(str).str.upper().str.contains('61|63|64', regex=True, na=False)
            mask_60m = df_ans['meses'].astype(str).str.replace('.0', '', regex=False).str.strip() == '60'
            c_60m_obj = (mask_tipos_obj & mask_60m).sum()
            
            def calcular_ans(row):
                tipo = str(row.get('tipo_orden', '')).strip().upper()
                consumo = str(row.get('consumo', '')).strip().upper()
                fecha_asig = row['fecha_asig_dt']
                
                if pd.isnull(fecha_asig): return "N/A", "Sin Fecha", 0, 0
                
                ahora = datetime.datetime.now()
                dias_sistema = (ahora - fecha_asig).days
                if dias_sistema < 0: dias_sistema = 0
                
                dias_habiles = 5 
                
                if '61' in tipo or '12161' in tipo:
                    dias_habiles = 2 if 'SUSPENDIDO' in consumo else 6
                elif '12162' in tipo:
                    dias_habiles = 2 if 'SUSPENDIDO' in consumo else 5
                elif '63' in tipo or '12163' in tipo:
                    dias_habiles = 2 if 'SUSPENDIDO' in consumo else 8
                elif '64' in tipo or '12164' in tipo:
                    dias_habiles = 2 if 'SUSPENDIDO' in consumo else 8
                else:
                    if 'SUSPENDIDO' in consumo:
                        dias_habiles = 2
                        
                vencimiento = fecha_asig + pd.offsets.BDay(dias_habiles)
                vencimiento = vencimiento.replace(hour=23, minute=59)
                
                diferencia = vencimiento - ahora
                horas_restantes = diferencia.total_seconds() / 3600
                
                if horas_restantes < 0: 
                    dias_retraso = abs(int(horas_restantes / 24))
                    return "🔴 VENCIDO", f"Venció hace {dias_retraso} días", horas_restantes, dias_sistema
                elif horas_restantes < 24: 
                    return "🟡 POR VENCER", f"Vence en {int(horas_restantes)}h", horas_restantes, dias_sistema
                else: 
                    dias_restantes = int(horas_restantes / 24)
                    return "🟢 A TIEMPO", f"Vence en {dias_restantes} días", horas_restantes, dias_sistema

            if not df_ans.empty:
                ans_results = df_ans.apply(calcular_ans, axis=1)
                df_ans['Estado ANS'] = [r[0] for r in ans_results]
                df_ans['Tiempo Restante'] = [r[1] for r in ans_results]
                df_ans['horas_num'] = [r[2] for r in ans_results]
                df_ans['Días Sistema'] = [r[3] for r in ans_results]
                
                col_izq_ans, col_der_ans = st.columns([1, 1.2])

                with col_izq_ans:
                    st.write("#### 📊 Contadores Especiales")
                    col_ce1, col_ce2 = st.columns(2)
                    col_ce1.metric("📌 Ext. 61 / 12161", c_61)
                    col_ce2.metric("📌 Ext. 63 / 12163", c_63)
                    
                    col_ce3, col_ce4 = st.columns(2)
                    col_ce3.metric("📌 Ext. 64 / 12164", c_64)
                    col_ce4.metric("📅 60 Meses (Objetivo)", c_60m_obj)
                    
                    st.markdown("---")
                    st.write("#### ⚠️ Alertas de Tiempos")
                    m1, m2 = st.columns(2)
                    m1.metric("🔴 Vencidos Totales", len(df_ans[df_ans['Estado ANS'] == "🔴 VENCIDO"]))
                    m2.metric("🟡 Críticos Totales (24h)", len(df_ans[df_ans['Estado ANS'] == "🟡 POR VENCER"]))

                with col_der_ans:
                    st.write("#### 📋 ANS por Tipo de Trabajo")
                    resumen_ans = df_ans.groupby(['tipo_orden', 'Estado ANS']).size().unstack(fill_value=0).reset_index()
                    for c in ["🔴 VENCIDO", "🟡 POR VENCER", "🟢 A TIEMPO"]:
                        if c not in resumen_ans.columns: resumen_ans[c] = 0
                    resumen_ans['TOTAL'] = resumen_ans["🔴 VENCIDO"] + resumen_ans["🟡 POR VENCER"] + resumen_ans["🟢 A TIEMPO"]
                    resumen_ans.rename(columns={'tipo_orden': 'Tipo Trabajo'}, inplace=True)
                    resumen_ans.set_index('Tipo Trabajo', inplace=True)
                    
                    st.table(centrar_df(resumen_ans))
                
                st.markdown("---")
                st.write("#### 📈 Rendimiento Operativo de Tiempos")
                x_col = "inspector" if "inspector" in df_ans.columns else "tipo_orden"
                fig_ans = px.bar(df_ans, x=x_col, color="Estado ANS", 
                                title=f"Distribución de Órdenes ({x_col.replace('_', ' ').title()})",
                                color_discrete_map={"🔴 VENCIDO": "#c62828", "🟡 POR VENCER": "#f57f17", "🟢 A TIEMPO": "#2e7d32"})
                st.plotly_chart(fig_ans, use_container_width=True)

                st.markdown("---")
                st.write("#### 🕵️ Auditoría Detallada de Tiempos y Visitas")
                cols_ans = ['Estado ANS', 'Tiempo Restante', 'Días Sistema', 'num_vne', 'fecha_asignacion', 'orden', 'contrato', 'tipo_orden', 'consumo']
                cols_disponibles = [c for c in cols_ans if c in df_ans.columns]
                
                df_ans_disp = df_ans.sort_values('horas_num')[cols_disponibles]
                
                map_nombres = {
                    'Estado ANS': 'Estado', 'Tiempo Restante': 'Reloj ANS (Hábiles)', 'Días Sistema': 'Días Sistema (Calendario)', 
                    'num_vne': '# VNE', 'fecha_asignacion': 'F. Asignación', 'orden': 'Orden', 'contrato': 'Contrato',
                    'tipo_orden': 'Tipo Trabajo', 'consumo': 'Estado Consumo'
                }
                df_ans_disp.rename(columns=map_nombres, inplace=True)
                
                def style_ans(val):
                    if 'VENCIDO' in str(val): return 'background-color: #ffebee; color: #c62828; font-weight: bold;'
                    if 'POR VENCER' in str(val): return 'background-color: #fffde7; color: #f57f17; font-weight: bold;'
                    if 'A TIEMPO' in str(val): return 'background-color: #e8f5e9; color: #2e7d32;'
                    return ''

                st.dataframe(centrar_df(df_ans_disp.style.map(style_ans, subset=['Estado'])), use_container_width=True)
            else:
                st.success("🎉 ¡No hay órdenes bajo seguimiento ANS actualmente!")
        else:
            st.warning("Las columnas 'Fecha asignación' o 'Tipo orden' no se detectaron en su matriz base para calcular los ANS.")

    # ------------------------------------------
    # TAB 4: HISTORIAL
    # ------------------------------------------
    with tab_hist:
        st.write("### 📦 Repositorio de Órdenes Cumplidas y Archivadas en la Nube")
        st.info("Aquí reposan todas las órdenes que cruzaron como 'Certificadas' o 'Cumplidas'. Estas ya no afectan la Base General.")
        
        df_hist_view = cargar_tabla(TABLA_HISTORIAL)
        
        if not df_hist_view.empty:
            st.metric("Total Órdenes Históricas", len(df_hist_view))
            st.dataframe(centrar_df(df_hist_view), use_container_width=True)
            
            buf = io.BytesIO()
            df_hist_view.to_excel(buf, index=False)
            st.download_button("📥 Descargar Historial Completo (Excel)", buf.getvalue(), "historial_completo.xlsx", use_container_width=True)
        else:
            st.info("El historial de la base de datos está vacío. Aún no se han cruzado órdenes cumplidas.")