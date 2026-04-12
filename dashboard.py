import streamlit as st
import pandas as pd
from PIL import Image
import io
import plotly.express as px  
import datetime
import gc 
import traceback # Para capturar errores exactos en la consola

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
    """Convierte fechas con meses en texto (ej: 12-abr-26) a formato YYYY-MM-DD."""
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
        # MAPEO DE ESTADO FINAL: Si la columna se llama solo "estado", la mapeamos a "estado_visita"
        elif col == 'estado' or col == 'estado visita' or col == 'estado de la orden': nuevos_nombres[col] = 'estado_visita'

    df.rename(columns=nuevos_nombres, inplace=True)
    return df.loc[:, ~df.columns.duplicated()]

def procesar_nuevas_bases(archivos_subidos):
    """
    Motor principal de carga y actualización cruzando ahora por CONTRATO.
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
                
                # --- PARCHE PARA ARCHIVOS DE EJECUCIÓN ---
                # Elimina las comillas dobles que algunos sistemas usan para encerrar toda la fila
                contenido = contenido.replace('"', '')
                
                sep = ';' if ';' in contenido.split('\n')[0] else ','
                df_temp = pd.read_csv(io.StringIO(contenido), sep=sep, low_memory=False)
                columnas_test = df_temp.columns.astype(str).str.strip().str.lower()
                
                # Agregamos 'ot' a la validación para que no se salte filas
                if 'orden' not in columnas_test and 'contrato' not in columnas_test and 'ot' not in columnas_test:
                    df_temp = pd.read_csv(io.StringIO(contenido), header=4, sep=sep, low_memory=False)
            else:
                try:
                    df_temp = pd.read_excel(archivo, sheet_name='Coordinación', header=4, engine='openpyxl')
                except:
                    df_temp = pd.read_excel(archivo, header=0, engine='openpyxl')
                
            df_temp = normalizar_columnas(df_temp)
            
            # --- CAMBIO DE LÓGICA: AHORA UTILIZAMOS EL CONTRATO COMO LLAVE MAESTRA ---
            if 'contrato' in df_temp.columns:
                df_temp['contrato'] = df_temp['contrato'].astype(str).str.replace('.0', '', regex=False).str.strip()
                if 'orden' in df_temp.columns:
                    df_temp['orden'] = df_temp['orden'].astype(str).str.replace('.0', '', regex=False).str.strip()
                
                # Filtro Cazafantasmas (ahora por contrato)
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
                # Eliminamos duplicados por contrato para no romper el índice
                df_nuevos = df_nuevos.drop_duplicates(subset=['contrato'], keep='last')
                df_base_index = df_base.drop_duplicates(subset=['contrato'], keep='last')
                
                df_nuevos = df_nuevos.set_index('contrato')
                df_base_index = df_base_index.set_index('contrato')
                
                # NUEVA LÓGICA DE CRUCE POR CONTRATO
                for col in df_base_index.columns:
                    if col not in df_nuevos.columns:
                        # Si el nuevo archivo no trae esta columna, conservamos la vieja
                        df_nuevos[col] = df_base_index[col]
                    else:
                        # Llenamos las celdas vacías del nuevo con las del viejo
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
                
                # Juntamos lo que no se tocó de la base con lo que se actualizó/creó
                df_consolidado = pd.concat([
                    df_base_index[~df_base_index.index.isin(df_nuevos.index)].reset_index(),
                    df_nuevos.reset_index()
                ], ignore_index=True)
            else:
                for col in ['estado_visita', 'estado_ejecucion']:
                    df_nuevos[col] = df_nuevos[col].fillna('Pendiente')
                df_nuevos['num_vne'] = df_nuevos['num_vne'].fillna(0)
                df_consolidado = df_nuevos
                
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

# NUEVA FUNCIÓN PARA AÑADIR ICONOS VISUALES
def formatear_estado_visita(df):
    """Añade iconos visuales a la columna estado_visita para que resalte más."""
    df_formateado = df.copy()
    if 'estado_visita' in df_formateado.columns:
        def agregar_icono(estado):
            estado_str = str(estado).upper().strip()
            if 'CERTIFICADO' in estado_str or 'NO CERTIFICADO' in estado_str:
                return f"✅ {estado}"
            elif 'VISITA NO EFECTIVA' in estado_str or 'VNE' in estado_str or 'NO EFECTIVA' in estado_str:
                return f"❌ {estado}"
            elif 'PENDIENTE' in estado_str:
                return f"⏳ {estado}"
            else:
                return estado # Devuelve tal cual si no coincide
        
        df_formateado['estado_visita'] = df_formateado['estado_visita'].apply(agregar_icono)
    return df_formateado


with st.sidebar:
    try: st.image(Image.open('Logo_CertiRedes_Transparente.png'), width=200)
    except: st.write("### CERTI-REDES")
    st.markdown("🟢 **Nube Activa**")
    st.markdown("---")
    
    st.markdown("### 📥 1. CARGA DE BASE GENERAL")
    archivos_bases = st.file_uploader("Suba su matriz original (.csv, .xlsx)", accept_multiple_files=True, key="side_uploader")
    if archivos_bases and st.button("🚀 Cargar a la Nube", use_container_width=True, key="side_btn"):
        with st.spinner("Limpiando y subiendo... Mire la consola/logs para ver los detalles."):
            res = procesar_nuevas_bases(archivos_bases)
            if res is True:
                st.success("¡Base actualizada!")
                st.rerun()
            else:
                st.error(res)

df_activa = cargar_tabla_segura(TABLA_BASE)

# --- NORMALIZAR LA BASE PARA LA PANTALLA ---
if not df_activa.empty:
    df_activa = normalizar_columnas(df_activa)

st.title("🚀 Panel Certi-Redes (Cloud)")

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
    t_wa, t_op, t_ans, t_hist, t_insp = st.tabs(["💬 Operación Diaria", "📊 Monitor", "⏱️ ANS", "📦 Historial", "⚙️ Inspectores"])

    with t_wa:
        st.write("### 📅 Centro de Mando Operativo")
        
        # --- NUEVA ESTRUCTURA SUPERIOR: FECHA Y CARGA DE EJECUCIÓN ---
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
            df_activa['fecha_prog_limpia'] = convertir_fechas_espanol(df_activa['fecha_programacion'])
            df_dia = df_activa[df_activa['fecha_prog_limpia'] == f_str]
            
            if not df_dia.empty:
                # --- CÁLCULO DE MÉTRICAS ---
                c_prog = len(df_dia)
                c_env = len(df_dia[df_dia['estado_whatsapp'].astype(str).str.upper().str.contains('ENVIADO', na=False)]) if 'estado_whatsapp' in df_dia.columns else 0
                c_no_env = c_prog - c_env
                
                estados_upper = df_dia['estado_visita'].astype(str).str.upper().str.strip()
                
                # Efectivas: Suma tanto CERTIFICADO como NO CERTIFICADO
                mask_efectivas = estados_upper.isin(['CERTIFICADO', 'NO CERTIFICADO'])
                c_cert = len(df_dia[mask_efectivas])
                
                # No Efectivas: VISITA NO EFECTIVA (o sus abreviaciones)
                mask_vne = estados_upper.str.contains('VISITA NO EFECTIVA', na=False) | estados_upper.isin(['VNE', 'NO EFECTIVA'])
                c_vne = len(df_dia[mask_vne])
                
                c_pend = c_prog - c_cert - c_vne
                
                st.write("---")
                
                # --- FILA 1: MENSAJERÍA ---
                st.markdown("#### 📱 1. Gestión de Mensajería WhatsApp")
                m1, m2, m3, btn_col = st.columns([1, 1, 1, 2])
                m1.metric("📅 Total Programados", c_prog)
                m2.metric("📤 Total Enviados", c_env)
                m3.metric("⏳ No Enviados", c_no_env)
                with btn_col:
                    st.write("") # Espaciador
                    if st.button("📤 Disparar Mensajes Pendientes", type="primary", use_container_width=True):
                        with st.spinner("Verificando credenciales y conectando con Twilio..."):
                            if "TWILIO_ACCOUNT_SID" not in st.secrets:
                                st.error("🚨 ERROR: No se encontraron credenciales de Twilio.")
                            else:
                                exito, msj = enviar_mensajes_agenda(df_dia)
                                if exito:
                                    st.success(msj)
                                    st.rerun()
                                else:
                                    st.error(msj)

                st.write("---")
                
                # --- FILA 2 Y 3: EFECTIVIDAD Y JORNADAS ---
                col_efect, col_jornada = st.columns(2)
                
                with col_efect:
                    st.markdown("#### 🛠️ 2. Efectividad de Visitas")
                    e1, e2, e3 = st.columns(3)
                    e1.metric("✅ Efectivas (Cert/No Cert)", c_cert)
                    e2.metric("❌ No Efectivas (VNE)", c_vne)
                    e3.metric("⏳ Pendientes", c_pend)
                    
                with col_jornada:
                    st.markdown("#### ⏱️ 3. Control de Jornada (Vencimientos)")
                    
                    # Lógica de cálculo de jornadas AM/PM
                    hoy = datetime.date.today()
                    hora_actual = datetime.datetime.now().hour
                    
                    jornada_am = df_dia[df_dia['jornada'].astype(str).str.upper().str.contains('AM', na=False)]
                    jornada_pm = df_dia[df_dia['jornada'].astype(str).str.upper().str.contains('PM', na=False)]
                    
                    def calc_jornada(df_j, es_am):
                        if df_j.empty: return 0, 0, 0
                        est = df_j.loc[:, 'estado_visita'].astype(str).str.upper().str.strip()
                        
                        # Actualizamos la fórmula para que cuente ambas como Ejecutadas
                        mask_ef = est.isin(['CERTIFICADO', 'NO CERTIFICADO'])
                        mask_vn = est.str.contains('VISITA NO EFECTIVA', na=False) | est.isin(['VNE', 'NO EFECTIVA'])
                        
                        cumplidos = len(df_j[mask_ef | mask_vn])
                        pendientes = len(df_j) - cumplidos
                        vencidos = 0
                        
                        # Si es un día en el pasado, todo lo pendiente está vencido
                        if fecha_select < hoy:
                            vencidos = pendientes
                        # Si es hoy, evaluamos la hora
                        elif fecha_select == hoy:
                            if es_am and hora_actual >= 12:
                                vencidos = pendientes
                            elif not es_am and hora_actual >= 18:
                                vencidos = pendientes
                        return len(df_j), cumplidos, vencidos

                    am_tot, am_cump, am_venc = calc_jornada(jornada_am, True)
                    pm_tot, pm_cump, pm_venc = calc_jornada(jornada_pm, False)
                    
                    j1, j2, j3, j4 = st.columns(4)
                    j1.metric("☀️ AM Ejecut.", f"{am_cump}/{am_tot}")
                    # Mostramos en ROJO (- inverse) si hay vencidos
                    j2.metric("☀️ AM Venc.", am_venc, delta=f"-{am_venc} Venc." if am_venc > 0 else None, delta_color="inverse")
                    j3.metric("🌙 PM Ejecut.", f"{pm_cump}/{pm_tot}")
                    j4.metric("🌙 PM Venc.", pm_venc, delta=f"-{pm_venc} Venc." if pm_venc > 0 else None, delta_color="inverse")

                st.write("---")
                
                st.write("#### 📋 Base Detallada de la Jornada")
                cols_vista = ['orden', 'contrato', 'nombre', 'municipio', 'fecha_programacion', 'jornada', 'inspector', 'estado_visita', 'estado_whatsapp']
                columnas_presentes = [c for c in cols_vista if c in df_dia.columns]
                
                # APLICAMOS EL NUEVO FORMATEO VISUAL A LA TABLA
                df_visual = formatear_estado_visita(df_dia[columnas_presentes])
                st.dataframe(centrar_df(df_visual), use_container_width=True)
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
        st.write("### 📊 Monitor Operativo General")
        
        # APLICAMOS EL NUEVO FORMATEO VISUAL AL MONITOR GENERAL TAMBIÉN
        df_activa_visual = formatear_estado_visita(df_activa)
        st.dataframe(centrar_df(df_activa_visual), use_container_width=True)

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