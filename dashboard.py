import streamlit as st
import pandas as pd
from PIL import Image
import io
import plotly.express as px  
import datetime
import os

# ==========================================
# 0. CONSTANTES Y CONFIGURACIÓN
# ==========================================
st.set_page_config(layout="wide", page_title="Panel Certi-Redes", page_icon="✅")

ARCHIVO_BASE = 'base_general.csv'
ARCHIVO_HISTORIAL = 'historial_certiredes.csv'
ARCHIVO_LOG_WA = 'log_certiredes.txt'

# ==========================================
# 1. FUNCIONES DEL MOTOR DE DATOS
# ==========================================

def normalizar_columnas(df):
    """Limpia y estandariza los nombres de las columnas para evitar errores de espacios."""
    df.columns = df.columns.astype(str).str.strip().str.lower()
    # Mapeo de seguridad por si cambian ligeramente los nombres
    mapeo = {
        'orden': 'orden', 'contrato': 'contrato', 'nombre': 'nombre', 
        'dirección': 'direccion', 'direccion': 'direccion', 'telefono': 'telefono',
        'fecha programación': 'fecha_programacion', 'fecha programacion': 'fecha_programacion',
        'jornada': 'jornada', 'tipo orden': 'tipo_orden', 'tipo trabajo': 'tipo_orden', 
        'fecha asignación': 'fecha_asignacion', 'fecha asignacion': 'fecha_asignacion', 
        '# vne': 'num_vne', 'consumo': 'consumo', 'meses': 'meses',
        'cabecera': 'municipio', 'cabeceras': 'municipio'
    }
    df.rename(columns=mapeo, inplace=True)
    return df

def procesar_nuevas_bases(archivos_subidos):
    """Lee los Excels subidos, extrae la pestaña Coordinación y actualiza la base general."""
    nuevos_registros = []
    
    for archivo in archivos_subidos:
        try:
            # Leer pestaña Coordinación, fila 5 como cabecera (header=4 en índice base 0)
            df_temp = pd.read_excel(archivo, sheet_name='Coordinación', header=4, engine='openpyxl')
            df_temp = normalizar_columnas(df_temp)
            
            # Limpiar formatos base
            if 'orden' in df_temp.columns and 'contrato' in df_temp.columns:
                df_temp['orden'] = df_temp['orden'].astype(str).str.replace('.0', '', regex=False).str.strip()
                df_temp['contrato'] = df_temp['contrato'].astype(str).str.replace('.0', '', regex=False).str.strip()
                df_temp = df_temp[df_temp['orden'] != 'nan'] # Eliminar filas vacías
                nuevos_registros.append(df_temp)
        except Exception as e:
            st.error(f"Error al procesar el archivo {archivo.name}. Verifique que tenga la pestaña 'Coordinación' y cabeceras en la fila 5. Detalle: {e}")

    if nuevos_registros:
        df_nuevos = pd.concat(nuevos_registros, ignore_index=True)
        
        # Formatear fechas estandarizadas al formato universal (YYYY-MM-DD) para evitar cruces
        if 'fecha_programacion' in df_nuevos.columns:
            df_nuevos['fecha_prog_limpia'] = pd.to_datetime(df_nuevos['fecha_programacion'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        if 'fecha_asignacion' in df_nuevos.columns:
            df_nuevos['fecha_asignacion'] = pd.to_datetime(df_nuevos['fecha_asignacion'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
            
        # Inicializar columnas operativas si no existen
        for col in ['estado_whatsapp', 'estado_ejecucion', 'num_vne']:
            if col not in df_nuevos.columns:
                df_nuevos[col] = 0 if col == 'num_vne' else 'Pendiente'
        
        # Limpiar columna Meses de decimales por seguridad
        if 'meses' in df_nuevos.columns:
            df_nuevos['meses'] = df_nuevos['meses'].astype(str).str.replace('.0', '', regex=False).str.strip()
                
        # Cargar base existente o crearla
        if os.path.exists(ARCHIVO_BASE):
            df_base = pd.read_csv(ARCHIVO_BASE, dtype=str)
            df_consolidado = pd.concat([df_base, df_nuevos]).drop_duplicates(subset=['orden'], keep='last')
        else:
            df_consolidado = df_nuevos
            
        df_consolidado.to_csv(ARCHIVO_BASE, index=False, encoding='utf-8-sig')
        return True
    return False

def procesar_godoworks(archivo_godo):
    """Procesa el CSV de GoDoWorks, archiva cumplidas y suma VNE."""
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
        
        if not os.path.exists(ARCHIVO_BASE):
            st.warning("No hay una Base General activa para actualizar.")
            return False
            
        df_base = pd.read_csv(ARCHIVO_BASE, dtype=str)
        df_historial = pd.read_csv(ARCHIVO_HISTORIAL, dtype=str) if os.path.exists(ARCHIVO_HISTORIAL) else pd.DataFrame()
        
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
            df_historial.to_csv(ARCHIVO_HISTORIAL, index=False, encoding='utf-8-sig')
            
            df_base = df_base[~df_base['orden'].isin(ordenes_a_archivar)]

        df_base.to_csv(ARCHIVO_BASE, index=False, encoding='utf-8-sig')
        return True
    except Exception as e:
        st.error(f"Error procesando GoDoWorks: {e}")
        return False

# ==========================================
# 2. BARRA LATERAL (MENÚ DE CARGA)
# ==========================================
with st.sidebar:
    try: st.image(Image.open('Logo_CertiRedes_Transparente.png'), width=200)
    except: st.write("### CERTI-REDES S.A.S")
    
    st.markdown("---")
    st.markdown("### 📥 1. ALIMENTAR BASE GENERAL")
    st.info("Suba uno o varios archivos de Excel. El sistema buscará la hoja 'Coordinación' a partir de la fila 5.")
    archivos_bases = st.file_uploader("Seleccionar Bases (.xlsm/.xlsx)", type=["xlsm", "xlsx"], accept_multiple_files=True)
    if archivos_bases and st.button("🚀 Procesar Bases de Datos", use_container_width=True):
        if procesar_nuevas_bases(archivos_bases):
            st.success("¡Base General actualizada correctamente!")
            st.rerun()

    st.markdown("---")
    st.markdown("### 🛠️ 2. ACTUALIZAR EJECUCIÓN (GoDoWorks)")
    st.info("Sube el archivo de GoDoWorks. Las órdenes 'Cumplidas' irán al Historial y las 'No Efectivas' sumarán VNE.")
    archivo_godo = st.file_uploader("Subir reporte GoDoWorks", type=["csv", "xlsx"])
    if archivo_godo and st.button("🔄 Ejecutar Cruce Automático", use_container_width=True):
        if procesar_godoworks(archivo_godo):
            st.success("¡Cruce realizado! Órdenes actualizadas y/o archivadas.")
            st.rerun()

# Cargar Base Activa a memoria para la visualización
df_activa = pd.DataFrame()
if os.path.exists(ARCHIVO_BASE):
    df_activa = pd.read_csv(ARCHIVO_BASE, dtype=str)
    # Rellenar columnas si el CSV antiguo no las tenía
    if 'consumo' not in df_activa.columns: df_activa['consumo'] = 'N/A'
    if 'meses' not in df_activa.columns: df_activa['meses'] = 'N/A'
    if 'tipo_orden' not in df_activa.columns: df_activa['tipo_orden'] = 'POR DEFECTO'
    if 'municipio' not in df_activa.columns: df_activa['municipio'] = 'SIN DEFINIR'

# ==========================================
# 3. INTERFAZ PRINCIPAL (PESTAÑAS)
# ==========================================

st.title("🚀 Panel de Control Operativo - Certi-Redes")

if df_activa.empty:
    st.warning("⚠️ La Base General está vacía. Por favor, cargue los archivos de Excel en el panel lateral para iniciar.")
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
                st.success(f"Se encontraron **{len(df_agenda_dia)}** órdenes para el {fecha_select.strftime('%d/%m/%Y')}.")
                
                def marcar_msj_enviado(orden_id):
                    df_temp = pd.read_csv(ARCHIVO_BASE, dtype=str)
                    df_temp.loc[df_temp['orden'] == orden_id, 'estado_whatsapp'] = '✅ MSJ ENVIADO'
                    df_temp.to_csv(ARCHIVO_BASE, index=False, encoding='utf-8-sig')
                    st.toast(f"Mensaje marcado como enviado para orden {orden_id}")

                cols_vista = ['orden', 'contrato', 'nombre', 'direccion', 'telefono', 'jornada', 'num_vne', 'estado_whatsapp']
                columnas_presentes = [c for c in cols_vista if c in df_agenda_dia.columns]
                
                # Renderizado SEGURO nativo
                st.dataframe(df_agenda_dia[columnas_presentes], use_container_width=True)
                
                st.markdown("---")
                st.write("#### Acciones de Mensajería")
                if st.button("📤 Marcar todas las órdenes del día como 'Mensaje Enviado'", type="primary"):
                    ordenes_dia = df_agenda_dia['orden'].tolist()
                    df_temp = pd.read_csv(ARCHIVO_BASE, dtype=str)
                    df_temp.loc[df_temp['orden'].isin(ordenes_dia), 'estado_whatsapp'] = '✅ MSJ ENVIADO'
                    df_temp.to_csv(ARCHIVO_BASE, index=False, encoding='utf-8-sig')
                    st.success("Toda la agenda del día ha sido marcada como 'Enviada'. Esperando confirmación del inspector.")
                    st.rerun()
        else:
            st.error("La columna de Fecha de Programación no fue encontrada en la base.")

    # ------------------------------------------
    # TAB 2: MONITOR OPERATIVO GENERAL (DISEÑO PARALELO SEGURO)
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
            
            # ✨ DISEÑO ESTÉTICO: Sub-pestañas para no saturar la pantalla
            tab_res_tipo, tab_res_muni = st.tabs(["🛠️ Por Tipo de Trabajo", "📍 Por Municipio"])
            
            with tab_res_tipo:
                if 'tipo_orden' in df_activa.columns:
                    resumen_op = df_activa.groupby(['tipo_orden', 'estado_ejecucion']).size().unstack(fill_value=0).reset_index()
                    for col in ['Pendiente', '❌ No efectiva', '✅ Cumplida (Archivada)']:
                        if col not in resumen_op.columns: resumen_op[col] = 0
                    resumen_op['TOTAL'] = resumen_op.iloc[:, 1:].sum(axis=1)
                    resumen_op.rename(columns={'tipo_orden': 'Tipo Trabajo'}, inplace=True)
                    resumen_op = resumen_op.sort_values(by='TOTAL', ascending=False)
                    
                    st.dataframe(resumen_op, use_container_width=True, hide_index=True)
            
            with tab_res_muni:
                if 'municipio' in df_activa.columns:
                    resumen_muni = df_activa.groupby(['municipio', 'estado_ejecucion']).size().unstack(fill_value=0).reset_index()
                    for col in ['Pendiente', '❌ No efectiva', '✅ Cumplida (Archivada)']:
                        if col not in resumen_muni.columns: resumen_muni[col] = 0
                    resumen_muni['TOTAL'] = resumen_muni.iloc[:, 1:].sum(axis=1)
                    resumen_muni.rename(columns={'municipio': 'Municipio'}, inplace=True)
                    resumen_muni = resumen_muni.sort_values(by='TOTAL', ascending=False)
                    
                    st.dataframe(resumen_muni, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.write("#### 🗃️ Detalle de Base Activa Completa")
        # Renderizado SEGURO nativo
        st.dataframe(df_activa, use_container_width=True)

    # ------------------------------------------
    # TAB 3: AUDITORÍA DE TIEMPOS (ANS) (DISEÑO PARALELO SEGURO + GRÁFICO)
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
                    resumen_ans = resumen_ans.sort_values(by='TOTAL', ascending=False)
                    
                    # Renderizado SEGURO nativo
                    st.dataframe(resumen_ans, use_container_width=True, hide_index=True)
                
                # GRÁFICO SEGURO
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

                # Renderizado SEGURO con colores
                st.dataframe(df_ans_disp.style.map(style_ans, subset=['Estado']), use_container_width=True, hide_index=True)
            else:
                st.success("🎉 ¡No hay órdenes bajo seguimiento ANS actualmente!")
        else:
            st.warning("Las columnas 'Fecha asignación' o 'Tipo orden' no se detectaron en su matriz base para calcular los ANS.")

    # ------------------------------------------
    # TAB 4: HISTORIAL
    # ------------------------------------------
    with tab_hist:
        st.write("### 📦 Repositorio de Órdenes Cumplidas y Archivadas")
        st.info("Aquí reposan todas las órdenes que cruzaron como 'Certificadas' o 'Cumplidas' en GoDoWorks. Estas ya no afectan la Base General.")
        if os.path.exists(ARCHIVO_HISTORIAL):
            df_hist_view = pd.read_csv(ARCHIVO_HISTORIAL, dtype=str)
            st.metric("Total Órdenes Históricas", len(df_hist_view))
            
            # Renderizado SEGURO nativo
            st.dataframe(df_hist_view, use_container_width=True, hide_index=True)
            
            buf = io.BytesIO()
            df_hist_view.to_excel(buf, index=False)
            st.download_button("📥 Descargar Historial Completo (Excel)", buf.getvalue(), "historial_completo.xlsx", use_container_width=True)
        else:
            st.info("El historial está vacío. Aún no se han cruzado órdenes cumplidas.")