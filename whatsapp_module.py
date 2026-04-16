import streamlit as st
import pandas as pd
import time
from twilio.rest import Client
from database import cargar_tabla, guardar_tabla
import traceback
import json
import io
import requests
import matplotlib.pyplot as plt
import textwrap
import smtplib
from email.message import EmailMessage

TABLA_BASE = 'base_general'
TABLA_INSPECTORES = 'directorio_inspectores'

def subir_imagen_supabase(image_buffer, filename, supabase_url, supabase_key):
    """Sube la imagen a Supabase."""
    url = f"{supabase_url}/storage/v1/object/programaciones/{filename}"
    headers = {"Authorization": f"Bearer {supabase_key}", "Content-Type": "image/png"}
    res = requests.post(url, headers=headers, data=image_buffer)
    if res.status_code == 200:
        return f"{supabase_url}/storage/v1/object/public/programaciones/{filename}"
    else:
        raise Exception(f"Fallo al subir a Supabase: {res.text}")

def generar_imagen_tabla(df_grupo, nombre_inspector, fecha, tipo_envio="programacion"):
    """Dibuja la imagen. Si es 'sancion', usa rojo y alertas. Si es 'programacion', usa azul."""
    
    if 'direccion' not in df_grupo.columns:
        df_grupo['direccion'] = 'Sin registro'

    columnas_vista = ['contrato', 'nombre', 'direccion', 'municipio', 'jornada']
    df_visual = df_grupo[columnas_vista].copy()
    df_visual.columns = ['Contrato', 'Nombre Usuario', 'Dirección', 'Municipio', 'Jornada']
    
    # Envolver texto largo
    df_visual['Nombre Usuario'] = df_visual['Nombre Usuario'].apply(lambda x: '\n'.join(textwrap.wrap(str(x), width=18)))
    df_visual['Dirección'] = df_visual['Dirección'].apply(lambda x: '\n'.join(textwrap.wrap(str(x), width=28)))
    df_visual['Municipio'] = df_visual['Municipio'].apply(lambda x: '\n'.join(textwrap.wrap(str(x), width=14)))
    
    fecha_limpia = str(fecha).split(' ')[0]
    if "-" in fecha_limpia and len(fecha_limpia.split("-")) == 3:
        y, m, d = fecha_limpia.split("-")
        fecha_limpia = f"{d}/{m}/{y}"
    
    alto_figura = max(4.5, len(df_visual) * 0.8 + 3) 
    fig, ax = plt.subplots(figsize=(13, alto_figura))
    ax.axis('tight')
    ax.axis('off')
    
    # --- LÓGICA DE COLORES SEGÚN EL TIPO DE ENVÍO ---
    if tipo_envio == "sancion":
        color_principal = "#DC2626" # Rojo Alerta
        color_secundario = "#FEE2E2" # Rojo Claro
        titulo_principal = f"🚨 ÓRDENES SANCIONADAS - {fecha_limpia}"
        instrucciones = "Estas órdenes han sido marcadas como INCUMPLIDAS.\nComuníquese con coordinación inmediatamente."
        color_instrucciones = "#FEF2F2"
        borde_instrucciones = "#DC2626"
    else:
        color_principal = "#1E3A8A" # Azul Corporativo
        color_secundario = "#F3F4F6" # Gris Claro
        titulo_principal = f"PROGRAMACIÓN DE VISITAS - {fecha_limpia}"
        instrucciones = "Las ordenes deben gestionarse como:\n• Cumplida\n• Visita no efectiva\n• Con todos los soportes en la app móvil"
        color_instrucciones = "#ECFCCB"
        borde_instrucciones = "#84CC16"

    # Título
    plt.figtext(0.5, 0.92, titulo_principal, ha="center", fontsize=19, weight="bold", color=color_principal)
    plt.figtext(0.5, 0.88, f"Inspector(a): {nombre_inspector}", ha="center", fontsize=14, weight="bold")
    
    # Dibujar Tabla
    datos = df_visual.values.tolist()
    columnas = df_visual.columns.tolist()
    anchos_columnas = [0.08, 0.23, 0.43, 0.16, 0.10]
    
    tabla = ax.table(cellText=datos, colLabels=columnas, loc='center', cellLoc='center', colWidths=anchos_columnas)
    tabla.auto_set_font_size(False)
    tabla.set_fontsize(11) 
    tabla.scale(1, 3.5) 
    
    # Colorear
    for (i, j), cell in tabla.get_celld().items():
        if i == 0:
            cell.set_text_props(weight='bold', color='white')
            cell.set_facecolor(color_principal)
        elif i % 2 == 0:
            cell.set_facecolor(color_secundario)
            
    # Instrucciones
    plt.figtext(0.5, 0.1, instrucciones, ha="center", fontsize=12, 
                bbox={"facecolor":color_instrucciones, "alpha":0.8, "pad":10, "edgecolor":borde_instrucciones, "boxstyle":"round,pad=1"})
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=300) 
    buf.seek(0)
    plt.close()
    
    return buf

def enviar_reporte_correo(df_pendientes):
    """Genera un Excel en memoria y lo envía por correo."""
    remitente = str(st.secrets.get("EMAIL_SENDER", "")).strip()
    password = str(st.secrets.get("EMAIL_PASSWORD", "")).strip()
    destinatarios_str = str(st.secrets.get("EMAIL_RECEIVERS", "")).strip()
    
    if not remitente or not password:
        print("⚠️ No hay credenciales de correo configuradas en secrets.toml")
        return False
        
    destinatarios = [d.strip() for d in destinatarios_str.split(',')]
    
    # 1. Crear el Excel en memoria
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        df_pendientes.to_excel(writer, index=False, sheet_name='Sancionadas')
    excel_buffer.seek(0)
    
    # 2. Armar el correo
    msg = EmailMessage()
    msg['Subject'] = f"🚨 Reporte Automático - Órdenes Sancionadas ({len(df_pendientes)} registros)"
    msg['From'] = remitente
    msg['To'] = ", ".join(destinatarios)
    
    cuerpo = f"""
    Cordial saludo,
    
    Se adjunta el reporte automático con las {len(df_pendientes)} órdenes que quedaron en estado PENDIENTE / SANCIONADA.
    Los técnicos responsables ya han sido notificados vía WhatsApp con su respectiva alerta visual en rojo.
    
    Atentamente,
    Bot Operativo de Certi-Redes
    """
    msg.set_content(cuerpo)
    
    # 3. Adjuntar Excel
    msg.add_attachment(excel_buffer.read(), maintype='application', subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet', filename='Reporte_Sanciones.xlsx')
    
    # 4. Enviar
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(remitente, password)
            smtp.send_message(msg)
        return True
    except Exception as e:
        print(f"Error enviando correo: {e}")
        return False

def enviar_mensajes_agenda(df_agenda_dia, tipo_envio="programacion"):
    """Módulo principal. tipo_envio puede ser 'programacion' o 'sancion'."""
    try:
        print(f"\n===========================================")
        print(f"📱 INICIANDO MÓDULO WA - MODO: {tipo_envio.upper()}")
        print(f"===========================================")
        
        account_sid = str(st.secrets.get("TWILIO_ACCOUNT_SID", "")).strip()
        auth_token = str(st.secrets.get("TWILIO_AUTH_TOKEN", "")).strip()
        twilio_phone = str(st.secrets.get("TWILIO_PHONE", "")).strip()
        
        # --- SELECCIÓN DE PLANTILLA SEGÚN EL MODO ---
        if tipo_envio == "sancion":
            template_sid = str(st.secrets.get("TWILIO_TEMPLATE_SID_SANCION", "")).strip()
        else:
            template_sid = str(st.secrets.get("TWILIO_TEMPLATE_SID_PROG", "")).strip()
            
        supabase_url = str(st.secrets.get("SUPABASE_URL", "")).strip()
        if supabase_url.startswith("[") and "](" in supabase_url:
            supabase_url = supabase_url.split("](")[1].strip(")")
        supabase_url = supabase_url.rstrip("/")
        supabase_key = str(st.secrets.get("SUPABASE_KEY", "")).strip()
        
        twilio_phone = twilio_phone.replace('whatsapp:', '').replace(' ', '')
        if twilio_phone and not twilio_phone.startswith('+'):
            twilio_phone = '+' + twilio_phone
            
        if not account_sid or not template_sid or not supabase_url:
            return False, f"🚨 FALTAN CREDENCIALES: Revise su secrets.toml (Falta template para {tipo_envio})."
            
        cliente_twilio = Client(account_sid, auth_token)
        df_inspectores = cargar_tabla(TABLA_INSPECTORES)
        df_activa_temp = cargar_tabla(TABLA_BASE)
        
        if df_activa_temp.empty:
            return False, "La base general está vacía."
            
        # Filtro de pendientes según el modo
        if tipo_envio == "sancion":
            df_pendientes = df_agenda_dia[~df_agenda_dia['estado_whatsapp'].astype(str).str.upper().str.contains('SANCIONADO', na=False)]
        else:
            df_pendientes = df_agenda_dia[~df_agenda_dia['estado_whatsapp'].astype(str).str.upper().str.contains('ENVIADO', na=False)]
        
        if df_pendientes.empty:
            return True, "No hay órdenes pendientes para procesar en esta modalidad."

        grupos_inspector = df_pendientes.groupby('codigo_tecnico')
        
        mensajes_exitosos = 0
        mensajes_fallidos = 0
        ordenes_cubiertas = 0
        ultimo_error = ""
        
        texto_progreso = "Generando SANCIONES en rojo..." if tipo_envio == "sancion" else "Generando PROGRAMACIONES en alta resolución..."
        barra_wa = st.progress(0, text=texto_progreso)
        total_grupos = len(grupos_inspector)
        actual = 0
        
        for cod_tecnico, df_grupo in grupos_inspector:
            actual += 1
            cod_tecnico_str = str(cod_tecnico).strip().replace('.0', '').replace('nan', '')
            
            if df_inspectores.empty:
                continue
                
            filtro_insp = df_inspectores[df_inspectores['codigo_tecnico'].astype(str).str.strip() == cod_tecnico_str]
            if filtro_insp.empty:
                continue
            
            celular_tecnico = str(filtro_insp.iloc[0].get('celular', '')).strip().replace('.0', '').replace(' ', '').replace('nan', '')
            if not celular_tecnico.startswith('+'): celular_tecnico = '+57' + celular_tecnico
            
            nombre_tecnico = str(filtro_insp.iloc[0].get('nombre', '')).strip()
            fecha_agenda = str(df_grupo.iloc[0].get('fecha_programacion', '')).strip().split(' ')[0]
            
            try:
                # 1. GENERAR LA FOTO (Pasando el tipo_envio)
                buffer_imagen = generar_imagen_tabla(df_grupo, nombre_tecnico, fecha_agenda, tipo_envio)
                
                # 2. SUBIR A SUPABASE
                prefijo = "sancion_" if tipo_envio == "sancion" else "agenda_"
                nombre_archivo = f"{prefijo}{cod_tecnico_str}_{int(time.time())}.png"
                link_imagen = subir_imagen_supabase(buffer_imagen, nombre_archivo, supabase_url, supabase_key)
                
                # 3. ENVIAR POR TWILIO
                remitente_wa = f"whatsapp:{twilio_phone}"
                variables_plantilla = json.dumps({"1": nombre_tecnico, "2": fecha_agenda, "3": link_imagen})
                
                cliente_twilio.messages.create(
                    content_sid=template_sid,
                    content_variables=variables_plantilla,
                    from_=remitente_wa,
                    to=f"whatsapp:{celular_tecnico}"
                )
                
                # 4. MARCAR EN BASE DE DATOS
                estado_final = '🚨 SANCIONADO' if tipo_envio == "sancion" else '✅ MSJ ENVIADO'
                df_activa_temp.loc[df_activa_temp['orden'].isin(df_grupo['orden']), 'estado_whatsapp'] = estado_final
                mensajes_exitosos += 1
                ordenes_cubiertas += len(df_grupo)
                
            except Exception as e:
                ultimo_error = str(e)
                mensajes_fallidos += 1
                
            barra_wa.progress(min(actual / total_grupos, 1.0), text=f"Procesando inspector {actual}/{total_grupos}...")
            time.sleep(1)
            
        guardar_tabla(df_activa_temp, TABLA_BASE)
        
        # --- CORREO FINAL (SOLO EN SANCIONES) ---
        if tipo_envio == "sancion" and mensajes_exitosos > 0:
            barra_wa.progress(1.0, text="Enviando reporte Excel a Coordinación...")
            enviado = enviar_reporte_correo(df_pendientes)
            if enviado:
                return True, f"¡Sanciones aplicadas! {mensajes_exitosos} técnicos notificados ({ordenes_cubiertas} órdenes). Reporte enviado por correo."
            else:
                return True, f"Sanciones aplicadas por WhatsApp, pero falló el envío del correo de reporte. Revise sus credenciales de email."
        
        if mensajes_fallidos > 0:
            return False, f"⚠️ Alerta: {mensajes_fallidos} fallaron (Se enviaron {mensajes_exitosos}).\n\n**ERROR:**\n`{ultimo_error}`"
        else:
            return True, f"¡Éxito! Se procesaron {mensajes_exitosos} inspectores ({ordenes_cubiertas} órdenes)."
        
    except Exception as e:
        return False, f"Error fatal: {str(e)}"