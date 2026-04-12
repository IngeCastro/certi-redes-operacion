import streamlit as st
import pandas as pd
import time
from twilio.rest import Client
from database import cargar_tabla, guardar_tabla
import traceback

TABLA_BASE = 'base_general'
TABLA_INSPECTORES = 'directorio_inspectores'

def enviar_mensajes_agenda(df_agenda_dia):
    try:
        print("\n===========================================")
        print("📱 INICIANDO MÓDULO DE WHATSAPP (DIAGNÓSTICO)")
        print("===========================================")
        
        # 1. FORZAR TEXTO EN SECRETOS CON .get() PARA EVITAR CAÍDAS
        account_sid = str(st.secrets.get("TWILIO_ACCOUNT_SID", "")).strip()
        auth_token = str(st.secrets.get("TWILIO_AUTH_TOKEN", "")).strip()
        twilio_phone = str(st.secrets.get("TWILIO_PHONE", "")).strip()
        
        print(f"🔑 Verificando credenciales locales/nube:")
        print(f" - TWILIO_ACCOUNT_SID: {'✅ ENCONTRADO' if account_sid else '❌ FALTANTE'}")
        print(f" - TWILIO_AUTH_TOKEN:  {'✅ ENCONTRADO' if auth_token else '❌ FALTANTE'}")
        print(f" - TWILIO_PHONE:       {'✅ ENCONTRADO' if twilio_phone else '❌ FALTANTE'}")
        
        if not account_sid or not auth_token:
            print("🚨 ABORTANDO: Faltan credenciales de Twilio en secrets.toml")
            return False, "Faltan las credenciales de Twilio en los secretos locales (.toml) o de la nube."
            
        print("🔌 Conectando con los servidores de Twilio...")
        cliente_twilio = Client(account_sid, auth_token)
        
        print("☁️ Descargando base de inspectores y base general...")
        df_inspectores = cargar_tabla(TABLA_INSPECTORES)
        df_activa_temp = cargar_tabla(TABLA_BASE)
        
        if df_activa_temp.empty:
            print("❌ La base general está vacía, abortando envíos.")
            return False, "La base general está vacía."
        
        mensajes_enviados = 0
        errores = 0
        barra_wa = st.progress(0, text="Iniciando envío masivo...")
        
        print(f"📨 Iniciando ciclo de envíos para {len(df_agenda_dia)} órdenes...")
        
        for idx, row in df_agenda_dia.iterrows():
            # Extraer variables con "Armadura Anti-Vacíos"
            orden = str(row.get('orden', '')).strip().replace('.0', '').replace('nan', '')
            if not orden:
                continue
                
            estado_actual = str(row.get('estado_whatsapp', '')).upper()
            if 'ENVIADO' in estado_actual:
                print(f"⏩ Orden {orden} omitida (Ya fue enviada previamente).")
                continue
                
            cod_tecnico = str(row.get('codigo_tecnico', '')).strip().replace('.0', '').replace('nan', '')
            
            if df_inspectores.empty:
                df_activa_temp.loc[df_activa_temp['orden'].astype(str) == orden, 'estado_whatsapp'] = '❌ ERROR: NO HAY INSPECTORES EN BD'
                errores += 1
                continue
                
            filtro_insp = df_inspectores[df_inspectores['codigo_tecnico'].astype(str).str.strip() == cod_tecnico]
            
            if filtro_insp.empty:
                df_activa_temp.loc[df_activa_temp['orden'].astype(str) == orden, 'estado_whatsapp'] = '❌ ERROR: CÓDIGO TÉCNICO NO EXISTE'
                print(f"⚠️ Orden {orden}: Código de técnico '{cod_tecnico}' no encontrado en el directorio.")
                errores += 1
                continue
            
            celular_tecnico = str(filtro_insp.iloc[0].get('celular', '')).strip().replace('.0', '').replace(' ', '').replace('nan', '')
            if not celular_tecnico.startswith('+'):
                celular_tecnico = '+57' + celular_tecnico
            
            # VARIABLES DE LA PLANTILLA BLINDADAS CONTRA VACÍOS
            v1_contrato = str(row.get('contrato', '')).strip().replace('.0', '').replace('nan', '')
            v2_nombre = str(row.get('nombre', '')).strip().replace('nan', '')
            v3_direccion = str(row.get('direccion', '')).strip().replace('nan', '')
            v4_municipio = str(row.get('municipio', '')).strip().replace('nan', '')
            v5_fecha = str(row.get('fecha_programacion', '')).strip().replace('nan', '')
            v6_jornada = str(row.get('jornada', '')).strip().replace('nan', '')
            v7_inspector = str(row.get('inspector', '')).strip().replace('nan', '')
            
            mensaje_plantilla = (
                f"Hola {v2_nombre}, le recordamos su visita técnica de Certi-Redes para el contrato {v1_contrato}. "
                f"Dirección: {v3_direccion}, {v4_municipio}. "
                f"Programada para el {v5_fecha} en jornada {v6_jornada}. "
                f"Inspector asignado: {v7_inspector}. "
                f"Por favor responda SI para confirmar o CANCELAR para rechazar."
            )
            
            try:
                print(f"📤 Intentando enviar WhatsApp a {celular_tecnico} (Orden: {orden})...")
                
                # CORRECCIÓN DE TWILIO: Aseguramos que el remitente tenga 'whatsapp:'
                remitente_wa = twilio_phone if twilio_phone.startswith('whatsapp:') else f"whatsapp:{twilio_phone}"
                
                message = cliente_twilio.messages.create(
                    body=mensaje_plantilla,
                    from_=remitente_wa,  # <-- Aquí aplicamos la solución
                    to=f"whatsapp:{celular_tecnico}"
                )
                df_activa_temp.loc[df_activa_temp['orden'].astype(str) == orden, 'estado_whatsapp'] = '✅ MSJ ENVIADO'
                print(f"✅ ÉXITO: Mensaje enviado a {celular_tecnico}. SID de Twilio: {message.sid}")
                mensajes_enviados += 1
            except Exception as e:
                df_activa_temp.loc[df_activa_temp['orden'].astype(str) == orden, 'estado_whatsapp'] = '❌ ERROR TWILIO'
                print(f"❌ ERROR DE TWILIO al enviar a {celular_tecnico}: {e}")
                errores += 1
                
            progreso = (mensajes_enviados + errores) / len(df_agenda_dia)
            barra_wa.progress(min(progreso, 1.0), text=f"Enviando... ({mensajes_enviados} enviados, {errores} errores)")
            time.sleep(0.5)
            
        print("\n💾 Guardando resultados de envío en la Base de Datos...")
        guardar_tabla(df_activa_temp, TABLA_BASE)
        print("===========================================")
        print(f"🏁 CICLO TERMINADO: {mensajes_enviados} Enviados | {errores} Errores")
        print("===========================================\n")
        return True, f"¡Proceso terminado! {mensajes_enviados} enviados, {errores} errores."
        
    except Exception as e:
        print("\n🚨🚨 ERROR FATAL EN MODULO WHATSAPP 🚨🚨")
        print(traceback.format_exc())
        print("===========================================\n")
        return False, f"El sistema atrapó un error. Detalle técnico: {str(e)}"