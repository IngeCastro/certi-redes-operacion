import streamlit as st
import pandas as pd
import time
from twilio.rest import Client
from database import cargar_tabla, guardar_tabla # Importamos las funciones de nuestro nuevo archivo

TABLA_BASE = 'base_general'
TABLA_INSPECTORES = 'directorio_inspectores'

def enviar_mensajes_agenda(df_agenda_dia):
    try:
        account_sid = st.secrets["TWILIO_ACCOUNT_SID"]
        auth_token = st.secrets["TWILIO_AUTH_TOKEN"]
        twilio_phone = st.secrets["TWILIO_PHONE"]
        cliente_twilio = Client(account_sid, auth_token)
        
        df_inspectores = cargar_tabla(TABLA_INSPECTORES)
        df_activa_temp = cargar_tabla(TABLA_BASE)
        
        mensajes_enviados = 0
        errores = 0
        barra_wa = st.progress(0, text="Iniciando envío masivo...")
        
        for idx, row in df_agenda_dia.iterrows():
            orden = row['orden']
            estado_actual = str(row['estado_whatsapp']).upper()
            
            if 'ENVIADO' in estado_actual:
                continue
                
            cod_tecnico = str(row['codigo_tecnico']).strip().replace('.0', '')
            filtro_insp = df_inspectores[df_inspectores['codigo_tecnico'] == cod_tecnico]
            
            if filtro_insp.empty:
                df_activa_temp.loc[df_activa_temp['orden'] == orden, 'estado_whatsapp'] = '❌ ERROR: CÓDIGO TÉCNICO NO EXISTE'
                errores += 1
                continue
            
            celular_tecnico = str(filtro_insp.iloc[0]['celular']).strip()
            if not celular_tecnico.startswith('+'):
                celular_tecnico = '+57' + celular_tecnico
            
            # VARIABLES DE LA PLANTILLA
            v1_contrato = str(row['contrato'])
            v2_nombre = str(row['nombre'])
            v3_direccion = str(row['direccion'])
            v4_municipio = str(row['municipio'])
            v5_fecha = str(row['fecha_programacion'])
            v6_jornada = str(row['jornada'])
            v7_inspector = str(row['inspector'])
            
            mensaje_plantilla = (
                f"Hola {v2_nombre}, le recordamos su visita técnica de Certi-Redes para el contrato {v1_contrato}. "
                f"Dirección: {v3_direccion}, {v4_municipio}. "
                f"Programada para el {v5_fecha} en jornada {v6_jornada}. "
                f"Inspector asignado: {v7_inspector}. "
                f"Por favor responda SI para confirmar o CANCELAR para rechazar."
            )
            
            try:
                message = cliente_twilio.messages.create(
                    body=mensaje_plantilla,
                    from_=twilio_phone,
                    to=f"whatsapp:{celular_tecnico}"
                )
                df_activa_temp.loc[df_activa_temp['orden'] == orden, 'estado_whatsapp'] = '✅ MSJ ENVIADO'
                mensajes_enviados += 1
            except Exception as e:
                df_activa_temp.loc[df_activa_temp['orden'] == orden, 'estado_whatsapp'] = '❌ ERROR TWILIO'
                errores += 1
                
            progreso = (mensajes_enviados + errores) / len(df_agenda_dia)
            barra_wa.progress(min(progreso, 1.0), text=f"Enviando... ({mensajes_enviados} enviados, {errores} errores)")
            time.sleep(0.5)
            
        guardar_tabla(df_activa_temp, TABLA_BASE)
        return True, f"¡Proceso terminado! {mensajes_enviados} mensajes enviados. {errores} errores."
    except Exception as e:
        return False, f"Error en Twilio o base de datos: {e}"