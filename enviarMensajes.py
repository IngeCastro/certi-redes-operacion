import pandas as pd # Librería para leer y manipular archivos de Excel
from twilio.rest import Client # Herramienta oficial de Twilio para enviar los WhatsApps
import tkinter as tk # Librería para crear la ventana visual (interfaz gráfica)
from tkcalendar import Calendar # Herramienta para mostrar el calendario en la ventana
from datetime import datetime # Librería para manejar fechas y horas
import time # Librería para hacer pausas en el código si es necesario
import json # Librería para empaquetar las variables en el formato que exige Twilio

# ==========================================
# 1. CONFIGURACIÓN DE CREDENCIALES OFICIALES
# ==========================================
# Aquí ponemos las llaves de acceso de tu cuenta de Twilio
ACCOUNT_SID = 'ACe411b7d301357600771550712214d873'
AUTH_TOKEN = 'dbd33bde262bb08538309c92676c697a' 

# Este es el código único de la plantilla (v10) que Twilio te aprobó
CONTENT_SID = 'HX8a0789521437fb76f489c025a2be5513' 

# Tu número oficial de Certi-Redes configurado en Twilio
NUMERO_TWILIO = 'whatsapp:+15559416718' 

# Iniciamos el motor de Twilio con tus credenciales para tener autorización de enviar
client = Client(ACCOUNT_SID, AUTH_TOKEN)


# ==========================================
# 2. INTERFAZ DE USUARIO (CALENDARIO)
# ==========================================
def iniciar_aplicacion():
    # Creamos la ventana principal
    root = tk.Tk()
    # Le ponemos título a la ventana
    root.title("Certiredes S.A.S - Control de Inspectores")
    # Definimos el tamaño de la ventana (ancho x alto)
    root.geometry("350x450")

    # Creamos un texto de instrucciones en la ventana
    label = tk.Label(root, text="Selecciona la fecha de programación:", font=("Arial", 10, "bold"))
    label.pack(pady=10) # Lo colocamos en la ventana con un poco de margen superior e inferior

    # Creamos el calendario visual, configurado en español (dd/mm/yyyy) y fijando una fecha inicial
    cal = Calendar(root, selectmode='day', year=2026, month=3, day=27, date_pattern='dd/mm/yyyy')
    cal.pack(pady=10, padx=10) # Lo colocamos en la ventana

    # Esta función se ejecuta ÚNICAMENTE cuando presionas el botón verde
    def confirmar():
        # Obtenemos la fecha exacta que el usuario seleccionó en el calendario
        fecha_elegida = cal.get_date()
        # Imprimimos en la consola negra lo que vamos a hacer
        print(f"\n--- Procesando fecha seleccionada: {fecha_elegida} ---")
        # Cerramos la ventanita visual del calendario para continuar con el proceso en segundo plano
        root.destroy() 
        # Llamamos a la función que hace el trabajo pesado, pasándole la fecha elegida
        procesar_envio_masivo(fecha_elegida)

    # Creamos el botón verde de "ENVIAR"
    btn = tk.Button(root, text="ENVIAR A INSPECTORES", command=confirmar, bg="#25D366", fg="white", font=("Arial", 10, "bold"))
    btn.pack(pady=20) # Lo colocamos en la ventana

    # Mantenemos la ventana abierta y escuchando hasta que el usuario haga clic en enviar o la cierre
    root.mainloop()


# ==========================================
# 3. LÓGICA DE FILTRADO Y ENVÍO MASIVO
# ==========================================
def procesar_envio_masivo(fecha_seleccionada_str):

    try:
        # 1. ESTANDARIZAR LA FECHA ELEGIDA
        # Convertimos la fecha del calendario a un formato de texto estricto (Día/Mes/Año)
        fecha_filtro = pd.to_datetime(fecha_seleccionada_str, dayfirst=True).strftime('%d/%m/%Y')
    
        # 2. LEER EL EXCEL
        # Le decimos explícitamente que busque en la pestaña 'base'
        # 2. LEER EL EXCEL (Apuntando al archivo con macros)
        df = pd.read_excel('agenda.xlsm', sheet_name='base', engine='openpyxl')        
        # Limpiamos los títulos de las columnas: quitamos espacios a los lados y los pasamos a minúsculas para evitar errores
        df.columns = df.columns.str.strip().str.lower()

        # 3. ESTANDARIZAR LA COLUMNA DE FECHAS DEL EXCEL
        # Forzamos a que toda la columna 'fecha' del Excel se convierta a texto idéntico al de nuestro filtro
        df['fecha_limpia'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
        # --- LÍNEA DE DIAGNÓSTICO (Solo para ver qué está leyendo Python) ---
        print(f"Buscando: {fecha_filtro} | Fechas que Python ve en su Excel: {df['fecha_limpia'].unique()}")
        # ---------------------------------------------------------------------
    
        # 4. FILTRAR LA AGENDA
        # Creamos una sub-tabla ('citas_del_dia') que solo contenga las filas donde la fecha limpia coincide con nuestra fecha filtro
        citas_del_dia = df[df['fecha_limpia'] == fecha_filtro]

        # Si la sub-tabla está vacía (no hubo coincidencias), avisamos y detenemos el código aquí
        if citas_del_dia.empty:
            print(f"⚠️ No hay citas para el {fecha_filtro}.")
            return

        # Si sí encontró datos, imprimimos cuántos registros vamos a enviar
        print(f"✅ Se encontraron {len(citas_del_dia)} registros. Enviando...\n")

        # 5. BUCLE DE ENVÍO (MÁQUINA DE MENSAJES)
        # Recorremos fila por fila los registros encontrados de ese día
        for indice, fila in citas_del_dia.iterrows():
            try:
                # 5.1. EMPAQUETAR VARIABLES PARA TWILIO
                # Construimos el diccionario exacto que Twilio exige para llenar los "huecos" {{1}}, {{2}} de la plantilla
                variables_json = {
                    "1": str(fila['contrato']).strip(), # Quitamos espacios al número de contrato
                    "2": str(fila['nombre']).strip(), # Quitamos espacios al nombre del usuario
                    "3": str(fila['direccion']).strip(), # Quitamos espacios a la dirección
                    "4": str(fila['barrio']).strip(), # Quitamos espacios al barrio
                    "5": fecha_filtro, # PASO CORREGIDO: Usamos la fecha limpia que ya es texto
                    "6": str(fila['hora']).strip(), # Quitamos espacios a la hora
                    "7": str(fila['ciudad']).strip() # Quitamos espacios al municipio
                }

                # 5.2. LIMPIAR EL NÚMERO DE TELÉFONO
                # Si el excel trae "3151234567.0", lo partimos por el punto y nos quedamos con lo primero (3151234567)
                tel_crudo = str(fila['telefono']).split('.')[0].strip()
                
                # Le agregamos la palabra "whatsapp:" y el signo "+" que exige Twilio obligatoriamente
                if not tel_crudo.startswith('+'):
                    tel_final = f"whatsapp:+{tel_crudo}" 
                else:
                    tel_final = f"whatsapp:{tel_crudo}"

                # 5.3. DAR LA ORDEN DE DISPARO A TWILIO
                message = client.messages.create(
                    from_=NUMERO_TWILIO, # Remitente (Certiredes)
                    to=tel_final, # Destinatario (Inspector/Usuario)
                    content_sid=CONTENT_SID, # La plantilla aprobada v10
                    content_variables=json.dumps(variables_json) # Pasamos las 7 variables convertidas a formato JSON
                )

                # Confirmación visual en la pantalla negra de que el mensaje salió con éxito
                print(f"🟢 Enviado a {fila['nombre']} (Contrato {fila['contrato']})")

            except Exception as e_fila:
                # Si falla el envío de una fila específica (ej. teléfono malo), avisa pero NO detiene los demás envíos
                print(f"🔴 Error en fila {indice}: {e_fila}")

    except Exception as e_general:
        # Si hay un error masivo (ej. el Excel no existe o está abierto por otro programa), atrapa el error aquí
        print(f"❌ Error crítico: {e_general}")

# Este es el punto de arranque de Python. Cuando ejecutas el archivo, lo primero que hace es llamar a iniciar_aplicacion()
if __name__ == "__main__":
    iniciar_aplicacion()