from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
from datetime import datetime

app = Flask(__name__)

@app.route("/whatsapp", methods=['POST'])
def whatsapp_reply():
    # 1. Capturamos el número
    numero_colab = request.form.get('From', '').replace('whatsapp:', '')
    
    # ✨ LA MAGIA ESTÁ AQUÍ: Leemos la caja secreta (Payload) y la caja normal (Body)
    payload_oculto = request.form.get('ButtonPayload')
    texto_visible = request.form.get('Body', '').strip().upper()

    # Si el botón trae carga oculta (ej. CONFIRMAR-1256), usamos esa. 
    # Si no trae nada, usamos lo que sea que haya escrito el usuario.
    if payload_oculto:
        mensaje_recibido = payload_oculto.strip().upper()
    else:
        mensaje_recibido = texto_visible
        
    ahora = datetime.now().strftime("%d/%m/%Y %H:%M")

    # 2. Lógica para separar Acción y Contrato
    contrato = "Desconocido"
    estado = "MENSAJE_OTRO"

    if "-" in mensaje_recibido:
        partes = mensaje_recibido.split("-")
        estado = partes[0] # "CONFIRMAR" o "CANCELAR"
        contrato = partes[1] # "1256"
    else:
        # Por si alguien escribe manualmente sin usar el botón
        estado = "MANUAL"
        contrato = "N/A"

    # 3. Guardar en el Log de texto
    with open("log_certiredes.txt", "a", encoding='latin-1') as f:
        f.write(f"[{ahora}] {estado} - Contrato: {contrato} - Inspector: {numero_colab}\n")

    # 4. Respuesta automática al inspector
    respuesta_wa = MessagingResponse()
    if "CONFIRM" in estado:
        respuesta_wa.message(f"✅ Recibido. Orden {contrato} confirmada. ¡Buen turno!")
    elif "CANCEL" in estado:
        respuesta_wa.message(f"🚨 Reportado. La orden {contrato} ha sido marcada como rechazada.")
    else:
        respuesta_wa.message("🤖 Certiredes: Por favor usa los botones del mensaje para responder.")

    return str(respuesta_wa)

if __name__ == "__main__":
    # Corremos el servidor en el puerto 5000
    app.run(port=5000)