import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

Deno.serve(async (req) => {
  if (req.method === 'GET') {
    const url = new URL(req.url)
    return new Response(url.searchParams.get("hub.challenge") || "OK", { status: 200 })
  }

  if (req.method === 'POST') {
    try {
      const body = await req.json()
      const message = body.entry?.[0]?.changes?.[0]?.value?.messages?.[0]
      
      if (message && message.type === "button") {
        const textoBoton = message.button.text
        const telefonoMeta = message.from // Ej: "573182672814"
        
        // PASO 1: Quitar el "57" inicial para que coincida con tu directorio
        let celularLocal = telefonoMeta;
        if (telefonoMeta.startsWith("57") && telefonoMeta.length === 12) {
          celularLocal = telefonoMeta.substring(2);
        }

        console.log(`🔘 Botón presionado. Buscando el celular: ${celularLocal}`);

        const supabase = createClient(
          Deno.env.get('SUPABASE_URL') ?? '',
          Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
        )

        // PASO 2: Buscar en el directorio de inspectores
        // NOTA: Asumo que en el directorio hay una columna llamada 'codigo_tecnico'
        const { data: inspector, error: errorDirectorio } = await supabase
          .from('directorio_inspectores_test')
          .select('codigo_tecnico') 
          .eq('celular', celularLocal)
          .single()

        if (errorDirectorio || !inspector) {
           console.error(`❌ El celular ${celularLocal} no existe en el directorio o hubo un error.`)
        } else {
           const codigo = inspector.codigo_tecnico;
           console.log(`✅ Se encontró al técnico con código: ${codigo}. Actualizando base_general...`)

           // PASO 3: Actualizar la base general usando el código
           const { error: errorUpdate } = await supabase
             .from('base_general_test')
             .update({ estado_whatsapp: '✅ RECIBIDO' })
             .eq('codigo_tecnico', codigo)
             .eq('estado_whatsapp', '✅ MSJ ENVIADO') // Solo actualiza si estaba en 'ENVIADO'
             
           if (errorUpdate) {
             console.error("❌ Error actualizando base_general:", errorUpdate.message)
           } else {
             console.log("✅ ¡MISIÓN CUMPLIDA! Estado actualizado a ✅ RECIBIDO")
           }
        }
      }
      return new Response("OK", { status: 200 })
    } catch (e) {
      console.error("❌ ERROR CRÍTICO:", e.message)
      return new Response("Error", { status: 400 })
    }
  }
  return new Response("No permitido", { status: 405 })
})