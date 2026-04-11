import pandas as pd
from sqlalchemy import create_engine

print("Abriendo el archivo COLUMNA.xlsx...")
# Usamos read_excel para archivos .xlsx
try:
    df = pd.read_excel('COLUMNA.xlsx')
except FileNotFoundError:
    # Si falla, intentamos con la ruta completa por si acaso
    print("Archivo no encontrado en la carpeta raíz, intentando ruta completa...")
    df = pd.read_excel(r'C:\prueba\ENVIO MASIVO DE MENSAJES 2026 MARZO\COLUMNA.xlsx')

print("Conectando a Supabase...")
# Su enlace oficial con el pase de seguridad SSL
engine = create_engine("postgresql://postgres.jepgfqxeukmxjusctnut:Certiredes2027@aws-0-us-east-1.pooler.supabase.com:6543/postgres?sslmode=require")

print("Creando el molde en la nube (sin datos, solo columnas)...")
# head(0) crea la estructura vacía
df.head(0).to_sql('base_general', engine, if_exists='replace', index=False)

print("¡ÉXITO TOTAL! La tabla 'base_general' ya existe en Supabase.")