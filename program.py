import pandas as pd
import pyodbc
from tkinter import Tk, Button, Label, filedialog, PhotoImage, Frame, Scrollbar, Text
import chardet
from dateutil import parser
from datetime import datetime, time, timedelta
import sys
import os
from PIL import Image, ImageTk
from tkinter import messagebox
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading

# Constante para los parámetros de conexión
CONN_PARAMS = {
    'DRIVER': '{ODBC Driver 17 for SQL Server}',
    'SERVER': '10.10.10.120',
    'DATABASE': 'Client1',
    'UID': 'OPTIMUM',
    'PWD': 'OPTIMUM'
}

# --- Funciones de la aplicación ---

def escribir_en_consola(mensaje):   
    console.insert("end", mensaje + "\n")  # Insertar el mensaje al final del Text
    console.yview("end")  # Desplazar el scrollbar al final del Text
    root.update_idletasks()  # Actualiza la interfaz para que se muestre el texto inmediatamente

def get_connection():
    conn_str = ';'.join([f'{key}={value}' for key, value in CONN_PARAMS.items()])
    return pyodbc.connect(conn_str)

def limpiar_csv(archivo, encoding):
    with open(archivo, 'r', encoding=encoding) as f:
        lines = f.readlines()
    lines = [line.strip() + ";" * (10 - line.count(";")) + "\n" for line in lines if line.strip()]
    archivo_corregido = archivo.replace('.csv', '_limpiado.csv')
    with open(archivo_corregido, 'w', encoding=encoding) as f:
        f.writelines(lines)
    escribir_en_consola(f"Archivo limpiado guardado como: {archivo_corregido}")
    return archivo_corregido

def obtener_ke(meter_id):
    conn = get_connection()
    cursor = conn.cursor()
    query = '''SELECT ke, channel FROM m_reg_desc_map WHERE meter_id = ? AND reg_type = 4 AND log = 0'''
    cursor.execute(query, (meter_id,))
    rows = cursor.fetchall()
    conn.close()
    return {row.channel: row.ke for row in rows}

def obtener_raw_unit(meter_id, channel):
    conn = get_connection()
    cursor = conn.cursor()
    query = '''SELECT descr FROM m_raw_units WHERE id = (
               SELECT id_raw_unit FROM m_reg_desc_map 
               WHERE meter_id = ? AND reg_type = 4 AND channel = ? AND log = 0)'''
    cursor.execute(query, (meter_id, channel))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None

def obtener_reg_descr_id(meter_id, channel):
    conn = get_connection()
    cursor = conn.cursor()
    query = '''SELECT id FROM m_reg_desc_map 
               WHERE meter_id = ? AND reg_type = 4 AND channel = ? AND log = 0'''
    cursor.execute(query, (meter_id, channel))
    row = cursor.fetchone()
    conn.close()
    if row:
        return row[0]
    else:
        messagebox.showinfo("Proceso Interrumpido", "El medidor no tiene lecturas previas de los canales.")
        sys.exit()

def cargar_csv(archivo=None):
    if not archivo:
        archivo = filedialog.askopenfilename(filetypes=[("Archivos CSV", "*.csv")])
    if archivo:
        estado_label.config(text="PROCESANDO...")
        root.update()
        with open(archivo, 'rb') as f:
            result = chardet.detect(f.read())
            encoding = result['encoding']
        escribir_en_consola(f"Codificación detectada: {encoding}")
        try:
            archivo = limpiar_csv(archivo, encoding)
            df = pd.read_csv(archivo, encoding=encoding, delimiter=';', header=None, skip_blank_lines=True, on_bad_lines='skip')
            escribir_en_consola("Contenido del archivo CSV cargado:")
            meter_id = df.iloc[1, 0].split(' - ')[0].strip()
            escribir_en_consola(f"Meter ID extraído: {meter_id}")
            ke_dict = obtener_ke(meter_id)
            conn = get_connection()
            cursor = conn.cursor()
            for index, row in df.iloc[5:].iterrows():
                try:
                    fecha = str(row[8]).strip() if pd.notna(row[8]) else ''
                    fecha_convertida = parser.parse(fecha, fuzzy=False) if fecha else None
                    fecha_formateada_t0 = fecha_convertida.strftime('%Y-%m-%d %H:%M:%S') if fecha_convertida else None
                    fecha_formateada_tf = (fecha_convertida + timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S') if fecha_convertida else None
                    for col_idx in range(8):
                        valor = str(row[col_idx]).strip() if pd.notna(row[col_idx]) else ''
                        valor_insert = float(valor.replace(',', '.')) if valor else None
                        channel = col_idx + 1
                        val_demand = valor_insert * 4 if channel in [1, 2] else valor_insert
                        ke = ke_dict.get(channel, 1)
                        raw_unit = obtener_raw_unit(meter_id, channel)
                        reg_descr_id = obtener_reg_descr_id(meter_id, channel)
                        val_factor = valor_insert * ke if valor_insert is not None else None
                        cursor.execute('''INSERT INTO M_PROFILE (meter_id, meter_t0, meter_tf, local_t0, local_tf, 
                                                              channel, reg_descr_id, qualifier, val, raw_unit, 
                                                              val_demand, val_edit, val_factor, ke, origin_id)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                           (meter_id, fecha_formateada_t0, fecha_formateada_tf, fecha_formateada_t0, fecha_formateada_tf,
                            channel, reg_descr_id, 0, valor_insert, raw_unit, val_demand, val_factor, val_factor, ke, 2))
                        escribir_en_consola(f"Registro insertado: Meter ID={meter_id}, Channel={channel}, Val={valor_insert}")
                except Exception as e:
                    escribir_en_consola(f"Error procesando fila: {e}")
            conn.commit()
            conn.close()
            escribir_en_consola("Carga de CSV e inserción completada.")
            estado_label.config(text="Carga finalizada con éxito.")
        except Exception as e:
            escribir_en_consola(f"Error al procesar el archivo: {e}")

# --- Monitoreo de carpeta con watchdog ---

class CSVHandler(FileSystemEventHandler):
    
    
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv') and '_limpiado' not in event.src_path:
            escribir_en_consola(f"Nuevo archivo detectado: {event.src_path}")
            cargar_csv(event.src_path)
    
    
def iniciar_monitoreo():
    directorio = filedialog.askdirectory()
    if directorio:
        observer = Observer()
        event_handler = CSVHandler()
        observer.schedule(event_handler, path=directorio, recursive=True)
        observer_thread = threading.Thread(target=observer.start, daemon=True)
        observer_thread.start()
        escribir_en_consola(f"Monitoreando cambios en: {directorio}")

# --- Interfaz gráfica ---

root = Tk()
root.title("Carga de CSV y procesamiento de datos")
root.geometry("800x600")
frame = Frame(root)



# Obtener la ruta al directorio actual del script o ejecutable
base_path = os.path.dirname(os.path.abspath(__file__))

# Construir la ruta al archivo de la imagen
image_path = os.path.join(base_path, 'images', 'logo_16b.jpg')



# Cargar la imagen de fondo
background_image = Image.open(image_path) 
background_image = background_image.resize((300, 200), Image.Resampling.LANCZOS)  # Redimensionar la imagen a un tamaño más pequeño
background_photo = ImageTk.PhotoImage(background_image)

# Crear un Label para mostrar la imagen
background_label = Label(root, image=background_photo, width=300, height=200)
background_label.grid(row=0, column=0, padx=10, pady=10)

estado_label = Label(root, text="Esperando archivo...", font=("Arial", 14), width=50, height=2)
console = Text(root, height=10, width=80)

estado_label.grid(row=1, column=0, padx=20, pady=10)
frame.grid(row=2, column=0, padx=20, pady=10)
console.grid(row=3, column=0, padx=20, pady=20)

cargar_button = Button(frame, text="Cargar archivo CSV", font=("Arial", 14), command=lambda: cargar_csv())
monitorear_button = Button(frame, text="Monitorear carpeta", font=("Arial", 14), command=iniciar_monitoreo)

cargar_button.pack(pady=10)
monitorear_button.pack(pady=10)

root.mainloop()
