import pandas as pd
import pyodbc
from tkinter import Tk, Button, Label, filedialog
import chardet
from dateutil import parser

# Función para limpiar y procesar el archivo CSV
def limpiar_csv(archivo, encoding):
    with open(archivo, 'r', encoding=encoding) as f:
        lines = f.readlines()

    # Normalizar líneas (eliminar espacios en blanco al final y agregar delimitadores si faltan)
    lines = [line.strip() + ";" * (10 - line.count(";")) + "\n" for line in lines if line.strip()]
    archivo_corregido = archivo.replace('.csv', '_limpiado.csv')
    with open(archivo_corregido, 'w', encoding=encoding) as f:
        f.writelines(lines)

    print(f"Archivo limpiado guardado como: {archivo_corregido}")
    return archivo_corregido

# Función para cargar el archivo CSV y realizar inserciones en la base de datos
def cargar_csv():
    archivo = filedialog.askopenfilename(filetypes=[("Archivos CSV", "*.csv")])
    if archivo:
        with open(archivo, 'rb') as f:
            result = chardet.detect(f.read())
            encoding = result['encoding']
        print(f"Codificación detectada: {encoding}")

        try:
            # Limpiar y corregir el archivo
            archivo = limpiar_csv(archivo, encoding)

            # Leer archivo corregido
            df = pd.read_csv(archivo, encoding=encoding, delimiter=';', header=None, skip_blank_lines=True, on_bad_lines='skip')
            print("Contenido del archivo CSV cargado:")
            print(df.head())

            # Extraer Meter ID
            meter_id = df.iloc[1, 0].split(' - ')[0].strip()
            print(f"Meter ID extraído: {meter_id}")

            conn = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};'
                'SERVER=10.10.10.120;'
                'DATABASE=Client1;'
                'UID=OPTIMUM;'
                'PWD=OPTIMUM;'
            )
            cursor = conn.cursor()

            for index, row in df.iloc[5:].iterrows():
                try:
                    fecha = str(row[8]).strip()
                    fecha_convertida = parser.parse(fecha, fuzzy=False)
                    fecha_formateada = fecha_convertida.strftime('%Y-%m-%d %H:%M:%S')

                    # Insertar en la base de datos
                    cursor.execute('''INSERT INTO M_PROFILE (METER_ID, METER_T0, METER_TF, local_t0, local_tf, 
                                      channel, reg_descr_id, qualifier, val, val_demand, val_edit, val_factor, ke)
                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                   (meter_id, fecha_formateada, fecha_formateada, fecha_formateada, fecha_formateada,
                                    1, 1, 0, 0, 0, 0, 0, 1))
                    print(f"Registro insertado: {meter_id}, {fecha_formateada}")

                except (ValueError, TypeError) as e:
                    print(f"Fecha no válida: '{row[8]}' en la fila {index + 6}, saltando registro.")

            conn.commit()
            conn.close()
            print("Proceso completado. Todos los registros válidos han sido insertados.")
        except Exception as e:
            print(f"Error general: {str(e)}")

ventana = Tk()
ventana.title("Cargar CSV a Base de Datos")
ventana.geometry("600x400")

boton_cargar = Button(ventana, text="Cargar archivo CSV", command=cargar_csv)
boton_cargar.pack(pady=20)

label_resultado = Label(ventana, text="Esperando archivo CSV...")
label_resultado.pack(pady=20)

ventana.mainloop()