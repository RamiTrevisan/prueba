import pandas as pd
import pyodbc
from tkinter import Tk, Button, Label, filedialog, messagebox
import chardet

# Función para cargar el archivo CSV y realizar inserciones en la base de datos
def cargar_csv():
    # Abrir el diálogo para seleccionar el archivo
    archivo = filedialog.askopenfilename(filetypes=[("Archivos CSV", "*.csv")])
    if archivo:
        # Detectar la codificación del archivo
        with open(archivo, 'rb') as f:
            result = chardet.detect(f.read())
            encoding = result['encoding']

        try:
            # Leer el archivo CSV usando la codificación detectada
            df = pd.read_csv(archivo, encoding=encoding, header=None, on_bad_lines='skip')

            # Verificar que los datos se han cargado correctamente
            print(f"Archivo cargado correctamente con {df.shape[0]} filas y {df.shape[1]} columnas.")
            print("Primeros 5 registros del archivo CSV:")
            print(df.head())

            # Verificar si la fila 2 y la columna I (índice 8) existen
            if df.shape[0] < 6:
                raise ValueError("El archivo CSV no tiene suficientes filas (mínimo 6 filas necesarias).")
            if df.shape[1] <= 8:
                raise ValueError("El archivo CSV no tiene suficientes columnas (mínimo 9 columnas necesarias).")

            # Obtener el meter_id de la fila 2, columna A
            meter_id = df[0].iloc[1].split(' - ')[0].strip()  # Extraemos el meter_id de la columna A, fila 2

            # Conectar a la base de datos SQL Server
            conn = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};'
                'SERVER=10.10.10.120;'
                'DATABASE=Client1;'
                'UID=OPTIMUM;'
                'PWD=OPTIMUM;'
            )
            cursor = conn.cursor()

            # Iterar sobre las filas del DataFrame a partir de la fila 6 (índice 5) y obtener las fechas de la columna I
            for index, fecha in enumerate(df.iloc[5:, 8]):
                # Formatear la fecha para SQL
                fecha_formateada = f"'{fecha}'"  # Formato correcto para SQL

                # Mostrar los datos a insertar y pedir confirmación
                print(f"Procesando fila {index + 6} - Meter ID: {meter_id}, Fecha: {fecha_formateada}")

                # Mostrar la confirmación antes de insertar
                confirmar = messagebox.askyesno("Confirmar inserción", 
                    f"¿Deseas insertar los siguientes datos?\n\n"
                    f"Meter ID: {meter_id}\n"
                    f"Fecha: {fecha_formateada}\n"
                )
                
                # Si el usuario confirma, insertar en la base de datos
                if confirmar:
                    cursor.execute('''
                        INSERT INTO M_PROFILE (METER_ID, METER_T0, METER_TF, local_t0, local_tf, 
                        channel, reg_descr_id, qualifier, val, val_demand, val_edit, val_factor, ke)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (meter_id, fecha_formateada, fecha_formateada, fecha_formateada, fecha_formateada, 1, 1, 0, 0, 0, 0, 0, 1))
                else:
                    print(f"Fila {index + 6} no fue insertada.")

            # Guardar cambios y cerrar la conexión
            conn.commit()
            conn.close()

            label_resultado.config(text="Datos cargados e insertados correctamente.")
        except Exception as e:
            label_resultado.config(text=f"Error: {str(e)}")

# Crear la ventana principal
ventana = Tk()
ventana.title("Cargar CSV a Base de Datos")

# Crear un botón para cargar el archivo
boton_cargar = Button(ventana, text="Cargar archivo CSV", command=cargar_csv)
boton_cargar.pack(pady=20)

# Etiqueta para mostrar resultados
label_resultado = Label(ventana, text="")
label_resultado.pack(pady=20)

# Iniciar el bucle de la interfaz gráfica
ventana.mainloop()
