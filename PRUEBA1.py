import pandas as pd
import pyodbc
from tkinter import Tk, Button, Label, filedialog, messagebox
import chardet
from dateutil import parser

# Función para cargar el archivo CSV y realizar inserciones en la base de datos
def cargar_csv():
    archivo = filedialog.askopenfilename(filetypes=[("Archivos CSV", "*.csv")])
    if archivo:
        with open(archivo, 'rb') as f:
            result = chardet.detect(f.read())
            encoding = result['encoding']
        print(f"Codificación detectada: {encoding}")

        try:
            df = pd.read_csv(archivo, encoding=encoding, delimiter=';', header=None, on_bad_lines='skip', skip_blank_lines=True)
            print("Contenido del archivo CSV:")
            print(df)

            meter_id = df[0].iloc[1].split(' - ')[0].strip()
            print(f"Meter ID extraído: {meter_id}")

            conn = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};'
                'SERVER=10.10.10.120;'
                'DATABASE=Client1;'
                'UID=OPTIMUM;'
                'PWD=OPTIMUM;'
            )
            cursor = conn.cursor()

            # Lista para almacenar las fechas no insertadas
            fechas_no_insertadas = []

            for index, fecha in enumerate(df.iloc[5:, 8]):
                try:
                    # Eliminar espacios y formatear la fecha
                    fecha = str(fecha).strip()  # Eliminar espacios
                    fecha_convertida = parser.parse(fecha, fuzzy=False)
                    fecha_formateada = fecha_convertida.strftime('%Y-%m-%d %H:%M:%S')  # Formato deseado
                    print(f"Fecha válida y formateada: {fecha_formateada}")
                except (ValueError, TypeError) as e:
                    continuar = messagebox.askyesno(
                        "Fecha no válida detectada",
                        f"Se encontró una fecha no válida en la fila {index + 6}: '{fecha}'\n"
                        f"Tipo de dato: {type(fecha).__name__}\n\n"
                        "¿Deseas continuar con la inserción de esta fecha?"
                    )
                    if not continuar:
                        print(f"Fecha no insertada: {fecha} en la fila {index + 6}")
                        fechas_no_insertadas.append((index + 6, fecha))  # Guardamos las fechas no insertadas
                        continue
                    else:
                        print(f"Fecha no válida aprobada para inserción: {fecha}")
                        fecha_formateada = None  # No insertar si no es válida

                if fecha_formateada:
                    try:
                        cursor.execute('''INSERT INTO M_PROFILE (METER_ID, METER_T0, METER_TF, local_t0, local_tf, 
                                          channel, reg_descr_id, qualifier, val, val_demand, val_edit, val_factor, ke)
                                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                       (meter_id, fecha_formateada, fecha_formateada, fecha_formateada, fecha_formateada, 1, 1, 0, 0, 0, 0, 0, 1))
                        print(f"Inserción realizada: {meter_id}, {fecha_formateada}")
                    except pyodbc.Error as db_err:
                        print(f"Error en la inserción: {db_err}")
                        messagebox.showerror("Error de base de datos", f"Error en la inserción: {db_err}")
                        break

            conn.commit()
            conn.close()

            # Mostrar los registros no insertados en consola
            if fechas_no_insertadas:
                print("Fechas no insertadas:")
                for fila, fecha in fechas_no_insertadas:
                    print(f"Fila {fila}: {fecha}")

            # Mostrar resultado en la interfaz gráfica
            if fechas_no_insertadas:
                fechas_str = '\n'.join([f"Fila {f[0]}: {f[1]}" for f in fechas_no_insertadas])
                label_resultado.config(text=f"Datos cargados. No se insertaron las siguientes fechas:\n{fechas_str}")
            else:
                label_resultado.config(text="Datos cargados e insertados correctamente.")
        except Exception as e:
            print(f"Error: {str(e)}")
            label_resultado.config(text=f"Error: {str(e)}")

ventana = Tk()
ventana.title("Cargar CSV a Base de Datos")
ventana.geometry("600x400")

boton_cargar = Button(ventana, text="Cargar archivo CSV", command=cargar_csv)
boton_cargar.pack(pady=20)

label_resultado = Label(ventana, text="")
label_resultado.pack(pady=20)

ventana.mainloop()
