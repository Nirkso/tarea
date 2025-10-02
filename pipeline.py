# --- 1. Importar las "herramientas" necesarias ---
import apache_beam as beam # La biblioteca principal de Apache Beam.
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions # Clases para configurar el runner (motor de ejecución) y otras opciones del pipeline.
import argparse # Para manejar los paths de entrada y salida desde la línea de comandos.
import json # Para trabajar con archivos JSON (deserializar la entrada y serializar la salida).
import re # Para usar expresiones regulares (limpieza de texto).
import csv # Para leer y procesar el archivo CSV.
import io # Para ayudar a leer el CSV línea por línea y parsear correctamente.

# --- 2. TRANSFORMACIÓN CON ParDo (DoFn - Do Function) ---
     # Estandariza 'RaceID': convierte a minúsculas, quita símbolos y maneja errores de formato JSON o de clave.
class NormalizarIDCarrera(beam.DoFn):
  
    def process(self, elemento):
        try:
            # Convierte la línea de texto (string) de entrada a un diccionario.
            registro = json.loads(elemento)

            # 1. Convertir a minúsculas.
            id_carrera = registro['RaceID'].lower()

            # 2. Eliminación de caracteres que NO sean letra (a-z) o número (0-9).
            normalized_id = re.sub(r'[^a-z0-9]+', '', id_carrera)

            # Actualiza el registro con el ID normalizado.
            registro['RaceID'] = normalized_id

            # Devuelve el registro (diccionario) transformado.
            yield registro

        except json.JSONDecodeError:
            # Si la línea no es un JSON válido, la omite.
            pass
        except KeyError:
            # Si el registro no tiene la clave 'RaceID', también lo omite.
            pass

    # Filtra y elimina registros donde 'DeviceType' es igual a "Other".
class FiltrarTipoDispositivo(beam.DoFn):

    def process(self, registro):
        # Utiliza .get() para evitar KeyError si la clave no existe. Si 'DeviceType' NO es 'Other', se mantiene el registro.
        if registro.get('DeviceType') != 'Other':
            yield registro

# --- 3. Realiza el JOIN (unión) usando un Side Input (datos_pais, cargados desde el CSV).---
class EnriquecerConDatosPais(beam.DoFn):

    # El argumento 'datos_pais' es el Side Input (vista de diccionario del CSV).
    def process(self, registro, datos_pais):

        # 1. Obtener la clave de unión ('ViewerLocationCountry') y la elimina del registro original.
        # El segundo argumento de pop (None) evita un KeyError si la clave no existe.
        clave_pais = registro.pop('ViewerLocationCountry', None)

        # 2. JOIN/Búsqueda (Lookup): Busca la información en el Side Input.
        info_ubicacion = datos_pais.get(clave_pais, None)

        if info_ubicacion:
            # Si hay match y encuentra la clave_pais en el diccionario del CSV se construye la estructura anidada 'LocationData' con los campos seleccionados.
            registro['LocationData'] = {
                'country': info_ubicacion['Country'],
                'capital': info_ubicacion['Capital'],
                'continent': info_ubicacion['Continent'],
                'official language': info_ubicacion['Main Official Language'],
                'currency': info_ubicacion['Currency']
            }
        else:
            # Si no hay match (el país no está en el CSV), se deja vacío.
            registro['LocationData'] = {}

        # Devuelve el registro enriquecido.
        yield registro

# --- 4. PREPARACIÓN DEL CSV (SIDE INPUT) ---
   # Carga el CSV, lo transforma a pares (clave, valor) y crea la PCollectionView (AsDict).
def leer_y_preparar_csv(p, ruta_csv):

    pcoll_clave_valor = (
        p
        # Extracción. Lee el archivo CSV línea por línea, saltando el encabezado (línea 1).
        | 'Leer CSV' >> beam.io.ReadFromText(ruta_csv, skip_header_lines=1)
        # Transformación. Utiliza el módulo CSV para parsear correctamente cada línea.
        | 'Separar Lineas CSV' >> beam.Map(lambda line: next(csv.reader(io.StringIO(line))))
        # /Transformación. Mapea la lista de columnas (row) a un diccionario.
        | 'Mapear a Diccionario' >> beam.Map(lambda row: {
            # Mapeo de columnas por índice (ejemplo: row[0] es 'Country'):
            'Country': row[0], 'Capital': row[1], 'GDP': row[2], 'Population': row[3],
            'Pop. Growth Rate': row[4], 'Life Expectancy': row[5], 'Median Age': row[6],
            'Urban Population': row[7], 'Continent': row[8], 'Main Official Language': row[9],
            'Currency': row[10]
        })
        # Transformación. Convierte el diccionario a un par (Clave_Pais, Datos_Requeridos).
        # Esta es la estructura necesaria para un Side Input de tipo diccionario.
        | 'A Par Clave-Valor' >> beam.Map(lambda row: (
            row['Country'],
            # Selecciona solo los campos que se van a usar para el enriquecimiento.
            {k: row[k] for k in ['Country', 'Capital', 'Continent', 'Main Official Language', 'Currency']}
        ))
    )

    # Creación de PCollectionView: Convierte la PCollection de pares en una vista
    # de diccionario eficiente para el ParDo de enriquecimiento.
    return beam.pvalue.AsDict(pcoll_clave_valor)

# 5. --- FUNCIÓN PRINCIPAL DE EJECUCIÓN (MAIN) ---

def ejecutar():

    # Ruta base predefinida para los archivos (útil en entornos como Colab).
    RUTA_BASE = '/content/tarea/'

    # Configuración de Paths de entrada y salida con argparse.
    parser = argparse.ArgumentParser(description="Pipeline ETL para datos de HRL.")
    # Ruta de entrada: busca todos los archivos .json en RUTA_BASE.
    parser.add_argument('--ruta_json_entrada', dest='ruta_json_entrada', default=f'{RUTA_BASE}*.json')
    # Ruta del archivo CSV de enriquecimiento.
    parser.add_argument('--ruta_csv_entrada', dest='ruta_csv_entrada', default=f'{RUTA_BASE}country_data_v2.csv')
    # Ruta de la carpeta y nombre base para el archivo de resultado.
    parser.add_argument('--ruta_salida', dest='ruta_salida', default=f'{RUTA_BASE}resultado/resultado_final')

    # Parsea los argumentos conocidos y deja el resto para PipelineOptions.
    conocidos_args, args_pipeline = parser.parse_known_args()

    # Configuración del PipelineOptions.
    opciones = PipelineOptions(args_pipeline)
    # Usamos DirectRunner para ejecución local/prueba.
    opciones.view_as(StandardOptions).runner = 'DirectRunner'

    # Inicio del Pipeline de Apache Beam.
    print("Iniciando Pipeline Apache Beam")
    # El bloque 'with' asegura que el pipeline se ejecute y se cierren los recursos.
    with beam.Pipeline(options=opciones) as p:

        # 1. Preparar side input (CSV): Carga el CSV en la vista de diccionario.
        vista_datos_pais = leer_y_preparar_csv(p, conocidos_args.ruta_csv_entrada)

        # 2. Procesamiento de los JSON (ETL)
        (
            p
            # Extracción. Lee los archivos JSON línea por línea (como strings).
            | 'Leer Archivos JSON' >> beam.io.ReadFromText(conocidos_args.ruta_json_entrada)
            # Transformación 1. Limpia y normaliza el RaceID.
            | '1-Normalizar ID Carrera' >> beam.ParDo(NormalizarIDCarrera())
            # Transformación 2. Filtra los registros con DeviceType 'Other'.
            | '2-Filtrar Dispositivos' >> beam.ParDo(FiltrarTipoDispositivo())
            # Transformación 3. Enriquecimiento (JOIN) con el Side Input del CSV.
            | '3-Enriquecer Datos' >> beam.ParDo(
                EnriquecerConDatosPais(),
                datos_pais=vista_datos_pais # Se pasa la PCollectionView (Side Input) aquí.
            )
            # Transformación 4. Convierte el diccionario final a una línea de texto JSON (string).
            | '4-Convertir a Texto JSON' >> beam.Map(json.dumps)

            # 3. Carga (Load)
            # Carga. Escribe las líneas de texto JSON al archivo de salida.
            | '5-Escribir a Archivo' >> beam.io.WriteToText(
                conocidos_args.ruta_salida,  # Carpeta y nombre base del archivo
                file_name_suffix='.jsonl',   # Agrega la extensión .jsonl al archivo final
                shard_name_template=''       # Indica que se debe escribir todo en un solo archivo
            )
        )

    print("Pipeline finalizado con éxito. Resultado guardado en la carpeta /resultado con el nombre resultado_final.jsonl.")

# Punto de entrada principal del script.
if __name__ == '__main__':
    ejecutar()

