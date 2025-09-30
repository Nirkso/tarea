# --- 1. Importar las "herramientas" necesarias ---
import apache_beam as beam  # La biblioteca principal de Apache Beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse  # Para manejar los paths de entrada y salida
import json      # Para trabajar con archivos JSON
import re        # Para usar expresiones regulares (limpieza de texto)
import csv       # Para leer y procesar el archivo CSV
import io        # Para ayudar a leer el CSV línea por línea

# ==============================================================================
# TRANSFORMACIÓN (ParDo)
# ==============================================================================

class NormalizarIDCarrera(beam.DoFn):
    """
    Estandariza 'RaceID', quita símbolos y maneja errores de formato JSON.
    """
    def process(self, elemento):
        try:
            registro = json.loads(elemento)

            # 1. Convertir a minúsculas.
            id_carrera = registro['RaceID'].lower()

            # 2. Limpieza estricta: Elimina CUALQUIER carácter que NO sea letra (a-z) o número (0-9).
            normalized_id = re.sub(r'[^a-z0-9]+', '', id_carrera)

            registro['RaceID'] = normalized_id

            yield registro

        except json.JSONDecodeError:
            pass
        except KeyError:
            pass

class FiltrarTipoDispositivo(beam.DoFn):
    """
    Filtra y elimina registros donde 'DeviceType' es igual a "Other".
    """
    def process(self, registro):
        if registro.get('DeviceType') != 'Other':
            yield registro

class EnriquecerConDatosPais(beam.DoFn):
    """
    Realiza el JOIN (unión) usando el Side Input del CSV.
    """
    def process(self, registro, datos_pais):
        # 'datos_pais' es el diccionario completo que Beam resuelve del CSV.

        # 1. Obtener la clave de unión y la eliminamos del registro original.
        clave_pais = registro.pop('ViewerLocationCountry', None)

        # 2. JOIN/Búsqueda (Lookup)
        info_ubicacion = datos_pais.get(clave_pais, None)

        if info_ubicacion:
            # Si hay match, construimos la estructura anidada 'LocationData'
            registro['LocationData'] = {
                'country': info_ubicacion['Country'],
                'capital': info_ubicacion['Capital'],
                'continent': info_ubicacion['Continent'],
                'official language': info_ubicacion['Main Official Language'],
                'currency': info_ubicacion['Currency']
            }
        else:
            # Si no hay match, la dejamos vacía.
            registro['LocationData'] = {}

        yield registro

# ==============================================================================
# PREPARACIÓN DEL CSV (SIDE INPUT)
# ==============================================================================

def leer_y_preparar_csv(p, ruta_csv):
    """
    Carga el CSV, lo transforma a pares (clave, valor) y crea la PCollectionView.
    """

    pcoll_clave_valor = (
        p
        | 'Leer CSV' >> beam.io.ReadFromText(ruta_csv, skip_header_lines=1)
        | 'Separar Lineas CSV' >> beam.Map(lambda line: next(csv.reader(io.StringIO(line))))
        | 'Mapear a Diccionario' >> beam.Map(lambda row: {
            # Mapeo de columnas:
            'Country': row[0], 'Capital': row[1], 'GDP': row[2], 'Population': row[3],
            'Pop. Growth Rate': row[4], 'Life Expectancy': row[5], 'Median Age': row[6],
            'Urban Population': row[7], 'Continent': row[8], 'Main Official Language': row[9],
            'Currency': row[10]
        })
        # Creamos pares (Clave_Pais, Datos_Requeridos)
        | 'A Par Clave-Valor' >> beam.Map(lambda row: (
            row['Country'],
            {k: row[k] for k in ['Country', 'Capital', 'Continent', 'Main Official Language', 'Currency']}
        ))
    )

    # Creamos y retornamos la PCollectionView (vista de diccionario)
    return beam.pvalue.AsDict(pcoll_clave_valor)

# ==============================================================================
# FUNCIÓN PRINCIPAL DE EJECUCIÓN (MAIN)
# ==============================================================================

def ejecutar():
    
    # ⚠️ AJUSTE CLAVE: Usamos la carpeta actual './' (tarea/) para buscar los archivos
    RUTA_BASE = './'

    # Configuración de Paths
    parser = argparse.ArgumentParser(description="Pipeline ETL para datos de HRL.")
    # Las rutas apuntan directamente a los archivos en la carpeta de ejecución
    parser.add_argument('--ruta_json_entrada', dest='ruta_json_entrada', default=f'{RUTA_BASE}*.json')
    parser.add_argument('--ruta_csv_entrada', dest='ruta_csv_entrada', default=f'{RUTA_BASE}country_data.csv')
    parser.add_argument('--ruta_salida', dest='ruta_salida', default='./resultado/datos_enriquecidos_finales')

    conocidos_args, args_pipeline = parser.parse_known_args()

    # Opciones de Ejecución: Usamos DirectRunner
    opciones = PipelineOptions(args_pipeline)
    opciones.view_as(StandardOptions).runner = 'DirectRunner'

    # Inicio del Pipeline de Apache Beam
    print("🚀 Iniciando Pipeline Apache Beam...")
    with beam.Pipeline(options=opciones) as p:

        # 1. Preparar Side Input (CSV)
        vista_datos_pais = leer_y_preparar_csv(p, conocidos_args.ruta_csv_entrada)

        # 2. Procesamiento de los JSON (ETL)
        (
            p
            | 'Leer Archivos JSON' >> beam.io.ReadFromText(conocidos_args.ruta_json_entrada)
            | '1-Normalizar ID Carrera' >> beam.ParDo(NormalizarIDCarrera())
            | '2-Filtrar Dispositivos' >> beam.ParDo(FiltrarTipoDispositivo())
            # Enriquecimiento: pasamos la vista como side_input
            | '3-Enriquecer Datos' >> beam.ParDo(
                EnriquecerConDatosPais(),
                datos_pais=vista_datos_pais
            )
            | '4-Convertir a Texto JSON' >> beam.Map(json.dumps)

            # 3. Carga (Load)
            | '5-Escribir a Archivo' >> beam.io.WriteToText(
                conocidos_args.ruta_salida,
                file_name_suffix='.jsonl',
                shard_name_template='' # Escribir en un solo archivo
            )
        )

    print("Pipeline finalizado con éxito. Resultado guardado en la carpeta /resultado.")

if __name__ == '__main__':
    ejecutar()