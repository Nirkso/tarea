🚁 Pipeline desarrollado en Python con Apache Beam para procesar y enriquecer la Liga de Carreras de Helicópteros.

🚁 ¿Qué hace?
  * Estandarice los identificadores de carrera (RaceID).
  * Filtre los datos no deseados (DeviceType: Other).
  * Enriquezca los datos de los fans (JSON) con información geográfica detallada de países (CSV) mediante un Side Input.
  * Genera un archivo de salida limpio en formato JSON Lines.


🚁 ¿Qué se necesita para ejecutar?

Para que esto funcione se debe dejar los archivos de la siguiente manera en la carpeta data/.


├── pipeline.py

├── requirements.txt

├── README.md

├── data/ 
│   ├── *.json # Los tres archivos JSON. cup25_fan_engagement-000-of-001, league04_fan_engagement-000-of-001, race11_fan_engagement-000-of-001.

│   └── country_data.csv  # (El archivo CSV).

└── resultado/



🚁 ¿Qué debo hacer?
1) Instalar herramientas del sistema Linux: Colab, Ubuntu, Debian, etc.
  - En el caso de colab correr estos 3  códigos:
!sudo apt-get update
!sudo apt-get install build-essential python3-dev -y
!pip install --upgrade pip setuptools wheel 

2) Instalar las dependencias:
  * Instala las dependencias listadas en requirements.txt
  - En el caso de colab correr el siguiente código: pip install -r requirements.txt

3) Crear las carpetas para los archivos JSON y CSV y para el achivo con el resultado final JSONL.
  - En el caso de colab correr el siguiente código: correr los códigos:
  !mkdir data
  !mkdir resultado

4) Poner los arhivos en la carpeta data/.

5) Ejecutar la pipeline
  - En el caso de colab correr el siguiente código: python pipeline.py

🚁 ¿Qué debo obtengo?
  * en la carpeta de salida queda guardado el archivo JSONL final.
    ./resultado/resultado_pipeline.jsonl# tarea
