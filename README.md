*********************************************************************************************************************************
🚁 Pipeline desarrollado en Python con Apache Beam para procesar y enriquecer la Liga de Carreras de Helicópteros.
*********************************************************************************************************************************

🚁 ¿Qué hace?
  * Estandarizar los identificadores de carrera (RaceID). Solo letras y números, sin espacios ni otros caracteres.
  * Filtrar los datos no deseados (DeviceType: Other).
  * Enriquece los datos de los fans (JSON) con información geográfica detallada de países (CSV) mediante un Side Input.
  * Genera un archivo de salida limpio en formato JSON Lines.
*********************************************************************************************************************************

🚁 ¿Qué se necesita para ejecutar?

Para que esto funcione se debe dejar los archivos de la siguiente manera en la carpeta ./tarea/.


├── pipeline.py

├── requirements.txt

├── README.md

├── *.json # Los tres archivos JSON. cup25_fan_engagement-000-of-001, league04_fan_engagement-000-of-001, race11_fan_engagement-000-of-001.

├── country_data.csv  # (El archivo CSV).

└── resultado/
*********************************************************************************************************************************

🚁 ¿Qué debo hacer?
1) Generar un nuevo cuaderno en google colab
2) Instalar herramientas del sistema Linux: Colab, Ubuntu, Debian, etc.
  - Correr estos 3  códigos:
!sudo apt-get install python3.10 python3.10-dev
!sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1
!sudo apt-get install python3.10-distutils

3) Copiar los archivos al espacio de trabajo de colab desde el repositorio github
  - Correr este código:
!git clone https://github.com/Nirkso/tarea.git
    
4) Instalar las dependencias:
  * Instala las dependencias listadas en requirements.txt
  - Correr el siguiente código:
!pip install -r tarea/requeriments.txt --prefer-binary --no-build-isolation

   4.1 Reiniciar la sesión

   4.2 Volver a correr el siguiente código:
!pip install -r tarea/requeriments.txt --prefer-binary --no-build-isolation

5) Ejecutar la pipeline
  -Correr el siguiente código:
   !python tarea/pipeline.py
*********************************************************************************************************************************

🚁 ¿Qué debo obtengo?
  * en la carpeta de salida queda guardado el archivo JSONL final.
    ./resultado/resultado_final.jsonl
