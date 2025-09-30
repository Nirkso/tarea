🚁 Pipeline desarrollado en Python con Apache Beam para procesar y enriquecer la Liga de Carreras de Helicópteros.

🚁 ¿Qué hace?
* Estandarice los identificadores de carrera (RaceID).
* Filtre los datos no deseados (DeviceType: Other).
* Enriquezca los datos de los fans (JSON) con información geográfica detallada de países (CSV) mediante un Side Input.
* Genera un archivo de salida limpio en formato JSON Lines.


🚁 ¿Qué se necesita para ejecutar?
Para que esto funcione se debe dejar los archivos de la siguiente manera en la carpeta data/.

.
├── pipeline.py
├── requirements.txt
├── README.md
├── data/ 
│   ├── *.json 
│   └── country_data.csv
└── resultado/



🚁 ¿Qué debo hacer?
1) Instalar las dependencias:
* Instala las dependencias listadas en requirements.txt
- En el caso de colab correr el siguiente código: pip install -r requirements.txt

2) Crear las carpetas para los archivos JSON y CSV y para el achivo con el resultado final JSONL.
- En el caso de colab correr el siguiente código: correr los códigos:
!mkdir data
!mkdir resultado

3) Poner los arhivos en la carpeta data/.

4) Ejecutar la pipeline
- En el caso de colab correr el siguiente código: python pipeline.py

🚁 ¿Qué debo obtengo?
* en la carpeta de salida queda guardado el archivo JSONL final.
./resultado/resultado_pipeline.jsonl# tarea
