# Arquitectura de productos de datos - ITAM 2020

# NYC Open Data: DOHMH Childcare Center Inspections

## ETL
Afortunadamente el set de datos que utilizaremos se expone a través de un API REST en la plataforma [NYC Open Data](https://dev.socrata.com/foundry/data.cityofnewyork.us/dsg6-ifza). 

Este API nos puede entregar los datos en formato `csv`, `xml` y `json`.

> Elegimos formato `json` para evitar conflictos con comas, comillas dobles, o cualquier otro error de <em>parseo</em> que pudiera surgir si utilizáramos `csv`. (`xml` no estaba en la jugada).

En este punto del proyecto nos es difícil saber qué transformaciónes haremos a los datos. Sin embargo, sabemos bien cómo será la **extracción** y **carga** (<em>extract</em> y <em>loading</em>) de los datos.

### Cron
Los datos de DOHMH Childcare Center Inspections se actualizan diaramente. Esto nos permite automatizar fácilemente el proceso de extracción, transformación y carga de datos. 

Dado que usaremos un servidor Ubuntu, podemos hacer uso de [Cron](https://en.wikipedia.org/wiki/Cron), el <em>job scheduler</em> por excelencia de sistemas UNIX. 

La rutina que programemos en Cron ejecutará un script de Python que realice lo siguiente:
1. Extrear los nuevos datos del endpoint del API.
2. Convertir las filas a `INSERTs` de SQL
3. Ejecutar los `INSERTS` en nuestro esquema de Postgres
4. Enviar notificación por correo a nuestro equipo cuando el script haya finalizado.

Nuestro `crontab` lucirá de la siguiente manera:

**Nota: Los nombres de archivos y directorios no son finales.**


**Contenido de nuestroo `crontab`**
~~~
MAILTO=miembros-equipo-5@dpa-itam-2020.com
0 10 * * * python3 /home/ubuntu/scripts/etl/execute.sh
~~~

**archivo `execute.sh`**
~~~bash
#entrar al directorio del repo de nuestro etl
cd /home/ubuntu/scripts/etl/dohmh-childcare-center-inspections-etl
#actualizarlo por si lo mejoramos un día anterior :)
git pull
#correr el script de etl
python3 main.py
~~~

### Transformaciones
TODO: Definir el tipo de transformaciones que tendremos que hacer a los datos antes de cargarlos al esquema de Postgre. 

Tenemoso planeado que nuestra base de datos de Postgres tenga por lo menos dos esquemas, tal que uno de estos sean los datos crudos tal y como los entrega el API.


---


## Implicaciones éticas
- El ranking podría no ser justo pues calificaría mal a planteles que quizá no tienen los recursos económicos para solucionar en corto tiempo los problemas sanitarios. Aquí se estaría discriminando dependiendo al nivel socioeconómico de los planteles o su lozalicación etc.
- Podría fungir como una fuente de segregación pues los padres con mayores recursos mandarían a sus hijos a los mejores centros etc.
- Faltar al principio de desarrollo sostenible, el cual significa que el desarrollo y uso de Sistemas de Inteligencia Artificial debe llevarse a cabo para garantizar una fuerte sostenibilidad ambiental del planeta, por lo tanto, el ser una base de datos de frecuencia diaria, el almacenamiento de la misma afecta el ambiente, es decir, se gasta energía.
- Podría generarse un exceso de demanda en los planteles que estén mejor rankeados y viceversa para los peor rankeados haciendo que se tengan que instituir criterios de selección de los estudiantes que pueden ser costosos y consumir tiempo. 


## ETL Orquestado por Luigi
El ETL está en [este otro repositorio](https://github.com/dpa-2020-equipo-5/nyc-ccci-etl) para tratarlo como una unidad <em>deployable</em> independiente.