Para correr Airflow

Instala Docker Desktop

Levanta el contenedor con el archivo docker-compose.yaml corriendo el siguiente comando en la terminal justo en el directorio donde estan los archivos docker

`docker-compose up -d`

Instala las librerias python en el airflow enviroment con este otro comando

`docker build . --tag extending_airflow:latest`

Nota: Tal vez veas algunas advertencias de configuracion de variables "AIRFLOW_UID". No es de preocupar por ahora. Esto se atiende despues.

Una vez levantado los contenedores, puedes acceder a la interface grafica de airflow desde cualquier navegador en el

`localhost:8080`

Para configurar la variable `AIRFLOW_UID`, en la interface grafica en el menu superior, en Admin >> Variable agrega las variables con esto. 

Por ejemplo
> key es AIRFLOW_UID

> val es 50000

```
AIRFLOW_UID=50000
AIRFLOW_GID=0
```

La base de datos en el airflow enviroment es postgres y está mapeada al puerto `5430`. Esto evitara cualquier conflicto con la conexion en caso de que ya tengas posteriormente instalado postgres



