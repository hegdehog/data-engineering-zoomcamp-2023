# data-engineering-zoomcamp-work
Notas de la primera semana del zoomcamp Data Engineering.

Otras notas: https://www.n4gash.com/2023/data-engineering-zoomcamp-semana-1/

## Creamos el contenedor de postgresql
docker run -it  -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -v C:\Users\Marcos\Downloads\DataEngineeringBootcamp\Week1\NY_Taxis_DBny_taxi_postgres_data:/var/lib/postgresql/data -p 5432:5432 postgres:13


docker run -it ^
	-e POSTGRES_USER="root" ^
	-e POSTGRES_PASSWORD="root" ^
	-e POSTGRES_DB="ny_taxi" ^
	-v c:\Users\Marcos\Downloads\Data Engineering Bootcamp\Week 1 - Docker\NY_Taxis_DBny_taxi_postgres_data:var/lib/postgresql/data ^
	-p 5432:5432 postgres:13


## Conectarse a pgcli por linea de comandos
pgcli -h  localhost -p 5432 -u root -d ny_taxi


## Creamos el contenedor con pgadmin
docker run -it -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" -e PGADMIN_DEFAULT_PASSWORD="root" -p 8080:80 dpage/pgadmin4


docker run -it \\
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \\
  -e PGADMIN_DEFAULT_PASSWORD="root" \\
  -p 8080:80 \\
  dpage/pgadmin4


## Creamos la red en docker
docker network create pg-network

docker run -it  -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -v C:\Users\Marcos\Downloads\DataEngineeringBootcamp\Week1\NY_Taxis_DBny_taxi_postgres_data:/var/lib/postgresql/data -p 5432:5432 --network=pg-network --name pg-database postgres:13 
docker run -it -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" -e PGADMIN_DEFAULT_PASSWORD="root" -p 8080:80 --network=pg-network --name pgadmin dpage/pgadmin4 


## Ingesta automatica
URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
python ingest_data.py --user=root --password=root --host=localhost --port=5432 --db=ny_taxi --table_name=yellow_taxi_trips --url=${URL}


python ingest_data.py \
--user=root \
--password=root \
--host=localhost \
--port=5432 \
--db=ny_taxi \
--table_name=yellow_taxi_trips\ 
--url=${URL}
