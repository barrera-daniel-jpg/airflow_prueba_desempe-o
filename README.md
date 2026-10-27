# Pipeline ETL con Apache Airflow y Data Warehouse Caso: DataMart S.A.S.

Resultado de la prueba de desempeño que procesa archivos CSV de ventas y devoluciones con Apache Airflow, transforma los registros con pandas y los carga en un Data Warehouse PostgreSQL. Una API FastAPI permite consultar el catálogo de productos.

## Arquitectura

```mermaid
flowchart LR
	CSV[CSV en data/raw] --> EX[extract.py]
	EX --> TR[transform.py]
	TR --> PQ[Parquet en data/processed]
	PQ --> LD[load.py]
	LD --> DW[(PostgreSQL Data Warehouse)]
	DW --> API[FastAPI]
	DAG[Airflow DAG] --> EX
	DAG --> TR
	DAG --> LD
	SQL[Consultas analíticas] --> DW
```
## Decisiones Técnicas Clave
### A. Idempotencia y Cargas Incrementales
Para garantizar que reejecutar el DAG no duplique registros ni altere métricas financieras, se implementó una estrategia combinada:
1. Identificador a nivel de línea (line_index): Se agrega un índice secuencial por archivo para identificar cada ítem de una misma factura.
2. Restricción de unicidad compuesta: (invoice_no, product_key, transaction_timestamp_utc, line_index).
3. Manejo de conflictos: En las sentencias de carga masiva se utiliza ON CONFLICT (...) DO NOTHING, permitiendo reejecuciones seguras (at-least-once processing).
### B. Rendimiento y Optimización de Inserción
- La carga se realiza utilizando iteradores de lotes (chunked_iterable con chunk_size = 5.000$).
- Reduce el tiempo total de procesamiento en entornos Docker/WSL2 de más de 40 minutos (registro a registro) a ~2 minutos, optimizando el uso de conexiones del pool de SQLAlchemy.
- Para el log de auditoría se utiliza CAST(:raw_payload AS jsonb), garantizando compatibilidad con los drivers de PostgreSQL sin colisiones con los parámetros nombrados de SQLAlchemy.
### C. Calidad de Datos y Codificación
- Manejo de Encoding: Tratamiento explícito de caracteres especiales de divisas (como el símbolo de libra £) mediante fallbacks de decodificación utf-8 y latin-1.
- Reglas de Descarte: Todo registro con UnitPrice <= 0 es desviado a cuarentena (PRECIO_INVALIDO_MENOR_O_IGUAL_A_CERO).


Airflow coordina tres tareas en orden: `extract_and_transform_sources`, `load_dimensions_dw` y `load_facts_and_rejects_dw`. El traspaso entre tareas se hace mediante archivos Parquet en `data/processed`, montado como un volumen compartido entre los contenedores de Airflow.

## Componentes

| Ruta | Responsabilidad |
| --- | --- |
| `dags/dag_datamart_pipeline.py` | Define el DAG diario, reintentos, dependencias y las funciones de entrada de cada etapa. |
| `src/extract.py` | Lee los CSV de ventas e historial desde la ruta configurada en Airflow. |
| `src/transform.py` | Normaliza datos, aplica controles de calidad y produce dimensiones, hechos y rechazos. |
| `src/load.py` | Inserta dimensiones, hechos y rechazos en PostgreSQL por lotes. |
| `src/utils/db.py` | Obtiene la conexión de Airflow al DWH y crea el engine SQLAlchemy. |
| `api/main.py` | Crea la aplicación FastAPI, registra el router y expone el health check. |
| `api/routers/products.py` | Implementa las consultas paginadas y por código del catálogo. |
| `api/schemas.py` | Define los modelos Pydantic de respuesta de la API. |
| `api/database.py` | Configura SQLAlchemy para la API y gestiona las sesiones por petición. |
| `sql/init_dw.sql` | Crea el esquema `dwh`, dimensiones, hechos y tabla de auditoría. |
| `sql/queries_business_questions.sql` | Incluye consultas de ejemplo para validar métricas y preguntas de negocio. |
| `docker-compose.yml` | Orquesta PostgreSQL de Airflow, PostgreSQL del DWH, servicios Airflow y API. |
| `Dockerfile.api` | Construye la imagen Docker de la API. `api/Dockerfile` contiene una definición equivalente. |
| `run_pipeline.ps1` | Automatiza el arranque en Windows, la carga de archivos, el disparo del DAG y las consultas. |
| `run_pipeline.sh` | Actualmente contiene el mismo script PowerShell; no es ejecutable como shell script. |
| `config/airflow.cfg` | Configuración de Airflow montada en los contenedores. Compose sobrescribe parte de estos valores mediante variables de entorno. |
| `requirements.txt`, `pyproject.toml`, `uv.lock` | Archivos de dependencias y configuración Python. Docker usa `requirements.txt`; `pyproject.toml` conserva el esqueleto del paquete inicial. |

### Flujo de transformación

1. Se leen `Transacciones de ventas.csv` y, si está disponible, `Historial extendido de transacciones.csv`.
2. Se normalizan algunos nombres de columnas del histórico y se combinan ambas fuentes, dando prioridad a las filas del archivo de ventas en la deduplicación.
3. Se normalizan códigos de producto, factura, país e identificadores de cliente. Los identificadores ausentes se convierten en `UNKNOWN_CUSTOMER`.
4. Se parsean fechas y se consideran inválidas las filas con fecha no interpretable o precio nulo/no positivo. Se guardan en `data_quality_rejects` con el contenido original relevante y el motivo.
5. Se eliminan duplicados usando factura, producto, cantidad, precio, fecha, cliente y se generan `dim_customers`, `dim_products` y `dim_date`.
6. Cantidades positivas se convierten en ventas; cantidades cero o negativas se clasifican como devoluciones. Se calculan ingreso bruto y valor reembolsado.
7. Se escriben seis archivos Parquet. Las tareas siguientes cargan primero las dimensiones y luego los hechos y rechazos.

## Modelo del Data Warehouse

El esquema PostgreSQL `dwh` contiene:

| Tabla | Tipo | Contenido |
| --- | --- | --- |
| `dim_customers` | Dimensión | Identificador de cliente y clasificación identificado/anónimo. |
| `dim_products` | Dimensión | Código, descripción canónica, categoría, origen y estado del producto. |
| `dim_date` | Dimensión | Fecha calendario, clave `YYYYMMDD`, año, mes y día. |
| `fact_sales` | Hechos | Líneas de venta, cantidades e ingreso bruto con referencias a dimensiones. |
| `fact_returns` | Hechos | Líneas de devolución, unidades devueltas y valor reembolsado. |
| `data_quality_rejects` | Auditoría | Archivo fuente, carga JSON del registro y motivo de rechazo. |

La carga de dimensiones usa `ON CONFLICT` para evitar repetir clientes/fechas y actualizar la descripción del producto. Las líneas de ventas y devoluciones también usan una clave única para evitar duplicados al reejecutar el ETL. La carga de rechazos es append-only y puede duplicarlos en una reejecución.

![alt text](image.png)

## Respuestas a las preguntas de negocio

Resultados calculados sobre los datos cargados en el DWH. Los importes se expresan en la moneda de origen (£). 

### 1. Evolución mensual de ventas netas

Las ventas netas se calculan como ingreso bruto de ventas menos el importe de los reembolsos registrados en el mismo mes.

| Mes | Ventas brutas (£) | Devoluciones (£) | Ventas netas (£) |
| --- | ---: | ---: | ---: |
| Dic 2010 | 821.452,73 | 74.729,12 | 746.723,61 |
| Ene 2011 | 689.811,61 | 131.363,05 | 558.448,56 |
| Feb 2011 | 522.545,56 | 25.519,15 | 497.026,41 |
| Mar 2011 | 716.215,26 | 34.201,28 | 682.013,98 |
| Abr 2011 | 536.968,49 | 44.600,65 | 492.367,84 |
| May 2011 | 769.281,76 | 47.202,51 | 722.079,25 |
| Jun 2011 | 760.547,01 | 70.569,78 | 689.977,23 |
| Jul 2011 | 718.076,12 | 37.919,13 | 680.156,99 |
| Ago 2011 | 757.841,38 | 54.330,80 | 703.510,58 |
| Sep 2011 | 1.056.435,19 | 38.838,51 | 1.017.596,68 |
| Oct 2011 | 1.151.263,73 | 81.895,50 | 1.069.368,23 |
| Nov 2011 | 1.503.329,78 | 47.720,98 | 1.455.608,80 |
| Dic 2011* | 637.790,33 | 205.089,27 | 432.701,06 |

El máximo del periodo fue noviembre de 2011 (£1.455.608,80 netos). La cifra de diciembre de 2011 no es comparable con meses completos porque los datos terminan el día 9.

### 2. Categorías y devoluciones
Dentro de la logica del negocio se trabajo con una categoria generalizada dado que los 3.828 productos de `dwh.dim_products` fueron asignados a la categoría genérica `General`. Para esa categoría, el DWH registra £10.641.558,95 de ventas brutas y £893.979,73 en devoluciones, equivalente a 8,40 % del ingreso bruto. Es un indicador global, no una comparación entre categorías.

### 3. Productos con mayor ingreso neto

Ingreso neto por producto = ingresos de venta menos reembolsos, agregados independientemente por producto.

| Código | Producto | Ingreso neto (£) |
| --- | --- | ---: |
| DOT | Dotcom Postage | 206.245,48 |
| 22423 | Regency Cakestand 3 Tier | 164.459,49 |
| 85123A | White Hanging Heart T-Light Holder | 99.790,93 |
| 47566 | Party Bunting | 98.243,88 |
| 85099B | Jumbo Bag Red Retrospot | 92.175,79 |
| 23084 | Rabbit Night Light | 66.661,63 |
| POST | Postage | 66.230,64 |
| 22086 | Paper Chain Kit 50'S Christmas | 63.715,24 |
| 84879 | Assorted Colour Bird Ornament | 58.792,42 |
| 79321 | Chilli Lights | 53.746,66 |

### 4. Productos con mayor tasa de devolución

Tasa = unidades devueltas / unidades vendidas. Se limita el ranking a productos con al menos 20 unidades vendidas para reducir resultados dominados por volúmenes muy pequeños. Una tasa mayor a 100 % significa que las unidades devueltas registradas superan las vendidas en este periodo y requiere revisar la correspondencia temporal y la calidad de origen; no debe interpretarse automáticamente como una tasa comercial normal.

| Código | Producto | Vendidas | Devueltas | Tasa |
| --- | --- | ---: | ---: | ---: |
| 84750A | Pink Small Glass Cake Stand | 24 | 27 | 112 % |
| 23843 | Paper Craft, Little Birdie | 80.995 | 80.995 | 100 % |
| 21655 | Hanging Ridge Glass T-Light Holder | 36 | 36 | 100 % |
| 84347 | Rotating Silver Angels T-Light Hldr | 9.461 | 9.376 | 99 % |
| 23166 | Medium Ceramic Top Storage Jar | 78.033 | 74.494 | 95 % |
| 85232B | Set Of 3 Babushka Stacking Tins | 274 | 255 | 93 % |
| 23113 | Pantry Chopping Board | 1.154 | 946 | 81 % |
| 85023B | Eau De Nile Jewelled Photoframe | 105 | 73 | 69 % |
| 23055 | Ivory Chandelier T-Light Holder | 657 | 432 | 65 % |
| 90185C | Black Diamante Expandable Ring | 26 | 16 | 61 % |

### 5. Países y ticket promedio

El ticket promedio se calcula sumando las líneas de cada factura por país y promediando el total por factura. Reino Unido concentra 18.019 de 19.960 facturas de venta (90,28 %).

| País | Facturas | Participación de facturas | Ingreso bruto (£) | Ticket promedio (£) |
| --- | ---: | ---: | ---: | ---: |
| United Kingdom | 18.019 | 90,28 % | 9.001.192,24 | 499,54 |
| Germany | 457 | 2,29 % | 228.678,40 | 500,39 |
| France | 392 | 1,96 % | 209.625,37 | 534,76 |
| EIRE | 288 | 1,44 % | 283.140,52 | 983,13 |
| Belgium | 98 | 0,49 % | 41.196,34 | 420,37 |

Aunque Países Bajos solo registra 94 facturas, su ticket promedio es £3.036,66; Australia registra 57 facturas y £2.429,01. Estos promedios altos con poco volumen conviene analizarlos por factura y cliente antes de generalizar el comportamiento del mercado.

### 6.0 Clientes identificados y anónimos

Se conservaron transacciones sin `CustomerID`, asignándoles `UNKNOWN_CUSTOMER` y el tipo `GUEST/ANONYMOUS`.

| Tipo de cliente | Facturas | Ingreso bruto (£) | Ticket promedio (£) | Unidades vendidas |
| --- | ---: | ---: | ---: | ---: |
| Identificado | 18.532 | 8.886.657,04 | 479,53 | 5.151.699 |
| Anónimo/sin ID | 1.428 | 1.754.901,91 | 1.228,92 | 420.418 |

El ticket promedio anónimo es aproximadamente 2,56 veces el identificado. Esta diferencia describe las facturas observadas, pero no demuestra por sí sola un mayor valor de cliente: la ausencia de identificador limita la deduplicación de compradores y puede concentrar compras grandes o transacciones especiales.

### 6.1 Consistencia de descripciones y códigos

En los dos CSV originales se encontraron **3.958 códigos únicos**. Tras limpieza y filtros de calidad, la dimensión contiene **3.828 códigos**. Al normalizar mayúsculas y espacios, **638 códigos (16,12 %)** tienen más de una descripción no vacía, y **110 códigos** no tienen descripción no vacía. Por ejemplo, el código `85123A` aparece asociado tanto a `White Hanging Heart T-Light Holder` como a una descripción distinta (`Cream Hanging Heart T-Light Holder`), además de anotaciones operativas. La dimensión elige la descripción válida más frecuente; esa regla genera un nombre canónico, pero no resuelve los conflictos de origen.

### Recomendación para el equipo de producto

Priorizar una revisión de catálogo y trazabilidad de devoluciones para los códigos `23843`, `23166` y `84347`: en el periodo aparecen, respectivamente, 80.995/80.995 unidades vendidas/devueltas (100 %), 78.033/74.494 (95 %) y 9.461/9.376 (99 %). Antes de tomar decisiones de surtido, producto debe validar si son devoluciones asociadas a ventas de periodos anteriores, códigos mal asignados o problemas del artículo; después, registrar el motivo de devolución y seguir la tasa por cohorte de venta. Para el piloto comercial, enfocar primero el análisis en Reino Unido, que concentra el 90,28 % de las facturas, y contrastar el resultado con los mercados de menor volumen.

## Requisitos y configuración

- Docker Desktop con Docker Compose v2.
- Los archivos CSV de origen en `data/raw/` con los nombres exactos indicados arriba. Se leen con codificación `latin1`.
- Un archivo `.env` local con credenciales y parámetros del entorno. No guardes credenciales reales en el repositorio. `.env.example` es una plantilla que se debe completar; sus valores vacíos o de ejemplo no constituyen una configuración lista para producción.

En `AIRFLOW_CONN_POSTGRES_DW`, usa el formato `postgresql://usuario:contraseña@postgres-dw:5432/base_de_datos`. El puerto `5432` es el puerto interno del servicio Docker; `DW_POSTGRES_PORT` (por defecto `5433`) es únicamente el puerto publicado en el host. La cadena que aparece en `.env.example` contiene marcadores entre corchetes y usa `AIRFLOW_DB_PORT`, así que debe reemplazarse antes de ejecutar el ETL.

Variables principales de `.env`:

| Variable | Uso |
| --- | --- |
| `AIRFLOW_DB_USER`, `AIRFLOW_DB_PASSWORD`, `AIRFLOW_DB_NAME` | Credenciales de la base de metadatos de Airflow. |
| `DW_POSTGRES_USER`, `DW_POSTGRES_PASSWORD`, `DW_POSTGRES_DB`, `DW_POSTGRES_PORT` | Credenciales y puerto publicado del Data Warehouse. |
| `_AIRFLOW_WWW_USER_USERNAME`, `_AIRFLOW_WWW_USER_PASSWORD` | Usuario inicial de la interfaz de Airflow. |
| `AIRFLOW_UID`, `AIRFLOW_PROJ_DIR` | Usuario y ruta local montada por los contenedores de Airflow. |
| `AIRFLOW_VAR_RAW_DATA_PATH` | Ruta de entrada de los CSV dentro del contenedor; por defecto `/opt/airflow/data/raw`. |

El puerto publicado por defecto para PostgreSQL del DWH es `5433` en el host (`5432` dentro de Docker). Airflow usa otra instancia PostgreSQL interna para sus metadatos.

## Ejecución

En Windows PowerShell, desde la raíz del proyecto:

```powershell
.\run_pipeline.ps1
```

El script prepara directorios, levanta los servicios, espera a las dependencias, aplica el esquema, copia los CSV al contenedor del scheduler, dispara el DAG y ejecuta las consultas SQL al terminar. El archivo `.env` debe estar configurado antes de ejecutar el proceso.

Para levantar los servicios manualmente:

```powershell
docker compose up -d --build
```

Luego se puede disparar el DAG desde la interfaz de Airflow (`http://localhost:8080`) o desde el contenedor scheduler:

```powershell
docker exec airflow-scheduler airflow dags trigger datamart_etl_daily_pipeline
```

La interfaz de Airflow usa el usuario/contraseña configurados para el usuario inicial. La API y su documentación OpenAPI se publican en los enlaces siguientes.

## API

- `GET http://localhost:8000/health`: estado del servicio.
- `GET http://localhost:8000/products?page=1&limit=50`: catálogo paginado; `limit` admite de 1 a 200.
- `GET http://localhost:8000/products/{code}`: busca un producto por código; responde `404` si no existe.
- `http://localhost:8000/docs`: Swagger UI.
- `http://localhost:8000/redoc`: ReDoc.

El listado acepta los parámetros `status` y `category`; a día de hoy `status` filtra por `is_active`, pero `category` no se aplica en la consulta. Además, las respuestas asignan actualmente `Hogar` y `Reino Unido` como valores fijos para categoría y origen, en vez de devolver los valores almacenados en las dimensiones.

## Notas conocidas

- La URL de catálogo configurada como variable de Airflow apunta a `/api/v1/products` y al host `catalog-api`, mientras que el servicio de Compose se llama `api` y sus rutas reales comienzan en `/products`. Esa variable no se consume en el DAG actual.
- La extracción del historial es tolerante a cualquier excepción: si falla, el pipeline continúa solo con el archivo de ventas. La fuente de ventas principal sí es obligatoria.
- La transformación asigna `General` y `Unknown` como categoría y origen iniciales de productos; no infiere esos atributos desde los CSV.
- En las consultas 3(A) y 3(B) de `queries_business_questions.sql`, ventas y devoluciones se unen directamente a nivel de producto. Si hay varias filas de ambos tipos para el producto, la unión puede multiplicar importes/unidades. Para métricas exactas conviene agregar cada hecho por producto antes de unirlo.
- El DAG limita a una ejecución activa y desactiva `catchup`; procesa la ejecución diaria programada sin crear ejecuciones históricas pendientes.
- `run_pipeline.sh` está escrito en PowerShell pese a su extensión. En Windows se debe usar `run_pipeline.ps1`.
- `src/airflow_prueba_desempe_o/` es el esqueleto del paquete Python generado por la configuración inicial; el ETL y la API importan los módulos de nivel superior `src/` y `api/`, no ese punto de entrada de ejemplo.

## Operación

```powershell
docker compose ps
docker compose logs -f airflow-scheduler
docker compose logs -f api
docker compose down
```

`docker compose down` detiene y elimina los contenedores, pero conserva los volúmenes de datos. Los resultados Parquet quedan en `data/processed/`; los logs de tareas de Airflow se guardan en `logs/`.