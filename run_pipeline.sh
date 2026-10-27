Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "        ETL DATA WAREHOUSE & ANALYTICS PIPELINE AUTOMATION       " -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

# 1. Configuracion de entorno y directorios
Write-Host "`n[1/6] Configurando variables de entorno y directorios..." -ForegroundColor Yellow
if (-not (Test-Path ".\.env")) {
    Write-Host "  > Generando .env automatico..."
    "AIRFLOW_UID=50000`nAIRFLOW_PROJ_DIR=." | Set-Content -Path .\.env -Encoding UTF8
}

New-Item -ItemType Directory -Force -Path .\data\raw, .\logs, .\plugins, .\config | Out-Null

# 2. Levantar contenedores
Write-Host "`n[2/6] Inicializando contenedores con Docker Compose..." -ForegroundColor Yellow
docker compose up -d --build

# 3. Esperar que postgres-dw, scheduler y api respondan
Write-Host "`n[3/6] Esperando disponibilidad de bases de datos y servicios..." -ForegroundColor Yellow
Write-Host "  > Esperando a postgres-dw..." -NoNewline
do {
    Start-Sleep -Seconds 3
    docker exec postgres-dw pg_isready -U dw_admin -d datamart_dw 2>$null | Out-Null
    Write-Host "." -NoNewline
} while ($LASTEXITCODE -ne 0)
Write-Host " [LISTO]" -ForegroundColor Green

Write-Host "  > Esperando a airflow-scheduler..." -NoNewline
do {
    Start-Sleep -Seconds 5
    docker exec airflow-scheduler airflow version 2>$null | Out-Null
    Write-Host "." -NoNewline
} while ($LASTEXITCODE -ne 0)
Write-Host " [LISTO]" -ForegroundColor Green

Write-Host "  > Esperando a dwh-api..." -NoNewline
do {
    Start-Sleep -Seconds 3
    docker exec dwh-api curl -s http://localhost:8000/health 2>$null | Out-Null
    Write-Host "." -NoNewline
} while ($LASTEXITCODE -ne 0)
Write-Host " [LISTO]" -ForegroundColor Green

# 4. Asegurar esquema e inyectar archivos
Write-Host "`n[4/6] Verificando esquema DWH e inyectando datasets..." -ForegroundColor Yellow
if (Test-Path ".\sql\init_dw.sql") {
    Get-Content .\sql\init_dw.sql -Raw | docker exec -i postgres-dw psql -U dw_admin -d datamart_dw 2>$null | Out-Null
}

docker exec airflow-scheduler mkdir -p /opt/airflow/data/raw

if (Test-Path ".\data\raw\Transacciones de ventas.csv") {
    docker cp ".\data\raw\Transacciones de ventas.csv" airflow-scheduler:/opt/airflow/data/raw/
}

if (Test-Path ".\data\raw\Historial extendido de transacciones.csv") {
    docker cp ".\data\raw\Historial extendido de transacciones.csv" airflow-scheduler:/opt/airflow/data/raw/
}

# 5. Disparar el DAG y esperar su finalizacion
Write-Host "`n[5/6] Disparando DAG 'datamart_etl_daily_pipeline'..." -ForegroundColor Yellow
docker exec airflow-scheduler airflow dags unpause datamart_etl_daily_pipeline 2>$null | Out-Null
docker exec airflow-scheduler airflow dags trigger datamart_etl_daily_pipeline | Out-Null

Write-Host "Esperando finalizacion del ETL (toma entre 2 y 3 minutos)..." -ForegroundColor Yellow
$status = "running"
while ($status -eq "running" -or $status -eq "queued") {
    Start-Sleep -Seconds 10
    # Consulta directa y limpia al metastore de Airflow
    $dbState = docker exec postgres-airflow psql -U airflow -d airflow -t -A -c "SELECT state FROM dag_run WHERE dag_id='datamart_etl_daily_pipeline' ORDER BY logical_date DESC LIMIT 1;" 2>$null
    if ($dbState) {
        $status = $dbState.Trim()
        Write-Host "  > Estado actual del DAG: $status..."
    }
}

if ($status -ne "success") {
    Write-Host "`n[ERROR] El pipeline finalizo con estado: $status" -ForegroundColor Red
    exit 1
}

Write-Host "  [EXITO] Todas las tareas completaron en verde!" -ForegroundColor Green

# 6. Consultas de Negocio y Enlaces
Write-Host "`n[6/6] RESULTADOS DE LAS PREGUNTAS DE NEGOCIO (DWH):" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan
if (Test-Path ".\sql\queries_business_questions.sql") {
    Get-Content .\sql\queries_business_questions.sql -Raw | docker exec -i postgres-dw psql -U dw_admin -d datamart_dw
}

Write-Host "`n=================================================================" -ForegroundColor Green
Write-Host "   PROCESO COMPLETADO EXITOSAMENTE Y SIN INTERVENCION MANUAL    " -ForegroundColor Green
Write-Host "   - Swagger UI API: http://localhost:8000/docs                  " -ForegroundColor Yellow
Write-Host "   - Airflow UI:     http://localhost:8080                       " -ForegroundColor Yellow
Write-Host "=================================================================" -ForegroundColor Green
