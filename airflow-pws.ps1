# Script de PowerShell para gestionar Airflow
# Uso: .\airflow.ps1 [comando]

param(
    [Parameter(Position=0)]
    [string]$Command
)

function Show-Help {
    Write-Host ""
    Write-Host "Comandos disponibles:" -ForegroundColor Cyan
    Write-Host "  down-airflow   - Detiene y elimina contenedores con volumenes"
    Write-Host "  reset-airflow  - Resetea completamente Airflow (down + limpieza)"
    Write-Host "  init-airflow   - Inicializa la base de datos y crea usuario admin"
    Write-Host "  up-airflow     - Levanta los servicios de Airflow"
    Write-Host "  ver-db         - Muestra el contenido de la base de datos"
    Write-Host "  start          - Secuencia completa: reset + init + up"
    Write-Host ""
    Write-Host "Ejemplos:" -ForegroundColor Yellow
    Write-Host "  .\airflow.ps1 start"
    Write-Host "  .\airflow.ps1 ver-db"
    Write-Host ""
}

# Ejecutar comando
switch ($Command) {
    "down-airflow" {
        Write-Host "[*] Deteniendo contenedores y eliminando volumenes..." -ForegroundColor Yellow
        docker-compose down --volumes
        if ($LASTEXITCODE -eq 0) {
            Write-Host "[OK] Contenedores detenidos correctamente" -ForegroundColor Green
        } else {
            Write-Host "[ERROR] Error al detener contenedores" -ForegroundColor Red
            exit 1
        }
    }
    "reset-airflow" {
        Write-Host "[*] Reseteando Airflow..." -ForegroundColor Yellow
        
        # Detener contenedores
        Write-Host "[*] Deteniendo contenedores y eliminando volumenes..." -ForegroundColor Yellow
        docker-compose down --volumes
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] Error al detener contenedores" -ForegroundColor Red
            exit 1
        }
        Write-Host "[OK] Contenedores detenidos correctamente" -ForegroundColor Green
        
        # Limpiar directorios
        Write-Host "[*] Limpiando directorios..." -ForegroundColor Yellow
        
        if (Test-Path "logs") {
            Remove-Item -Path "logs\*" -Recurse -Force -ErrorAction SilentlyContinue
        } else {
            New-Item -ItemType Directory -Path "logs" -Force | Out-Null
        }
        
        if (Test-Path "dags") {
            Remove-Item -Path "dags\*" -Recurse -Force -ErrorAction SilentlyContinue
        } else {
            New-Item -ItemType Directory -Path "dags" -Force | Out-Null
        }
        
        if (Test-Path "plugins") {
            Remove-Item -Path "plugins\*" -Recurse -Force -ErrorAction SilentlyContinue
        } else {
            New-Item -ItemType Directory -Path "plugins" -Force | Out-Null
        }
        
        Write-Host "[OK] Directorios limpiados" -ForegroundColor Green
    }
    "init-airflow" {
        Write-Host "[*] Inicializando base de datos de Airflow..." -ForegroundColor Yellow
        
        # Inicializar base de datos
        docker-compose run --rm webserver airflow db init
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] Error al inicializar la base de datos" -ForegroundColor Red
            exit 1
        }
        
        Write-Host "[*] Creando usuario admin..." -ForegroundColor Yellow
        
        # Crear usuario admin
        docker-compose run --rm webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
        if ($LASTEXITCODE -eq 0) {
            Write-Host "[OK] Usuario admin creado correctamente" -ForegroundColor Green
            Write-Host ""
            Write-Host "Credenciales:" -ForegroundColor Cyan
            Write-Host "  Usuario: admin" -ForegroundColor White
            Write-Host "  Contrasena: admin" -ForegroundColor White
            Write-Host ""
        } else {
            Write-Host "[ERROR] Error al crear usuario admin" -ForegroundColor Red
            exit 1
        }
    }
    "up-airflow" {
        Write-Host "[*] Levantando servicios de Airflow..." -ForegroundColor Yellow
        docker-compose up -d
        if ($LASTEXITCODE -eq 0) {
            Write-Host "[OK] Servicios levantados correctamente" -ForegroundColor Green
            Write-Host ""
            Write-Host "Accede a Airflow en: http://localhost:8080" -ForegroundColor Cyan
            Write-Host ""
        } else {
            Write-Host "[ERROR] Error al levantar servicios" -ForegroundColor Red
            exit 1
        }
    }
    "ver-db" {
        Write-Host "[*] Mostrando contenido de la base de datos..." -ForegroundColor Yellow
        docker-compose exec webserver python /opt/airflow/scripts/ver_db.py
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] Error al consultar la base de datos" -ForegroundColor Red
            exit 1
        }
    }
    "start" {
        Write-Host "[*] Iniciando secuencia completa..." -ForegroundColor Cyan
        Write-Host ""
        
        # Reset
        Write-Host "[*] Reseteando Airflow..." -ForegroundColor Yellow
        docker-compose down --volumes
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] Error al detener contenedores" -ForegroundColor Red
            exit 1
        }
        
        Write-Host "[*] Limpiando directorios..." -ForegroundColor Yellow
        if (Test-Path "logs") {
            Remove-Item -Path "logs\*" -Recurse -Force -ErrorAction SilentlyContinue
        } else {
            New-Item -ItemType Directory -Path "logs" -Force | Out-Null
        }
        if (Test-Path "dags") {
            Remove-Item -Path "dags\*" -Recurse -Force -ErrorAction SilentlyContinue
        } else {
            New-Item -ItemType Directory -Path "dags" -Force | Out-Null
        }
        if (Test-Path "plugins") {
            Remove-Item -Path "plugins\*" -Recurse -Force -ErrorAction SilentlyContinue
        } else {
            New-Item -ItemType Directory -Path "plugins" -Force | Out-Null
        }
        Write-Host "[OK] Limpieza completada" -ForegroundColor Green
        Write-Host ""
        
        # Init
        Write-Host "[*] Inicializando base de datos..." -ForegroundColor Yellow
        docker-compose run --rm webserver airflow db init
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] Error al inicializar la base de datos" -ForegroundColor Red
            exit 1
        }
        
        Write-Host "[*] Creando usuario admin..." -ForegroundColor Yellow
        docker-compose run --rm webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] Error al crear usuario admin" -ForegroundColor Red
            exit 1
        }
        Write-Host ""
        
        # Up
        Write-Host "[*] Levantando servicios..." -ForegroundColor Yellow
        docker-compose up -d
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] Error al levantar servicios" -ForegroundColor Red
            exit 1
        }
        
        Write-Host ""
        Write-Host "Airflow esta listo!" -ForegroundColor Green
        Write-Host "   Accede a: http://localhost:8080" -ForegroundColor Cyan
        Write-Host "   Usuario: admin | Contrasena: admin" -ForegroundColor White
        Write-Host ""
    }
    default {
        if ($Command) {
            Write-Host "[ERROR] Comando desconocido: $Command" -ForegroundColor Red
        }
        Show-Help
        exit 1
    }
}