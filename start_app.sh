#!/bin/bash

# Переменные 
BASE_DIR=$(pwd)
AIRFLOW_DIR="$BASE_DIR/airflow"
MLFLOW_DIR="$BASE_DIR/mlflow_server"
APP_DIR="$BASE_DIR/app"

echo "Текущая директория: $(pwd)"

# Функция для проверки успешности выполнения команды
check_success() {
    if [ $? -ne 0 ]; then
        echo "Ошибка при выполнении команды: $1"
        exit 1
    fi
}

# Запуск Airflow в Docker контейнере
echo "Запуск Airflow..."
cd $AIRFLOW_DIR
docker-compose up --build -d  # ПЕРВЫЙ запуск
check_success "Запуск Airflow"

sleep 10

# Запуск MLflow
echo "Запуск MLflow..."
cd $MLFLOW_DIR
# Фоновый режим, логи в файл
nohup ./run_mlflow.sh > mlflow.log 2>&1 &
check_success "Запуск MLflow"

sleep 5

# Запуск Prometheus и микросервисов через Docker Compose
echo "Запуск микросервисов..."
cd $APP_DIR
docker-compose up --build -d # ПЕРВЫЙ запуск
check_success "Запуск Prometheus и микросервисов"

echo "Все сервисы успешно запущены!"