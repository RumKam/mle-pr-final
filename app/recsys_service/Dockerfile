FROM python:3.11-slim
# возьмем образ, который мы скачали ранее и в котором уже установлен python

WORKDIR /recommendation_service

# копируем файлы в Docker
COPY requirements.txt .

# инструкция для установки библиотек
RUN pip3 install -r requirements.txt

# копируем остальные файлы в Docker
COPY . .

# инструкции для открытия порта
EXPOSE 8000

CMD uvicorn recommendation_service:app --reload --port 8000 --host 0.0.0.0