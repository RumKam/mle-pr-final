FROM python:3.11-slim
# возьмем образ, который мы скачали ранее и в котором уже установлен python

WORKDIR /events_service

# копируем файлы в Docker
COPY requirements.txt .

# инструкция для установки библиотек
RUN pip3 install -r requirements.txt

COPY . .

# инструкции для открытия порта
EXPOSE 8020

CMD uvicorn events_service:app --reload --port 8020 --host 0.0.0.0