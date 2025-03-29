# kafka_example

# 1) Поднять базу данных postgres и брокер сообщений
```
sudo docker compose up -d
```

# 2) Запуск продюсера и консьюмера
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

Запускаем app_producer
python3 app_producer/main.py

Запускаем app_consumer
python3 app_consumer/main.py
```