from fastapi import FastAPI, HTTPException, Request

from kafka import KafkaProducer
from pydantic import BaseModel
from typing import Optional

import json

app = FastAPI(
    title="Пример API",
    description="Простое демо API с FastAPI и Uvicorn",
    version="1.0.0"
)

class ErrorMessage(BaseModel):
    code: int
    message: str = None
    details: str = None


# Корневой endpoint
@app.post("/errors")
async def read_root(errorMessage: ErrorMessage):
    producer = KafkaProducer(
        bootstrap_servers='localhost:29092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    producer.send('errors', value=errorMessage.model_dump())

    return {"message": "Сообщение было успешно отправлено"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)