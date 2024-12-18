import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from dotenv import load_dotenv
from DatabaseHandler import DatabaseHandler  # Подключаем ваш класс для работы с Neo4j

# Загружаем переменные окружения
load_dotenv()

# Чтение учетных данных базы данных из переменных окружения
DB_URI = "bolt://localhost:7687"
DB_USERNAME = os.getenv("NEO4J_USERNAME")
DB_PASSWORD = os.getenv("NEO4J_PASSWORD")
API_TOKEN = os.getenv("API_TOKEN","sekretniy_token")

# Проверка наличия всех переменных окружения
if not DB_USERNAME or not DB_PASSWORD or not API_TOKEN:
    raise EnvironmentError("Отсутствуют необходимые переменные окружения: NEO4J_USERNAME, NEO4J_PASSWORD, API_TOKEN")

# Инициализация схемы авторизации с помощью OAuth2
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# Функция для проверки текущего токена
def get_current_token(token: str = Depends(oauth2_scheme)):
    if token != API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token {token}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return token


# Создание контекста lifespan для инициализации и закрытия соединения с базой данных
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Инициализация базы данных с использованием вашего класса
    app.state.db = DatabaseHandler(DB_URI, DB_USERNAME, DB_PASSWORD)
    yield
    app.state.db.close_connection()


app = FastAPI(lifespan=lifespan)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Разрешаем доступ с фронтенда
    allow_credentials=True,
    allow_methods=["*"],  # Разрешаем все HTTP-методы
    allow_headers=["*"],  # Разрешаем все заголовки
)


# Модели данных для работы с API

class Node(BaseModel):
    label: str
    properties: dict
    relationships: list


# Эндпоинты API

@app.get("/entities")
async def get_all_entities():
    entities = app.state.db.fetch_all_entities()
    return entities


@app.get("/entities_with_associations")
async def get_all_entities_with_relations():
    entities = app.state.db.fetch_all_entities_with_associations()
    return entities


@app.get("/entities/{id}")
async def get_entity(id: int):
    entity = app.state.db.fetch_entity_with_associations(id)
    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")
    return entity


@app.post("/entities", dependencies=[Depends(get_current_token)])
async def add_entity(entity: Node):
    app.state.db.create_entity_and_relations(entity.label, entity.properties, entity.relationships)
    return {"message": "Entity and relationships added successfully"}


@app.delete("/entities/{id}", dependencies=[Depends(get_current_token)])
async def delete_entity(id: int):
    app.state.db.remove_entity(id)
    return {"message": "Entity and relationships deleted successfully"}
