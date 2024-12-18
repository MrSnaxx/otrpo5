import os
import pytest
from fastapi.testclient import TestClient
from main import app
from dotenv import load_dotenv
from DatabaseHandler import DatabaseHandler  # Подключаем класс для работы с Neo4j

# Загружаем переменные окружения
load_dotenv()

# Получаем токен из переменной окружения
DB_URI = os.getenv("DB_URI","bolt://localhost:7687")
DB_USERNAME = os.getenv("NEO4J_USERNAME")
DB_PASSWORD = os.getenv("NEO4J_PASSWORD")
API_TOKEN = os.getenv("API_TOKEN","sekretniy_token")

# Фикстура для инициализации клиента FastAPI
@pytest.fixture(scope="function")
def client():
    # Создаем клиента FastAPI для выполнения запросов
    with TestClient(app) as client:
        yield client

# Фикстура для инициализации и очистки базы данных
@pytest.fixture(scope="function", autouse=True)
async def setup_and_teardown():
    # Инициализируем соединение с базой данных в контексте тестов
    app.state.db = DatabaseHandler(DB_URI, DB_USERNAME, DB_PASSWORD)

    # Очистка базы данных перед тестами (если необходимо)
    app.state.db.clear_all_data()  # Например, метод для очистки всех данных в базе
    yield  # Выполнение тестов

    # Закрытие соединения с базой данных после выполнения тестов
    app.state.db.close_connection()

# Тесты для эндпоинтов API

def test_get_all_entities(client):
    response = client.get("/entities", headers={"Authorization": f"Bearer {API_TOKEN}"})
    assert response.status_code == 200
    assert isinstance(response.json(), list)  # Должен вернуться список сущностей

def test_get_entity_by_id(client):
    # Запрос на получение пользователя по ID
    response = client.get("/entities/34280599", headers={"Authorization": f"Bearer {API_TOKEN}"})
    
    assert response.status_code == 200  # Проверяем, что запрос прошел успешно
    data = response.json()
    
    # Проверяем, что ответ содержит хотя бы один элемент
    assert isinstance(data, list)  # Данные должны быть списком
    
    # Проверяем, что в первом элементе списка есть поле 'id'
    assert len(data) > 0  # Убедитесь, что список не пустой
    user = data[0]  # Извлекаем первый элемент списка (предполагаем, что это нужный пользователь)
    
    # Проверяем, что ID пользователя соответствует ожидаемому
    assert user['entity']['properties']['id'] == 34280599
    
    # Проверяем, что в ответе есть поле 'name'
    assert 'name' in user['entity']['properties']  # Проверяем, что имя присутствует
    assert user['entity']['properties']['name'] == "Анастасия Цыган"  # Проверяем конкретное имя


def test_add_entity(client):
    new_entity = {
        "label": "User",
        "properties": {
            "id": 12345678,
            "name": "Тестовый пользователь",
            "screen_name": "testuser",
            "home_town": "Москва",
            "sex": "Female"
        },
        "relationships": []
    }
    response = client.post("/entities", json=new_entity, headers={"Authorization": f"Bearer {API_TOKEN}"})
    assert response.status_code == 200
    assert response.json()['message'] == "Entity and relationships added successfully"

def test_delete_entity(client):
    response = client.delete("/entities/13327918", headers={"Authorization": f"Bearer {API_TOKEN}"})
    assert response.status_code == 200
    assert response.json()['message'] == "Entity and relationships deleted successfully"
