import os
from neo4j import GraphDatabase, Transaction
from dotenv import load_dotenv

# Загрузка настроек из файла .env
load_dotenv()

class DatabaseHandler:
    def __init__(self, connection_uri, username, password):
        self.driver = GraphDatabase.driver(connection_uri, auth=(username, password))

        # Проверка соединения
        with self.driver.session() as session:
            check = session.run("RETURN 1")
            if check.single() is None:
                raise Exception("Ошибка подключения к Neo4j")
            print("Подключение к базе данных Neo4j успешно выполнено")

    def close_connection(self):
        self.driver.close()

    def fetch_all_entities(self):
        query = "MATCH (n) RETURN n.id AS id, labels(n) AS label"
        with self.driver.session() as session:
            results = session.run(query)
            return [{"id": rec["id"], "label": rec["label"][0]} for rec in results]

    def fetch_entity_with_associations(self, entity_id):
        query = """
        MATCH (n)-[rel]-(linked)
        WHERE n.id = $entity_id
        RETURN n AS node, rel AS relation, linked AS connected_node
        """
        with self.driver.session() as session:
            results = session.run(query, entity_id=entity_id)
            entity_data = []
            for rec in results:
                entity_data.append({
                    "entity": {
                        "id": rec["node"].element_id,
                        "label": rec["node"].labels,
                        "properties": dict(rec["node"]),
                    },
                    "association": {
                        "type": rec["relation"].type,
                        "properties": dict(rec["relation"]),
                    },
                    "target_entity": {
                        "id": rec["connected_node"].element_id,
                        "label": rec["connected_node"].labels,
                        "properties": dict(rec["connected_node"]),
                    }
                })
            return entity_data

    def fetch_all_entities_with_associations(self):
        query = """
        MATCH (n)-[rel]-(linked)
        RETURN n AS node, rel AS relation, linked AS connected_node
        """
        with self.driver.session() as session:
            results = session.run(query)
            entities_with_relations = {}

            for rec in results:
                node = rec["node"]
                node_id = node.element_id
                if node_id not in entities_with_relations:
                    entities_with_relations[node_id] = {
                        "entity": {
                            "id": node.element_id,
                            "label": node.labels,
                            "properties": dict(node),
                        },
                        "associations": []
                    }

                entities_with_relations[node_id]["associations"].append({
                    "association": {
                        "type": rec["relation"].type,
                        "properties": dict(rec["relation"]),
                    },
                    "target_entity": {
                        "id": rec["connected_node"].element_id,
                        "label": rec["connected_node"].labels,
                        "properties": dict(rec["connected_node"]),
                    }
                })

            return list(entities_with_relations.values())

    def create_entity_and_relations(self, label, attributes, associations):
        with self.driver.session() as session:
            session.execute_write(self._create_entity_with_relations, label, attributes, associations)

    @staticmethod
    def _create_entity_with_relations(tx: Transaction, label, attributes, associations):
        # Создание узла
        create_query = f"CREATE (n:{label} $attributes) RETURN n"
        node = tx.run(create_query, attributes=attributes).single()["n"]
        node_id = node.element_id

        # Создание связей
        for assoc in associations:
            tx.run(""" 
            MATCH (n), (target)
            WHERE n.id = $node_id AND target.id = $target_id
            CREATE (n)-[r:RELATION_TYPE]->(target)
            SET r = $relation_attributes
            """, node_id=node_id, target_id=assoc['target_id'],
                   relation_attributes=assoc['attributes'])

    def remove_entity(self, entity_id):
        with self.driver.session() as session:
            session.execute_write(self._delete_entity, entity_id)

    @staticmethod
    def _delete_entity(tx: Transaction, entity_id):
        # Удаление узла и связей
        tx.run("MATCH (n) WHERE n.id = $id DETACH DELETE n", id=entity_id)


if __name__ == "__main__":
    # Конфигурация подключения из файла .env
    connection_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")

    # Проверка конфигурации
    if not username or not password:
        print("Ошибка: В файле .env отсутствуют параметры подключения к базе данных.")
        exit(1)

    print(f"Подключение к Neo4j: URI={connection_uri}, пользователь={username}")

    # Работа с базой данных
    db_manager = DatabaseHandler(connection_uri, username, password)

    print("Получение всех узлов с ассоциациями:")
    all_entities = db_manager.fetch_all_entities_with_associations()
    for entity in all_entities[:100]:
        print(entity, end="\n\n")

    # Завершение работы
    db_manager.close_connection()
