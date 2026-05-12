import argparse
import json
import time
import random
from datetime import datetime
from tqdm import tqdm

def generate_document(doc_id: int):
    """
    Генерирует тестовый документ для имитации реальных данных в RAG-системе;
    документы создаются разного размера: имитирует поступление разных данных 
    """
    sizes = [600, 1500, 4444, 7500]  # возможные размеры документов в символах
    
    # Создаём повторяющийся текст, чтобы документ имел осмысленный объём
    content = "Это тестовые данные для исследования в КР по потоковой обработке в RAG-системе. " * random.randint(8, 45)
    
    return {
        "doc_id": doc_id,
        "timestamp": datetime.utcnow().isoformat(),
        "title": f"Документ {doc_id}",
        "content": content[:random.choice(sizes)],  # обрезка до выбранного размера
        "source": "test_generator"
    }


def main():
    """
    Главная функция продьюсера:
    позволяет запускать генерацию сообщений для Кафуи или RabbitMQ.
    """
    # Настройка параметров командной строки
    parser = argparse.ArgumentParser(description="Отправитель для нагрузочного тестирования брокеров")
    parser.add_argument("--broker", choices=["kafka", "rabbitmq"], required=True,
                        help="Выбор брокера сообщений")
    parser.add_argument("--count", type=int, default=2000,
                        help="Количество данных для генерации)") #по умолчанию 2000
    parser.add_argument("--rate", type=float, default=150,
                        help="Скорость генерации (сообщений в секунду)")
    args = parser.parse_args()

    #инициализируем брокеры (в зависимости от того для чего будем запускать)
    if args.broker == "kafka":
        from kafka import KafkaProducer
        # Создаем продьюсера для Kafka
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        topic = "rag-documents"
    else:
        # для RabbitMQ
        import pika
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        # создаём очередь (сохраняется после перезапуска)
        channel.queue_declare(queue='rag-documents', durable=True)

    print(f"Запускаем продьюсера для {args.broker.upper()} | {args.count} документов")

    #основной цикл отправки
    for i in tqdm(range(args.count), desc="Отправка документов"):
        doc = generate_document(i)
        
        if args.broker == "kafka":
            producer.send("rag-documents", value=doc)
        else:
            channel.basic_publish(
                exchange='',
                routing_key='rag-documents',
                body=json.dumps(doc).encode('utf-8'),
                properties=pika.BasicProperties(delivery_mode=2)
            )
        
        # контроль скорости отправки
        time.sleep(1 / args.rate)


if __name__ == "__main__":
    main()
