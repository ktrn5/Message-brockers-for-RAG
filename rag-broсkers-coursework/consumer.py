import argparse
import json
import time
import statistics
import matplotlib.pyplot as plt
from sentence_transformers import SentenceTransformer

# загружаем модель 
# библиотека, которая превращает текст в вектор (эмбеддинг)
model = SentenceTransformer('all-MiniLM-L6-v2') 

#переменные для сбора статистики
latencies = []           # список задержек обработки
processed_count = 0
start_time = time.time()


def save_results(broker: str):
    """Сохраняет результаты тестирования и строит график"""
    if not latencies:
        print("Нет данных для сохранения результатов.")
        return

    #расче средн скорость обработки сообщений в секунду
    total_time = time.time() - start_time
    if total_time > 0:
      throughput = processed_count / total_time
    else:
      throughput = 0

    #отображение в консоли
    print("\n" + "="*60)
    print(f"РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ ДЛЯ {broker.upper()}") #для вбыранного брокера
    print("="*60)
    print(f"Обработано документов: {processed_count}")
    print(f"Average Latency:     {statistics.mean(latencies)*1000:.1f} мс")
    print(f"Throughput:          {throughput:.1f} соо/с")
    print(f"Общее время теста:   {total_time:.1f} секунд")
    print("="*60)

    # построение и сохранение графика распределения latency
    plt.figure(figsize=(10, 6))
    plt.hist([x * 1000 for x in latencies], bins=30, alpha=0.75, color='royalblue', edgecolor='black')
    plt.title(f'Распределение времени обработки документов — {broker.upper()}')
    plt.xlabel('Latency (миллисекунды)')
    plt.ylabel('Количество документов')
    plt.grid(True, alpha=0.3)
    plt.savefig(f'latency_distribution_{broker}.png', dpi=300, bbox_inches='tight')
    plt.close()

def main():
    global processed_count
    parser = argparse.ArgumentParser(description="Consumer для тестирования брокеров")
    parser.add_argument("--broker", choices=["kafka", "rabbitmq"], required=True)
    args = parser.parse_args()

    if args.broker == "kafka":
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            'rag-documents',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        for message in consumer:
            doc = message.value
            start = time.time()

            # эмуляция реальной обработки в RAG (chunking + embedding)
            chunks = [doc["content"][i:i+512] for i in range(0, len(doc["content"]), 512)] #разбиваем на чакни
            embeddings = model.encode(chunks)   # создаём векторные представления чанков

            latency = time.time() - start
            latencies.append(latency)
            processed_count += 1

            if processed_count % 50 == 0:
                print(f"Обработано {processed_count} документов | Latency: {latency*1000:.1f} мс")

    else:  # RabbitMQ
        import pika
        def callback(ch, method, properties, body):
            global processed_count
            start = time.time()
            doc = json.loads(body)

            chunks = [doc["content"][i:i+512] for i in range(0, len(doc["content"]), 512)]
            embeddings = model.encode(chunks)

            latency = time.time() - start
            latencies.append(latency)
            processed_count += 1

            if processed_count % 50 == 0:
                print(f"Обработано {processed_count} документов | Latency: {latency*1000:.1f} мс")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='rag-documents', durable=True)
        channel.basic_consume(queue='rag-documents', on_message_callback=callback)
        channel.start_consuming()

    # Сохраняем результаты при завершении
    save_results(args.broker)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\ошибка")
