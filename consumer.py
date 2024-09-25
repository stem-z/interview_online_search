import textwrap

import kafka
from clickhouse_driver import Client

KAFKA_TOPIC = "product-descriptions"
KAFKA_CONSUMER_GROUP = "product-crawler"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]


def consume_messages():
    consumer = kafka.KafkaConsumer(
        KAFKA_TOPIC,
        group_id=KAFKA_CONSUMER_GROUP,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )
    client = Client(host="localhost", port=9000)
    client.execute(
        textwrap.dedent(
            """\
            CREATE TABLE IF NOT EXISTS default.products
            (
                `words` String
            )
            ENGINE = MergeTree
            ORDER BY words;
            """
        )
    )
    for message in consumer:
        word = message.value.decode("utf-8")
        client.execute("INSERT INTO products (words) VALUES", [(word,)])


if __name__ == "__main__":
    consume_messages()
