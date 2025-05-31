# from fastapi import FastAPI
# from produce.kafka import Producer
# from dotenv import load_dotenv 
# from config.utils import get_env_value
# import threading

# load_dotenv()

# def produce(producer: Producer) -> None:
#     try:
#         producer.create_instance()
#         producer.produce()
#     except KeyboardInterrupt:
#         exit(1)

# app = FastAPI()

# @app.get("/health")
# def health():
#     return {"status": "ok"}

# kafka_broker = get_env_value('KAFKA_BROKER')
# kafka_topic = get_env_value('KAFKA_TOPIC')


# producer = Producer(
#     kafka_topic=kafka_topic,  # type: ignore
#     kafka_broker=kafka_broker # type: ignore
# )

# t_producer = threading.Thread(
#     target=produce,
#     args=(producer,),
#     daemon=True
# )

# t_producer.start()

from fastapi import FastAPI, Query
from produce.kafka import Producer
from dotenv import load_dotenv
from config.utils import get_env_value

load_dotenv()

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/weather-source-start")
def start_weather_source(count: int = Query(..., ge=1, le=10000)):
    """
    Endpoint untuk mengirim data uji sebanyak `count` kali ke Kafka.
    """
    sent = producer.produce_testing_data(count)
    return {"status": "done", "messages_sent": sent}

kafka_broker = get_env_value('KAFKA_BROKER')
kafka_topic = get_env_value('KAFKA_TOPIC')

producer = Producer(
    kafka_topic=kafka_topic,
    kafka_broker=kafka_broker
)

producer.create_instance()