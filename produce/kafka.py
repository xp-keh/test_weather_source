# import json
# import time
# import requests
# import logging
# import os
# from datetime import datetime
# from config.logging import Logger
# from kafka import KafkaProducer
# from kafka.errors import KafkaError
# from config.utils import get_env_value

# logging.basicConfig(level=logging.INFO) 
# logger = logging.getLogger(__name__)

# class Producer:
#     def __init__(self, kafka_broker: str, kafka_topic: str) -> None:
#         self._kafka_server = kafka_broker
#         self._kafka_topic = kafka_topic
#         self._instance = None
#         self.logger = Logger().setup_logger(service_name='producer')
    

#     def create_instance(self) -> KafkaProducer: 
#         self.logger.info(" [*] Starting Kafka producer...")
#         self._instance = KafkaProducer(
#             bootstrap_servers=self._kafka_server,
#             value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#             api_version=(0,11,5)
#         )  # type: ignore
#         return self._instance

#     def is_kafka_connected(self) -> bool:
#         try:
#             metadata = self._instance.bootstrap_connected() # type: ignore
#             if metadata:
#                 self.logger.info(" [*] Connected to Kafka cluster successfully!")
#                 return True
#             else:
#                 self.logger.error(" [X] Failed to connect to Kafka cluster.")
#                 return False
#         except KafkaError as e:
#             self.logger.error(f" [X] Kafka connection error: {e}")
#             return False
        
#     def ensure_bytes(self, message) -> bytes:
#         if not isinstance(message, bytes):
#             return bytes(message, encoding='utf-8')
#         return message
    
#     def produce(self) -> None:
#         try:
#             self.logger.info(" [*] Starting real-time Kafka producer.")
#             api_key = get_env_value('OPENWEATHER_API_KEY')

#             locations = {
#                 "TNTI": ("0.7718", "127.3667"),
#                 "PMBI": ("-2.9024", "104.6993"),
#                 "BKB": ("-1.1073", "116.9048"),
#                 "SOEI": ("-9.7553", "124.2672"),
#                 "MMRI": ("-8.6357", "122.2376"),
#             }

#             while True:
#                 for location, coords in locations.items():
#                     lat, lon = coords

#                     url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"

#                     try:
#                         response = requests.get(url, timeout=5)
#                         response.raise_for_status()
#                         response_json = response.json()

#                         response_json["location"] = location
#                         response_json["raw_produce_dt"] = int(datetime.now().timestamp() * 1_000_000)
#                         response_json["lat"] = lat
#                         response_json["lon"] = lon

#                         self._instance.send(self._kafka_topic, value=response_json)  # type: ignore

#                         self.logger.info(f"Data sent to Kafka topic: {response_json}")

#                     except requests.exceptions.RequestException as e:
#                         logger.error(f"Error fetching weather data for {location}: {e}")
#                         continue

#                 time.sleep(1)

#         except Exception as e:
#             pass
#             self.logger.error(f" Error in Kafka: {e}")
#             self.logger.info(" [*] Stopping data generation.")
#             os._exit(1)
#         finally:
#             self._instance.close()  # type: ignore

import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config.logging import Logger

logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__)

class Producer:
    def __init__(self, kafka_broker: str, kafka_topic: str) -> None:
        self._kafka_server = kafka_broker
        self._kafka_topic = kafka_topic
        self._instance = None
        self.logger = Logger().setup_logger(service_name='producer')

    def create_instance(self) -> KafkaProducer:
        self.logger.info(" [*] Starting Kafka producer...")
        self._instance = KafkaProducer(
            bootstrap_servers=self._kafka_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 11, 5)
        ) 
        return self._instance

    def is_kafka_connected(self) -> bool:
        try:
            metadata = self._instance.bootstrap_connected()
            if metadata:
                self.logger.info(" [*] Connected to Kafka cluster successfully!")
                return True
            else:
                self.logger.error(" [X] Failed to connect to Kafka cluster.")
                return False
        except KafkaError as e:
            self.logger.error(f" [X] Kafka connection error: {e}")
            return False

    def produce_testing_data(self, n: int) -> int:
        try:
            self.logger.info(f" [*] Producing {n} test messages...")

            locations = {
                "TNTI": ("0.7718", "127.3667"),
                "PMBI": ("-2.9024", "104.6993"),
                "BKB": ("-1.1073", "116.9048"),
                "SOEI": ("-9.7553", "124.2672"),
                "MMRI": ("-8.6357", "122.2376"),
            }

            sample_data = {
                "coord": {"lon": 110.2973, "lat": -7.9923},
                "weather": [{"id": 500, "main": "Rain", "description": "light rain", "icon": "10n"}],
                "base": "stations",
                "main": {
                    "temp": 299.04,
                    "feels_like": 299.72,
                    "temp_min": 299.04,
                    "temp_max": 299.04,
                    "pressure": 1013,
                    "humidity": 78,
                    "sea_level": 1013,
                    "grnd_level": 1006
                },
                "visibility": 6120,
                "wind": {"speed": 6.13, "deg": 129, "gust": 9.34},
                "rain": {"1h": 0.33},
                "clouds": {"all": 100},
                "dt": 1748705146,
                "sys": {
                    "type": 2, "id": 2103194, "country": "ID", "sunrise": 1748645158, "sunset": 1748687239
                },
                "timezone": 25200,
                "id": 1650434,
                "name": "Bambanglipuro",
                "cod": 200
            }

            location_keys = list(locations.keys())
            test_message = sample_data.copy()
            test_message["lat"] = 0.12
            test_message["lon"] = 0.13
            test_message["location"] = "PMBI"
            test_message["id"] = 9999
            test_message["source_dt"] = int(datetime.now().timestamp() * 1_000_000)
            byte_size_per_data = len(json.dumps(test_message).encode('utf-8'))

            start_time = time.time()

            for i in range(1, n + 1):
                message = sample_data.copy()
                message["coord"] = message["coord"].copy()
                message["sys"] = message["sys"].copy()

                loc = location_keys[(i - 1) % len(location_keys)]
                lat, lon = locations[loc]

                # message["coord"]["lat"] = float(lat)
                # message["coord"]["lon"] = float(lon)
                message["lat"] = message["coord"]["lat"]
                message["lon"] = message["coord"]["lon"]
                message["location"] = loc
                message["id"] = i
                message["source_dt"] = int(datetime.now().timestamp() * 1_000_000)
                self._instance.send(self._kafka_topic, value=message)

            end_time = time.time()

            total_time = end_time - start_time
            total_bytes = byte_size_per_data * n
            throughput = total_bytes / total_time if total_time > 0 else 0

            self.logger.info(f"[Metric] Byte size per message               : {byte_size_per_data} bytes")
            self.logger.info(f"[Metric] Total messages produced to Kafka    : {n}")
            self.logger.info(f"[Metric] Total data size produced to Kafka   : {total_bytes} bytes")
            self.logger.info(f"[Metric] Time taken to produce to Kafka      : {total_time:.4f} seconds")
            self.logger.info(f"[Metric] Kafka Producer Throughput           : {throughput:.2f} bytes/sec")

            return n
        except Exception as e:
            self.logger.error(f"[X] Error during produce_testing_data: {e}")
            return 0
