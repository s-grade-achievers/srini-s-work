import websocket
from kafka import KafkaProducer
import json

bootstrap_servers = "localhost:9092"
topic = "srini-s-warriors"

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def on_message(ws, message):
    producer.send(topic, value=message)


def on_error(ws, error):
    print("Error:", error)


def on_close(ws, arg1, arg2):
    print("Connection closed")
    producer.close()


def on_open(ws):
    print("Connection opened")


ws_url = "wss://stream.binance.com:9443/ws/btcusdt@trade"

ws = websocket.WebSocketApp(ws_url, on_message=on_message, on_error=on_error, on_close=on_close)
ws.on_open = on_open
ws.run_forever()
