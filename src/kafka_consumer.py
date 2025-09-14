from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "test4",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",   # force read from beginning
    enable_auto_commit=True,
    group_id="fresh-debug-group"    # NEW group so offsets reset
)

print("Listening for messages on test4...")

for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")
