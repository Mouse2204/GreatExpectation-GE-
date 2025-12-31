import time
import json
import random
from confluent_kafka import Producer
from datetime import datetime, timedelta
from faker import Faker

conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'mixed-producer',
}

producer = Producer(conf)
TOPIC_NAME = "user_data_topic"
fake = Faker()

def delivery_report(err, msg):
    if err is not None:
        print(f'Gửi lỗi: {err}')
    else:
        print(f'Sent Data: {msg.topic()} [{msg.partition()}]')

def generate_mixed_data():
    clean_probability = 0.7
    
    if random.random() < clean_probability:
        data = {
            "id": fake.uuid4(),
            "name": fake.name(),
            "age": random.randint(18, 65),
            "role": random.choice(["Admin", "User", "Dev", "Manager", "Analyst"]),
            "salary": round(random.uniform(1000, 10000), 2),
            "email": fake.email(),
            "transaction_date": datetime.now().strftime("%Y-%m-%d")
        }
        return data, "CLEAN"
    else:
        dirty_type = random.choice(["null_fields", "out_of_range", "invalid_format", "wrong_type", "duplicate"])
        base_data = {
            "id": fake.uuid4(),
            "name": fake.name(),
            "age": random.randint(18, 65),
            "role": random.choice(["Admin", "User", "Dev", "Manager", "Analyst"]),
            "salary": round(random.uniform(1000, 10000), 2),
            "email": fake.email(),
            "transaction_date": datetime.now().strftime("%Y-%m-%d")
        }
        
        if dirty_type == "null_fields":
            fields_to_null = [f for f in base_data.keys() if f != "id"]
            null_fields = random.sample(fields_to_null, random.randint(1, 3))
            for field in null_fields:
                base_data[field] = None
            return base_data, "NULL_FIELDS"
            
        elif dirty_type == "out_of_range":
            base_data["age"] = random.randint(100, 150)
            base_data["salary"] = round(random.uniform(-5000, -100), 2)
            return base_data, "OUT_OF_RANGE"
            
        elif dirty_type == "invalid_format":
            base_data["email"] = f"invalid_email_{random.randint(1, 1000)}"
            base_data["transaction_date"] = "2024-13-45"
            return base_data, "INVALID_FORMAT"
            
        elif dirty_type == "wrong_type":
            base_data["age"] = "twenty-five"
            base_data["salary"] = "high"
            return base_data, "WRONG_TYPE"
            
        else:
            base_data["id"] = "DUPLICATE_ID_FIXED"
            return base_data, "DUPLICATE_ID"

def safe_get_id(data):
    id_value = data.get('id')
    if id_value is None:
        return "NULL"
    elif isinstance(id_value, str):
        return id_value[:20]
    else:
        return str(id_value)[:20]

if __name__ == "__main__": 
    try:
        batch_counter = 0
        while True:
            batch_counter += 1
            for i in range(5):
                data, data_type = generate_mixed_data()
                
                value_bytes = json.dumps(data).encode('utf-8')
                
                producer.produce(
                    TOPIC_NAME, 
                    value=value_bytes, 
                    callback=delivery_report
                )
                
                print(f"Batch {batch_counter}, Record {i+1}: {data_type}")
                print(f"  Sample: ID={safe_get_id(data)}, Age={data.get('age')}, Salary={data.get('salary')}")
                
                producer.poll(0)
                time.sleep(0.2)
            
            print("-" * 60)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n Đang dừng...")
        producer.flush()
        print("Đã dừng gửi dữ liệu.")