import os
import time
import json
from datetime import datetime
import pymongo
from bson.objectid import ObjectId
import mysql.connector
import redis
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.environ.get("MONGO_URI")
MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASS = os.environ.get("MYSQL_PASS", "root")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")

PREFIX = "gemini_"

# Conexões
mongo_client = pymongo.MongoClient(MONGO_URI)
col_sensores = mongo_client["iot"]["sensores"] # Banco e coleção especificados no README[cite: 3]

mysql_db = mysql.connector.connect(
    host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS, database="iot_data"
)
mysql_cursor = mysql_db.cursor()
redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

counters = {
    "duplicatas_descartadas": 0,
    "timestamps_invalidos": 0,
    "pacotes_quarentena_schema": 0
}

def get_last_checkpoint():
    ckpt = redis_client.get(f"{PREFIX}checkpoint")
    return ObjectId(ckpt) if ckpt else None

def save_checkpoint(last_id):
    redis_client.set(f"{PREFIX}checkpoint", str(last_id))

def is_duplicate(packet_id):
    key = f"{PREFIX}dedup:{packet_id}"
    if redis_client.exists(key):
        return True
    redis_client.setex(key, 86400, "1")
    return False

def write_to_mariadb(node_id, ts_gateway, metrics):
    insert_query = f"""
        INSERT INTO {PREFIX}serie_limpa (node_id, ts_gateway, metric_name, metric_value)
        VALUES (%s, %s, %s, %s)
    """
    for metric, value in metrics.items():
        mysql_cursor.execute(insert_query, (node_id, ts_gateway, metric, value))
    mysql_db.commit()

def update_aggregates(node_id, ts_gateway, metrics):
    time_bucket = ts_gateway.replace(minute=0, second=0, microsecond=0)
    upsert_query = f"""
        INSERT INTO {PREFIX}agregados (node_id, metric_name, time_bucket, avg_val, min_val, max_val)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            avg_val = (avg_val + VALUES(avg_val)) / 2, 
            min_val = LEAST(min_val, VALUES(min_val)),
            max_val = GREATEST(max_val, VALUES(max_val))
    """
    for metric, value in metrics.items():
        mysql_cursor.execute(upsert_query, (node_id, metric, time_bucket, value, value, value))
    mysql_db.commit()

def update_redis_latest(node_id, ts_gateway, metrics):
    for metric, value in metrics.items():
        key = f"{PREFIX}ultimo:{node_id}:{metric}"
        data = {"value": value, "ts": ts_gateway.isoformat()}
        redis_client.set(key, json.dumps(data))

def send_to_quarantine(packet_id, reason, raw_payload):
    global counters
    counters["pacotes_quarentena_schema"] += 1
    query = f"INSERT INTO {PREFIX}quarentena (packet_id, motivo_rejeicao, payload_bruto) VALUES (%s, %s, %s)"
    mysql_cursor.execute(query, (str(packet_id), reason, str(raw_payload)))
    mysql_db.commit()

def process_packet(packet):
    global counters
    try:
        packet_id = packet.get("_id")
        
        if is_duplicate(packet_id):
            counters["duplicatas_descartadas"] += 1
            return True
            
        payload_data = packet.get("payload_data", {})
        node_id = payload_data.get("sender")
        payload = payload_data.get("payload", {})
        
        if not node_id:
            raise ValueError("Falta o identificador do nó (sender)")
            
        # Utiliza received_at conforme recomendado no README[cite: 3]
        ts_gateway = packet.get("received_at")
        if not ts_gateway:
            counters["timestamps_invalidos"] += 1
            ts_gateway = datetime.utcnow()
                
        metrics = {}
        # Mapeamento dos campos 't' (temperatura) e 'u' (humidade)[cite: 3]
        if "t" in payload and isinstance(payload["t"], (int, float)):
            metrics["temperature"] = float(payload["t"])
        if "u" in payload and isinstance(payload["u"], (int, float)):
            metrics["humidity"] = float(payload["u"])
            
        if not metrics:
            raise ValueError("Métricas 't' ou 'u' ausentes no payload")
            
        write_to_mariadb(node_id, ts_gateway, metrics)
        update_aggregates(node_id, ts_gateway, metrics)
        update_redis_latest(node_id, ts_gateway, metrics)
        
        return True
        
    except Exception as e:
        send_to_quarantine(packet.get("_id"), str(e), packet)
        return True

def run_pipeline():
    print("A iniciar pipeline contínua...")
    while True:
        last_id = get_last_checkpoint()
        # Filtro para garantir que apanhamos apenas as leituras de sensores e após o último ID lido[cite: 3]
        query = {"payload_data.payload.t": {"$exists": True}}
        if last_id:
            query["_id"] = {"$gt": last_id}
            
        # O servidor não suporta watch(), logo utilizamos consultas periódicas[cite: 3]
        cursor = col_sensores.find(query).sort("_id", 1).limit(500)
        
        docs_processed = 0
        latest_id = None
        
        for doc in cursor:
            if process_packet(doc):
                latest_id = doc["_id"]
                docs_processed += 1
                
        if latest_id:
            save_checkpoint(latest_id)
            print(f"Lote processado: {docs_processed}. Último _id: {latest_id}")
            print(f"Contadores Atuais: {counters}")
        else:
            time.sleep(5)

if __name__ == "__main__":
    run_pipeline()