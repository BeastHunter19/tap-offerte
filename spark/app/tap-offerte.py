import os, shutil, hashlib, time
from datetime import datetime, timezone
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField, FloatType
from google import genai
from google.genai import types
from pydantic import BaseModel
from typing import Optional
from elasticsearch import Elasticsearch
import pandas as pd
from dateparser import parse

# --- Configuration ---

KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
ELASTIC_HOST = os.getenv("ELASTIC_HOST")
ELASTIC_PORT = os.getenv("ELASTIC_PORT")
ELASTIC_FLYERS_INDEX = os.getenv("ELASTIC_FLYERS_INDEX")
ELASTIC_OFFERS_INDEX = os.getenv("ELASTIC_OFFERS_INDEX")
PDF_DOWNLOAD_PATH = "/tmp/"
SHARED_FOLDER = "/data/"
GEMINI_PROMPT_FILE = os.getenv("GEMINI_PROMPT_FILE")
GOOGLE_API_KEY_FILE = os.getenv("GOOGLE_API_KEY_FILE")

with open(GOOGLE_API_KEY_FILE, "r") as google_api_key_file:
    GOOGLE_API_KEY = google_api_key_file.read()

# --- Spark setup ---

spark_conf = SparkConf() \
    .set("es.nodes", ELASTIC_HOST) \
    .set("es.port", ELASTIC_PORT)

spark = SparkSession.builder \
    .appName("tap-offerte") \
    .config(conf = spark_conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Spark schemas ---

kafka_schema = StructType([
    StructField("filename", StringType(), False),
    StructField("url", StringType(), False),
    StructField("checksum", StringType(), False),
    StructField("source", StringType(), False),
])

offers_schema = StructType([
    StructField("name", StringType(), False),
    StructField("price", FloatType(), True),
    StructField("quantity", FloatType(), True),
    StructField("total_quantity", FloatType(), True),
    StructField("count", FloatType(), True),
    StructField("uom", StringType(), True),
    StructField("category", StringType(), True),
    StructField("type", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("source", StringType(), False),
    StructField("flyer_checksum", StringType(), False),
    StructField("validity_from", StringType(), False),
    StructField("validity_to", StringType(), False),
])

# --- Gemini structured output schema ---

class Product(BaseModel):
    name: str
    price: Optional[float] = None
    quantity: Optional[float] = None
    total_quantity: Optional[float] = None
    count: Optional[float] = None
    uom: Optional[str] = None
    category: Optional[str] = None
    type: Optional[str] = None
    notes: Optional[str] = None
    validity_from: Optional[str] = None
    validity_to: Optional[str] = None

class Flyer(BaseModel):
    validity_from: str
    validity_to: str
    offers: list[Product]

# --- Utility functions ---

def normalize_date(date, day_of_month = "current"):
    if not isinstance(date, str) or not date.strip():
        print(f"The given date is not a valid string: '{date}'")
        return None
    
    try:
        settings = {
            "PREFER_DAY_OF_MONTH": day_of_month,
            "PREFER_DATES_FROM": "future",
            "REQUIRE_PARTS": ["month", "year"]
        }
        parsed_date = parse(
            date,
            settings = settings
        )

        if parsed_date:
            if parsed_date.tzinfo is None:
                return parsed_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            else:
                return parsed_date.isoformat(timespec = "milliseconds")
        else:
            print(f"Unable to parse the given date '{date}' because of unrecognised format")
            return None
    except Exception as e:
        print(f"Unexpected error during parsing of '{date}': {e}")
        return None

def download_pdf(message):
    file_path = shutil.copy(SHARED_FOLDER + message.url, PDF_DOWNLOAD_PATH)
    with open(file_path, "rb") as f:
        checksum = hashlib.sha256(f.read()).hexdigest()
    if(checksum != message.checksum):
        return ""
    return file_path

SYSTEM_INSTRUCTION = """
Sei un estrattore di offerte da volantini promozionali.
Il tuo compito è analizzare il PDF fornito, che è in italiano,
e estrarre ogni singola offerta trovata. Per ogni offerta,
genera un oggetto JSON con i nomi dei campi in inglese.
Assicurati di estrarre la lista completa di tutte le promozioni
e sconti presenti nel volantino, senza tralasciare alcuna offerta.
La completezza dell'estrazione è cruciale.
"""

def gemini_request(file_path):
    client = genai.Client(api_key = GOOGLE_API_KEY)
    model = "gemini-2.5-flash"
    with open(GEMINI_PROMPT_FILE, "r") as gemini_prompt_file:
        prompt = gemini_prompt_file.read()
    pdf = client.files.upload(file = file_path)

    config = types.GenerateContentConfig(
        temperature = 0,
        response_mime_type = "application/json",
        response_schema = Flyer,
        max_output_tokens = 65536, # gemini flash max limit
        thinking_config = genai.types.ThinkingConfig(thinking_budget = 0), # disable thinking
        system_instruction = SYSTEM_INSTRUCTION
    )

    response = client.models.generate_content(
        model = model,
        contents = [prompt, pdf],
        config = config
    )
    return response

def process_pdf(pdf_iter):
    es_endpoint = f"http://{ELASTIC_HOST}:{ELASTIC_PORT}"
    es = Elasticsearch(hosts=[es_endpoint])

    for pdf_rows in pdf_iter:
        rows_out = []
        for _, row in pdf_rows.iterrows():
            try:
                checksum = row["checksum"]
                
                file_path = download_pdf(row)
                if not file_path:
                    print("Error downloading pdf.")
                    continue
                
                print(f"Processing file {row['filename']} with checksum {checksum}.")
                try:
                    start_perf_counter = time.perf_counter()
                    gemini_response = gemini_request(file_path)
                    end_perf_counter = time.perf_counter()
                    elapsed_perf_time = end_perf_counter - start_perf_counter
                    # --- DEBUG ---
                    print(f"--- GEMINI OUTPUT ---")
                    print(f"Request took: {elapsed_perf_time:.6f} seconds)")
                    print("Feedback: ", gemini_response.prompt_feedback)
                    print("Finish reason: ", gemini_response.candidates[0].finish_reason)
                    print("Usage metadata: ", gemini_response.usage_metadata)
                    print("Offers extracted: ", len(gemini_response.parsed.offers))

                    flyer = gemini_response.parsed
                    flyer_validity_from = normalize_date(flyer.validity_from, "first")
                    flyer_validity_to = normalize_date(flyer.validity_to, "last")
                except Exception as e:
                    print(f"[Gemini error] {e}")
                    continue

                for offer in flyer.offers:
                    offer_dict = offer.model_dump()
                    offer_dict.update({
                        "source": row["source"],
                        "flyer_checksum": checksum,
                        "validity_from": flyer_validity_from if offer.validity_from is None \
                            else normalize_date(offer.validity_from, "first"),
                        "validity_to": flyer_validity_from if offer.validity_from is None \
                            else normalize_date(offer.validity_from, "last")
                    })
                    rows_out.append(offer_dict)
                
                # getting current timestamp in a format accepted by elastic
                now = datetime.now(timezone.utc)
                now_strict_date_time = now.isoformat().replace("+00:00", "Z")

                flyer_document = {
                    "checksum": checksum,
                    "filename": row["filename"],
                    "source": row["source"],
                    "validity_from": flyer_validity_from,
                    "validity_to": flyer_validity_to,
                    "offers_count": len(flyer.offers),
                    "ai_model": gemini_response.model_version,
                    "ai_input_tokens": gemini_response.usage_metadata.prompt_token_count,
                    "ai_cached_tokens": gemini_response.usage_metadata.cached_content_token_count,
                    "ai_output_tokens": gemini_response.usage_metadata.candidates_token_count,
                    "ai_finish_reason": gemini_response.candidates[0].finish_message,
                    "processed_at": now_strict_date_time,
                    "processing_time": elapsed_perf_time
                }

                try:
                    es.index(index = ELASTIC_FLYERS_INDEX, id = checksum, document = flyer_document)
                except Exception as e:
                    print(f"[Elasticsearch error] {e}")

            except Exception as e:
                print(f"[General error] {e}")

        yield pd.DataFrame(rows_out)

# --- Spark application ---

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

pdf_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), kafka_schema).alias("data")) \
    .select("data.*")

offers_df = pdf_df.mapInPandas(process_pdf, schema = offers_schema)

offers_df.writeStream \
    .format("es") \
    .option("checkpointLocation", "/tmp/") \
    .outputMode("append") \
    .start(ELASTIC_OFFERS_INDEX) \
    .awaitTermination()