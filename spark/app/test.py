import os, base64
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
from google import genai
from google.genai import types

def download_pdf(url):
    file_path = ""
    return file_path

def gemini_request(file_path):
    client = genai.Client(api_key = GOOGLE_API_KEY)
    model = "gemini-2.5-flash-preview-05-20"
    with open(GEMINI_PROMPT_FILE, "r") as gemini_prompt_file:
        prompt = gemini_prompt_file.read()
    pdf = client.files.upload(file = file_path)
    config = types.GenerateContentConfig(
        temperature = 0,
        response_mime_type = "application/json",
        response_schema=genai.types.Schema(
            type = genai.types.Type.ARRAY,
            items = genai.types.Schema(
                type = genai.types.Type.OBJECT,
                required = ["name", "price"],
                properties = {
                    "name": genai.types.Schema(
                        type = genai.types.Type.STRING,
                    ),
                    "price": genai.types.Schema(
                        type = genai.types.Type.NUMBER,
                    ),
                    "uom": genai.types.Schema(
                        type = genai.types.Type.STRING,
                    ),
                    "price_per_unit": genai.types.Schema(
                        type = genai.types.Type.STRING,
                    ),
                    "pieces_available": genai.types.Schema(
                        type = genai.types.Type.STRING,
                    ),
                    "normalized_name": genai.types.Schema(
                        type = genai.types.Type.STRING,
                    ),
                    "type": genai.types.Schema(
                        type = genai.types.Type.STRING,
                    ),
                    "category": genai.types.Schema(
                        type = genai.types.Type.STRING,
                        enum = ["dispensa", "dolciario", "bevande", "salumeria", "macelleria", "ortofrutta", "pescheria", "surgelati", "gelati", "casa", "cura", "altro"],
                    ),
                    "quantity": genai.types.Schema(
                        type = genai.types.Type.STRING,
                    ),
                },
            ),
        )
    )
    response = client.models.generate_content(
        model = model,
        contents = [prompt, pdf]
    )
    return response.text

def handle_message(message):
    return

def handle_batch(batch_df, batch_id):
    for row in batch_df.collect():
        handle_message(row)
    return

KAFKA_SERVER = "broker:9092"
TOPIC = "pdf-metadata"
PDF_DOWNLOAD_PATH = "/tmp/pdfs/"
GOOGLE_API_KEY_FILE = os.getenv("GOOGLE_API_KEY_FILE")
with open(GOOGLE_API_KEY_FILE, "r") as google_api_key_file:
    GOOGLE_API_KEY = google_api_key_file.read()
GEMINI_PROMPT_FILE = "/opt/tap/prompt.txt"

schema = StructType() \
    .add("filename", StringType()) \
    .add("url", StringType()) \
    .add("checksum", StringType())

spark = SparkSession.builder \
    .appName("KafkaPDFListener") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", TOPIC) \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

query = df.writeStream \
    .foreachBatch(handle_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()