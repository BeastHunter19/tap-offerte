import os, shutil, hashlib
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField
from google import genai
from google.genai import types

def download_pdf(message):
    file_path = shutil.copy(SHARED_FOLDER + message.url, PDF_DOWNLOAD_PATH)
    with open(file_path, "rb") as f:
        checksum = hashlib.sha256(f.read()).hexdigest()
    if(checksum != message.checksum):
        return ""
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
    pdf_path = download_pdf(message)
    if(pdf_path == ""):
        print("Errore nel download del pdf.")
        return
    ai_output = gemini_request(pdf_path)
    print(ai_output)

def handle_batch(batch_df, batch_id):
    for row in batch_df.collect():
        handle_message(row)

KAFKA_SERVER = "broker:9092"
TOPIC = "pdf-metadata"
PDF_DOWNLOAD_PATH = "/tmp/"
SHARED_FOLDER = "/data/"
GOOGLE_API_KEY_FILE = os.getenv("GOOGLE_API_KEY_FILE")
with open(GOOGLE_API_KEY_FILE, "r") as google_api_key_file:
    GOOGLE_API_KEY = google_api_key_file.read()
GEMINI_PROMPT_FILE = "/opt/tap/prompt.txt"

schema = StructType([
    StructField("filename", StringType(), True),
    StructField("url", StringType(), True),
    StructField("checksum", StringType(), True),
    StructField("source", StringType(), True),
    StructField("validity", StructType([
        StructField("from", StringType(), True),
        StructField("to", StringType(), True)
    ]), True)
])

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

df.writeStream \
    .foreachBatch(handle_batch) \
    .outputMode("append") \
    .start() \
    .awaitTermination()