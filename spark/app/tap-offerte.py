# --- Imports ---

import os, shutil, hashlib, time
from datetime import datetime, timezone

from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import (
    from_json, col, udf, size, explode, posexplode, coalesce, current_timestamp
)
from pyspark.sql.types import (
    StructType, StringType, StructField, FloatType, ArrayType, LongType, DoubleType, IntegerType
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from google import genai
from google.genai.types import GenerateContentConfig, ThinkingConfig, GenerateContentResponse

from pypdf import PdfReader, PdfWriter

from pydantic import BaseModel
from typing import Optional, List, Dict, Any, Iterator, Tuple
from dateparser import parse
import pandas as pd

# --- Configuration ---

KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
ELASTIC_HOST = os.getenv("ELASTIC_HOST")
ELASTIC_PORT = os.getenv("ELASTIC_PORT")
ELASTIC_FLYERS_INDEX = os.getenv("ELASTIC_FLYERS_INDEX")
ELASTIC_OFFERS_INDEX = os.getenv("ELASTIC_OFFERS_INDEX")
PDF_DOWNLOAD_PATH = "/tmp/spark_pdfs/"
SHARED_FOLDER = "/data/"
GEMINI_PROMPT_FILE = os.getenv("GEMINI_PROMPT_FILE")
GEMINI_SYSTEM_PROMPT_FILE = os.getenv("GEMINI_SYSTEM_PROMPT_FILE")
GOOGLE_API_KEY_FILE = os.getenv("GOOGLE_API_KEY_FILE")
SPARK_PARALLELISM = int(os.getenv("SPARK_PARALLELISM"))
SPARK_CHECKPOINTS_LOCATION = os.getenv("SPARK_CHECKPOINTS_LOCATION")

with open(GOOGLE_API_KEY_FILE, "r") as google_api_key_file:
    GOOGLE_API_KEY = google_api_key_file.read()
with open(GEMINI_SYSTEM_PROMPT_FILE, "r") as gemini_system_prompt_file:
    GEMINI_SYSTEM_PROMPT = gemini_system_prompt_file.read()
with open(GEMINI_PROMPT_FILE, "r") as gemini_prompt_file:
    GEMINI_PROMPT = gemini_prompt_file.read()

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
    StructField("validity_from", StringType(), True),
    StructField("validity_to", StringType(), True),
])

gemini_schema = StructType([
    StructField("validity_from", StringType(), True),
    StructField("validity_to", StringType(), True),
    StructField("offers_count", LongType(), True),
    StructField("ai_model", StringType(), True),
    StructField("ai_input_tokens", LongType(), True),
    StructField("ai_cached_tokens", LongType(), True),
    StructField("ai_output_tokens", LongType(), True),
    StructField("ai_finish_reason", StringType(), True),
    StructField("processed_at", StringType(), True),
    StructField("processing_time_seconds", DoubleType(), True),
    StructField("offers_data", ArrayType(offers_schema), False),
])

aggregated_schema = StructType([
    StructField("checksum", StringType(), False),
    StructField("url", StringType(), False),
    StructField("filename", StringType(), False),
    StructField("source", StringType(), False),
    StructField("chunk_total", IntegerType(), False),
    StructField("validity_from", StringType(), False),
    StructField("validity_to", StringType(), False),
    StructField("offers_count", LongType(), False),
    StructField("ai_input_tokens", LongType(), False),
    StructField("ai_cached_tokens", LongType(), False),
    StructField("ai_output_tokens", LongType(), False),
    StructField("processing_time_seconds", DoubleType(), False),
    StructField("offers_data", ArrayType(offers_schema), False),
])

state_schema = StructType([
    StructField("validity_from", StringType()),
    StructField("validity_to", StringType()),
    StructField("offers_count", LongType()),
    StructField("ai_input_tokens", LongType()),
    StructField("ai_cached_tokens", LongType()),
    StructField("ai_output_tokens", LongType()),
    StructField("processing_time_seconds", DoubleType()),
    StructField("offers_data", ArrayType(offers_schema)),
    StructField("chunks_seen", ArrayType(IntegerType())),
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

def normalize_date(date: str, day_of_month: str = "current") -> Optional[str]:
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

def download_pdf(url: str, expected_checksum: str) -> Optional[str]:
    try:
        print(f"Downloading file from url: {url}") # DEBUG

        local_path = os.path.join(PDF_DOWNLOAD_PATH, f"{expected_checksum}.pdf")
        shutil.copy(os.path.join(SHARED_FOLDER, url), local_path)
        with open(local_path, "rb") as f:
            checksum = hashlib.sha256(f.read()).hexdigest()
        if(checksum != expected_checksum):
            print(f"Checksum mismatch for {url}. Expected {expected_checksum}, got {checksum}.")
            os.remove(local_path)
            return None
        return local_path
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        return None

def split_pdf(original_path: str, pages_per_chunk: int = 5) -> List[str]:
    chunk_paths = []
    try:
        print(f"Splitting PDF: {original_path}") # DEBUG
        
        reader = PdfReader(original_path)
        base_name = os.path.splitext(os.path.basename(original_path))[0] # pdf name without extension
        for i in range(0, len(reader.pages), pages_per_chunk):
            writer = PdfWriter()
            chunk_end = min(i + pages_per_chunk, len(reader.pages))
            for page_num in range(i, chunk_end):
                writer.add_page(reader.pages[page_num])
            
            chunk_path = os.path.join(PDF_DOWNLOAD_PATH, f"{base_name}_chunk_{i // pages_per_chunk}.pdf")
            with open(chunk_path, "wb") as f:
                writer.write(f)

            print(f"Created chunk {i // pages_per_chunk}: {chunk_path}") # DEBUG
            
            chunk_paths.append(chunk_path)
    except Exception as e:
        print(f"Failed to split PDF {original_path}: {e}")
        for path in chunk_paths:
            if os.path.exists(path):
                os.remove(path)
        return []
    return chunk_paths

@udf(returnType = ArrayType(StringType()))
def download_and_split_pdf(url: str, expected_checksum: str) -> List[str]:
    try:
        local_path = download_pdf(url, expected_checksum)
        if local_path is None:
            return []
        
        print(f"PDF local path: {local_path}") # DEBUG
        print(f"PDF size: {os.path.getsize(local_path)}") # DEBUG
        
        chunk_paths = split_pdf(local_path, pages_per_chunk = 5)

        print(f"PDF chunk_paths: {chunk_paths}") # DEBUG

        return chunk_paths
    except Exception as e:
        print(f"Unexpected error in download_and_split_pdf: {e}")
        return []

def gemini_request(file_path: str) -> GenerateContentResponse:
    try:
        client = genai.Client(api_key = GOOGLE_API_KEY)
        model = "gemini-2.5-flash"
        pdf = client.files.upload(file = file_path)

        config = GenerateContentConfig(
            temperature = 0,
            response_mime_type = "application/json",
            response_schema = Flyer,
            max_output_tokens = 65536, # gemini flash max limit
            thinking_config = ThinkingConfig(thinking_budget = 0), # disable thinking
            system_instruction = GEMINI_SYSTEM_PROMPT
        )

        response = client.models.generate_content(
            model = model,
            contents = [GEMINI_PROMPT, pdf],
            config = config
        )
        return response
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

@udf(returnType = gemini_schema)
def process_chunk(chunk_path: str) -> Dict[str, Any]:
    start_perf_counter = time.perf_counter()
    gemini_response = gemini_request(chunk_path)
    end_perf_counter = time.perf_counter()
    elapsed_perf_time = end_perf_counter - start_perf_counter

    flyer = gemini_response.parsed
    flyer_validity_from = normalize_date(flyer.validity_from, "first")
    flyer_validity_to = normalize_date(flyer.validity_to, "last")

    offers_data = [offer.model_dump() for offer in flyer.offers]
    offers_count = len(offers_data)

    now = datetime.now(timezone.utc)
    now_strict_date_time = now.isoformat().replace("+00:00", "Z")

    # DEBUG
    print(f""" Elaborazione gemini chunk {chunk_path}:
    - validity_from: {flyer_validity_from}
    - validity_to: {flyer_validity_to}
    - offers_count: {offers_count}
    - ai_input_tokens: {gemini_response.usage_metadata.prompt_token_count}
    - ai_cached_tokens: {gemini_response.usage_metadata.cached_content_token_count}
    - ai_output_tokens: {gemini_response.usage_metadata.candidates_token_count}
    - ai_finish_reason: {gemini_response.candidates[0].finish_message}
    - processing_time_seconds: {elapsed_perf_time:.2f}
    - offers_data (len): {len(offers_data)}
    """)

    return {
        "validity_from": flyer_validity_from,
        "validity_to": flyer_validity_to,
        "offers_count": offers_count,
        "ai_model": gemini_response.model_version,
        "ai_input_tokens": gemini_response.usage_metadata.prompt_token_count,
        "ai_cached_tokens": gemini_response.usage_metadata.cached_content_token_count,
        "ai_output_tokens": gemini_response.usage_metadata.candidates_token_count,
        "ai_finish_reason": gemini_response.candidates[0].finish_message,
        "processed_at": now_strict_date_time,
        "processing_time_seconds": elapsed_perf_time,
        "offers_data": offers_data
    }

def aggregate_pdf(key: Tuple[str], pd_iterator: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
    # get current state or create one
    if state.exists:
        (validity_from, validity_to, offers_count,
         ai_input_tokens, ai_cached_tokens, ai_output_tokens,
         processing_time_seconds, offers_data, chunks_seen_list) = state.get
        chunks_seen = set(chunks_seen_list)
    else:
        validity_from = None
        validity_to = None
        offers_count = 0
        ai_input_tokens = 0
        ai_cached_tokens = 0
        ai_output_tokens = 0
        processing_time_seconds = 0.0
        offers_data = []
        chunks_seen = set()

    # merge all pandas microbatches
    chunks = pd.concat(pd_iterator, ignore_index = True)

    for _, chunk in chunks.iterrows():
        chunk_n = chunk.chunk_n
        if chunk_n in chunks_seen:
            # Skip chunk if by chance already processed
            continue
        chunks_seen.add(chunk_n)

        print(f"Aggregate pdf chunk seen: {chunk_n}") # DEBUG

        # I only want to use the validity dates from the first chunk (flyer start)
        if chunk_n == 0:
            validity_from = chunk.validity_from
            validity_to = chunk.validity_to

        # Run numerical aggregations
        offers_count += chunk.offers_count
        ai_input_tokens += chunk.ai_input_tokens
        ai_cached_tokens += chunk.ai_cached_tokens
        ai_output_tokens += chunk.ai_output_tokens
        processing_time_seconds += chunk.processing_time_seconds

        # Aggregate offers
        offers_data.extend(chunk.offers_data)

        # Only output a new pdf row if all chunks have been processed
        if len(chunks_seen) == chunk.chunk_total:
            checksum = key[0]
            pdf_data = {
                "checksum": [checksum],
                "url": [chunk.url],
                "filename": [chunk.filename],
                "source": [chunk.source],
                "chunk_total": [int(chunk.chunk_total)],
                "validity_from": [validity_from],
                "validity_to": [validity_to],
                "offers_count": [offers_count],
                "ai_input_tokens": [int(ai_input_tokens) if pd.notna(ai_input_tokens) else 0],
                "ai_cached_tokens": [int(ai_cached_tokens) if pd.notna(ai_cached_tokens) else 0],
                "ai_output_tokens": [int(ai_output_tokens) if pd.notna(ai_output_tokens) else 0],
                "processing_time_seconds": [processing_time_seconds],
                "offers_data": [offers_data]
            }
            # Delete state because the pdf is complete
            state.remove()
            
            # DEBUG
            print(f""" PDF Aggregato:
            - checksum: {checksum}
            - url: {chunk.url}
            - filename: {chunk.filename}
            - source: {chunk.source}
            - chunk_total: {chunk.chunk_total}
            - validity_from: {validity_from}
            - validity_to: {validity_to}
            - offers_count: {offers_count}
            - ai_input_tokens: {ai_input_tokens}
            - ai_cached_tokens: {ai_cached_tokens}
            - ai_output_tokens: {ai_output_tokens}
            - processing_time_seconds: {processing_time_seconds:.2f}
            - offers_data (len): {len(offers_data)}
            """)

            yield pd.DataFrame(pdf_data)
        
    # If after processing all chunks there are still missing ones just update the state
    state.update((
        validity_from,
        validity_to,
        offers_count,
        ai_input_tokens,
        ai_cached_tokens,
        ai_output_tokens,
        processing_time_seconds,
        offers_data,
        list(chunks_seen)
    ))
    yield pd.DataFrame()

def write_micro_batch_to_elastic(batch_df, batch_id):
    print(f"Final forEachBatch rocessing micro-batch ID: {batch_id}")
    batch_df.cache()

    # all offers
    offers_df = batch_df.withColumn("offer", explode(col("offers_data"))) \
        .withColumnRenamed("validity_from", "flyer_validity_from") \
        .withColumnRenamed("validity_to", "flyer_validity_to") \
        .select(
            col("offer.name").alias("name"),
            col("offer.price").alias("price"),
            col("offer.quantity").alias("quantity"),
            col("offer.total_quantity").alias("total_quantity"),
            col("offer.count").alias("count"),
            col("offer.uom").alias("uom"),
            col("offer.category").alias("category"),
            col("offer.type").alias("type"),
            col("offer.notes").alias("notes"),
            col("source"),
            col("checksum").alias("flyer_checksum"),
            coalesce(col("offer.validity_from"), col("flyer_validity_from")).alias("validity_from"),
            coalesce(col("offer.validity_to"), col("flyer_validity_to")).alias("validity_to")
        )

    # remove offers from aggregated pdf data before writing to elastic
    aggregated_pdf_df = batch_df.drop("offers_data")

    try:
        # write the static dataframes to elastic
        aggregated_pdf_df.write \
            .format("es") \
            .option("es.mapping.id", "checksum") \
            .mode("append") \
            .save(ELASTIC_FLYERS_INDEX)
        
        offers_df.write \
            .format("es") \
            .mode("append") \
            .save(ELASTIC_OFFERS_INDEX)
        
        print(f"Micro-batch {batch_id} successfully written to elastic")
    except Exception as e:
        print(f"[ERROR] Elasticsearch write failed: {e}")

# --- Spark setup ---

os.makedirs(PDF_DOWNLOAD_PATH, exist_ok = True)
os.makedirs(SPARK_CHECKPOINTS_LOCATION, exist_ok = True)

spark_conf = SparkConf() \
    .set("es.nodes", ELASTIC_HOST) \
    .set("es.port", ELASTIC_PORT)

spark = SparkSession.builder \
    .appName("tap-offerte") \
    .config(conf = spark_conf) \
    .master(f"local[{SPARK_PARALLELISM}]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Spark application ---

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("checkpointLocation", os.path.join(SPARK_CHECKPOINTS_LOCATION, "kafka")) \
    .load()

# all pdfs
pdf_df = kafka_df.select(col("value").cast("string").alias("json_string")) \
    .select(from_json(col("json_string"), kafka_schema).alias("data")) \
    .select("data.*")

# set the udf as non deterministic to avoid spark calling it multiple times
download_and_split_pdf = download_and_split_pdf.asNondeterministic()

# chunks split from pdfs
pdf_df = pdf_df.withColumn("chunks", download_and_split_pdf(col("url"), col("checksum"))) \
    .withColumn("chunk_total", size(col("chunks")))
chunk_df = pdf_df.select("*", posexplode(col("chunks")).alias("chunk_n", "chunk_path")) \
    .drop("chunks")

# repartitioning to ensure parallelism on gemini calls
chunk_df = chunk_df.repartition(SPARK_PARALLELISM)

# gemini call results (metadata + offers)
gemini_df = chunk_df.withColumn("gemini_result", process_chunk(col("chunk_path"))) \
    .withColumns({ field.name: col(f"gemini_result.{field.name}") for field in gemini_schema }) \
    .drop("gemini_result")

# aggregated metadata for each pdf
aggregated_df = gemini_df.groupBy("checksum") \
    .applyInPandasWithState(
        func = aggregate_pdf,
        outputMode = "append",
        stateStructType = state_schema,
        outputStructType = aggregated_schema,
        timeoutConf = GroupStateTimeout.NoTimeout
    ).withColumn("processed_at", current_timestamp())

# Use foreachBatch to write to multiple elastic sinks without duplicating all the previous passages
aggregated_df.writeStream \
    .foreachBatch(write_micro_batch_to_elastic) \
    .option("checkpointLocation", os.path.join(SPARK_CHECKPOINTS_LOCATION, "elasticsearch")) \
    .outputMode("append") \
    .start() \
    .awaitTermination()