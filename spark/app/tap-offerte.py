# --- Imports ---

import hashlib
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Literal, Optional, Tuple

import httpx
import pandas as pd
from dateparser import parse
from google import genai
from httpx_retries import Retry, RetryTransport
from pydantic import BaseModel
from pypdf import PdfReader, PdfWriter
from pyspark.conf import SparkConf
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    explode,
    from_json,
    lit,
    posexplode,
    size,
    struct,
    udf,
    when,
)
from pyspark.sql.session import SparkSession
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# --- Configuration ---

KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
ELASTIC_HOST = os.getenv("ELASTIC_HOST")
ELASTIC_PORT = os.getenv("ELASTIC_PORT")
ELASTIC_FLYERS_INDEX = os.getenv("ELASTIC_FLYERS_INDEX")
ELASTIC_OFFERS_INDEX = os.getenv("ELASTIC_OFFERS_INDEX")
PDF_DOWNLOAD_PATH = "/tmp/spark_pdfs/"
GEMINI_PROMPT_FILE = os.getenv("GEMINI_PROMPT_FILE")
GEMINI_SYSTEM_PROMPT_FILE = os.getenv("GEMINI_SYSTEM_PROMPT_FILE")
GOOGLE_API_KEY_FILE = os.getenv("GOOGLE_API_KEY_FILE")
SPARK_PARALLELISM = int(os.getenv("SPARK_PARALLELISM"))
SPARK_CHECKPOINTS_LOCATION = os.getenv("SPARK_CHECKPOINTS_LOCATION")
EMBEDDINGS_URL = os.getenv("EMBEDDINGS_URL")

with open(GOOGLE_API_KEY_FILE, "r") as google_api_key_file:
    GOOGLE_API_KEY = google_api_key_file.read()
with open(GEMINI_SYSTEM_PROMPT_FILE, "r") as gemini_system_prompt_file:
    GEMINI_SYSTEM_PROMPT = gemini_system_prompt_file.read()
with open(GEMINI_PROMPT_FILE, "r") as gemini_prompt_file:
    GEMINI_PROMPT = gemini_prompt_file.read()

# --- Spark schemas ---

location_schema = StructType(
    [
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ]
)

source_schema = StructType(
    [
        StructField("placeId", StringType(), True),
        StructField("location", location_schema, True),
    ]
)

kafka_schema = StructType(
    [
        StructField("filename", StringType(), False),
        StructField("url", StringType(), False),
        StructField("checksum", StringType(), False),
        StructField("source", source_schema, False),
    ]
)

offers_schema = StructType(
    [
        StructField("name", StringType(), False),
        StructField("price", FloatType(), True),
        StructField("quantity", FloatType(), True),
        StructField("total_quantity", FloatType(), True),
        StructField("count", FloatType(), True),
        StructField("uom", StringType(), True),
        StructField("category", StringType(), False),
        StructField("type", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("validity_from", StringType(), True),
        StructField("validity_to", StringType(), True),
        StructField("embeddings", ArrayType(DoubleType()), True),
    ]
)

gemini_schema = StructType(
    [
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
    ]
)

aggregated_schema = StructType(
    [
        StructField("checksum", StringType(), False),
        StructField("url", StringType(), False),
        StructField("filename", StringType(), False),
        StructField("source_placeId", StringType(), True),
        StructField("source_location_lat", DoubleType(), True),
        StructField("source_location_lon", DoubleType(), True),
        StructField("chunk_total", IntegerType(), False),
        StructField("validity_from", StringType(), False),
        StructField("validity_to", StringType(), False),
        StructField("offers_count", LongType(), False),
        StructField("ai_input_tokens", LongType(), False),
        StructField("ai_cached_tokens", LongType(), False),
        StructField("ai_output_tokens", LongType(), False),
        StructField("processing_time_seconds", DoubleType(), False),
        StructField("offers_data", ArrayType(offers_schema), False),
    ]
)

state_schema = StructType(
    [
        StructField("validity_from", StringType()),
        StructField("validity_to", StringType()),
        StructField("offers_count", LongType()),
        StructField("ai_input_tokens", LongType()),
        StructField("ai_cached_tokens", LongType()),
        StructField("ai_output_tokens", LongType()),
        StructField("processing_time_seconds", DoubleType()),
        StructField("offers_data", ArrayType(offers_schema)),
        StructField("chunks_seen", ArrayType(IntegerType())),
        StructField("source_placeId", StringType()),
        StructField("source_location_lat", DoubleType()),
        StructField("source_location_lon", DoubleType()),
    ]
)

# --- Gemini structured output schema ---

ProductCategory = Literal[
    "Bevande",
    "Latticini e uova",
    "Carne e salumi",
    "Pesce e surgelati",
    "Frutta e verdura",
    "Dispensa secca",
    "Snack e dolci",
    "Cura della casa",
    "Cura della persona",
    "Prodotti per l'infanzia",
    "Giochi",
    "Elettrodomestici",
    "Abbigliamento",
    "Decorazioni per la casa",
]


class Product(BaseModel):
    name: str
    price: Optional[float] = None
    quantity: Optional[float] = None
    total_quantity: Optional[float] = None
    count: Optional[float] = None
    uom: Optional[str] = None
    category: ProductCategory
    type: Optional[str] = None
    notes: Optional[str] = None
    validity_from: Optional[str] = None
    validity_to: Optional[str] = None


class Flyer(BaseModel):
    validity_from: str
    validity_to: str
    offers: list[Product]


# --- Utility functions ---


def convert_numpy_to_python(obj):
    """Recursively convert numpy types to native Python types"""
    import numpy as np

    if isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, (np.integer, np.floating)):
        return obj.item()
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, dict):
        return {key: convert_numpy_to_python(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_to_python(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_numpy_to_python(item) for item in obj)
    else:
        return obj


def extract_source_components(
    value: Any,
) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    if value is None:
        return None, None, None

    if hasattr(value, "asDict"):
        value = value.asDict(recursive=True)

    value = convert_numpy_to_python(value)

    if not isinstance(value, dict):
        return None, None, None

    location = value.get("location")
    if hasattr(location, "asDict"):
        location = location.asDict(recursive=True)

    location = convert_numpy_to_python(location)

    if isinstance(location, dict):
        lat = location.get("lat")
        lon = location.get("lon")
    else:
        lat = None
        lon = None

    return value.get("placeId"), lat, lon


def normalize_date(date: str, day_of_month: str = "current") -> Optional[str]:
    if not isinstance(date, str) or not date.strip():
        print(f"The given date is not a valid string: '{date}'")
        return None

    try:
        settings = {
            "PREFER_DAY_OF_MONTH": day_of_month,
            "PREFER_DATES_FROM": "future",
            "REQUIRE_PARTS": ["month", "year"],
        }
        parsed_date = parse(date, settings=settings)

        if parsed_date:
            if parsed_date.tzinfo is None:
                return parsed_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            else:
                return parsed_date.isoformat(timespec="milliseconds")
        else:
            print(
                f"Unable to parse the given date '{date}' because of unrecognised format"
            )
            return None
    except Exception as e:
        print(f"Unexpected error during parsing of '{date}': {e}")
        return None


def download_pdf(url: str, expected_checksum: str) -> Optional[str]:
    try:
        print(f"Downloading file from url: {url}")  # DEBUG

        local_path = os.path.join(PDF_DOWNLOAD_PATH, f"{expected_checksum}.pdf")
        with httpx.Client(follow_redirects=True, timeout=30.0) as client:
            with client.stream("GET", url) as response:
                response.raise_for_status()
                with open(local_path, "wb") as output_file:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        if chunk:
                            output_file.write(chunk)

        with open(local_path, "rb") as downloaded_file:
            checksum = hashlib.sha256(downloaded_file.read()).hexdigest()
        if checksum != expected_checksum:
            print(
                f"Checksum mismatch for {url}. Expected {expected_checksum}, got {checksum}."
            )
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
        print(f"Splitting PDF: {original_path}")  # DEBUG

        reader = PdfReader(original_path)
        base_name = os.path.splitext(os.path.basename(original_path))[
            0
        ]  # pdf name without extension
        for i in range(0, len(reader.pages), pages_per_chunk):
            writer = PdfWriter()
            chunk_end = min(i + pages_per_chunk, len(reader.pages))
            for page_num in range(i, chunk_end):
                writer.add_page(reader.pages[page_num])

            chunk_path = os.path.join(
                PDF_DOWNLOAD_PATH, f"{base_name}_chunk_{i // pages_per_chunk}.pdf"
            )
            with open(chunk_path, "wb") as f:
                writer.write(f)

            print(f"Created chunk {i // pages_per_chunk}: {chunk_path}")  # DEBUG

            chunk_paths.append(chunk_path)
    except Exception as e:
        print(f"Failed to split PDF {original_path}: {e}")
        for path in chunk_paths:
            if os.path.exists(path):
                os.remove(path)
        return []
    return chunk_paths


@udf(returnType=ArrayType(StringType()))
def download_and_split_pdf(url: str, expected_checksum: str) -> List[str]:
    try:
        local_path = download_pdf(url, expected_checksum)
        if local_path is None:
            return []

        print(f"PDF local path: {local_path}")  # DEBUG
        print(f"PDF size: {os.path.getsize(local_path)}")  # DEBUG

        chunk_paths = split_pdf(local_path, pages_per_chunk=5)

        print(f"PDF chunk_paths: {chunk_paths}")  # DEBUG

        return chunk_paths
    except Exception as e:
        print(f"Unexpected error in download_and_split_pdf: {e}")
        return []


def gemini_request(file_path: str) -> genai.types.GenerateContentResponse:
    try:
        client = genai.Client(
            api_key=GOOGLE_API_KEY,
            http_options=genai.types.HttpOptions(
                retry_options=genai.types.HttpRetryOptions(
                    attempts=5, exp_base=4, initial_delay=5.0, max_delay=60.0
                )
            ),
        )
        model = "gemini-2.5-flash"
        pdf = client.files.upload(file=file_path)

        config = genai.types.GenerateContentConfig(
            temperature=0,
            response_mime_type="application/json",
            response_schema=Flyer,
            max_output_tokens=65536,  # gemini flash max limit
            thinking_config=genai.types.ThinkingConfig(
                thinking_budget=0  # disable thinking
            ),
            system_instruction=GEMINI_SYSTEM_PROMPT,
        )

        response = client.models.generate_content(
            model=model, contents=[GEMINI_PROMPT, pdf], config=config
        )
        return response
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


def get_embeddings(texts: List[str]) -> List[List[float]]:
    if (
        not isinstance(texts, list)
        or not texts
        or not all(isinstance(t, str) and t.strip() for t in texts)
    ):
        return []
    try:
        retry = Retry(total=5, backoff_factor=0.5)
        transport = RetryTransport(retry=retry)
        with httpx.Client(transport=transport) as client:
            body = {"input": texts, "model": "BAAI/bge-m3"}
            response = client.post(EMBEDDINGS_URL, json=body).raise_for_status().json()
            return [embedding.get("embedding") for embedding in response.get("data")]
    except Exception as e:
        print(f"Error generating embeddings: {e}")
        return [[] for _ in texts]


@udf(returnType=gemini_schema)
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

    embeddings = get_embeddings(
        [f"{offer.category} {offer.type} {offer.name}" for offer in flyer.offers]
    )
    for offer, embedding in zip(offers_data, embeddings):
        offer["embeddings"] = embedding if embedding else None

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
        "offers_data": offers_data,
    }


def aggregate_pdf(
    key: Tuple[str], pd_iterator: Iterator[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:
    # get current state or create one
    if state.exists:
        (
            validity_from,
            validity_to,
            offers_count,
            ai_input_tokens,
            ai_cached_tokens,
            ai_output_tokens,
            processing_time_seconds,
            offers_data,
            chunks_seen_list,
            source_place_id,
            source_location_lat,
            source_location_lon,
        ) = state.get
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
        source_place_id = None
        source_location_lat = None
        source_location_lon = None

    # merge all pandas microbatches
    chunks = [df for df in pd_iterator if not df.empty and not df.isna().all().all()]
    if chunks:
        chunks = pd.concat(chunks, ignore_index=True)
    else:
        chunks = pd.DataFrame()

    for _, chunk in chunks.iterrows():
        chunk_n = chunk.chunk_n
        if chunk_n in chunks_seen:
            # Skip chunk if by chance already processed
            continue
        chunks_seen.add(chunk_n)

        print(f"Aggregate pdf chunk seen: {chunk_n}")  # DEBUG

        # I only want to use the validity dates from the first chunk (flyer start)
        if chunk_n == 0:
            validity_from = chunk.validity_from
            validity_to = chunk.validity_to
            (
                source_place_id,
                source_location_lat,
                source_location_lon,
            ) = extract_source_components(chunk.source)
        elif (
            source_place_id is None
            and source_location_lat is None
            and source_location_lon is None
        ):
            (
                source_place_id,
                source_location_lat,
                source_location_lon,
            ) = extract_source_components(chunk.source)

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

            # Convert any numpy types to native Python types
            clean_offers_data = convert_numpy_to_python(offers_data)
            if (
                source_place_id is None
                and source_location_lat is None
                and source_location_lon is None
            ):
                (
                    source_place_id,
                    source_location_lat,
                    source_location_lon,
                ) = extract_source_components(chunk.source)

            pdf_data = {
                "checksum": [checksum],
                "url": [chunk.url],
                "filename": [chunk.filename],
                "source_placeId": [source_place_id],
                "source_location_lat": [source_location_lat],
                "source_location_lon": [source_location_lon],
                "chunk_total": [int(chunk.chunk_total)],
                "validity_from": [validity_from],
                "validity_to": [validity_to],
                "offers_count": [offers_count],
                "ai_input_tokens": [
                    int(ai_input_tokens) if pd.notna(ai_input_tokens) else 0
                ],
                "ai_cached_tokens": [
                    int(ai_cached_tokens) if pd.notna(ai_cached_tokens) else 0
                ],
                "ai_output_tokens": [
                    int(ai_output_tokens) if pd.notna(ai_output_tokens) else 0
                ],
                "processing_time_seconds": [processing_time_seconds],
                "offers_data": [clean_offers_data],
            }
            # Delete state because the pdf is complete
            state.remove()

            # DEBUG
            print(f""" PDF Aggregato:
            - checksum: {checksum}
            - url: {chunk.url}
            - filename: {chunk.filename}
            - source.placeId: {source_place_id}
            - source.location.lat: {source_location_lat}
            - source.location.lon: {source_location_lon}
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
    clean_offers_data = convert_numpy_to_python(offers_data)
    state.update(
        (
            validity_from,
            validity_to,
            offers_count,
            ai_input_tokens,
            ai_cached_tokens,
            ai_output_tokens,
            processing_time_seconds,
            clean_offers_data,
            list(chunks_seen),
            source_place_id,
            source_location_lat,
            source_location_lon,
        )
    )
    yield pd.DataFrame()


def write_micro_batch_to_elastic(batch_df, batch_id):
    print(f"Final forEachBatch rocessing micro-batch ID: {batch_id}")
    batch_df.cache()

    # all offers
    offers_df = (
        batch_df.withColumn("offer", explode(col("offers_data")))
        .withColumnRenamed("validity_from", "flyer_validity_from")
        .withColumnRenamed("validity_to", "flyer_validity_to")
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
            coalesce(col("offer.validity_from"), col("flyer_validity_from")).alias(
                "validity_from"
            ),
            coalesce(col("offer.validity_to"), col("flyer_validity_to")).alias(
                "validity_to"
            ),
            col("offer.embeddings").alias("embeddings"),
        )
    )

    # remove offers from aggregated pdf data before writing to elastic
    aggregated_pdf_df = batch_df.drop("offers_data")

    try:
        # write the static dataframes to elastic
        aggregated_pdf_df.write.format("es").option("es.mapping.id", "checksum").mode(
            "append"
        ).save(ELASTIC_FLYERS_INDEX)

        offers_df.write.format("es").mode("append").save(ELASTIC_OFFERS_INDEX)

        print(f"Micro-batch {batch_id} successfully written to elastic")
    except Exception as e:
        print(f"[ERROR] Elasticsearch write failed: {e}")


# --- Spark setup ---

os.makedirs(PDF_DOWNLOAD_PATH, exist_ok=True)
os.makedirs(SPARK_CHECKPOINTS_LOCATION, exist_ok=True)

spark_conf = SparkConf().set("es.nodes", ELASTIC_HOST).set("es.port", ELASTIC_PORT)

spark = (
    SparkSession.builder.appName("tap-offerte")
    .config(conf=spark_conf)
    .master(f"local[{SPARK_PARALLELISM}]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --- Spark application ---

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("checkpointLocation", os.path.join(SPARK_CHECKPOINTS_LOCATION, "kafka"))
    .load()
)

# all pdfs
pdf_df = (
    kafka_df.select(col("value").cast("string").alias("json_string"))
    .select(from_json(col("json_string"), kafka_schema).alias("data"))
    .select("data.*")
)

# set the udf as non deterministic to avoid spark calling it multiple times
download_and_split_pdf = download_and_split_pdf.asNondeterministic()

# chunks split from pdfs
pdf_df = pdf_df.withColumn(
    "chunks", download_and_split_pdf(col("url"), col("checksum"))
).withColumn("chunk_total", size(col("chunks")))
chunk_df = pdf_df.select(
    "*", posexplode(col("chunks")).alias("chunk_n", "chunk_path")
).drop("chunks")

# repartitioning to ensure parallelism on gemini calls
chunk_df = chunk_df.repartition(SPARK_PARALLELISM)

# gemini call results (metadata + offers)
gemini_df = (
    chunk_df.withColumn("gemini_result", process_chunk(col("chunk_path")))
    .withColumns(
        {field.name: col(f"gemini_result.{field.name}") for field in gemini_schema}
    )
    .drop("gemini_result")
)

# aggregated metadata for each pdf
aggregated_df = gemini_df.groupBy("checksum").applyInPandasWithState(
    func=aggregate_pdf,
    outputMode="append",
    stateStructType=state_schema,
    outputStructType=aggregated_schema,
    timeoutConf=GroupStateTimeout.NoTimeout,
)

aggregated_df = aggregated_df.withColumn("processed_at", current_timestamp())

aggregated_df = (
    aggregated_df.withColumn(
        "source_location_struct",
        when(
            col("source_location_lat").isNull() | col("source_location_lon").isNull(),
            lit(None).cast(location_schema),
        ).otherwise(
            struct(
                col("source_location_lat").alias("lat"),
                col("source_location_lon").alias("lon"),
            )
        ),
    )
    .withColumn(
        "source",
        struct(
            col("source_placeId").alias("placeId"),
            col("source_location_struct").alias("location"),
        ),
    )
    .drop(
        "source_placeId",
        "source_location_lat",
        "source_location_lon",
        "source_location_struct",
    )
)

# Use foreachBatch to write to multiple elastic sinks without duplicating all the previous passages
aggregated_df.writeStream.foreachBatch(write_micro_batch_to_elastic).option(
    "checkpointLocation", os.path.join(SPARK_CHECKPOINTS_LOCATION, "elasticsearch")
).outputMode("append").start().awaitTermination()
