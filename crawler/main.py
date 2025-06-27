import hashlib, json, os, socket, shutil
from time import sleep

PDF_PATH = "/tmp/"
SHARED_FOLDER = "/data/"
LOGSTASH_HOST = os.getenv("LOGSTASH_HOST", "localhost")
LOGSTASH_PORT = int(os.getenv("LOGSTASH_PORT", 5044))

def sha256sum(filepath):
    with open(filepath, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

def send_to_logstash(payload):
    print("Connecting to:", LOGSTASH_HOST, LOGSTASH_PORT)
    with socket.create_connection((LOGSTASH_HOST, LOGSTASH_PORT)) as sock:
        sock.sendall((json.dumps(payload) + "\n").encode("utf-8"))

def simulate_download(file_path):
    shutil.copy(file_path, SHARED_FOLDER)

def main():
    if not os.path.exists(PDF_PATH):
        print(f"Error: The base folder '{PDF_PATH}' does not exist.")
        return

    # Iterate through each item in the base folder
    for item in os.listdir(PDF_PATH):
        supermarket_path = os.path.join(PDF_PATH, item)

        # Check if the item is a directory (supermarket folder)
        if os.path.isdir(supermarket_path):
            supermarket_name = item  # The folder name is the supermarket source
            print(f"Processing supermarket: {supermarket_name}")

            # Iterate through files within the supermarket folder
            for filename in os.listdir(supermarket_path):
                if filename.lower().endswith(".pdf"):
                    pdf_filepath = os.path.join(supermarket_path, filename)

                    print(f"  - Processing PDF: {filename}")
                    try:
                        simulate_download(pdf_filepath)
                        checksum = sha256sum(pdf_filepath)
                        payload = {
                            "filename": filename,
                            "url": filename,
                            "checksum": checksum,
                            "source": supermarket_name
                        }
                        send_to_logstash(payload)
                    except Exception as e:
                        print(f"Error processing {pdf_filepath}: {e}")
                else:
                    print(f"  - Skipping non-PDF file: {filename}")
        else:
            print(f"Skipping non-directory item in base folder: {item}")

if __name__ == "__main__":
    main()
