import hashlib, json, os, socket, shutil
from time import sleep

PDF_PATH = "/tmp/2-DECO-SUPERMERCATI-13-22-MAGGIO.pdf"
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

def simulate_download():
    shutil.copy(PDF_PATH, SHARED_FOLDER)

def main():
    simulate_download()
    checksum = sha256sum(PDF_PATH)
    filename = os.path.basename(PDF_PATH)
    payload = {
        "filename": filename,
        "url": filename,
        "checksum": checksum,
        "source": "DECO",
        "validity": {
            "from": "13-05-2025",
            "to": "22-05-2025"
        }
    }
    print("Sending metadata:", payload)
    sleep(1)
    send_to_logstash(payload)

if __name__ == "__main__":
    main()
