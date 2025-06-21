#!/bin/sh

set -e

# Get the secret file path from the environment variable
SECRET_FILE_PATH="${ENCRYPTION_KEY_FILE}"

if [ -z "${SECRET_FILE_PATH}" ]; then
  echo "Error: ENCRYPTION_KEY_FILE environment variable is not set."
  exit 1
fi

if [ -f "${SECRET_FILE_PATH}" ]; then
  read -N 32 -r XPACK_ENCRYPTEDSAVEDOBJECTS_ENCRYPTIONKEY < "${SECRET_FILE_PATH}"
  export XPACK_ENCRYPTEDSAVEDOBJECTS_ENCRYPTIONKEY
else
  exit 1
fi

echo "Starting Kibana..."
exec /usr/local/bin/kibana-docker