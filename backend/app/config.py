import os

# Kredensial hanya dari environment (.env via docker compose), tidak ada default password di kode.
PRIMARY_DSN = os.environ["PRIMARY_DSN"]
REPLICA_DSN = os.environ["REPLICA_DSN"]
ES_URL = os.getenv("ES_URL", "http://localhost:9200")
