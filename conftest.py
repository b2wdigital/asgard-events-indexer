import json
import os

ENV = "TEST"
os.environ["ENV"] = ENV

VALUES = {
    "MESOS_MASTER_URLS": json.dumps(
        ["http://127.0.0.1:5050", "http://10.0.0.1:5050"]
    )
}


for name, value in VALUES.items():
    os.environ[f"{ENV}_{name}"] = os.getenv(f"{ENV}_{name}", value)

assert os.environ["ENV"] == "TEST"
