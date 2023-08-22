import json
import threading
from flask import Flask, jsonify
import requests
import schedule
import time
import gzip
import pandas as pd

app = Flask(__name__)


@app.route("/api/data/<ip_addr>", methods=["GET"])
def get_data(ip_addr):
    with open("uncompressed_output.txt", "r") as file:
        lines = file.readlines()
    json_data = json.dumps(lines, indent=4)
    df = pd.DataFrame(json_data)

    return df[df["start_ip"] == ip_addr].to_json()


def fetch_data_from_url():
    global data
    response = requests.get(
        "https://ipinfo.io/data/free/country_asn.json.gz?token=90bfb09995f7a8"
    )
    if response.status_code == 200:
        compressed_data = BytesIO(response.content)
        with gzip.GzipFile(fileobj=compressed_data, mode="rb") as f:
            uncompressed_content = f.read().decode("utf-8")
            with open("uncompressed_output.txt", "w") as output_file:
                output_file.write(uncompressed_content)


# Schedule the data fetching task every 8 hours
schedule.every(8).hours.do(fetch_data_from_url)


# Run the scheduled tasks in the background
def run_scheduled_tasks():
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    fetch_data_from_url()
    schedule_thread = threading.Thread(target=run_scheduled_tasks)
    schedule_thread.start()

    app.run(debug=True)
