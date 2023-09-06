from io import BytesIO
import json
import threading
from flask import Flask, jsonify
import requests
import schedule
import time
import gzip
import pandas as pd

app = Flask(__name__)

df = None


def push_df():
    try:
        json_obj = None
        json_objects = []
        with open("uncompressed_output.txt", "r") as file:
            for line in file:
                try:
                    json_obj = json.loads(line)
                    json_objects.append(json_obj)
                except Exception as e:
                    print("Error while parsing JSon", e)

        return pd.DataFrame.from_records(data=json_objects)
    except Exception as e:
        print("Error in get df", e)


@app.route("/api/data/<ip_addr>", methods=["GET"])
def get_data(ip_addr):
    if df.empty:
        return "Data not fetched"
    final_obj = df[df["start_ip"] == ip_addr].to_dict(orient="records")[0]
    if final_obj["start_ip"] == None:
        return "Ip not found"
    return json.dumps(final_obj)


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
    df = push_df()
    schedule_thread = threading.Thread(target=run_scheduled_tasks)
    schedule_thread.start()

    app.run(debug=True)
