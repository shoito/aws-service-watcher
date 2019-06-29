import boto3
import os
import json
import urllib.request
from chalice import Chalice

REGIONS = {
    "ap-northeast-1": "東京",
    "us-east-1": "米国東部(バージニア北部)",
    "us-east-2": "米国東部(オハイオ)",
    "us-west-1": "米国西部(北カリフォルニア)",
    "us-west-2": "米国西部(オレゴン)",
}
BUCKET_NAME = os.getenv("BUCKET_NAME")
JSON_PATH = "services.json"
SLACK_INCOMING_WEBHOOK_URL = os.getenv("SLACK_INCOMING_WEBHOOK_URL")

app = Chalice(app_name="aws-service-watcher")

ssm = boto3.client("ssm")
s3 = boto3.resource("s3")

# @app.route('/')
@app.schedule("rate(30 minutes)")
def handle_event(event):
    for region in REGIONS.keys():
        run(region)


def run(region):
    recent_services = load_recent_services(region)
    services = get_services(region)
    notify_news(region, recent_services, services)


def load_recent_services(region):
    response = s3.Object(BUCKET_NAME, f"{region}/{JSON_PATH}").get()
    services_json = response["Body"].read().decode("utf-8")
    if services_json == "":
        services_json = "[]"
    return json.loads(services_json)


def update_recent_services(region, services):
    s3obj = s3.Object(BUCKET_NAME, f"{region}/{JSON_PATH}")
    s3obj.put(Body=json.dumps(services))


def get_services(region):
    services = []
    response = ssm.get_parameters_by_path(
        Path=f"/aws/service/global-infrastructure/regions/{region}/services"
    )

    services = response["Parameters"]
    while "NextToken" in response:
        next_token = response["NextToken"]
        response = ssm.get_parameters_by_path(
            Path=f"/aws/service/global-infrastructure/regions/{region}/services",
            NextToken=next_token,
        )
        services = services + response["Parameters"]

    services = list(map(lambda s: s["Value"], services))
    return services


def notify_news(region, recent_services, services):
    added = list(set(services) - set(recent_services))
    if len(added) == 0:
        return

    update_recent_services(region, services)

    headers = {"Content-Type": "application/json"}

    added_str = ", ".join(added)
    message = {
        "username": "AWS Service Watcher",
        "text": f"{REGIONS.get(region)}リージョンに新たに *{added_str}* がやってきた。",
        "icon_emoji": ":robot_face:",
    }

    req = urllib.request.Request(
        SLACK_INCOMING_WEBHOOK_URL, json.dumps(message).encode(), headers
    )
    with urllib.request.urlopen(req) as res:
        body = res.read()
