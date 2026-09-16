#!/usr/bin/env python
import glob
import json
import math
import os
import re
import sys

import pandas as pd
import requests


def daily_dataframe(filename, stage_name):
    df = pd.read_csv(filename)
    df["test_name"] = stage_name
    metrics_df = pd.DataFrame(config['metrics'])
    merged = metrics_df.merge(df, how='right', right_on='metric', left_on='name')
    return merged


def collect_metrics(stage_name, path_name, name_pattern, today):
    file_pattern = os.path.join(path_name, f"report_*{name_pattern}*.csv")
    all_files = glob.glob(file_pattern)
    todays_files = [file for file in all_files if (today in file)]

    if todays_files:
        # load all CSV from today
        df = pd.concat(
            [daily_dataframe(filename, stage_name) for filename in todays_files], ignore_index=True
        )
        return df
    else:
        print("no csv files for today, nothing to do")
        return None


def create_datadog_metric_request(metrics_data):
    """Only create header and request"""
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'DD-API-KEY': datadog_api_key,
    }

    datalog_metric_prefix = 'custom.metrics.lapi_perf_test'

    collection = []
    for _, row in metrics_data.iterrows():
        ts = row['timestamp']
        value = row['value']
        metric = row['metric']
        lower = row['lower']
        upper = row['upper']
        test_name = row['test_name']

        if not (math.isnan(value)):
            collection.append(
                {
                    'metric': f"{datalog_metric_prefix}.{metric}",
                    'type': 3,
                    'points': [
                        {
                            'timestamp': int(ts),
                            'value': value,
                        }
                    ],
                    'tags': ["env:development", f"source:{test_name}"],
                }
            )
        if not (lower is None or math.isnan(lower)):
            collection.append(
                {
                    'metric': f"{datalog_metric_prefix}.lower_bound.{metric}",
                    'type': 3,
                    'points': [
                        {
                            'timestamp': int(ts),
                            'value': lower,
                        }
                    ],
                    'tags': ["env:development", f"source:{test_name}"],
                }
            )
        if not (upper is None or math.isnan(upper)):
            collection.append(
                {
                    'metric': f"{datalog_metric_prefix}.upper_bound.{metric}",
                    'type': 3,
                    'points': [
                        {
                            'timestamp': int(ts),
                            'value': upper,
                        }
                    ],
                    'tags': ["env:development", f"source:{test_name}"],
                }
            )

    payload = {'series': collection}

    return (headers, payload)


def send_datadog_metric(headers, payload):
    """Actual HTTP post"""
    url = "https://api.datadoghq.com/api/v2/series"
    try:
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(f"**failed request to Datadog with error: {err}**")
    else:
        data = resp.json()
        print("Metric sent to Datadog, status:", resp.status_code)
        print("Metric sent to Datadog, response:", data)


def create_slack_message_request(metrics_data, measurement_name, metrics):

    headers = {
        'Authorization': f"Bearer {slack_api_key}",
        'Content-Type': 'application/json; charset=utf-8',
    }
    payload = {
        'channel': slack_channel_id,
        'blocks': [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"LAPI performance test results for {measurement_name}",
                },
            }
        ],
    }

    lines = [f"Key metrics from {report_path}:"]
    grouped = (
        metrics_data[metrics_data['description'].notnull()]
        .groupby(['metric', 'description', 'upper', 'lower'])
        .agg({'value': 'mean'})
        .reset_index()
    )
    for _, row in grouped.iterrows():
        lines.append(
            f"- {row['description']}:  {row['value']:.2f} (expected range: {row['lower']}-{row['upper']})\n"
        )
        if row['value'] < row['lower']:
            lines.append(
                f" <!here> :warning: {row['description']} value is below lower bound of {row['lower']:.2f}\n"
            )
        if row['value'] > row['upper']:
            lines.append(
                f" <!here> :warning: {row['description']} value is above upper bound of {row['upper']:.2f}\n"
            )
    payload['blocks'].append(
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}
    )

    return (headers, payload)


def send_slack_message(headers, payload):
    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage", headers=headers, json=payload
        )
        resp.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(f"slack request failed with error: {err}")
    else:
        data = resp.json()
        if not data.get("ok"):
            raise RuntimeError(f"slack message sending failed with error: {data}")
        print("Message sent, ts:", data.get("ts"))


def report_measurement(metrics_data, name, metrics):
    if metrics_data is not None:
        headers, payload = create_slack_message_request(metrics_data, name, metrics)
        if slack_api_key:
            send_slack_message(headers, payload)
        else:
            print(f"payload: {payload}")
        datadog_headers, datadog_payload = create_datadog_metric_request(metrics_data)
        if datadog_api_key:
            send_datadog_metric(datadog_headers, datadog_payload)
        else:
            print(f"datadog_payload: {datadog_payload}")


def report_for(stage_name, measurements, metrics, today):
    for file_pattern, measurement_name in measurements.items():
        print(f"path: {report_path}")
        print(f"file_pattern: {file_pattern}")
        metrics_data = collect_metrics(stage_name, report_path, file_pattern, today)
        report_measurement(metrics_data, measurement_name, metrics)


if __name__ == "__main__":
    datadog_api_key = os.environ.get("LAPI_DATADOG_API_KEY")
    slack_channel_id = os.environ.get("LAPI_PERF_TEST_SLACK_CHANNEL_ID")
    slack_api_key = os.environ.get("LAPI_SLACK_API_KEY")
    if not slack_channel_id:
        slack_channel_id = 'C0BN6DB9T1C'  # tmp_lapi_performance

    # script arguments config.json, today, optional stage name
    if len(sys.argv) < 4:
        print(
            "Usage: lapi-perf-test-metric.py <config.json> <today: YYYY-MM-DD> <now: HH-MM-SS> [<stage_name>]"
        )
    elif len(sys.argv) > 2:
        config_file_path = sys.argv[1]
        today = sys.argv[2]
        now = sys.argv[3]
        if not os.path.isfile(config_file_path):
            raise AssertionError(f"Config file {config_file_path} does not exist.")
        if not bool(re.match(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$", today)):
            raise AssertionError(
                f"Invalid date format for today: {today}. Expected format: YYYY-MM-DD."
            )
        with open(config_file_path, "r") as config_file:
            try:
                config = json.load(config_file)
                report_path = os.path.join(config['report_path'], today, now)
                stages = {
                    stage['stage_name']: {stage['file_pattern']: stage['description']}
                    for stage in config['stages']
                    # in case we created a new db in this stage we don't report its metrics
                    # in the future we might want to separate these concerns in the config
                    if not stage['new_db']
                }
                if len(sys.argv) > 4 and sys.argv[4] in stages:
                    stage = sys.argv[4]
                    report_for(stage, stages[stage], config['metrics'], today)
                else:
                    for stage_name, stage in stages.items():
                        print(f" stage: {stage}")
                        report_for(stage_name, stage, config['metrics'], today)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON config file: {e}")
