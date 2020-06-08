import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import pprint
from decimal import Decimal
import datetime as dt
import pandas as pd

def lambda_handler(event, context):
    d = (dt.datetime.now(dt.timezone.utc))

    ddb_resource = boto3.resource('dynamodb')

    # define tables
    tbl_btc = ddb_resource.Table('arbitrator-btc-hist-minutely')
    tbl_forex = ddb_resource.Table('arbitrator-forex-hist-halfhourly')
    tbl_target = ddb_resource.Table('arbitrator-aggregations')

    # define range
    cutoff_max = d.replace(minute=0, second=0, microsecond=0)
    cutoff_min = cutoff_max.replace(minute=1) - dt.timedelta(hours=1)

    # Pull the last hour
    kraken = tbl_btc.query(
        KeyConditionExpression=Key('exchange').eq("kraken") & Key('timestamp_utc').between(int(cutoff_min.timestamp()), int(cutoff_max.timestamp())),
        ProjectionExpression='exchange, timestamp_utc, content.result_price_last'
        )
    luno = tbl_btc.query(
        KeyConditionExpression=Key('exchange').eq("luno") & Key('timestamp_utc').between(int(cutoff_min.timestamp()), int(cutoff_max.timestamp())),
        ProjectionExpression='exchange, timestamp_utc, content.result_price_last'
        )
    forex = tbl_forex.query(
        KeyConditionExpression=Key('source').eq("fixer") & Key('timestamp_utc').between(int(cutoff_min.replace(minute=0).timestamp()), int((cutoff_max - dt.timedelta(minutes=1)).timestamp()-60)),
        ProjectionExpression='exchange, timestamp_utc, content.rates.ZAR'
        )

    # conform the resompnses a bit
    kraken_eur = [{"timestamp": i["timestamp_utc"], "kraken_eur":i["content"]["result_price_last"]} for i in kraken["Items"]]
    luno_zar = [{"timestamp": i["timestamp_utc"], "luno":i["content"]["result_price_last"]} for i in luno["Items"]]
    forex_rate = [{"timestamp": i["timestamp_utc"] + 60, "rate":i["content"]["rates"]["ZAR"]} for i in forex["Items"]]

    # join into a pdf
    pdf_working = pd.DataFrame(kraken_eur)\
        .merge(pd.DataFrame(luno_zar), on="timestamp", how='left')\
        .merge(pd.DataFrame(forex_rate), on="timestamp", how='left').fillna(method='ffill')
    # calculations etc
    pdf_working = pdf_working.astype({"timestamp": "int", "luno":"float", "kraken_eur":"float", "rate":"float"})
    pdf_working["kraken"] = pdf_working["kraken_eur"] * pdf_working["rate"]
    pdf_working["timestamp_ms"] = min(pdf_working["timestamp"])*1000
    pdf_working["arb"] = (((pdf_working["luno"] - pdf_working["kraken"]) / pdf_working["kraken"]) * 100)

    pdf_working["kraken_min"] = pdf_working["kraken"].min()
    pdf_working["kraken_max"] = pdf_working["kraken"].max()
    pdf_working["luno_min"] = pdf_working["luno"].min()
    pdf_working["luno_max"] = pdf_working["luno"].max()
    pdf_working["arb_min"] = pdf_working["arb"].min()
    pdf_working["arb_max"] = pdf_working["arb"].max()

    # aggregation
    pdf_hourly = pdf_working.groupby(["timestamp_ms"]).mean().reset_index()
    cols_to_round = ["kraken_eur", "kraken", "luno", "arb", "rate", "kraken_min", "kraken_max", "luno_min", "luno_max", "arb_min", "arb_max"]
    pdf_hourly[cols_to_round] = pdf_hourly[cols_to_round].round(2)

    # prep for dynamodb
    series = pdf_hourly.to_dict(orient="records")
    series[0]["datetime_max"] = str(cutoff_max)

    series_json = json.dumps(series)
    series_dec = json.loads(series_json, parse_float=Decimal, parse_int=int)

    # check if there is hourly data in ddb
    prev_records = tbl_target.query(
        KeyConditionExpression=Key('range').eq("hourly") & Key('timestamp_utc').eq(int(cutoff_min.replace(minute=0).timestamp())),
        )

    # if there is then concatenate it, newest first
    if prev_records["Count"] > 0:
        series_dec = series_dec + prev_records["Items"][0]["series"][:24*7]

    # build the final payload
    hourly_update = {
        "range":"hourly",
        "timestamp_utc": int(cutoff_max.timestamp()),
        "series": series_dec
    }  

    # push
    tbl_target.put_item(
        Item=hourly_update
    )