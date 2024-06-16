#!/usr/bin/env python
import io
import json
import os
import sys
from glob import glob

import fastavro
import pika

from utils import Mongo, alert_mongify, format_fp_hists

mongo = Mongo(db="alerts")


def process_alert(alert):
    candid, objectId = alert["candid"], alert["objectId"]
    print(f"candid: {candid} - objectId: {objectId}")

    alert, prv_candidates, fp_hists = alert_mongify(alert)
    mongo.insert_one("alerts_rabbitmq", alert)
    # prv_candidates: pop nulls - save space
    prv_candidates = [
        {kk: vv for kk, vv in prv_candidate.items() if vv is not None}
        for prv_candidate in prv_candidates
    ]

    # fp_hists: pop nulls - save space
    fp_hists = [
        {kk: vv for kk, vv in fp_hist.items() if vv not in [None, -99999, -99999.0]}
        for fp_hist in fp_hists
    ]

    # format fp_hists, add alert_mag, alert_ra, alert_dec
    # and computing the FP's mag, magerr, snr, limmag3sig, limmag5sig
    fp_hists = format_fp_hists(alert, fp_hists)
    if mongo.db["alerts_aux_rabbitmq"].count_documents({"_id": objectId}, limit=1) == 0:
        alert_aux = {
            "_id": objectId,
            "prv_candidates": prv_candidates,
            "fp_hists": fp_hists,
        }
        mongo.insert_one(collection="alerts_aux_rabbitmq", document=alert_aux)
    else:
        mongo.db["alerts_aux_rabbitmq"].update_one(
            filter={"_id": objectId},
            update={
                "$addToSet": {
                    "prv_candidates": {"$each": prv_candidates},
                    "fp_hists": {"$each": fp_hists},
                }
            },
            upsert=True,
        )

    count = mongo.db["alerts_rabbitmq"].count_documents({})
    return count


def read_schema_data(bytes_io):
    """Read data that already has an Avro schema.
                                                                                               :param bytes_io: `_io.BytesIO` Data to be decoded.
    :return: `dict` Decoded data.
    """
    bytes_io.seek(0)
    message = fastavro.reader(bytes_io)
    return message


def decode_message(msg):
    """
    Decode Avro message according to a schema.

    :param msg: The Kafka message result from consumer.poll()
    :return:
    """

    try:
        bytes_io = io.BytesIO(msg)
        decoded_msg = read_schema_data(bytes_io)
    except AssertionError:
        decoded_msg = None
    except IndexError:
        literal_msg = literal_eval(str(msg, encoding="utf-8"))  # works to give bytes
        bytes_io = io.BytesIO(literal_msg)  # works to give <class '_io.BytesIO'>
        decoded_msg = read_schema_data(bytes_io)  # yields reader
    except Exception as e:
        print(f"Exception decoding msg: {str(e)}")
        decoded_msg = msg
    finally:
        return decoded_msg


host = os.getenv("RABBITMQ_HOST", "localhost")
port = os.getenv("RABBITMQ_PORT", 5672)


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host, port=port)
    )
    channel = connection.channel()

    channel.queue_declare(queue="alerts", durable=True)

    def callback(ch, method, properties, body):
        decoded = decode_message(body)
        records = [r for r in decoded]
        for alert in records:
            count = process_alert(alert)

        if count == 24:
            raise SystemExit

    channel.basic_consume(queue="alerts", on_message_callback=callback, auto_ack=True)

    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    main()
