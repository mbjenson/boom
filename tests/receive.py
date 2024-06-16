#!/usr/bin/env python
import os
import sys
import pika

host = os.getenv('RABBITMQ_HOST', 'localhost')
port = os.getenv('RABBITMQ_PORT', 5672)

def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=host,
            port=port
        )
    )
    channel = connection.channel()

    channel.queue_declare(queue='hello')

    def callback(ch, method, properties, body):
        print(f" [x] Received {body}")
        raise SystemExit

    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
