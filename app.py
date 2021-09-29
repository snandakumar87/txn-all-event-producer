import argparse
import json
import logging
import os
import random
import time
import uuid

from kafka import KafkaProducer

EVENT_TEMPLATES = [

{
  "correlationId":"123321",
  "allAccounts":False,
  "entityId": "desk123",
  "entityType": "desk",
  "confidence":0.99,
  "accounts":[
    {
      "accountId":"123456",
      "holdings":[
        {
          "symbol":"IBM",
          "description":"IBM",
          "quantity":200
        },
        {
          "symbol":"GME",
          "description":"GameStop",
          "quantity":90
        },
        {
          "symbol":"AMZN",
          "description":"Amazon",
          "quantity":500
        }
      ]
    },
    {
      "accountId":"123457",
      "holdings":[
        {
          "symbol":"DAL",
          "description":"Delta Air Lines",
          "quantity":1570
        },
        {
          "symbol":"DIS",
          "description":"Disney",
          "quantity":134
        }
      ]
    },
    {
      "accountId":"123458",
      "holdings":[
        {
          "symbol":"GE",
          "description":"General Electric",
          "quantity":12032
        },
        {
          "symbol":"GME",
          "description":"GameStop",
          "quantity":955
        },
        {
          "symbol":"GOOG",
          "description":"Alphabet Inc.",
          "quantity":40
        }
      ]
    }
  ]
}

]


CUSTOMER = [


    'CUST8788'
]

def generate_event():
    ret = EVENT_TEMPLATES[0]
    return ret




def main(args):
    logging.info('brokers={}'.format(args.brokers))
    logging.info('topic={}'.format(args.topic))
    logging.info('rate={}'.format(args.rate))

    logging.info('creating kafka producer')
    producer = KafkaProducer(bootstrap_servers=args.brokers)

    logging.info('begin sending events')
    while True:

        producer.send(args.topic,json.dumps(generate_event()).encode() , '123321'.encode())
        time.sleep(1000.0)
    logging.info('end sending events')




def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, '') is not '' else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.topic = get_arg('KAFKA_TOPIC', args.topic)
    args.rate = get_arg('RATE', args.rate)
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info('starting kafka-openshift-python emitter')
    parser = argparse.ArgumentParser(description='emit some stuff on kafka')
    parser.add_argument(
        '--brokers',
        help='The bootstrap servers, env variable KAFKA_BROKERS',
        default='my-cluster-kafka-brokers:9092')
    parser.add_argument(
        '--topic',
        help='Topic to publish to, env variable KAFKA_TOPIC',
        default='var-calc-request')
    parser.add_argument(
        '--rate',
        type=int,
        help='Lines per second, env variable RATE',
        default=1)
    args = parse_args(parser)
    main(args)
    logging.info('exiting')
