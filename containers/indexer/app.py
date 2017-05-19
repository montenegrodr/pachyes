import argparse
import os
import csv
from elasticsearch import Elasticsearch, helpers, exceptions

try:
    es = Elasticsearch(['192.168.0.26:9200'])
except exceptions.ConnectionError:
    raise Exception('Could not connect to 192.168.0.26:9200')

def main(args):
    for i in os.listdir(args.input):
        with open(os.path.join(args.input, i)) as f:
            it = csv.DictReader(f)
            helpers.bulk(
                client=es,
                actions=it,
                index='dummy_index',
                doc_type='person'
            )

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    return parser.parse_args()

if __name__ == '__main__':
    main(parse_args())
