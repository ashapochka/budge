import os

import logging

import sqlite3
import tempfile

import odo
import requests
import wget


def aws_url(path):
    base_url = 'https://pricing.us-east-1.amazonaws.com'
    return '{}{}'.format(base_url, path)


def download_offers(csv_directory, overwrite=False):
    r = requests.get(aws_url('/offers/v1.0/aws/index.json'))
    offers_index = r.json()
    local_offers = {}
    for offer in offers_index['offers'].values():
        offer_code = offer['offerCode']
        offer_json_path = offer['currentVersionUrl']
        offer_csv_path = '.csv'.join(offer_json_path.rsplit('.json', 1))
        offer_url = aws_url(offer_csv_path)
        local_path = os.path.join(csv_directory, '{}.csv'.format(offer_code))
        local_offers[offer_code] = local_path
        if not overwrite and os.path.exists(local_path):
            continue
        else:
            try:
                wget.download(offer_url, local_path)
            except:
                logging.exception(
                    "failed to download {} to {}".format(
                        offer_url, local_path))
    return local_offers


def behead_offer(offer_path):
    with open(offer_path, 'r') as source_file:
        lines = source_file.readlines()
    lines[5] = ','.join((title.replace(' ', '_')
                         for title in lines[5].split(',')))
    with open(offer_path, 'w') as target_file:
        target_file.writelines(lines[5:])


def copy_offer_to_sqlite(offer_path, sqlite_path):
    table_name = os.path.basename(offer_path).replace('.csv', '')
    try:
        with sqlite3.connect(sqlite_path) as connection:
            cur = connection.cursor()
            cur.execute('DELETE FROM {}'.format(table_name))
    except sqlite3.DatabaseError:
        logging.exception(
            'failed to truncate table {}, likely it does not exist yet'.format(
                table_name))
    dshape = odo.discover(odo.resource(offer_path))
    odo.odo(offer_path, 'sqlite:///{}::{}'.format(
        sqlite_path, table_name),
            dshape=dshape)


def fetch_offers_to_database(sqlite_path, csv_directory=None, overwrite=False):
    def run_steps(directory):
        local_offers = download_offers(directory, overwrite=overwrite)
        for offer_path in local_offers.values():
            behead_offer(offer_path)
            copy_offer_to_sqlite(offer_path, sqlite_path)
        return local_offers

    if not csv_directory:
        with tempfile.TemporaryDirectory() as tmpdirname:
            offers = run_steps(tmpdirname)
    elif os.path.isdir(csv_directory):
        os.mkdir(csv_directory)
        offers = run_steps(csv_directory)
    else:
        offers = run_steps(csv_directory)
    return offers
