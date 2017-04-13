import os
import re

import logging

import sqlite3
import tempfile
from collections import OrderedDict

import odo
import pandas as pd
import requests
import wget
from datashape import Record
from sqlalchemy import create_engine, MetaData


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
    header_line = 5
    if lines[0].startswith('"SKU","OfferTermCode","RateCode"'):
        header_line = 0
    lines[header_line] = ','.join((re.sub('[^0-9a-zA-Z\n"]+', '_', title)
                                   for title in lines[header_line].split(',')))
    with open(offer_path, 'w') as target_file:
        target_file.writelines(lines[header_line:])


def prepare_for_copy(table_name, frame, metadata):
    shape = odo.discover(frame)
    od = OrderedDict(shape.measure.fields)
    for key in od:
        type_name = str(od[key])
        if not type_name.startswith('?'):
            od[key] = odo.dshape('?{}'.format(type_name))
    target_shape = odo.dshape([shape.parameters[0], Record(od)])
    target_table = odo.backends.sql.dshape_to_table(table_name, target_shape, metadata=metadata)
    return (target_shape, target_table)


def copy_offer_to_sql_table(offer_path, alchemy_metadata):
    table_name = os.path.basename(offer_path).replace('.csv', '')
    offer_frame = pd.read_csv(offer_path, low_memory=False)
    target_shape, target_table = prepare_for_copy(table_name,
                                                  offer_frame,
                                                  alchemy_metadata)
    if target_table.exists():
        target_table.drop()
    odo.odo(offer_frame, target_table, dshape=target_shape)


def fetch_offers_to_database(sqlite_path, csv_directory=None, overwrite=False):
    engine = create_engine('sqlite:///{}'.format(sqlite_path))
    metadata = MetaData(bind=engine)

    def run_steps(directory):
        local_offers = download_offers(directory, overwrite=overwrite)
        for offer_path in local_offers.values():
            behead_offer(offer_path)
            copy_offer_to_sql_table(offer_path, metadata)
        return local_offers

    if not csv_directory:
        with tempfile.TemporaryDirectory() as tmpdirname:
            offers = run_steps(tmpdirname)
    elif not os.path.isdir(csv_directory):
        os.mkdir(csv_directory)
        offers = run_steps(csv_directory)
    else:
        offers = run_steps(csv_directory)
    return offers
