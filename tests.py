from pprint import pprint

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table

from awsprice import copy_offer_to_sql_table


def test_read_sql():
    engine = create_engine('sqlite:///local-data/budge.sqlite')
    ec2_offers = pd.read_sql('AmazonEC2', engine, coerce_float=False,
                             parse_dates={'EffectiveDate': '%Y-%m-%d'})
    print(ec2_offers.EffectiveDate.dtype)


def test_copy_offer_to_sqlite():
    copy_offer_to_sql_table('/Users/ashapoch/Temp/aws-offers/AmazonSES.csv', 'local-data/budge.sqlite')


def test_drop_table():
    engine = create_engine('sqlite:///local-data/budge.sqlite')
    engine.connect()
    metadata = MetaData(bind=engine)
    ec2 = Table('AmazonEC2', metadata, autoload=False)
    if ec2.exists():
        ec2.drop()


def test_schema():
    engine = create_engine('sqlite:///local-data/budge.sqlite')
    metadata = MetaData(bind=engine)
    metadata.reflect()
    pprint(metadata.tables['table_name'])
