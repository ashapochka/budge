from pprint import pprint

from invoke import task

import awsprice


@task
def download_offers(ctx, csv_directory='/tmp', overwrite=False, behead=False):
    offers = awsprice.download_offers(csv_directory, overwrite=overwrite)
    if behead:
        for offer_path in offers.values():
            awsprice.behead_offer(offer_path)
    pprint(offers)


@task
def behead_offer(ctx, offer_path):
    awsprice.behead_offer(offer_path)


@task
def offer_to_sqlite(ctx, offer_path, sqlite_path):
    awsprice.copy_offer_to_sql_table(offer_path, sqlite_path)


@task
def fetch_offers_to_database(ctx, sqlite_path,
                             csv_directory=None, overwrite=False):
    offers = awsprice.fetch_offers_to_database(sqlite_path,
                                               csv_directory=csv_directory,
                                               overwrite=overwrite)
    pprint(offers)
