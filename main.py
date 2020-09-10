from pytrends.request import TrendReq
from datetime import date, timedelta
from google.cloud import bigquery
import os


def get_trends_data(kw_list):
    # Get past day date
    pastday = date.today() - timedelta(days=3)

    pytrend = TrendReq()
    pytrend.build_payload(kw_list, timeframe='now 1-d')

    # Get interest by region from trends.google.com
    interest_by_region_df = pytrend.interest_by_region(inc_low_vol=True)

    # Rank search terms per geo
    ranked_interest_by_region_df = interest_by_region_df.rank(1, ascending=False, method='first')

    # Add date column
    ranked_interest_by_region_df.insert(0, 'date', pastday)

    # Write data to csv file
    ranked_interest_by_region_df.to_csv('keyword_list.csv', index=True)


get_trends_data(sorted(["vpn", "hack", "cyber", "stream", "torrent"]))


def upload_to_bigquery(project, dataset, table):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/ruta/Downloads/Homework Data-5572d3237302.json"

    client = bigquery.Client(project=project)

    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True

    with open('keyword_list.csv', "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()


upload_to_bigquery("homework-data2020", "data_engineer", "rm_data_engineer")
