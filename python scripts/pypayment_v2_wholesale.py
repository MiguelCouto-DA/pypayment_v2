## Importing all needed libraries and modules
## Setting up BigQuery access credentials for SQL parsing of reporting data
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import pandas as pd
import numpy as np
import os
import warnings
import datetime

pd.set_option('display.max_columns', None)
warnings.filterwarnings('ignore')

credentials = service_account.Credentials.from_service_account_file(
    "/Users/miguelcouto/Downloads/zattoo-dataeng-e5f45785174f.json"
)

project_id = "zattoo-dataeng"
client = bigquery.Client(credentials=credentials, project=project_id)

## Prepare dataframe
reporting_df = pd.read_csv('/Users/miguelcouto/PycharmProjects/pypayment_v2/raw/wholesale/2022-03_wholesale.csv')

reporting_month = reporting_df['reporting_month'].unique()

reporting_df['buyer_country_code'] = reporting_df['country_code']

reporting_df.replace(np.nan, 0, inplace=True)

## Reorder dataframe
reporting_df = reporting_df[
    [
        'transaction_id',
        'transaction_date',
        'term_end',
        'reporting_month',
        'country_name',
        'country_code',
        'buyer_country_code',
        'currency',
        'sku',
        'subscription_status',
        'type_of_transaction',
        'payment_method',
        'product_class',
        'product_group_finance',
        'product_length',
        'product_length_months',
        'product_term_length',
        'domestic_abroad',
        'vat_percentage',
        'exchange_rate_eur_to_chf',
        'store_fees_eur',
        'store_fees_chf',
        'store_fees_percentage',
        'units',
        'charge_eur',
        'sales_price_eur',
        'fee_eur',
        'vat_eur',
        'new_booking_net_eur',
        'renewal_booking_net_eur',
        'payout_eur',
        'total_revenue_net_eur',
        'charge_chf',
        'sales_price_chf',
        'fee_chf',
        'vat_chf',
        'new_booking_net_chf',
        'renewal_booking_net_chf',
        'payout_chf',
        'total_revenue_net_chf',
        'revenue_month_number',
        'revenue_month_date',
        'active_sub_month_end',
        'active_sub_content'
    ]
]

## Define BQ table schema
bq_schema = [
    {"name": "transaction_id", "type": "STRING"},
    {"name": "transaction_date", "type": "TIMESTAMP"},
    {"name": "term_end", "type": "TIMESTAMP"},
    {"name": "reporting_month", "type": "STRING"},
    {"name": "country_name", "type": "STRING"},
    {"name": "country_code", "type": "STRING"},
    {"name": "buyer_country_code", "type": "STRING"},
    {"name": "currency", "type": "STRING"},
    {"name": "sku", "type": "STRING"},
    {"name": "subscription_status", "type": "STRING"},
    {"name": "type_of_transaction", "type": "STRING"},
    {"name": "payment_method", "type": "STRING"},
    {"name": "product_class", "type": "STRING"},
    {"name": "product_group_finance", "type": "STRING"},
    {"name": "product_length", "type": "INTEGER"},
    {"name": "product_length_months", "type": "INTEGER"},
    {"name": "product_term_length", "type": "INTEGER"},
    {"name": "domestic_abroad", "type": "STRING"},
    {"name": "vat_percentage", "type": "FLOAT"},
    {"name": "exchange_rate_eur_to_chf", "type": "FLOAT"},
    {"name": "store_fees_eur", "type": "INTEGER"},
    {"name": "store_fees_chf", "type": "INTEGER"},
    {"name": "store_fees_percentage", "type": "INTEGER"},
    {"name": "units", "type": "INTEGER"},
    {"name": "charge_eur", "type": "FLOAT"},
    {"name": "sales_price_eur", "type": "FLOAT"},
    {"name": "fee_eur", "type": "FLOAT"},
    {"name": "vat_eur", "type": "FLOAT"},
    {"name": "new_booking_net_eur", "type": "FLOAT"},
    {"name": "renewal_booking_net_eur", "type": "FLOAT"},
    {"name": "payout_eur", "type": "FLOAT"},
    {"name": "total_revenue_net_eur", "type": "FLOAT"},
    {"name": "charge_chf", "type": "FLOAT"},
    {"name": "sales_price_chf", "type": "FLOAT"},
    {"name": "fee_chf", "type": "FLOAT"},
    {"name": "vat_chf", "type": "FLOAT"},
    {"name": "new_booking_net_chf", "type": "FLOAT"},
    {"name": "renewal_booking_net_chf", "type": "FLOAT"},
    {"name": "payout_chf", "type": "FLOAT"},
    {"name": "total_revenue_net_chf", "type": "FLOAT"},
    {"name": "revenue_month_number", "type": "INTEGER"},
    {"name": "revenue_month_date", "type": "TIMESTAMP"},
    {"name": "active_sub_month_end", "type": "INTEGER"},
    {"name": "active_sub_content", "type": "INTEGER"}
]

## Export to BQ table
pandas_gbq.to_gbq(
    dataframe=reporting_df,
    destination_table=f"finance.subs_reporting_wholesale",
    project_id="zattoo-dataeng",
    if_exists="append",
    progress_bar=None,
    table_schema=bq_schema,
)
