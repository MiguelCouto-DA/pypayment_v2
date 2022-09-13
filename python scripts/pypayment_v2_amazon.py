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
## Amazon data is very poor and will require a lot of transformations and the code will include Finance logic
df = pd.read_csv('/Users/miguelcouto/PycharmProjects/pypayment_v2/raw/amazon/2022-03_amazon.csv')

## I've manually renamed zattoo_amazon_firetv_hiq_german_1mo_90day_freetrial to zattoo_amazon_firetv_hiq_german_90days_freetrial. The objective is to be able to
## join with b2c_middleware_import.payment_amazontransaction_view and enrich the original Amazon sales report
df['Vendor SKU'].replace(
    {'zattoo_amazon_firetv_hiq_german_1mo_90day_freetrial': 'zattoo_amazon_firetv_hiq_german_90days_freetrial'},
    inplace=True)

## Renaming and reordering the dataframe so it keeps consistency with the reports from the other app shops
reporting_df = df[
    ['Transaction ID', 'Transaction Time', 'Transaction Type', 'Country/Region Code', 'Vendor SKU',
     'In-App Subscription Term',
     'In-App Subscription Status (Trial / Paid)', 'Sales Price (Marketplace Currency)',
     'Estimated Earnings (Marketplace Currency)',
     'Units']]

reporting_df.rename({'Transaction ID': 'transaction_id',
                     'Transaction Time': 'transaction_time',
                     'Transaction Type': 'type_of_transaction',
                     'Country/Region Code': 'country_code',
                     'Vendor SKU': 'vendor_sku',
                     'In-App Subscription Term': 'subscription_term',
                     'In-App Subscription Status (Trial / Paid)': 'subscription_status',
                     'Sales Price (Marketplace Currency)': 'sales_price',
                     'Estimated Earnings (Marketplace Currency)': 'earnings',
                     'Units': 'units',
                     },
                    axis=1, inplace=True)

reporting_df['subscription_status'] = reporting_df['subscription_status'].str.lower()
reporting_df['type_of_transaction'] = reporting_df['type_of_transaction'].str.lower()
reporting_df['buyer_country_code'] = reporting_df['country_code']

## Filtering out invalid transactions
reporting_df = reporting_df[pd.isna(reporting_df['subscription_status']) == False]

## Cleaning up country names based on codes
reporting_df['country_name'] = reporting_df['country_code'].replace(
    'DE', 'Germany').replace('CH', 'Switzerland').replace('AT', 'Austria')

reporting_df['country_name'].replace(
    'Switzerland', 'Germany', inplace=True)

## Converting date columns to datetime
reporting_df['transaction_date'] = pd.to_datetime(reporting_df['transaction_time'].str[:-4], format='%Y-%m-%d %H:%M:%S')
reporting_df.drop('transaction_time', axis=1, inplace=True)

## Finance Subs reporting will happen once at end of each month and for that the reporting_month will be defined based on the mode of the transaction_date.
## For amazon we report with 1month delay, therefore reporting_month should be month + 1
reporting_df['reporting_month'] = reporting_df['transaction_date'].dt.to_period('M').dt.strftime('%Y-%m')
reporting_month = reporting_df.reporting_month.mode()
reporting_df['reporting_month'] = reporting_df['reporting_month'].apply(lambda x: reporting_month)

reporting_df['reporting_month'] = pd.to_datetime(reporting_df['reporting_month'])
reporting_df['reporting_month'] = (reporting_df['reporting_month'] + pd.offsets.MonthBegin(1)).dt.strftime('%Y-%m')
reporting_month = reporting_df.reporting_month.mode()

## Adding payment_method column
reporting_df['payment_method'] = 'amazon'

## Cleaning up currencies based on country
reporting_df['currency'] = reporting_df['country_name'].apply(lambda x: 'CHF' if x == 'Switzerland' else 'EUR')

## SQL query to enrich the dataframe with more specific product data (term, sku, productid)
## Note: termsku == 'Vendor SKU' in Amazon's sales report
payment_amazon = """
select distinct
       pav.termsku,
       pav.term,
       pav.productid
from b2c_middleware_import.payment_amazontransaction_view pav
"""

df_skus = pandas_gbq.read_gbq(payment_amazon, project_id=project_id, progress_bar_type=None)

## Merging df_skus to reporting_df to get enriched data
reporting_df = reporting_df[
    ['transaction_id', 'country_name', 'country_code', 'buyer_country_code', 'vendor_sku', 'subscription_status', 'type_of_transaction',
     'sales_price',
     'earnings', 'units',
     'transaction_date',
     'reporting_month',
     'payment_method', 'currency']].merge(df_skus[['termsku', 'productid', 'term']], how='left',
                                          left_on='vendor_sku', right_on='termsku')

reporting_df.rename({'productid': 'sku'}, axis=1, inplace=True)

reporting_df['sku'][reporting_df['sku'].isnull()] = reporting_df['vendor_sku'][reporting_df['sku'].isnull()]
reporting_df.drop(['termsku'], axis=1, inplace=True)

## Some SKUs do not have 'term' data, therefore it needs to be manually written
reporting_df.term.fillna('1 Month', inplace=True)

## SQL query to enrich the dataframe with more specific product data (class, detailed class, length)
skus_list = str(set(reporting_df['sku'].to_list()))

skus_expand = f"""select distinct rlv.SKU as sku,
                rlv.product_class,
                rlv.detailed_product_class,
                rlv.product_length
from b2c_middleware.reporting_layer_view rlv
where true
  and rlv.SKU in ({skus_list[1:-1]})
  and rlv.app_shop_id = 'amazon'
  and rlv.product_class is not null
  and rlv.detailed_product_class is not null
  ;"""

df_skus_expand = pandas_gbq.read_gbq(skus_expand, project_id=project_id, progress_bar_type=None)

## Merging df_skus_expand to reporting_df to get enriched data
reporting_df = reporting_df.merge(df_skus_expand, how='left',
                                  left_on='sku', right_on='sku')

## avg_price_sales_per_sub calculation
reporting_df['avg_price_sales_per_sub'] = reporting_df['sales_price'] / reporting_df['units']

## Replacing product_length_months string with integers
reporting_df['product_length_months'] = reporting_df['term'].replace(
    {'1 Month': 1, '2 Months': 2, '3 Months': 3, '6 Months': 6, '12 Months': 12, '1 Year': 12})

## To get more VAT data a temp_vat_df is created to later be joined with b2c_middleware_import.payment_exchangerate and enrich the dataframe
temp_vat_df = reporting_df[['transaction_id', 'country_code', 'sales_price', 'transaction_date', 'currency']]
temp_vat_df['sales_price_cents'] = temp_vat_df['sales_price'] * 100

## Define BQ table schema
bq_schema_vat = [
    {"name": "transaction_id", "type": "INTEGER"},
    {"name": "country_code", "type": "STRING"},
    {"name": "sales_price_cents", "type": "INTEGER"},
    {"name": "transaction_date", "type": "TIMESTAMP"},
    {"name": "currency", "type": "STRING"},
]

## Export to BQ table
pandas_gbq.to_gbq(
    dataframe=temp_vat_df,
    destination_table="temp.vat_amazon_pypayment_v2",
    project_id="zattoo-dataeng",
    if_exists="replace",
    progress_bar=None,
    table_schema=bq_schema_vat,
)

## SQL query to enrich the dataframe with more specific product data (vat_percentage, exchange_rate_eur_to_chf)
vat_expand = """
select transaction_id,
       transaction_date,
--        udf.vat_chf(pe.from_currency,
--                    vat.rate,
--                    pe.exchange_rate,
--                    pe.from_currency_quantity,
--                    py2.sales_price_cents
--            )
--                             AS vat_CHF,
--        udf.vat_eur(pe.from_currency,
--                    vat.rate,
--                    pe.exchange_rate,
--                    pe_eur.from_currency_quantity,
--                    pe_eur.exchange_rate,
--                    py2.sales_price_cents
--            )
--                             AS vat_EUR,
       vat.rate / 100       AS vat_percentage,
       pe_eur.exchange_rate AS exchange_rate_eur_to_chf
FROM temp.vat_amazon_pypayment_v2 py2
         LEFT JOIN b2c_middleware_import.payment_exchangerate pe
                   ON pe.from_currency = py2.currency
                       AND DATE_TRUNC(pe.day, MONTH) =
                           DATE_TRUNC(DATE(py2.transaction_date), MONTH)
         LEFT JOIN b2c_middleware_import.payment_exchangerate pe_eur
                   ON pe_eur.from_currency = 'EUR'
                       AND DATE_TRUNC(pe_eur.day, MONTH) =
                           DATE_TRUNC(DATE(py2.transaction_date), MONTH)
         LEFT JOIN b2c_middleware_import.payment_vat_view vat
                   ON vat.country = py2.country_code
                       AND vat.created_at_date = DATE(py2.transaction_date)
where true
  AND pe.day >= DATE('2008-08-01')
  AND pe_eur.day >= DATE('2008-08-01')
  AND vat.created_at_date >= DATE('2008-08-01')
;
"""

df_vat_expand = pandas_gbq.read_gbq(vat_expand, project_id=project_id, progress_bar_type=None)

## Merging df_vat_expand to reporting_df to get enriched VAT data
reporting_df = reporting_df.merge(
    df_vat_expand[['transaction_date', 'transaction_id', 'vat_percentage', 'exchange_rate_eur_to_chf']], how='left',
    left_on=['transaction_id', pd.to_datetime(reporting_df['transaction_date'], utc=True)],
    right_on=['transaction_id',
              pd.to_datetime(df_vat_expand['transaction_date'], utc=True)]).drop(
    ['transaction_date_y', 'key_1'], axis=1)

reporting_df.rename({'transaction_date_x': 'transaction_date',
                     },
                    axis=1, inplace=True)

## We don't book VAT for Amazon, because they do it. If we do include VAT though, that could lead to issues. Keep the columns, but set all VAT values to zero
## (vat_percent, vat_eur, vat_chf)
reporting_df[['vat_percentage', 'vat_eur', 'vat_chf']] = 0

## Amazon's "Sales price" is OUR "Charge"
reporting_df['charge_eur'] = reporting_df['sales_price']

## Adding more manual fields
reporting_df['store_fees_percentage'] = 15
reporting_df['domestic_abroad'] = 'domestic'

## And extra calculations
reporting_df['sales_price_eur'] = round(reporting_df['charge_eur'] + (
        reporting_df['charge_eur'] * (reporting_df['vat_percentage'] / 100)), 2)
reporting_df['fee_eur'] = reporting_df['sales_price'] - abs(reporting_df['earnings'])

## Luckily our net booking is earnings in Amazon's sales report
reporting_df['new_booking_net_eur'] = reporting_df['earnings']
reporting_df['payout_eur'] = reporting_df['sales_price_eur'] - reporting_df['fee_eur']

## Decided to add a more detailed product that also aligns with the Finance product class
reporting_df['detailed_product_class'] = reporting_df["detailed_product_class"].fillna(reporting_df["product_class"])
reporting_df['detailed_product_class'].replace({'base_hiq': 'premium',
                                                'base_ultimate': 'ultimate'}, inplace=True)

reporting_df.drop('product_class', inplace=True, axis=1)
reporting_df.rename({'detailed_product_class': 'product_class'
                     },
                    axis=1, inplace=True)

## Cleaning up a bit
reporting_df.replace(np.inf, 0, inplace=True)
reporting_df.replace(-np.inf, 0, inplace=True)

## And taking care of inverting the values for refunds and chargebacks
reporting_df_negs = reporting_df[reporting_df['type_of_transaction'] != 'charge']

reporting_df.drop(reporting_df[reporting_df['type_of_transaction'] != 'charge'].index, inplace=True)

cols = ['sales_price_eur', 'sales_price', 'units', 'avg_price_sales_per_sub', 'charge_eur', 'fee_eur', 'vat_eur',
        'payout_eur']

## Exactly here is where the negative magic happens
reporting_df_negs[cols] = - reporting_df_negs[cols]

reporting_df = pd.concat([reporting_df, reporting_df_negs])

## Product lengths also need to be manually treated as there's not enough detailed data in the reports
reporting_df['product_length'] = pd.np.where(
    reporting_df['vendor_sku'].str.contains("1mo"), 31, pd.np.where(reporting_df[
                                                                        'vendor_sku'] == "zattoo_amazon_firetv_hiq_german_freetrial_2mo",
                                                                    31,
                                                                    pd.np.where(
                                                                        reporting_df['vendor_sku'].str.contains("_2mo"),
                                                                        62,
                                                                        pd.np.where(
                                                                            reporting_df['vendor_sku'].str.contains(
                                                                                "3mo"),
                                                                            90, pd.np.where(
                                                                                reporting_df['vendor_sku'].str.contains(
                                                                                    "12mo"), 365,
                                                                                pd.np.where(reporting_df[
                                                                                                'vendor_sku'] == "zattoo_amazon_firetv_hiq_german_90days_freetrial",
                                                                                            31,
                                                                                            reporting_df[
                                                                                                'product_length']))))))

## Adding artificially created term_end_date based on initial transaction_date
reporting_df['term_end'] = reporting_df['transaction_date'] + reporting_df['product_length'].astype('timedelta64[D]')

## Calculate product_term_length_months
reporting_df["product_term_length_months"] = (
        (reporting_df["term_end"].dt.year - reporting_df["transaction_date"].dt.year) * 12
        + (reporting_df["term_end"].dt.month - reporting_df["transaction_date"].dt.month)
        + 1
)

## A bit of housekeeping
reporting_df["product_term_length_months"][reporting_df["product_term_length_months"] < 0] = 0

## Replacing product_term_length_months for exceptions where value is 2 instead of 1
shorter_subs = (reporting_df['term_end'] - reporting_df['transaction_date']).dt.days <= 30
shorter_subs_replacer = reporting_df[shorter_subs][
    (reporting_df["product_term_length_months"] == 2) & ((reporting_df['term_end']).dt.day == 1)].index.to_list()
reporting_df["product_term_length_months"].loc[shorter_subs_replacer] = 1

## Adding revenue_month_number - this will help in further calculations
reporting_df = reporting_df.loc[reporting_df.index.repeat(reporting_df['product_term_length_months'])].reset_index(
    drop=True)
reporting_df['revenue_month_number'] = 1
reporting_df['revenue_month_number'] = reporting_df.groupby(["transaction_id", 'type_of_transaction'])[
    'revenue_month_number'].cumsum()

## Adding max_month_date to tackle the specifications of active_sub_month_end
reporting_df['max_month_date'] = reporting_df.groupby([reporting_df['term_end'].dt.to_period('M'), 'type_of_transaction'])[
    'term_end'].transform('max')
reporting_df['max_month_date'] = reporting_df['max_month_date'].dt.normalize() + pd.Timedelta('23:59:59')

## Get last indices of each transaction_id group
last_idxs_charges = (
        len(reporting_df[reporting_df['type_of_transaction'] == 'charge'])
        - np.unique(
    reporting_df['transaction_id'][reporting_df['type_of_transaction'] == 'charge'].values[::-1],
    return_index=1,
)[1]
        - 1
)

## Add revenue_month_date
reporting_df["revenue_month_date"] = reporting_df["transaction_date"].to_numpy().astype("datetime64[M]")
reporting_df["revenue_month_date"][reporting_df["type_of_transaction"] == "refund"] = (
    reporting_df["transaction_date"].to_numpy().astype("datetime64[M]")
)

reporting_df["revenue_month_date"] = reporting_df.apply(
    lambda x: x["revenue_month_date"]
              + pd.offsets.MonthEnd(x["revenue_month_number"])
              + pd.offsets.MonthBegin(-1),
    axis=1,
)

## Add product_term_length
reporting_df["product_term_length"] = reporting_df["revenue_month_date"].apply(
    lambda t: pd.Period(t, freq="S").days_in_month
)

reporting_df["product_term_length"][reporting_df["transaction_date"] > reporting_df["revenue_month_date"]] = (
                                                                                                                     reporting_df[
                                                                                                                         "transaction_date"].dt.daysinmonth -
                                                                                                                     reporting_df[
                                                                                                                         "transaction_date"].dt.day
                                                                                                             ) + 1

## Fix last position of product_term_length per transaction_id for charges
reporting_df["product_term_length"].iloc[last_idxs_charges] = (
        reporting_df["term_end"].iloc[last_idxs_charges] -
        reporting_df["revenue_month_date"].iloc[last_idxs_charges]
).dt.days

reporting_df = reporting_df[reporting_df['product_term_length'] > 0]

last_idxs = (
        len(reporting_df)
        - np.unique(
    reporting_df['transaction_id'].values[::-1],
    return_index=1,
)[1]
        - 1
)

reporting_df["product_term_length"].iloc[last_idxs] = (
        reporting_df["term_end"].iloc[last_idxs] -
        reporting_df["revenue_month_date"].iloc[last_idxs]
).dt.days

## Set active_sub_month_end = 0 by default
reporting_df['active_sub_month_end'] = 0
reporting_df['active_sub_month_end'][
    (reporting_df['term_end'] > (reporting_df['revenue_month_date'] + pd.offsets.MonthBegin(1)))] = 1
reporting_df["active_sub_month_end"][reporting_df["type_of_transaction"] == "trial"] = 0

## Mark all refund transactions as 'active_sub_month_end'] * -1
reporting_df["active_sub_month_end"][reporting_df["type_of_transaction"] != "charge"] = reporting_df[
                                                                                            'active_sub_month_end'] * -1

reporting_df.reset_index(drop=True, inplace=True)

## active_sub_content follows the same logic as active_sub_month_end except it doesn't count the last month
reporting_df["active_sub_content"] = reporting_df.active_sub_month_end

reporting_df["active_sub_content"][(reporting_df["revenue_month_number"] > reporting_df['product_length_months'])] = 0

reporting_df["active_sub_content"][reporting_df["type_of_transaction"] == "trial"] = 0

## Total_days of product_term_length per transaction_id
reporting_df["total_days"] = reporting_df.groupby(["transaction_id", 'type_of_transaction'])[
    "product_term_length"].transform("sum")

## Amazon is reported with one month delay, therefore a quick fix of adding 1 month to all dates in revenue_month_date. This would shift everything by exactly one month and more than it has to be
reporting_df["revenue_month_date"] = reporting_df["revenue_month_date"] + pd.offsets.MonthBegin(1)

## Adding product_term_length_months
reporting_df['product_term_length_months'] = reporting_df.groupby(["transaction_id", 'type_of_transaction'])[
    'revenue_month_number'].transform('max')

## Calculate total_revenue_net fields
reporting_df["total_revenue_net_eur"] = (
        reporting_df["new_booking_net_eur"] / reporting_df["total_days"] * reporting_df["product_term_length"]
)

## Calculating CHF columns
reporting_df['charge_chf'] = reporting_df['charge_eur'] * reporting_df['exchange_rate_eur_to_chf']
reporting_df['sales_price_chf'] = reporting_df['sales_price_eur'] * reporting_df['exchange_rate_eur_to_chf']
reporting_df['fee_chf'] = reporting_df['fee_eur'] * reporting_df['exchange_rate_eur_to_chf']
reporting_df['new_booking_net_chf'] = reporting_df['new_booking_net_eur'] * reporting_df['exchange_rate_eur_to_chf']
reporting_df['payout_chf'] = reporting_df['payout_eur'] * reporting_df['exchange_rate_eur_to_chf']
reporting_df['payout_chf'] = reporting_df['payout_eur'] * reporting_df['exchange_rate_eur_to_chf']
reporting_df["total_revenue_net_chf"] = (
        reporting_df["new_booking_net_chf"] / reporting_df["total_days"] * reporting_df["product_term_length"]
)

## Remove values from all lines of cols group except first
one_line_cols = ["vat_eur", 'vat_chf', 'payout_eur', 'payout_chf', 'sales_price_eur', 'sales_price_chf', 'fee_eur',
                 'fee_chf', 'new_booking_net_eur',
                 'new_booking_net_chf', 'renewal_booking_net_eur',
                 'renewal_booking_net_chf', 'store_fees_eur', 'store_fees_chf',
                 'charge_eur', 'charge_chf',
                 'vat_percentage',
                 'avg_price_sales_per_sub',
                 'units'
                 ]

for col in one_line_cols:
    reporting_df.loc[
        reporting_df["revenue_month_number"] > 1,
        [col],
    ] = 0.0

## Standardizing the report
reporting_df[['product_group_finance']] = ''

reporting_df[
    ['store_fees_eur', 'store_fees_percentage', 'store_fees_percentage', 'store_fees_chf', 'renewal_booking_net_eur',
     'renewal_booking_net_chf']] = 0

## Final clean up
reporting_df['revenue_month_date'] = pd.to_datetime(reporting_df['revenue_month_date'])

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
    destination_table=f"finance.subs_reporting_amazon",
    project_id="zattoo-dataeng",
    if_exists="append",
    progress_bar=None,
    table_schema=bq_schema,
)
