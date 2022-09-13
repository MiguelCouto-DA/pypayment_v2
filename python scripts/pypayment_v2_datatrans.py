## Importing all needed libraries and modules
## Setting up BigQuery access credentials for SQL parsing of reporting data
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import pandas as pd
import numpy as np
import os
import warnings

pd.set_option('display.max_columns', None)
warnings.filterwarnings('ignore')

credentials = service_account.Credentials.from_service_account_file(
    "/Users/miguelcouto/Downloads/zattoo-dataeng-e5f45785174f.json"
)

project_id = "zattoo-dataeng"
client = bigquery.Client(credentials=credentials, project=project_id)

## SQL query that will create the main reporting dataframe
sql_calcs = """
select mt.transaction_id,
       mt.zuid,
       mt.payment_method,
       mt.currency,
       mt.exchange_rate_eur_to_chf,
       mt.sku,
       product_offer_view.length as product_length,
       product_service_view.classification as product_class,
       mt.type_of_transaction,
       case
           when mt.country_name = 'Germany' then 'Germany'
           when mt.country_name = 'Austria' then 'Austria'
           else 'Switzerland' end                   as country_name,
       c.countrycode                                as country_code,
       mt.new_booking_net_chf,
       mt.renewal_booking_net_chf,
       mt.new_booking_net_eur,
       mt.renewal_booking_net_eur,
       mt.transaction_date,
       mt.term_start,
       mt.term_end,
       product_service_view.zuya_account_permission as product_group_finance,
       mt.vat_eur,
       mt.vat_chf,
       mt.vat_percentage,
       mt.store_fees_chf,
       mt.store_fees_eur
from b2c_middleware.middlelayer_transactions mt
         left join dim.countries c on c.name = mt.country_name
         LEFT JOIN b2c_middleware_import.product_product_view AS product_product_view
                   ON mt.sku = product_product_view.sku
                       AND DATE(mt.transaction_date) = product_product_view.inserted_at_date
         LEFT JOIN b2c_middleware_import.product_subscriptionproduct_view AS product_subscriptionproduct_view
                   ON product_product_view.id = product_subscriptionproduct_view.id
                       AND
                      product_product_view.inserted_at_date = product_subscriptionproduct_view.inserted_at_date
         LEFT JOIN b2c_middleware_import.product_offer_view AS product_offer_view
                   ON product_subscriptionproduct_view.offer_id = product_offer_view.id
                       AND product_subscriptionproduct_view.inserted_at_date = product_offer_view.inserted_at_date
         LEFT JOIN b2c_middleware_import.product_service_view AS product_service_view
                   ON product_offer_view.service_id = product_service_view.id
                       AND product_offer_view.inserted_at_date = product_service_view.inserted_at_date
where true
  and mt.app_shop_id = 'datatrans'
  and mt.transaction_date >= '2022-03-01 00:00:00'
  and mt.transaction_date <= '2022-03-31 23:59:59'
-- Due to discrepancies, we will NOT be including refunds for now in datatrans
 -- and mt.type_of_transaction != 'refund'
           """

## Prepare dataframe
df = pandas_gbq.read_gbq(sql_calcs, project_id=project_id, progress_bar_type=None)

## List of transaction types (new and renewal) to be included in the dataframe (as mentioned before refunds will NOT be included in datatrans for now)
type_trans_lst = ["new_sale",
                  "renewal",
                  "refund"
                  ]

## Parsing date columns and converting them to datetime
parse_dates = ["term_start", "term_end"]
reporting_df = df[df["type_of_transaction"].isin(type_trans_lst)].copy()

for date in parse_dates:
    reporting_df[date] = pd.to_datetime(reporting_df[date]).dt.tz_convert(None)

## Finance Subs reporting will happen once at end of each month and for that the reporting_month will be defined based on the mode of the transaction_date
reporting_month = reporting_df['transaction_date'].mode().dt.to_period('M').dt.strftime('%Y-%m')[0]
reporting_df['reporting_month'] = reporting_month

## Calculate total_booking_net_eur/chf columns
reporting_df["total_booking_net_chf"] = (
        reporting_df["new_booking_net_chf"] + reporting_df["renewal_booking_net_chf"]
)
reporting_df["total_booking_net_eur"] = (
        reporting_df["new_booking_net_eur"] + reporting_df["renewal_booking_net_eur"]
)

## Calculate product_length_months (the max number of months that each transaction_id is in - e.g., transaction_id 36385353 is a 1 month sub that started on
## 23.03.22 18:45, which means product_length_months = 2 (March and April))
reporting_df["product_length_months"] = (
        (reporting_df["term_end"].dt.year - reporting_df["term_start"].dt.year) * 12
        + (reporting_df["term_end"].dt.month - reporting_df["term_start"].dt.month)
        + 1
)

## Some transactions have a weird behavior (probably refunds issued manually by Support), therefore product_length_months needs to be cleaned
reporting_df["product_length_months"][reporting_df["product_length_months"] < 0] = 0

## Replacing product_length_months for exceptions where value is 2 instead of 1
shorter_subs = (reporting_df['term_end'] - reporting_df['term_start']).dt.days <= 30
shorter_subs_replacer = reporting_df[shorter_subs][
    (reporting_df["product_length_months"] == 2) & ((reporting_df['term_end']).dt.day == 1)].index.to_list()
reporting_df["product_length_months"].loc[shorter_subs_replacer] = 1

## Reseting index
reporting_df = reporting_df.reindex(reporting_df.index.repeat(reporting_df["product_length_months"]))

## Adding revenue_month_number (same logic as product_length_months but it iterates each month and adds +1)
reporting_df["revenue_month_number"] = reporting_df.groupby(["transaction_id"]).cumcount() + 1

## Add revenue_month_date (same as revenue_month_number but with date for month begin)
reporting_df["revenue_month_date"] = reporting_df["term_start"].to_numpy().astype("datetime64[M]")
reporting_df["revenue_month_date"][reporting_df["type_of_transaction"] == "refund"] = (
    reporting_df["transaction_date"].to_numpy().astype("datetime64[M]")
)

reporting_df["revenue_month_date"] = reporting_df.apply(
    lambda x: x["revenue_month_date"]
              + pd.offsets.MonthEnd(x["revenue_month_number"])
              + pd.offsets.MonthBegin(-1),
    axis=1,
)

reporting_df["product_term_length"] = reporting_df["revenue_month_date"].apply(
    lambda t: pd.Period(t, freq="S").days_in_month
)

reporting_df["product_term_length"][reporting_df["term_start"] > reporting_df["revenue_month_date"]] = (
                                                                                                               reporting_df[
                                                                                                                   "term_start"].dt.daysinmonth -
                                                                                                               reporting_df[
                                                                                                                   "term_start"].dt.day
                                                                                                       ) + 1

## Reseting index
reporting_df.reset_index(drop=True, inplace=True)

## Set active_sub_month_end = 1 by default
# reporting_df["active_sub_month_end"] = 1

## Get last indices of each transaction_id group
last_idxs = (
        len(reporting_df)
        - np.unique(
    reporting_df.transaction_id.values[::-1],
    return_index=1,
)[1]
        - 1
)

## Fix last position of product_term_length per transaction_id
reporting_df["product_term_length"].iloc[last_idxs] = (
        reporting_df["term_end"].iloc[last_idxs] -
        reporting_df["revenue_month_date"].iloc[last_idxs]
).dt.days

## Fix to tackle dynamic term_end for refunds (same as product_length_months)
reporting_df["product_term_length"][reporting_df["product_term_length"] < 0] = 0

## Total_days of product_term_length per transaction_id
reporting_df["total_days"] = reporting_df.groupby("transaction_id")["product_term_length"].transform(
    "sum"
)

## Marking 1 day transactions correctly
reporting_df['total_days'][reporting_df['sku'].str.contains('1day')] = 1
reporting_df['product_term_length'][reporting_df['sku'].str.contains('1day')] = 1

## Get 12mo subs that didn't stay for 12 months for posterior treatment of exception
reporting_df['max_revenue_month_number'] = reporting_df.groupby(['transaction_id'])['revenue_month_number'].transform(
    max)

## Calculate total_revenue_net fields
reporting_df["total_revenue_net_eur"] = (
        reporting_df["total_booking_net_eur"] / reporting_df["total_days"] * reporting_df["product_term_length"]
)

reporting_df["total_revenue_net_chf"] = (
        reporting_df["total_booking_net_chf"] / reporting_df["total_days"] * reporting_df["product_term_length"]
)

## Remove total_booking values from all lines of group except first
reporting_df.loc[
    reporting_df["revenue_month_number"] > 1,
    ["total_booking_net_chf", "total_booking_net_eur"],
] = 0.0

## Adding units
reporting_df['units'] = 1
reporting_df['units'][reporting_df['type_of_transaction'] == 'refund'] = -1

## Remove following values from all lines of group except first. This is done to avoid future aggregation issues
one_line_cols = ["vat_eur", 'vat_chf', 'payout_eur', 'payout_chf', 'sales_price_eur', 'sales_price_chf', 'fee_eur', 'fee_chf', 'new_booking_net_eur',
                 'new_booking_net_chf', 'renewal_booking_net_eur',
                 'renewal_booking_net_chf','store_fees_eur', 'store_fees_chf',
                 'charge_eur', 'charge_chf',
                 'vat_percentage',
                 'avg_price_sales_per_sub', 'units']

for col in one_line_cols:
    reporting_df.loc[
        reporting_df["revenue_month_number"] > 1,
        [col, "total_booking_net_eur"],
    ] = 0.0

## Prepare df_nocalcs for free trials and full discounts
reporting_df_nocalcs = df[~df["type_of_transaction"].isin(type_trans_lst)].copy()

for date in parse_dates:
    reporting_df_nocalcs[date] = pd.to_datetime(reporting_df_nocalcs[date]).dt.tz_convert(None)

## Adding logic for subscription_status and type_of_transaction
reporting_df['subscription_status'] = ['trial' if v == 'free_trial' else 'full discount' if v == 'full_discount' else 'paid' if v == 'refund' else 'paid' for v in reporting_df['type_of_transaction']]

reporting_df['type_of_transaction'] = ['charge' if v == 'renewal' else 'charge' if v == 'new_sale' else 'charge' if v == 'full_discount' else 'charge' if v == 'free_trial' else 'refund' for v in reporting_df['type_of_transaction']]

## Set active_sub_month_end = 0 by default
reporting_df['active_sub_month_end'] = 0
reporting_df['active_sub_month_end'][
    (reporting_df['term_end'] > (reporting_df['revenue_month_date'] + pd.offsets.MonthBegin(1)))] = 1
reporting_df["active_sub_month_end"][reporting_df["subscription_status"] != "paid"] = 0

## Mark all refund transactions as 'active_sub_month_end'] * -1
reporting_df["active_sub_month_end"][reporting_df["type_of_transaction"] == "refund"] = reporting_df[
                                                                                            'active_sub_month_end'] * -1

reporting_df.reset_index(drop=True, inplace=True)

## active_sub_content follows the same logic as active_sub_month_end except it doesn't count the last month
reporting_df["active_sub_content"] = reporting_df.active_sub_month_end

reporting_df["active_sub_content"][(reporting_df["revenue_month_number"] > reporting_df['product_length_months'])] = 0

## Total_days of product_term_length per transaction_id
reporting_df["total_days"] = reporting_df.groupby(["transaction_id", 'type_of_transaction'])[
    "product_term_length"].transform("sum")

## Extract list of transaction ids whose subscription float between one month and the other, and are 1, 3 or 7 days (== product_length < 31)
trx_lst_more_1month_subs = reporting_df['transaction_id'][
    (reporting_df["revenue_month_number"] > 1) & (reporting_df.sku.str.contains('day'))]

## This logic makes sure the days are allocated correctly for these very specific subscriptions
reporting_df['product_term_length'][
    (reporting_df.transaction_id.isin(trx_lst_more_1month_subs)) & (reporting_df.revenue_month_number == 1)] = - (
        reporting_df.term_start - reporting_df.revenue_month_date.shift(1)).dt.days

reporting_df['product_term_length'][
    (reporting_df.transaction_id.isin(trx_lst_more_1month_subs)) & (reporting_df.revenue_month_number > 1)] = (
        reporting_df.term_end - reporting_df.revenue_month_date).dt.days

## Fix those one line transactions that still have wrong product_term_length
trx_id_counts = reporting_df['transaction_id'].value_counts(sort=False)
check_length_mask = reporting_df[
    reporting_df['transaction_id'].isin(trx_id_counts.index[trx_id_counts == 1])].index.to_list()

reporting_df["product_term_length"].loc[check_length_mask] = reporting_df[
                                                                 'term_end'].dt.day - reporting_df.term_start.dt.day

## Calculate total_booking_net columns
reporting_df_nocalcs["total_booking_net_chf"] = (
        reporting_df_nocalcs["new_booking_net_chf"] + reporting_df_nocalcs["renewal_booking_net_chf"]
)
reporting_df_nocalcs["total_booking_net_eur"] = (
        reporting_df_nocalcs["new_booking_net_eur"] + reporting_df_nocalcs["renewal_booking_net_eur"]
)

## Calculating sales_prices
reporting_df['sales_price_eur'] = reporting_df['total_booking_net_eur'] * (
        1 + (reporting_df['vat_percentage'] / 100))
reporting_df['sales_price_chf'] = reporting_df['total_booking_net_chf'] * (
        1 + (reporting_df['vat_percentage'] / 100))

## Append dataframes
reporting_df = reporting_df.append(reporting_df_nocalcs).reset_index(drop=True)

## Convert revenue_month_date to date
reporting_df["revenue_month_date"] = pd.to_datetime(
    reporting_df["revenue_month_date"]
).dt.date

## Standardizing the report so it's in line with Amazon, Google and Apple
reporting_df['domestic_abroad'] = np.nan

reporting_df[['charge_chf', 'charge_eur', 'store_fees_percentage', 'payout_chf', 'fee_eur', 'fee_chf',
              'payout_eur']] = 0

## Final clean up
reporting_df['revenue_month_date'] = pd.to_datetime(reporting_df['revenue_month_date'])
reporting_df['buyer_country_code'] = reporting_df['country_code']

reporting_df['product_group_finance'] = reporting_df['product_group_finance'].replace(
    {'base_hiq': 'premium', 'base_ultimate': 'ultimate'})

reporting_df['reporting_month'] = reporting_df['reporting_month'].replace(
    {np.nan: reporting_month})

reporting_df['product_length_months'] = pd.np.where(reporting_df['product_length'] == 1, 0,
                                                 pd.np.where(reporting_df['product_length'] == 3, 0,
                                                             pd.np.where(reporting_df['product_length'] == 7, 0,
                                                                         pd.np.where(reporting_df['product_length'] == 31, 1,
                                                                                     pd.np.where(reporting_df['product_length'] == 62, 2,
                                                                                                 pd.np.where(reporting_df['product_length'] == 90, 3,
                                                                                                             pd.np.where(reporting_df['product_length'] == 92, 3,
                                                                                                                    pd.np.where(reporting_df['product_length'] == 365, 12,
                                                                                                                    pd.np.where(reporting_df['product_length'] == 366, 12,
                                                                                                                         1)))))))))

reporting_df["active_sub_content"][reporting_df["subscription_status"] == "trial"] = 0
reporting_df["active_sub_content"][reporting_df["type_of_transaction"] == "full discount"] = 0

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
    destination_table=f"finance.subs_reporting_datatrans",
    project_id="zattoo-dataeng",
    if_exists="append",
    progress_bar=None,
    table_schema=bq_schema,
)
