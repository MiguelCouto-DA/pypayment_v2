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
df = pd.read_csv('/Users/miguelcouto/PycharmProjects/pypayment_v2/raw/google/2022-08_google.csv')

## Renaming and reordering the dataframe so it keeps consistency with the reports from the other app shops
reporting_df = df[
    ['Description', 'Transaction Date', 'Transaction Time', 'Tax Type', 'Transaction Type',
     'Refund Type', 'Product Title', 'Sku Id', 'Buyer Country',
     'Buyer Currency', 'Amount (Buyer Currency)', 'Currency Conversion Rate',
     'Merchant Currency', 'Amount (Merchant Currency)', 'Offer ID']]

reporting_df.rename({'Description': 'description',
                     'Transaction Time': 'transaction_time',
                     'Transaction Date': 'transaction_date',
                     'Transaction Type': 'type_of_transaction',
                     'Tax Type': 'tax_type',
                     'Refund Type': 'refund_type',
                     'Product Title': 'product_title',
                     'Sku Id': 'vendor_sku',
                     'Buyer Country': 'buyer_country_code',
                     'Buyer Currency': 'buyer_currency',
                     'Merchant Currency': 'merchant_currency',
                     'Currency Conversion Rate': 'currency_conversion_rate',
                     'Amount (Buyer Currency)': 'amount_buyer_currency',
                     'Amount (Merchant Currency)': 'amount_chf',
                     'Offer ID': 'offer_id',
                     },
                    axis=1, inplace=True)

## Converting date columns to datetime and getting rid of unnecessary columns
reporting_df['transaction_date'] = pd.to_datetime(df['Transaction Date'])
reporting_df['transaction_time'] = pd.to_timedelta(df['Transaction Time'])

reporting_df['transaction_date'] = pd.to_datetime(reporting_df['transaction_date'] + reporting_df['transaction_time'],
                                                  format='%m-%d-%Y%H:%M:%S')

reporting_df.drop('transaction_time', inplace=True, axis=1)

## Since google has their own exchange rate we need to derive it from the amounts reported by them. Then we‘d have one fx rate we could also extend to the transactions paid in usd.
exchange_rate_eur_to_chf = reporting_df['amount_chf'][reporting_df['buyer_currency'].str.lower() == 'eur'].sum() / reporting_df['amount_buyer_currency'][reporting_df['buyer_currency'].str.lower() == 'eur'].sum()

reporting_df['exchange_rate_eur_to_chf'] = exchange_rate_eur_to_chf

## We only report DE, CH, AT. Everything except DACH goes to CH attributed abroad in the column domestic_abroad.
reporting_df['domestic_abroad'] = pd.np.where(
    reporting_df['buyer_country_code'].str.lower() == 'de', 'domestic',
    pd.np.where(reporting_df['buyer_country_code'].str.lower() == 'at', 'domestic',
                pd.np.where(reporting_df['buyer_country_code'].str.lower() == 'ch', 'domestic', 'abroad')))

reporting_df['country_code_clean'] = pd.np.where(
    reporting_df['buyer_country_code'].str.lower() == 'de', 'DE',
    pd.np.where(reporting_df['buyer_country_code'].str.lower() == 'at', 'AT',
                pd.np.where(reporting_df['buyer_country_code'].str.lower() == 'ch', 'CH', 'CH')))

## Finance Subs reporting will happen once at end of each month and for that the reporting_month will be defined based on the mode of the transaction_date.
reporting_df['reporting_month'] = reporting_df['transaction_date'].dt.to_period('M').dt.strftime('%Y-%m')
reporting_month = reporting_df.reporting_month.mode()
reporting_df['reporting_month'] = reporting_df['reporting_month'].apply(lambda x: reporting_month)

## Adding payment_method column
reporting_df['payment_method'] = 'google'

## SQL query to enrich the dataframe with more specific product data (class, detailed class, length)
skus_list = str(set(reporting_df['vendor_sku'].to_list()))

skus_expand = f"""select distinct rlv.SKU as sku,
                rlv.product_class,
                rlv.detailed_product_class,
                rlv.product_length
from b2c_middleware.reporting_layer_view rlv
where true
  and rlv.SKU in ({skus_list[1:-1]})
  and rlv.app_shop_id = 'google'
  and rlv.product_class is not null
  and rlv.detailed_product_class is not null
  ;"""

df_skus_expand = pandas_gbq.read_gbq(skus_expand, project_id=project_id, progress_bar_type=None)

reporting_df = reporting_df.merge(df_skus_expand, how='left',
                                  left_on='vendor_sku', right_on='sku').drop('vendor_sku', axis=1)

## Adding units column
reporting_df['units'] = pd.np.where(
    reporting_df['type_of_transaction'].str.lower() == 'charge', 1,
    pd.np.where(reporting_df['type_of_transaction'].str.lower() == 'charge refund', -1, 0))

## Creating product_length_months column
reporting_df['product_length_months'] = reporting_df['product_length']

reporting_df['product_length_months'] = reporting_df['product_length'].replace(
    {31: 1, 92: 3, 365: 12})

## Enriching google with data from our datamarts which already has a lot of clean business logic
trxs_list = str(set(reporting_df['description'].to_list()))

trxs_expand = f"""
select rlv.transaction_id,
       rlv.term_end,
       pav.latest_receipt_orderid,
       rlv.creation_time,
       rlv.store_fees_applicable_percentage as store_fees_percentage,
       rlv.vat_percentage,
       rlv.is_renewal
from b2c_middleware.middlelayer_androidtransaction rlv
         left join b2c_middleware_import.payment_androidtransaction_view pav
                   on pav.id = rlv.transaction_id
where true
  and pav.latest_receipt_orderid in ({trxs_list[1:-1]})
"""

df_trxs_expand = pandas_gbq.read_gbq(trxs_expand, project_id=project_id, progress_bar_type=None)

reporting_df = reporting_df.merge(df_trxs_expand, how='left',
                                  left_on='description', right_on='latest_receipt_orderid').drop(
    'latest_receipt_orderid', axis=1)

reporting_df.drop_duplicates(subset=["description", 'type_of_transaction'], inplace=True)

## 12M subs in DE and AT should be allocated to CH + abroad
reporting_df['domestic_abroad'][(reporting_df['product_length_months'] == 12) & (
    reporting_df['buyer_country_code'].str.lower().isin(['de', 'at']))] = 'abroad'

reporting_df['country_code_clean'][
    (reporting_df['product_length_months'] == 12) & (
        reporting_df['buyer_country_code'].str.lower().isin(['de', 'at']))] = 'CH'

## Build join to dim.countries and get country_names
dim_countries = """
select distinct countrycode as country_code,
                name as country_name
from dim.countries
;
"""

df_countries = pandas_gbq.read_gbq(dim_countries, project_id=project_id, progress_bar_type=None)

reporting_df = reporting_df.merge(df_countries, how='left', left_on='country_code_clean', right_on='country_code')

## Calculating VAT from google instead of getting them from our datamarts because google has its own exchange rates
df_vat_chf = reporting_df[(reporting_df['type_of_transaction'].str.lower().isin(['tax', 'tax refund']))].groupby('description')[
    'amount_chf'].sum().reset_index()

df_vat_chf.rename({'amount_chf': 'vat_chf'
                   },
                  axis=1, inplace=True)

reporting_df = reporting_df.merge(df_vat_chf, how='left', left_on='description', right_on='description')

df_vat_buyer_currency = reporting_df[(reporting_df['type_of_transaction'].str.lower().isin(['tax', 'tax refund']))].groupby('description')[
    'amount_buyer_currency'].sum().reset_index()

df_vat_buyer_currency.rename({'amount_buyer_currency': 'vat_buyer_currency'
                              },
                             axis=1, inplace=True)

reporting_df = reporting_df.merge(df_vat_buyer_currency, how='left', left_on='description',
                                  right_on='description')

reporting_df['vat_eur'] = reporting_df['vat_chf'] / exchange_rate_eur_to_chf

## Clean up the house, yeah.
reporting_df['vat_eur'].replace(np.nan, 0, inplace=True)
reporting_df['vat_chf'].replace(np.nan, 0, inplace=True)
reporting_df['vat_buyer_currency'].replace(np.nan, 0, inplace=True)

## Calculating google fee refund
df_fee_chf = reporting_df[(reporting_df['type_of_transaction'].str.lower().isin(['google fee refund', 'google fee']))].groupby(
    'description')['amount_chf'].sum().reset_index()

df_fee_chf.rename({'amount_chf': 'fee_chf'
                   },
                  axis=1, inplace=True)

reporting_df = reporting_df.merge(df_fee_chf, how='left', left_on='description', right_on='description')

reporting_df['fee_chf'] = reporting_df['fee_chf'].abs()

df_fee_buyer_currency = reporting_df[(reporting_df['type_of_transaction'].str.lower().isin(['google fee refund', 'google fee']))].groupby(
    'description')['amount_buyer_currency'].sum().reset_index()

df_fee_buyer_currency.rename({'amount_buyer_currency': 'fee_buyer_currency'
                              },
                             axis=1, inplace=True)

reporting_df = reporting_df.merge(df_fee_buyer_currency, how='left', left_on='description',
                                  right_on='description')

reporting_df['fee_buyer_currency'] = reporting_df['fee_buyer_currency'].abs()

reporting_df['fee_eur'] = reporting_df['fee_chf'] / exchange_rate_eur_to_chf

## Clean up the house, yeah.
reporting_df['fee_eur'].replace(np.nan, 0, inplace=True)
reporting_df['fee_chf'].replace(np.nan, 0, inplace=True)
reporting_df['fee_buyer_currency'].replace(np.nan, 0, inplace=True)

## And taking care of inverting the values for refunds and chargebacks
reporting_df_negs = reporting_df[reporting_df['type_of_transaction'].str.lower() == 'charge refund']

reporting_df.drop(reporting_df[reporting_df['type_of_transaction'].str.lower() == 'charge refund'].index, inplace=True)

cols = ['fee_chf', 'fee_eur', 'fee_buyer_currency']

## Exactly here is where the negative magic happens
reporting_df_negs[cols] = - reporting_df_negs[cols]

reporting_df = pd.concat([reporting_df, reporting_df_negs])

## Calculating charge from google instead of getting them from our datamarts because google has its own exchange rates
df_charge_chf = reporting_df[(reporting_df['type_of_transaction'].str.lower().isin(['charge', 'charge refund']))].groupby('description')[
    'amount_chf'].sum().reset_index()

df_charge_chf.rename({'amount_chf': 'charge_chf'
                   },
                  axis=1, inplace=True)

reporting_df = reporting_df.merge(df_charge_chf, how='left', left_on='description', right_on='description')

reporting_df['charge_chf'][~(reporting_df['type_of_transaction'].str.lower().isin(['charge', 'charge refund']))] = 0

df_charge_buyer_currency = reporting_df[(reporting_df['type_of_transaction'].str.lower().isin(['charge', 'charge refund']))].groupby('description')[
    'amount_buyer_currency'].sum().reset_index()

df_charge_buyer_currency.rename({'amount_buyer_currency': 'charge_buyer_currency'
                   },
                  axis=1, inplace=True)

reporting_df = reporting_df.merge(df_charge_buyer_currency, how='left', left_on='description', right_on='description')

reporting_df['charge_buyer_currency'][~(reporting_df['type_of_transaction'].str.lower().isin(['charge', 'charge refund']))] = 0

reporting_df['charge_eur'] = reporting_df['charge_chf'] / exchange_rate_eur_to_chf

## Clean up the house, yeah.
reporting_df['charge_eur'].replace(np.nan, 0, inplace=True)
reporting_df['charge_chf'].replace(np.nan, 0, inplace=True)
reporting_df['charge_buyer_currency'].replace(np.nan, 0, inplace=True)

## A bit of housekeeping before moving values to new and renewal bookings
reporting_df['is_renewal'].replace(np.nan, 0, inplace=True)

## Calculating new net bookings from google instead of getting them from our datamarts because google has its own exchange rates
reporting_df['new_booking_net_chf'] = reporting_df['charge_chf'][reporting_df['is_renewal'] == 0] - reporting_df['fee_chf'][reporting_df['is_renewal'] == 0]
reporting_df['new_booking_net_eur'] = reporting_df['charge_eur'][reporting_df['is_renewal'] == 0] - reporting_df['fee_eur'][reporting_df['is_renewal'] == 0]
reporting_df['new_booking_net_buyer_currency'] = reporting_df['charge_buyer_currency'][reporting_df['is_renewal'] == 0] - reporting_df['fee_buyer_currency'][reporting_df['is_renewal'] == 0]

## Clean up the house, yeah.
reporting_df['new_booking_net_chf'].replace(np.nan, 0, inplace=True)
reporting_df['new_booking_net_eur'].replace(np.nan, 0, inplace=True)
reporting_df['new_booking_net_buyer_currency'].replace(np.nan, 0, inplace=True)

## Calculating renewal net bookings from google instead of getting them from our datamarts because google has its own exchange rates
reporting_df['renewal_booking_net_chf'] = reporting_df['charge_chf'][reporting_df['is_renewal'] == 1] - reporting_df['fee_chf'][reporting_df['is_renewal'] == 1]
reporting_df['renewal_booking_net_eur'] = reporting_df['charge_eur'][reporting_df['is_renewal'] == 1] - reporting_df['fee_eur'][reporting_df['is_renewal'] == 1]
reporting_df['renewal_booking_net_buyer_currency'] = reporting_df['charge_buyer_currency'][reporting_df['is_renewal'] == 1] - reporting_df['fee_buyer_currency'][reporting_df['is_renewal'] == 1]

## Clean up the house, yeah.
reporting_df['renewal_booking_net_chf'].replace(np.nan, 0, inplace=True)
reporting_df['renewal_booking_net_eur'].replace(np.nan, 0, inplace=True)
reporting_df['renewal_booking_net_buyer_currency'].replace(np.nan, 0, inplace=True)

## Calculating sales price from google instead of getting them from our datamarts because google has its own exchange rates
reporting_df['sales_price_chf'] = reporting_df['charge_chf'] + reporting_df['vat_chf']
reporting_df['sales_price_eur'] = reporting_df['charge_eur'] + reporting_df['vat_eur']
reporting_df['sales_price_buyer_currency'] = reporting_df['charge_buyer_currency'] + reporting_df['vat_buyer_currency']

## Clean up the house, yeah.
reporting_df['sales_price_chf'].replace(np.nan, 0, inplace=True)
reporting_df['sales_price_eur'].replace(np.nan, 0, inplace=True)
reporting_df['sales_price_buyer_currency'].replace(np.nan, 0, inplace=True)

## Calculating payout from google instead of getting them from our datamarts because google has its own exchange rates
reporting_df['payout_chf'] = reporting_df['sales_price_chf'] - reporting_df['fee_chf']
reporting_df['payout_eur'] = reporting_df['sales_price_eur'] - reporting_df['fee_eur']
reporting_df['payout_buyer_currency'] = reporting_df['sales_price_buyer_currency'] - reporting_df['fee_buyer_currency']

## Clean up the house, yeah.
reporting_df['payout_chf'].replace(np.nan, 0, inplace=True)
reporting_df['payout_eur'].replace(np.nan, 0, inplace=True)
reporting_df['payout_buyer_currency'].replace(np.nan, 0, inplace=True)

## avg_price_sales_per_sub calculation
reporting_df['avg_price_sales_per_sub_chf'] = reporting_df['sales_price_chf'] / reporting_df['units']
reporting_df['avg_price_sales_per_sub_eur'] = reporting_df['sales_price_eur'] / reporting_df['units']
reporting_df['avg_price_sales_per_sub_buyer_currency'] = reporting_df['sales_price_buyer_currency'] / reporting_df['units']

## Clean up the house, yeah.
reporting_df['avg_price_sales_per_sub_chf'].replace(np.nan, 0, inplace=True)
reporting_df['avg_price_sales_per_sub_eur'].replace(np.nan, 0, inplace=True)
reporting_df['avg_price_sales_per_sub_buyer_currency'].replace(np.nan, 0, inplace=True)

## Cleaning up a bit more
reporting_df.replace(np.inf, 0, inplace=True)
reporting_df.replace(-np.inf, 0, inplace=True)

## After all the VAT, fee and sales price calculations, we can only keep the 'charge' and 'refund' transaction types
reporting_df = reporting_df[reporting_df['type_of_transaction'].str.lower().isin(['charge', 'charge refund'])].reset_index(drop=True)

## Removing UTC timezone from term_end
reporting_df['term_end'] = pd.to_datetime(reporting_df['term_end'])
reporting_df['term_end'] = reporting_df['term_end'].dt.strftime('%Y-%m-%d %H:%M:%S')
reporting_df['term_end'] = pd.to_datetime(reporting_df['term_end'])

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

## Cleaning up a bit
reporting_df.replace(np.inf, 0, inplace=True)
reporting_df.replace(-np.inf, 0, inplace=True)

## Adding revenue_month_number - this will help in further calculations
reporting_df = reporting_df.loc[reporting_df.index.repeat(reporting_df['product_term_length_months'])].reset_index(
    drop=True)
reporting_df['revenue_month_number'] = 1
reporting_df['revenue_month_number'] = reporting_df.groupby(["description", 'type_of_transaction'])[
    'revenue_month_number'].cumsum()

## Adding max_month_date to tackle the specifications of active_sub_month_end
reporting_df['max_month_date'] = reporting_df.groupby([reporting_df['term_end'].dt.to_period('M'), 'type_of_transaction'])[
    'term_end'].transform('max')
reporting_df['max_month_date'] = reporting_df['max_month_date'].dt.normalize() + pd.Timedelta('23:59:59')

## Get last indices of each transaction_id group
frames = reporting_df[reporting_df['type_of_transaction'].str.lower().isin(['charge', 'charge refund'])].reset_index().groupby(["description", 'type_of_transaction'])["index"].last().to_frame()
last_idxs_charges = np.array(frames['index'].to_list())

## Add revenue_month_date
reporting_df["revenue_month_date"] = reporting_df["transaction_date"].to_numpy().astype("datetime64[M]")

reporting_df["revenue_month_date"][reporting_df["type_of_transaction"].str.lower().isin(['google fee refund', 'charge refund'])] = (
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

## Set active_sub_month_end = 0 by default
reporting_df['active_sub_month_end'] = 0
# reporting_df['revenue_month_date'] = pd.to_datetime(reporting_df['revenue_month_date']).dt.date
reporting_df['active_sub_month_end'][
    (reporting_df['term_end'] > (reporting_df['revenue_month_date'] + pd.offsets.MonthBegin(1)))] = 1

reporting_df["active_sub_month_end"][reporting_df["type_of_transaction"].str.lower() != "charge"] = reporting_df[
                                                                                            'active_sub_month_end'] * -1

reporting_df.reset_index(drop=True, inplace=True)

## active_sub_content follows the same logic as active_sub_month_end except it doesn't count the last month
reporting_df["active_sub_content"] = reporting_df.active_sub_month_end

reporting_df["active_sub_content"][(reporting_df["revenue_month_number"] > reporting_df['product_length_months'])] = 0

## Total_days of product_term_length per transaction_id
reporting_df["total_days"] = reporting_df[reporting_df['type_of_transaction'].str.lower().isin(['charge', 'charge refund'])].groupby(["description", 'type_of_transaction'])[
    "product_term_length"].transform("sum")

## Adding product_term_length_months
reporting_df['product_term_length_months'] = reporting_df.groupby(["description", 'type_of_transaction'])[
    'revenue_month_number'].transform('max')

## A bit more housekeeping
reporting_df[['new_booking_net_chf', 'new_booking_net_buyer_currency', 'renewal_booking_net_chf', 'renewal_booking_net_buyer_currency']].replace(np.nan, 0, inplace=True)

## Calculate total_revenue_net fields
reporting_df["total_revenue_net_chf"] = (
        (reporting_df["new_booking_net_chf"] + reporting_df['renewal_booking_net_chf']) / reporting_df["total_days"] * reporting_df["product_term_length"]
)
reporting_df["total_revenue_net_eur"] = (
        (reporting_df["new_booking_net_eur"] + reporting_df['renewal_booking_net_eur']) / reporting_df["total_days"] * reporting_df["product_term_length"]
)

reporting_df["total_revenue_net_buyer_currency"] = (
        (reporting_df["new_booking_net_buyer_currency"] + reporting_df['renewal_booking_net_buyer_currency']) / reporting_df["total_days"] * reporting_df["product_term_length"]
)

## Remove values from all lines of cols group except first
one_line_cols = ["vat_eur", 'vat_chf', 'vat_buyer_currency', 'payout_eur',
                 'payout_chf', 'payout_buyer_currency'
                 'sales_price_eur', 'sales_price_chf',
                 'sales_price_buyer_currency', 'fee_eur',
                 'fee_chf', 'fee_buyer_currency', 'new_booking_net_eur', 'new_booking_net_buyer_currency',
                 'new_booking_net_chf', 'renewal_booking_net_eur', 'renewal_booking_net_buyer_currency',
                 'renewal_booking_net_chf', 'store_fees_eur', 'store_fees_chf',
                 'charge_eur', 'charge_buyer_currency', 'charge_chf',
                 'vat_percentage',
                 'avg_price_sales_per_sub', 'avg_price_sales_per_chf', 'avg_price_sales_per_buyer_currency'
                 'units', 'amount_chf', 'amount_buyer_currency', 'store_fees_percentage', 'payout_buyer_currency', 'avg_price_sales_per_sub_chf',
                 'avg_price_sales_per_sub_buyer_currency'
                 ]

for col in one_line_cols:
    reporting_df.loc[
        reporting_df["revenue_month_number"] > 1,
        [col],
    ] = 0.0

## Google reports tax only for CH. All other countries (DE, AT & CH abroad are not reported and therefore 0). The VAT for CH should be all 7.7%.
reporting_df['vat_percentage'] = 7.7

## Remove values from all lines of cols group except first
reporting_df.loc[
        reporting_df["revenue_month_number"] > 1,
        ['units', 'vat_percentage', 'sales_price_eur'],
    ] = 0.0

## Standardizing the report
reporting_df[['product_group_finance']] = ''
reporting_df[['currency', 'subscription_status']] = np.nan

## We only have paid subs in google
reporting_df['subscription_status'] = 'paid'

## Dropping dups
reporting_df = reporting_df.drop_duplicates()

## A bit more housekeeping
reporting_df[['fee_eur', 'vat_eur', 'charge_eur', 'new_booking_net_eur', 'renewal_booking_net_eur', 'payout_eur', 'new_booking_net_chf', 'renewal_booking_net_chf', 'total_revenue_net_chf', 'exchange_rate_eur_to_chf']].replace(np.nan, 0, inplace=True)

## Final clean up
reporting_df['store_fees_eur'].replace(np.nan, 0, inplace=True)
reporting_df['store_fees_chf'].replace(np.nan, 0, inplace=True)
reporting_df['store_fees_percentage'].replace(np.nan, 0, inplace=True)
reporting_df['vat_percentage'].replace(np.nan, 0, inplace=True)

reporting_df['total_revenue_net_chf'].replace(np.nan, 0, inplace=True)
reporting_df['total_revenue_net_eur'].replace(np.nan, 0, inplace=True)

reporting_df['revenue_month_date'] = pd.to_datetime(reporting_df['revenue_month_date'])

## Logic to mark transactions that are upgrades
def upgrade(s):
    if (s['product_class'] == 'ultimate') and (s['product_length'] == 31) and (s['new_booking_net_eur'] > 0 or s['new_booking_net_chf'] > 0):
        if (s['country_code'] == 'CH' and s['new_booking_net_chf'] <= 8) or (s['country_code'] == 'DE' and s['new_booking_net_eur'] <= 5) or (s['country_code'] == 'AT' and s['new_booking_net_eur'] <= 5):
            return 1
    else:
        return 0

reporting_df['is_upgrade'] = reporting_df.apply(upgrade, axis=1)

## Reorder dataframe
reporting_df = reporting_df[
    [
        # 'transaction_id',
        'description',
        'transaction_date',
        'term_end',
        'reporting_month',
        'country_name',
        'country_code',
        'buyer_country_code',
        # 'currency',
        'buyer_currency',
        # 'merchant_currency',
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
        'active_sub_content',
        'is_upgrade'
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
    # {"name": "buyer_currency", "type": "STRING"},
    # {"name": "merchant_currency", "type": "STRING"},
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
    {"name": "active_sub_content", "type": "INTEGER"},
    {"name": "is_upgrade", "type": "INTEGER"}
]

## Final clean up before exporting
reporting_df.rename({'buyer_currency': 'currency',
                    'description': 'transaction_id'},
                    axis=1, inplace=True)

reporting_df['type_of_transaction'] = reporting_df['type_of_transaction'].replace(
    {'Charge': 'charge', 'Charge refund': 'refund'})

## Export to BQ table
pandas_gbq.to_gbq(
    dataframe=reporting_df,
    destination_table=f"finance.subs_reporting_google",
    project_id="zattoo-dataeng",
    if_exists="append",
    progress_bar=None,
    table_schema=bq_schema,
)
