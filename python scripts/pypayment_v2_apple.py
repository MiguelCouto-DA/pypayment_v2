## Importing all needed libraries and modules
## Setting up BigQuery access credentials for SQL parsing of reporting data
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import pandas as pd
import numpy as np
import os
import warnings
from datetime import datetime, timedelta

pd.set_option('display.max_columns', None)
warnings.filterwarnings('ignore')

credentials = service_account.Credentials.from_service_account_file(
    "/Users/miguelcouto/Downloads/zattoo-dataeng-e5f45785174f.json"
)

project_id = "zattoo-dataeng"
client = bigquery.Client(credentials=credentials, project=project_id)

## Prepare dataframe
dtype = ({'Apple Identifier': 'float64',
          'SKU': 'object',
          'Title': 'object',
          'Developer Name': 'object',
          'Product Type Identifier': 'object',
          'Country of Sale': 'object',
          'Quantity': 'float64',
          'Partner Share': 'float64',
          'Extended Partner Share': 'float64',
          'Partner Share Currency': 'object',
          'Customer Price': 'float64',
          'Customer Currency': 'object',
          'Sale or Return': 'object',
          'Promo Code': 'object',
          'Order Type': 'object',
          'Region': 'object'})

parse_dates = ['Transaction Date', 'Settlement Date']

df1 = pd.read_csv('/Users/miguelcouto/PycharmProjects/pypayment_v2/raw/apple/2022-03_appleCH.txt', header=0,
                  skiprows=3, delimiter="\t", names=['Transaction Date', 'Settlement Date', 'Apple Identifier', 'SKU',
                                                     'Title', 'Developer Name', 'Product Type Identifier',
                                                     'Country of Sale',
                                                     'Quantity', 'Partner Share', 'Extended Partner Share',
                                                     'Partner Share Currency', 'Customer Price', 'Customer Currency',
                                                     'Sale or Return', 'Promo Code', 'Order Type', 'Region'],
                  )

df2 = pd.read_csv('/Users/miguelcouto/PycharmProjects/pypayment_v2/raw/apple/2022-03_appleDE.txt', header=0,
                  skiprows=3, delimiter="\t", names=['Transaction Date', 'Settlement Date', 'Apple Identifier', 'SKU',
                                                     'Title', 'Developer Name', 'Product Type Identifier',
                                                     'Country of Sale',
                                                     'Quantity', 'Partner Share', 'Extended Partner Share',
                                                     'Partner Share Currency', 'Customer Price', 'Customer Currency',
                                                     'Sale or Return', 'Promo Code', 'Order Type', 'Region'],
                  )

df1 = df1.reset_index().shift(1, axis=1)
df1 = df1.loc[:, df1.columns != 'index']

df2 = df2.reset_index().shift(1, axis=1)
df2 = df2.loc[:, df2.columns != 'index']

df = pd.concat([df1, df2])

## Renaming and reordering the dataframe so it keeps consistency with the reports from the other app shops
reporting_df = df[
    ['Settlement Date', 'SKU',
     'Country of Sale', 'Quantity', 'Partner Share', 'Extended Partner Share',
     'Partner Share Currency', 'Customer Price', 'Customer Currency',
     'Sale or Return']]

reporting_df.rename({
    'Settlement Date': 'transaction_date',
    'SKU': 'vendor_sku',
    'Country of Sale': 'buyer_country_code',
    'Customer Currency': 'buyer_currency',
    'Partner Share': 'net_booking',
    'Extended Partner Share': 'extended_net_booking',
    'Partner Share Currency': 'merchant_currency',
    'Currency Conversion Rate': 'currency_conversion_rate',
    'Customer Price': 'sales_price',
    'Quantity': 'units',
    'Sale or Return': 'sale_return',
},
    axis=1, inplace=True)

## Converting date columns to datetime and getting rid of unnecessary columns
reporting_df['transaction_date'] = pd.to_datetime(reporting_df['transaction_date'])
## Adding fake timestamp due to lack of it in the original apple reports
reporting_df['transaction_date'] = reporting_df['transaction_date'] + timedelta(hours=23, minutes=59, seconds=59)

## We only report DE, CH, AT. Everything except DACH goes to CH attributed abroad in the column domestic_abroad.
reporting_df['domestic_abroad'] = 'domestic'

reporting_df['country_code_clean'] = pd.np.where(
    reporting_df['buyer_country_code'].str.lower() == 'de', 'DE',
    pd.np.where(reporting_df['buyer_country_code'].str.lower() == 'at', 'AT',
                pd.np.where(reporting_df['buyer_country_code'].str.lower() == 'ch', 'CH', 'DE')))

## Finance Subs reporting will happen once at end of each month and for that the reporting_month will be defined based on the mode of the transaction_date.
reporting_df['reporting_month'] = reporting_df['transaction_date'].dt.to_period('M').dt.strftime('%Y-%m')
reporting_month = reporting_df.reporting_month.mode()
reporting_df['reporting_month'] = reporting_df['reporting_month'].apply(lambda x: reporting_month)

## Adding payment_method column
reporting_df['payment_method'] = 'apple'

## SQL query to enrich the dataframe with more specific product data (class, detailed class, length)
skus_list = str(set(reporting_df['vendor_sku'].to_list()))

skus_expand = f"""select distinct rlv.SKU as sku,
                rlv.product_class,
                rlv.detailed_product_class,
                rlv.product_length
from b2c_middleware.reporting_layer_view rlv
where true
  and rlv.SKU in ({skus_list[1:-1]})
  and rlv.app_shop_id = 'apple'
  and rlv.product_class is not null
  and rlv.detailed_product_class is not null
  ;"""

df_skus_expand = pandas_gbq.read_gbq(skus_expand, project_id=project_id, progress_bar_type=None)

reporting_df = reporting_df.merge(df_skus_expand, how='left',
                                  left_on='vendor_sku', right_on='sku').drop('vendor_sku', axis=1)

## Creating product_length_months column
reporting_df['product_length_months'] = reporting_df['product_length']

reporting_df['product_length_months'] = reporting_df['product_length'].replace(
    {31: 1, 92: 3, 365: 12})

## Build join to dim.countries and get country_names
dim_countries = """
select distinct countrycode as country_code,
                name as country_name
from dim.countries
;
"""

df_countries = pandas_gbq.read_gbq(dim_countries, project_id=project_id, progress_bar_type=None)

reporting_df = reporting_df.merge(df_countries, how='left', left_on='country_code_clean', right_on='country_code')

## SQL query to enrich the dataframe with exchange rate data
currencies = reporting_df[
    (~reporting_df['buyer_country_code'].isin(['CH', 'AT', 'DE'])) & (reporting_df['buyer_currency'] != 'EUR')][
    'buyer_currency'].unique()
currencies_lst = str(set(reporting_df[(~reporting_df['buyer_country_code'].isin(['CH', 'AT', 'DE'])) & (
            reporting_df['buyer_currency'] != 'EUR')]['buyer_currency'].to_list()))

currencies_expand = f"""
select pe.day,
       pe.from_currency,
       pe.to_currency,
       pe.exchange_rate,
       pe_eur.exchange_rate AS exchange_rate_eur_to_chf,
       pe.from_currency_quantity
from b2c_middleware_import.payment_exchangerate pe
         LEFT JOIN b2c_middleware_import.payment_exchangerate pe_eur
                   ON pe_eur.from_currency = 'EUR' AND DATE_TRUNC(pe_eur.day, MONTH) =
                           "{reporting_month[0]}-01"
where true
  and pe.from_currency in ('CHF', 'EUR', {currencies_lst[1:-1]})
  and pe.day = "{reporting_month[0]}-01"
  and pe_eur.day = "{reporting_month[0]}-01"
  ;"""

df_currencies_expand = pandas_gbq.read_gbq(currencies_expand, project_id=project_id, progress_bar_type=None)

reporting_df = reporting_df.merge(df_currencies_expand, how='left',
                                  left_on='buyer_currency', right_on='from_currency')

## Renaming fields for later calculations
reporting_df.rename({'index': 'description'},
                    axis=1, inplace=True)

## Adding is_refund field based on apple's sale_return
reporting_df['is_refund'] = [0 if x == "S" else 1 for x in reporting_df["sale_return"]]

## Dropping unnecessary fields
reporting_df.drop(['extended_net_booking', 'sale_return'], axis=1, inplace=True)

## Transforming units sign to negative for refunds
reporting_df['net_booking'][reporting_df['units'] < 0] = reporting_df['net_booking'][reporting_df['units'] < 0] * -1

reporting_df = reporting_df.loc[reporting_df.index.repeat(reporting_df.units.abs())].reset_index()
reporting_df['units'] = [-1 if x < 0 else 1 if x > 0 else 0 for x in reporting_df["units"]]

## Calculating new net bookings for apple
reporting_df['new_booking_net_chf'] = reporting_df['net_booking'][(reporting_df['buyer_currency'] == 'CHF')]

reporting_df['new_booking_net_chf'][
    (reporting_df.new_booking_net_chf.isna()) & (reporting_df.buyer_currency != 'CHF') & (
                reporting_df.buyer_currency == 'EUR')] = reporting_df['net_booking'][
                                                             (reporting_df.new_booking_net_chf.isna()) & (
                                                                         reporting_df.buyer_currency != 'CHF') & (
                                                                         reporting_df.buyer_currency == 'EUR')] * reporting_df[(reporting_df.new_booking_net_chf.isna()) & (
                                                                     reporting_df.buyer_currency != 'CHF') & (
                                                                                  reporting_df.buyer_currency == 'EUR')][
                                                             'exchange_rate'] / reporting_df[
                                                             (reporting_df.new_booking_net_chf.isna()) & (
                                                                         reporting_df.buyer_currency != 'CHF') & (
                                                                         reporting_df.buyer_currency == 'EUR')]['from_currency_quantity']

reporting_df['new_booking_net_eur'] = 0

reporting_df['new_booking_net_eur'][
    (reporting_df.buyer_currency == 'CHF') & (reporting_df.buyer_country_code == 'CH')] = reporting_df['net_booking'][(
                                                                                                                                  reporting_df.buyer_currency == 'CHF') & (
                                                                                                                                  reporting_df.buyer_country_code == 'CH')] / (
                                                                                          reporting_df[(
                                                                                                                   reporting_df.buyer_currency == 'CHF') & (
                                                                                                                   reporting_df.buyer_country_code == 'CH')][
                                                                                              'exchange_rate_eur_to_chf']) / (
                                                                                          reporting_df[(
                                                                                                                   reporting_df.buyer_currency == 'CHF') & (
                                                                                                                   reporting_df.buyer_country_code == 'CH')][
                                                                                              'from_currency_quantity'])

## For those currencies != CHF and EUR and outside of CH will first need to convert to CHF and then EUR
reporting_df['new_booking_net_eur'][
    (reporting_df.new_booking_net_chf.isna()) & (reporting_df.buyer_currency != 'CHF') & (
                reporting_df.buyer_currency != 'EUR')] = reporting_df['net_booking'][
                                                             (reporting_df.new_booking_net_chf.isna()) & (
                                                                         reporting_df.buyer_currency != 'CHF') & (
                                                                         reporting_df.buyer_currency != 'EUR')] * (
                                                         reporting_df[(reporting_df.new_booking_net_chf.isna()) & (
                                                                     reporting_df.buyer_currency != 'CHF') & (
                                                                                  reporting_df.buyer_currency != 'EUR')][
                                                             'exchange_rate']) / (reporting_df[
    (reporting_df.new_booking_net_chf.isna()) & (reporting_df.buyer_currency != 'CHF') & (
                reporting_df.buyer_currency != 'EUR')]['from_currency_quantity'])

reporting_df['new_booking_net_eur'][(reporting_df.buyer_currency == 'EUR')] = reporting_df['net_booking'][
    (reporting_df.buyer_currency == 'EUR')]

reporting_df['new_booking_net_chf'][
    (reporting_df.new_booking_net_chf.isna()) & (reporting_df.buyer_currency != 'CHF') & (
                reporting_df.buyer_currency != 'EUR')] = reporting_df['new_booking_net_eur'][
                                                             (reporting_df.new_booking_net_chf.isna()) & (
                                                                         reporting_df.buyer_currency != 'CHF') & (
                                                                         reporting_df.buyer_currency != 'EUR')] * (
                                                         reporting_df[(reporting_df.new_booking_net_chf.isna()) & (
                                                                     reporting_df.buyer_currency != 'CHF') & (
                                                                                  reporting_df.buyer_currency != 'EUR')][
                                                             'exchange_rate_eur_to_chf'])

reporting_df['new_booking_net_chf'].replace(np.nan, 0, inplace=True)
reporting_df['new_booking_net_eur'].replace(np.nan, 0, inplace=True)

## Calculating sales price for apple
reporting_df['sales_price_chf'] = reporting_df['sales_price'][(reporting_df['buyer_currency'] == 'CHF')]

reporting_df['sales_price_chf'][(reporting_df.sales_price_chf.isna()) & (reporting_df.buyer_currency != 'CHF') & (
            reporting_df.buyer_currency == 'EUR')] = reporting_df['sales_price'][
                                                         (reporting_df.sales_price_chf.isna()) & (
                                                                     reporting_df.buyer_currency != 'CHF') & (
                                                                     reporting_df.buyer_currency == 'EUR')] * reporting_df[(reporting_df.sales_price_chf.isna()) & (
                                                                 reporting_df.buyer_currency != 'CHF') & (
                                                                              reporting_df.buyer_currency == 'EUR')][
                                                         'exchange_rate'] / reporting_df[
                                                         (reporting_df.sales_price_chf.isna()) & (
                                                                     reporting_df.buyer_currency != 'CHF') & (
                                                                     reporting_df.buyer_currency == 'EUR')][
                                                         'from_currency_quantity']

reporting_df['sales_price_eur'] = 0

reporting_df['sales_price_eur'][(reporting_df.buyer_currency == 'CHF') & (reporting_df.buyer_country_code == 'CH')] = reporting_df['sales_price'][(reporting_df.buyer_currency == 'CHF') & (reporting_df.buyer_country_code == 'CH')] / (
reporting_df[(reporting_df.buyer_currency == 'CHF') & (reporting_df.buyer_country_code == 'CH')][
    'exchange_rate_eur_to_chf']) / (
reporting_df[(reporting_df.buyer_currency == 'CHF') & (reporting_df.buyer_country_code == 'CH')][
    'from_currency_quantity'])

## For those currencies != CHF and EUR and outside of CH will first need to convert to CHF and then EUR
reporting_df['sales_price_eur'][(reporting_df.sales_price_chf.isna()) & (reporting_df.buyer_currency != 'CHF') & (
            reporting_df.buyer_currency != 'EUR')] = reporting_df['sales_price'][
                                                         (reporting_df.sales_price_chf.isna()) & (
                                                                     reporting_df.buyer_currency != 'CHF') & (
                                                                     reporting_df.buyer_currency != 'EUR')] * (
                                                     reporting_df[(reporting_df.sales_price_chf.isna()) & (
                                                                 reporting_df.buyer_currency != 'CHF') & (
                                                                              reporting_df.buyer_currency != 'EUR')][
                                                         'exchange_rate']) / (reporting_df[
    (reporting_df.sales_price_chf.isna()) & (reporting_df.buyer_currency != 'CHF') & (
                reporting_df.buyer_currency != 'EUR')]['from_currency_quantity'])

reporting_df['sales_price_eur'][(reporting_df.buyer_currency == 'EUR')] = reporting_df['sales_price'][
    (reporting_df.buyer_currency == 'EUR')]

reporting_df['sales_price_chf'][(reporting_df.sales_price_chf.isna()) & (reporting_df.buyer_currency != 'CHF') & (
            reporting_df.buyer_currency != 'EUR')] = reporting_df['sales_price_eur'][
                                                         (reporting_df.sales_price_chf.isna()) & (
                                                                     reporting_df.buyer_currency != 'CHF') & (
                                                                     reporting_df.buyer_currency != 'EUR')] * (
                                                     reporting_df[(reporting_df.sales_price_chf.isna()) & (
                                                                 reporting_df.buyer_currency != 'CHF') & (
                                                                              reporting_df.buyer_currency != 'EUR')][
                                                         'exchange_rate_eur_to_chf'])

reporting_df['sales_price_chf'].replace(np.nan, 0, inplace=True)
reporting_df['sales_price_eur'].replace(np.nan, 0, inplace=True)

## Calculating store fees
reporting_df['store_fees_percentage'] = 15
reporting_df['fee_eur'] = reporting_df['sales_price_eur'] * reporting_df['store_fees_percentage'] / 100
reporting_df['fee_chf'] = reporting_df['sales_price_chf'] * reporting_df['store_fees_percentage'] / 100

## Calculating payout for apple
reporting_df['payout_chf'] = reporting_df['sales_price_chf'] - reporting_df['fee_chf']
reporting_df['payout_eur'] = reporting_df['sales_price_eur'] - reporting_df['fee_eur']

## Calculating VAT for apple
reporting_df['vat_chf'] = 0
reporting_df['vat_eur'] = 0

reporting_df['vat_chf'] = reporting_df['payout_chf'] - reporting_df['new_booking_net_chf']
reporting_df['vat_eur'] = reporting_df['payout_eur'] - reporting_df['new_booking_net_eur']

## Calculating charge for apple
reporting_df['charge_chf'] = reporting_df['sales_price_chf'] - reporting_df['vat_chf']
reporting_df['charge_eur'] = reporting_df['sales_price_eur'] - reporting_df['vat_eur']

## apple vat percentage
reporting_df['vat_percentage'] = 0

reporting_df['vat_percentage'] = round(reporting_df['vat_chf'] / reporting_df['new_booking_net_chf'] * 100, 1)

# ## avg_price_sales_per_sub calculation
reporting_df['avg_price_sales_per_sub_chf'] = reporting_df['sales_price_chf'] / reporting_df['units']
reporting_df['avg_price_sales_per_sub_eur'] = reporting_df['sales_price_eur'] / reporting_df['units']

## Clean up the house, yeah.
reporting_df['avg_price_sales_per_sub_chf'].replace(np.nan, 0, inplace=True)
reporting_df['avg_price_sales_per_sub_eur'].replace(np.nan, 0, inplace=True)

## Cleaning up a bit more
reporting_df.replace(np.inf, 0, inplace=True)
reporting_df.replace(-np.inf, 0, inplace=True)

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
)
reporting_df['revenue_month_number'] = 1
reporting_df['revenue_month_number'] = reporting_df.groupby(["level_0"])[
    'revenue_month_number'].cumsum()

## Renaming for better understanding of process throughout all reports
reporting_df.rename({'level_0': 'description'}, axis=1, inplace=True)
reporting_df.drop("index", axis=1, inplace=True)

## Adding max_month_date to tackle the specifications of active_sub_month_end
reporting_df['max_month_date'] = reporting_df.groupby([reporting_df['term_end'].dt.to_period('M')])[
    'term_end'].transform('max')
reporting_df['max_month_date'] = reporting_df['max_month_date'].dt.normalize() + pd.Timedelta('23:59:59')

## Get last indices of each transaction_id group
frames = reporting_df.reset_index().groupby(
    ["description"])["index"].last().to_frame()
last_idxs_charges = np.array(frames['index'].to_list())

## Add revenue_month_date
reporting_df["revenue_month_date"] = reporting_df["transaction_date"].to_numpy().astype("datetime64[M]")

## Adding revenue_month_date
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

reporting_df['active_sub_month_end'][
    (reporting_df['term_end'] > (reporting_df['revenue_month_date'] + pd.offsets.MonthBegin(1)))] = 1

reporting_df.reset_index(drop=True, inplace=True)

## active_sub_content follows the same logic as active_sub_month_end except it doesn't count the last month
reporting_df["active_sub_content"] = reporting_df.active_sub_month_end

reporting_df["active_sub_content"][(reporting_df["revenue_month_number"] > reporting_df['product_length_months'])] = 0

## Total_days of product_term_length per transaction_id
reporting_df["total_days"] = reporting_df.groupby(
    ["description"])[
    "product_term_length"].transform("sum")

## Turning them into negative for refunds
reporting_df['active_sub_content'][reporting_df['units'] == -1] = reporting_df['active_sub_content'][
                                                                      reporting_df['units'] == -1] * -1
reporting_df['active_sub_month_end'][reporting_df['units'] == -1] = reporting_df['active_sub_month_end'][
                                                                        reporting_df['units'] == -1] * -1

## Apple is reported with a different cycle, therefore a quick fix of adding 1 month to all dates in revenue_month_date. This would shift everything by exactly one month and more than it has to be
reporting_month_int = int(reporting_month.str[6:][0])
reporting_df['belongs_reporting_cycle'] = [1 if a.month == reporting_month_int else 0 for a in reporting_df['transaction_date']]

reporting_df["revenue_month_date"][reporting_df['belongs_reporting_cycle'] == 0] = reporting_df["revenue_month_date"] + pd.offsets.MonthBegin(1)

## Adding product_term_length_months
reporting_df['product_term_length_months'] = reporting_df.groupby(["description"])[
    'revenue_month_number'].transform('max')

## Calculate total_revenue_net fields
reporting_df["total_revenue_net_chf"] = (
        (reporting_df["new_booking_net_chf"]) / reporting_df["total_days"] *
        reporting_df["product_term_length"]
)

reporting_df["total_revenue_net_eur"] = (
        (reporting_df["new_booking_net_eur"]) / reporting_df["total_days"] *
        reporting_df["product_term_length"]
)

## Remove values from all lines of cols group except first
reporting_df.loc[
    reporting_df["revenue_month_number"] > 1,
    ['units', 'net_booking', 'renewal_booking_net_chf', 'new_booking_net_chf', 'renewal_booking_net_eur',
     'new_booking_net_eur', 'sales_price_eur', 'sales_price_chf', 'vat_chf', 'vat_eur', 'charge_chf', 'charge_eur',
     'payout_chf', 'payout_eur', 'vat_percentage', 'exchange_rate_eur_to_chf', 'store_fees_eur', 'store_fees_chf',
     'store_fees_percentage', 'fee_eur', 'fee_chf'],
] = 0.0

## Standardizing the report
reporting_df[['product_group_finance']] = ''
reporting_df[['subscription_status']] = np.nan
reporting_df['store_fees_eur'] = 0
reporting_df['store_fees_chf'] = 0

## We only have paid subs in apple
reporting_df['subscription_status'] = 'paid'

## type of transaction based on is_refund
reporting_df['type_of_transaction'] = ['refund' if x == 1 else 'sale' for x in reporting_df["is_refund"]]

## Final clean up
reporting_df['total_revenue_net_eur'].replace(np.nan, 0, inplace=True)
reporting_df['total_revenue_net_chf'].replace(np.nan, 0, inplace=True)
reporting_df['new_booking_net_chf'].replace(np.nan, 0, inplace=True)
reporting_df['new_booking_net_eur'].replace(np.nan, 0, inplace=True)
reporting_df['renewal_booking_net_chf'].replace(np.nan, 0, inplace=True)
reporting_df['renewal_booking_net_eur'].replace(np.nan, 0, inplace=True)
reporting_df['sales_price_eur'].replace(np.nan, 0, inplace=True)
reporting_df['sales_price_chf'].replace(np.nan, 0, inplace=True)

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
    {"name": "store_fees_eur", "type": "FLOAT"},
    {"name": "store_fees_chf", "type": "FLOAT"},
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

## Final clean up before exporting
reporting_df.rename({'buyer_currency': 'currency',
                     'description': 'transaction_id'},
                    axis=1, inplace=True)

## Export to BQ table
pandas_gbq.to_gbq(
    dataframe=reporting_df,
    destination_table=f"finance.subs_reporting_apple",
    project_id="zattoo-dataeng",
    if_exists="append",
    progress_bar=None,
    table_schema=bq_schema,
)
