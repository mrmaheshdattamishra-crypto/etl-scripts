Feature: sales_summary_by_channel

Background:
  Given source tables with the following schemas:
    | table_name    | column_name           | data_type    |
    | store_sales   | ss_sold_date_sk       | NUMBER       |
    | store_sales   | ss_item_sk            | NUMBER       |
    | store_sales   | ss_quantity           | NUMBER       |
    | store_sales   | ss_ext_sales_price    | NUMBER       |
    | store_sales   | ss_net_profit         | NUMBER       |
    | web_sales     | ws_sold_date_sk       | NUMBER       |
    | web_sales     | ws_item_sk            | NUMBER       |
    | web_sales     | ws_quantity           | NUMBER       |
    | web_sales     | ws_ext_sales_price    | NUMBER       |
    | web_sales     | ws_net_profit         | NUMBER       |
    | catalog_sales | cs_sold_date_sk       | NUMBER       |
    | catalog_sales | cs_item_sk            | NUMBER       |
    | catalog_sales | cs_quantity           | NUMBER       |
    | catalog_sales | cs_ext_sales_price    | NUMBER       |
    | catalog_sales | cs_net_profit         | NUMBER       |
    | date_dim      | d_date_sk             | NUMBER       |
    | date_dim      | d_date                | DATE         |
    | date_dim      | d_year                | NUMBER       |
    | date_dim      | d_moy                 | NUMBER       |
    | date_dim      | d_quarter_name        | STRING       |
    | item          | i_item_sk             | NUMBER       |
    | item          | i_category            | STRING       |
    | item          | i_brand               | STRING       |
    | item          | i_product_name        | STRING       |

  And target table schema:
    | table_name           | column_name        | data_type |
    | sales_summary        | sale_date          | DATE      |
    | sales_summary        | year               | NUMBER    |
    | sales_summary        | month              | NUMBER    |
    | sales_summary        | quarter            | STRING    |
    | sales_summary        | channel            | STRING    |
    | sales_summary        | item_category      | STRING    |
    | sales_summary        | item_brand         | STRING    |
    | sales_summary        | total_quantity     | NUMBER    |
    | sales_summary        | total_sales_amount | NUMBER    |
    | sales_summary        | total_profit       | NUMBER    |
    | sales_summary        | transaction_count  | NUMBER    |

Scenario: Create sales summary dataset across all channels
  Given store sales data joined with date dimension on sold date key equals date key
  And store sales data joined with item dimension on item key equals item key
  And web sales data joined with date dimension on sold date key equals date key
  And web sales data joined with item dimension on item key equals item key
  And catalog sales data joined with date dimension on sold date key equals date key
  And catalog sales data joined with item dimension on item key equals item key
  
  When transforming store sales data
  Then map date dimension date to sale date
  And map date dimension year to year
  And map date dimension month of year to month
  And map date dimension quarter name to quarter
  And map literal value store to channel
  And map item category to item category
  And map item brand to item brand
  And sum store sales quantity to total quantity
  And sum store sales extended sales price to total sales amount
  And sum store sales net profit to total profit
  And count distinct store sales records to transaction count
  
  When transforming web sales data
  Then map date dimension date to sale date
  And map date dimension year to year
  And map date dimension month of year to month
  And map date dimension quarter name to quarter
  And map literal value web to channel
  And map item category to item category
  And map item brand to item brand
  And sum web sales quantity to total quantity
  And sum web sales extended sales price to total sales amount
  And sum web sales net profit to total profit
  And count distinct web sales records to transaction count
  
  When transforming catalog sales data
  Then map date dimension date to sale date
  And map date dimension year to year
  And map date dimension month of year to month
  And map date dimension quarter name to quarter
  And map literal value catalog to channel
  And map item category to item category
  And map item brand to item brand
  And sum catalog sales quantity to total quantity
  And sum catalog sales extended sales price to total sales amount
  And sum catalog sales net profit to total profit
  And count distinct catalog sales records to transaction count
  
  And union all channel data into single sales summary dataset
  And group by sale date, year, month, quarter, channel, item category, and item brand