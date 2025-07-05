-- models/staging/stg_raw_prices.sql
-- Staging model for raw scraped prices

{{ config(materialized='view') }}

with source_data as (

    select
        product_name,
        price,
        currency,
        store_name,
        category,
        scraped_at,
        url,
        -- Clean and standardize the data
        trim(upper(product_name)) as clean_product_name,
        cast(price as float64) as price_numeric,
        upper(trim(store_name)) as clean_store_name,
        date(scraped_at) as scraped_date,
        datetime(scraped_at) as scraped_datetime
    
    from {{ source('scrap_prices_dev', 'scraped_prices') }}
    where price is not null
      and product_name is not null
      and scraped_at is not null

)

select * from source_data
