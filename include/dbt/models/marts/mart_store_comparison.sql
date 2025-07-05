-- models/marts/mart_store_comparison.sql
-- Store comparison analytics

{{ config(materialized='table') }}

with latest_prices as (
    
    select
        clean_product_name,
        clean_store_name,
        price_numeric,
        scraped_date,
        row_number() over (
            partition by clean_product_name, clean_store_name 
            order by scraped_date desc
        ) as rn
    
    from {{ ref('stg_raw_prices') }}

),

current_prices as (
    
    select
        clean_product_name as product_name,
        clean_store_name as store_name,
        price_numeric as current_price,
        scraped_date as last_updated
    
    from latest_prices
    where rn = 1

),

price_rankings as (
    
    select
        product_name,
        store_name,
        current_price,
        last_updated,
        
        -- Rank stores by price for each product
        row_number() over (
            partition by product_name 
            order by current_price asc
        ) as price_rank,
        
        -- Calculate price differences vs cheapest
        min(current_price) over (partition by product_name) as min_price_for_product,
        max(current_price) over (partition by product_name) as max_price_for_product,
        count(*) over (partition by product_name) as stores_selling_product
    
    from current_prices

)

select
    product_name,
    store_name,
    current_price,
    last_updated,
    price_rank,
    stores_selling_product,
    min_price_for_product,
    max_price_for_product,
    
    -- Price difference vs cheapest option
    round(current_price - min_price_for_product, 2) as price_diff_vs_cheapest,
    
    -- Percentage difference vs cheapest
    case 
        when min_price_for_product > 0 
        then round((current_price - min_price_for_product) / min_price_for_product * 100, 2)
        else null 
    end as pct_diff_vs_cheapest,
    
    -- Is this the cheapest option?
    case when price_rank = 1 then true else false end as is_cheapest,
    
    current_datetime() as updated_at

from price_rankings
order by product_name, price_rank
