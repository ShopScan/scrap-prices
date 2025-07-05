-- models/marts/mart_price_analytics.sql
-- Analytics table for price analysis

{{ config(materialized='table') }}

with daily_prices as (
    
    select
        clean_product_name,
        clean_store_name,
        category,
        scraped_date,
        avg(price_numeric) as avg_daily_price,
        min(price_numeric) as min_daily_price,
        max(price_numeric) as max_daily_price,
        count(*) as price_observations
    
    from {{ ref('stg_raw_prices') }}
    group by 1, 2, 3, 4

),

price_trends as (
    
    select
        *,
        lag(avg_daily_price) over (
            partition by clean_product_name, clean_store_name 
            order by scraped_date
        ) as prev_day_price,
        
        avg(avg_daily_price) over (
            partition by clean_product_name, clean_store_name 
            order by scraped_date 
            rows between 6 preceding and current row
        ) as avg_7day_price
        
    from daily_prices

)

select
    clean_product_name as product_name,
    clean_store_name as store_name,
    category,
    scraped_date,
    avg_daily_price,
    min_daily_price,
    max_daily_price,
    price_observations,
    prev_day_price,
    avg_7day_price,
    
    -- Price change calculations
    case 
        when prev_day_price is not null 
        then round((avg_daily_price - prev_day_price) / prev_day_price * 100, 2)
        else null 
    end as daily_price_change_pct,
    
    case 
        when avg_7day_price is not null 
        then round((avg_daily_price - avg_7day_price) / avg_7day_price * 100, 2)
        else null 
    end as vs_7day_avg_change_pct,
    
    current_datetime() as updated_at

from price_trends
order by scraped_date desc, product_name, store_name
