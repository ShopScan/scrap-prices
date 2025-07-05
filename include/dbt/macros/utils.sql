-- macros/get_current_timestamp.sql
-- Macro to get current timestamp consistently

{% macro get_current_timestamp() %}
    current_datetime()
{% endmacro %}

-- Macro to clean product names
{% macro clean_product_name(product_name_field) %}
    trim(upper(regexp_replace({{ product_name_field }}, r'[^\w\s]', '')))
{% endmacro %}

-- Macro to generate price change calculation
{% macro calculate_price_change(current_price, previous_price) %}
    case 
        when {{ previous_price }} is not null and {{ previous_price }} > 0
        then round(({{ current_price }} - {{ previous_price }}) / {{ previous_price }} * 100, 2)
        else null 
    end
{% endmacro %}
