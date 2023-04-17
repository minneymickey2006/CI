with fct_sales as (
    select * from {{ ref('fct_sales') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

dim_credit_cards as (
    select * from {{ ref('dim_credit_cards') }}
),

dim_address as (
    select * from {{ ref('dim_address') }}
),

dim_order_status as (
    select * from {{ ref('dim_order_status') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

obt as (
    select
        {{ dbt_utils.star(from=ref('fct_sales'), relation_alias='fct_sales', except=[
            "sales_key", "product_key", "customer_key", "creditcard_key", "ship_address_key", "order_status_key", "order_date_key"
            ]) }},
        {{ dbt_utils.star(from=ref('dim_products'), relation_alias='dim_products', except=["product_key"]) }},
        {{ dbt_utils.star(from=ref('dim_customers'), relation_alias='dim_customers', except=["customer_key"]) }},
        {{ dbt_utils.star(from=ref('dim_credit_cards'), relation_alias='dim_credit_cards', except=["creditcard_key"]) }},
        {{ dbt_utils.star(from=ref('dim_address'), relation_alias='dim_address', except=["address_key"]) }},
        {{ dbt_utils.star(from=ref('dim_order_status'), relation_alias='dim_order_status', except=["order_status_key"]) }},
        {{ dbt_utils.star(from=ref('dim_date'), relation_alias='dim_date', except=["date_key"]) }}
    from fct_sales
    left join dim_products on fct_sales.product_key = dim_products.product_key
    left join
        dim_customers on fct_sales.customer_key = dim_customers.customer_key
    left join
        dim_credit_cards on
            fct_sales.creditcard_key = dim_credit_cards.creditcard_key
    left join
        dim_address on fct_sales.ship_address_key = dim_address.address_key
    left join
        dim_order_status on
            fct_sales.order_status_key = dim_order_status.order_status_key
    left join dim_date on fct_sales.order_date_key = dim_date.date_key
)

select *
from obt
