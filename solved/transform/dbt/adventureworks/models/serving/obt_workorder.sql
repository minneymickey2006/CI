with fct_workorder as (
    select * from {{ ref('fct_workorder') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
),

dim_scrapreason as (
    select * from {{ ref('dim_scrapreason') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

obt as (
    select
        {{ dbt_utils.star(from=ref('fct_workorder'), relation_alias='fct_workorder', except=[
            "workorder_key", "product_key", "scrapreason_key", "start_date_key", "end_date_key", "due_date_key"
            ]) }},
        {{ dbt_utils.star(from=ref('dim_products'), relation_alias='dim_products', except=["product_key"]) }},
        {{ dbt_utils.star(from=ref('dim_scrapreason'), relation_alias='dim_scrapreason', except=["scrapreason_key"]) }},
        start_date.date_day as start_date,
        end_date.date_day as end_date,
        due_date.date_day as due_date
    from fct_workorder
    left join
        dim_products on fct_workorder.product_key = dim_products.product_key
    left join
        dim_scrapreason on
            fct_workorder.scrapreason_key = dim_scrapreason.scrapreason_key
    left join
        dim_date as start_date on
            fct_workorder.start_date_key = start_date.date_key
    left join
        dim_date as end_date on fct_workorder.end_date_key = end_date.date_key
    left join
        dim_date as due_date on fct_workorder.due_date_key = due_date.date_key
)

select *
from obt
