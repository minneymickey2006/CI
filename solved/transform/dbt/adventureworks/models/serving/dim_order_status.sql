with stg_order_status as (
    select distinct order_status as order_status
    from
        {{ ref('stg_salesorderheader') }}
),

order_status as (
    select
        {{ dbt_utils.surrogate_key(['stg_order_status.order_status']) }} as order_status_key,
        stg_order_status.order_status,
        case
            when stg_order_status.order_status = 1 then 'in_process'
            when stg_order_status.order_status = 2 then 'approved'
            when stg_order_status.order_status = 3 then 'backordered'
            when stg_order_status.order_status = 4 then 'rejected'
            when stg_order_status.order_status = 5 then 'shipped'
            when stg_order_status.order_status = 6 then 'cancelled'
            else 'no_status'
        end as order_status_name
    from stg_order_status
)

select *
from order_status
