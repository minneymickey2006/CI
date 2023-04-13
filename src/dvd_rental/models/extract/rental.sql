{% set config = {
    "extract_type": "incremental", 
    "incremental_column": "last_update",
    "key_columns":["rental_id"]
} %}

select 
    rental_id,
    rental_date, 
    inventory_id, 
    customer_id, 
    return_date, 
    staff_id, 
    last_update 
from 
    {{ source_table }}

{% if is_incremental %}
    where last_update > '{{ incremental_value }}'
{% endif %}
