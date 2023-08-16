{% macro create_3NF(src_hub,src_sat,src_pk,src_ldts,src_payload,src_delete_field=none,deleted_value=none) %}

{%- set src_payload = dbtvault.escape_column_names(src_payload) -%}
{%- if src_delete_field is not none and deleted_value is not none -%}
    {%- set src_payload_init = dbtvault.escape_column_names(src_payload + [src_delete_field]) -%}
{%- else -%}
    {%- set src_payload_init = dbtvault.escape_column_names(src_payload) -%}
{%- endif -%}

with source_data_init as (
    select distinct {{ dbtvault.alias_all(src_payload_init, 'sat') }}
    from {{ src_hub }} hub
    inner join {{ src_sat }} sat on hub.{{ src_pk }} = sat.{{ src_pk }}
    qualify row_number() over (partition by sat.{{ src_pk }} order by sat.{{ src_ldts }} desc) = 1
)
select {{ dbtvault.alias_all(src_payload) }}
from source_data_init
{% if src_delete_field is not none and deleted_value is not none -%}
    where {{ src_delete_field }} <> {{ deleted_value }}
{% endif %}

{% endmacro %}