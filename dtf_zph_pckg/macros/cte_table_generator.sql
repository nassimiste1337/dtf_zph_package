{% macro cte_table_generator(table, source_name='RAW_JDE') %}
{{table}} AS (
  SELECT 
    {{ dbt_utils.star(source(source_name, table + '_HUB'), except=['RUNNO_INSERT','RUNNO_UPDATE'], relation_alias=table + '_HUB') | replace("\n ", "") }},
    {{ dbt_utils.star(source(source_name, table + '_S01'), except=['RUNNO_INSERT','RUNNO_UPDATE','ID'], relation_alias=table + '_S01') | replace("\n ", "") }}
  FROM 
    {{source(source_name,table + '_HUB')}} {{table}}_HUB 
    INNER JOIN {{source(source_name,table + '_S01')}} {{table}}_S01 ON {{table}}_HUB.ID = {{table}}_S01.ID
    where {{table}}_HUB.ID <> 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{table}}_S01.ID ORDER BY {{table}}_S01.FD DESC) = 1 
)
{%- endmacro -%}
