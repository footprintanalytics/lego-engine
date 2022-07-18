merge `{{internal_project_id}}.{{dataset_name}}.{{destination_table_name}}` dest
using {{dataset_name_temp}}.{{source_table}} source_table
on false
when not matched and DATE(block_timestamp) >= DATE(DATE_SUB(timestamp('{{ds}}'), INTERVAL 3 DAY)) and DATE(block_timestamp) <= '{{ds}}' then
insert (
    {% for column in table_schema %}
    {% if loop.index0 > 0 %},{% endif %}`{{ column.name }}`
    {% endfor %}
) values (
    {% for column in table_schema %}
    {% if loop.index0 > 0 %},{% endif %}`{{ column.name }}`
    {% endfor %}
)
when not matched by source and DATE(block_timestamp) >= DATE(DATE_SUB(timestamp('{{ds}}'), INTERVAL 3 DAY)) and DATE(block_timestamp) <= '{{ds}}' then
delete
