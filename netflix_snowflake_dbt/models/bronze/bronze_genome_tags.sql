{{ config(materialized='incremental') }}

select
    tagId as tag_id,
    tag
from {{ source('raw', 'raw_genome_tags') }}

{% if is_incremental() %}
    where tagId > (select coalesce(max(tag_id), 0) from {{ this }})
{% endif %}