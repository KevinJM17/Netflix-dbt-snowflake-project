select
    tagId as tag_id,
    tag
from {{ source('raw', 'raw_genome_tags') }}