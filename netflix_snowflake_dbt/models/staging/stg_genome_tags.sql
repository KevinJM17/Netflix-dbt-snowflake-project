with stg_genome_tags as (
    select
        tagId as tag_id,
        trim(tag) as tag,
        current_timestamp() as _loaded_at
    from {{ source('raw', 'raw_genome_tags') }}
    where tag_id is not null 
        and tag is not null
)

select * from stg_genome_tags
