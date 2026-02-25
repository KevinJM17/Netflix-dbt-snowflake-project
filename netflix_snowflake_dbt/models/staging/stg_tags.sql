{{ config(materialized='incremental') }}

with stg_tags as (
    select 
        userId as user_id,
        movieId as movie_id,
        lower(trim(regexp_replace(tag, '[^a-zA-Z0-9]', ' '))) as tag,
        to_timestamp(timestamp) as tag_timestamp,
        current_timestamp() as _loaded_at
    from {{ source('raw', 'raw_tags') }}

    {% if is_incremental() %}
        where to_timestamp(timestamp) > (select max(tag_timestamp) from {{ this }})
    {% endif %}
)

select * from stg_tags