{{ config(materialized='incremental') }}

with bronze_ratings as (
    select 
        movieId as movie_id,
        userId as user_id,
        rating,
        to_timestamp(timestamp) as rating_timestamp,
        current_timestamp() as _loaded_at
    from {{ source('raw', 'raw_ratings') }}

    {% if is_incremental()%}
        where to_timestamp(timestamp) > (select max(rating_timestamp) from {{ this }})
    {% endif %}
)

select * from bronze_ratings