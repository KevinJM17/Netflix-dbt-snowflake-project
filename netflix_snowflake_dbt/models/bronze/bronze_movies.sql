with bronze_movies as (
    select 
        movieId as movie_id,
        trim(title) as title,
        coalesce(try_to_number(regexp_substr(trim(title), '\\((\\d{4})\\)', 1, 1, 'e', 1)), -1) as release_year,
        genres,
        current_timestamp() as _loaded_at
    from {{ source('raw', 'raw_movies') }}
    where release_year != -1
)

select * from bronze_movies