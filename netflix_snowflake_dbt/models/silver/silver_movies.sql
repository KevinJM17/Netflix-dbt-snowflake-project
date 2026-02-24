with movies_rn as (
    select 
        movie_id,
        trim(title) as title,
        release_year,
        split(genres, '|') as genres,
        _loaded_at,
        row_number() over (partition by title order by _loaded_at desc) as rn
    from {{ ref("bronze_movies") }}
), movies_deduped as (
    select *
    from movies_rn
    where rn = 1
    order by movie_id
), movies_genres as (
    select 
        movie_id,
        title,
        release_year,
        trim(g.value::varchar) as genre,
        _loaded_at
    from movies_deduped,
    -- similar to explode in pyspark
    lateral flatten(input => genres, outer => true) g
)

select 
    m.*,
    l.imdb_id,
    l.tmdb_id
from movies_genres m
left join {{ ref("bronze_links") }} l 
    on m.movie_id = l.movie_id