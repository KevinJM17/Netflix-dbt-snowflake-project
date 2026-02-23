with bronze_genome_scores as (
    select 
        movieId as movie_id,
        tagId as tag_id,
        round(relevance, 4) as relevance,
        current_timestamp() as _loaded_at
    from {{ source('raw', 'raw_genome_scores') }}
    where movie_id is not null 
        and tag_id is not null
        and relevance is not null
        and relevance > 0.01
)

select * from bronze_genome_scores