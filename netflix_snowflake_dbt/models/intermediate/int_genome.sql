with ranked_tags as (
    select 
        gs.movie_id,
        gs.tag_id,
        gt.tag,
        gs.relevance,
        dense_rank() over (partition by gs.movie_id order by gs.relevance desc) as relevance_rnk
    from {{ ref('stg_genome_scores') }} gs
    join {{ ref('stg_genome_tags') }} gt using (tag_id)
), top_tags_per_movie as (
    select *
    from ranked_tags
    where relevance_rnk <= 20
)

select * from top_tags_per_movie