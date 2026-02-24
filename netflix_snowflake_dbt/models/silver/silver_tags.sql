with tags_deduped as (
    select *
    from {{ ref("bronze_tags") }}
    qualify (row_number() over (partition by user_id, movie_id, tag order by tag_timestamp)) = 1
), genome_lookup as (
    select 
        gs.movie_id,
        gs.tag_id,
        lower(trim(regexp_replace(gt.tag, '[^a-zA-Z0-9]', ' '))) as tag,
        gs.relevance
    from {{ ref("bronze_genome_tags") }} gt
    join {{ ref("bronze_genome_scores") }} gs 
        on gt.tag_id = gs.tag_id
), tags_with_genome as (
    select 
        t.*,
        g.relevance
    from tags_deduped t
    left join genome_lookup g 
        on t.movie_id = g.movie_id 
        and t.tag = g.tag
), movie_tags_agg as (
    select 
        movie_id,
        tag,
        count(distinct user_id) as users_tagged,
        round(avg(relevance), 4) as avg_relevance
    from tags_with_genome
    group by 1, 2
    having avg(relevance) is not null
)

select 
    t.movie_id,
    t.tag,
    t.tag_timestamp,
    m.users_tagged,
    m.avg_relevance
from tags_with_genome t
join movie_tags_agg m 
    on t.movie_id = m.movie_id
    and t.tag = m.tag
qualify (row_number() over (partition by t.movie_id, t.tag order by t.tag_timestamp)) = 1 
order by 1