with src_tags as (
    select * from MOVIELENS.DEV.src_genome_tags
)
select TAG_ID,
       initcap(trim(tag)) as tag_name from src_tags