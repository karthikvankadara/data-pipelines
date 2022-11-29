--query used in the blog -https://karthikvankadara.com/2022/11/19/generate-nested-json-using-bigquery/
with base as
(
    select
        Publisher
        ,Name
        ,Race
        ,Gender
        ,ARRAY_AGG(STRUCT(Height, Weight, SkinColor, EyeColor)) AS Attributes
    from `gcp-wow-project-name.dataset_name.marvel_characters_info`
    group by 1,2,3,4
)
select
    Publisher
    ,ARRAY_AGG(STRUCT(Name, Race, Gender, Attributes)) AS Superhero
from base
group by 1;