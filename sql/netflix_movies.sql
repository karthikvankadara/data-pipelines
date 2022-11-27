insert into `{{ Project_Id }}.{{ Target_Dataset }}.netflix_movies`
select 
 Title
 ,Genre
 ,PARSE_DATE("%d/%m/%Y", Premiere) AS Premiere
 ,Runtime
 ,Language
from `{{ Project_Id }}.{{ Staging_Dataset }}.netflix_movies_stg`