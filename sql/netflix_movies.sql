insert into `{{ Project_Id }}.{{ Target_Dataset }}.netflix_movies`
select *
from `{{ Project_Id }}.{{ Staging_Dataset }}.netflix_movies_stg`