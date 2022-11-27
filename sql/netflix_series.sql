insert into `{{ Project_Id }}.{{ Target_Dataset }}.netflix_series`
select *
from `{{ Project_Id }}.{{ Staging_Dataset }}.netflix_series_stg`