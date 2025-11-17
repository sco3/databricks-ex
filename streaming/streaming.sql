CREATE OR REFRESH STREAMING TABLE dz.dz.data_streaming 
SCHEDULE every 1 HOUR
as 
select * 
from stream read_files ('dbfs:/Volumes/dz/dz/dz-vol-streaming', format=>'parquet');