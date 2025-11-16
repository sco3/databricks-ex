COPY INTO dz.dz.data
FROM 'dbfs:/Volumes/dz/dz/dz-vol/data.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')
