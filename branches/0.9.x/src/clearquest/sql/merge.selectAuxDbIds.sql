SELECT
    dbid,
   (SELECT id FROM entitydef WHERE name = '%s')
FROM
    %s
WHERE
    dbid <> 0