INSERT INTO ${dstTable} (
    ${dstColumns}
)
SELECT
    ${srcColumns}
FROM
    ${srcTable} src
${where}