UPDATE
    ${dstPrefix}.parent_child_links
SET
    ${scope}_dbid = (${newDbId}),
    ratl_mastership = NULL
FROM
    ${dstTable} dst
INNER JOIN
    ${dstPrefix}.entitydef e ON
        e.name = '${entityDefName}'
INNER JOIN
    ${dstPrefix}.fielddef f ON
        f.name = '${fieldDefName}' AND
        f.entitydef_id = e.id
WHERE
    ${scope}_dbid <> 0 AND
    e.id = ${scope}_entitydef_id AND
    dst.dbid = (${newDbId}) AND
    ${dstPrefix}.parent_child_links.ratl_mastership = ${sourceReplicaId}
GO