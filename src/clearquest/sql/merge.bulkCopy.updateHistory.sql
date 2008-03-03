UPDATE
    ${dstPrefix}.history
SET
    entity_dbid = entity_dbid + ${dbidOffset},
    ratl_mastership = NULL
FROM
    ${dstTable} dst,
    ${dstPrefix}.entitydef e 
WHERE
    ${dstPrefix}.history.entitydef_name = e.name AND
    ${dstPrefix}.history.ratl_mastership = ${sourceReplicaId} AND
    dst.dbid = ${dstPrefix}.history.entity_dbid + ${dbidOffset} AND
    dst.ratl_mastership = ${sourceReplicaId} AND
    e.name = '${entityDefName}' AND
    e.id = entitydef_id