
-- Filter external-access events by date range in Lakehouse SQL (Fabric)
-- Adjust table and field names to match your export schema

SELECT Timestamp, Actor, Operation, WorkspaceId, ItemId
FROM activityEventEntities
WHERE Operation IN (
    'AcceptExternalDataShare',
    'AddExternalResource',
    'AddLinkToExternalResource',
    'AnalyzedByExternalApplication'
)
AND Timestamp BETWEEN '2025-11-01' AND '2025-12-02'
ORDER BY Timestamp DESC;

-- Summaries
SELECT Actor, COUNT(*) AS ExternalAccessCount
FROM activityEventEntities
WHERE Operation IN (
    'AcceptExternalDataShare',
    'AddExternalResource',
    'AddLinkToExternalResource',
    'AnalyzedByExternalApplication'
)
AND Timestamp BETWEEN '2025-11-01' AND '2025-12-02'
GROUP BY Actor
ORDER BY ExternalAccessCount DESC;

SELECT Operation, COUNT(*) AS AccessCount
FROM activityEventEntities
WHERE Operation IN (
    'AcceptExternalDataShare',
    'AddExternalResource',
    'AddLinkToExternalResource',
    'AnalyzedByExternalApplication'
)
AND Timestamp BETWEEN '2025-11-01' AND '2025-12-02'
GROUP BY Operation
ORDER BY AccessCount DESC;
