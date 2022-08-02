// This query reports the number of deaths and serious conditions reported by cases from the US.

CALL {
    MATCH (v:Vaccine)-[:ADMINISTERED]-(c:Case)-[:REPORTED_FROM]-(co:Country {CountryCode: 'US'})
    WHERE toUpper(v.GenericName) CONTAINS 'COVID'
    RETURN v.TradeName AS VaccineName, COUNT(DISTINCT c) AS TotalCases, 'AllReports' AS ReportType
    UNION 
    MATCH (v:Vaccine)-[:ADMINISTERED]-(c:Case)-[:REPORTED_FROM]-(co:Country {CountryCode: 'US'})
    WHERE toUpper(v.GenericName) CONTAINS 'COVID'
        AND c.PatientOutcome CONTAINS 'death'
    RETURN v.TradeName AS VaccineName, COUNT(DISTINCT c) AS TotalCases, 'FatalReports' AS ReportType
    UNION 
    MATCH (v:Vaccine)-[:ADMINISTERED]-(c:Case)-[:REPORTED_FROM]-(co:Country {CountryCode: 'US'})
    WITH v, c, [ 'death', 'hospitalization', 'er visit', 'life threatening', 'disabled' ] AS SeriousConditions
    WHERE toUpper(v.GenericName) CONTAINS 'COVID'
        AND ANY (seriousCondition in SeriousConditions WHERE c.PatientOutcome CONTAINS seriousCondition)
    RETURN v.TradeName AS VaccineName, COUNT(DISTINCT c) AS TotalCases, 'SeriousReports' AS ReportType
}
RETURN VaccineName, TotalCases, ReportType
ORDER BY VaccineName, ReportType