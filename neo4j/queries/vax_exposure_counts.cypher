// The query filters all exposures for COVID vaccines before 2021-04-12 and groups by country.

MATCH (v:Vaccine)-[:HAS]->(e:ExposureData)<-[:HAS]-(c:Country)
WHERE toUpper(v.TradeName) CONTAINS 'COVID'
    AND e.EndDate <= datetime('2021-04-12')
WITH v, e, c
ORDER BY v.TradeName, e.EndDate DESC
WITH v.TradeName AS VaxName,
    collect({
        date: e.EndDate,
        vaxCount: e.Count,
        expCountry: c.CountryCode
    })[0] AS argmax
RETURN VaxName AS VaccineName, argmax.date AS Date, argmax.vaxCount AS VaccineCount, argmax.expCountry AS CountryCode
ORDER BY VaxName