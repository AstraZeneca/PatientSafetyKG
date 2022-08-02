// Sample for visualizing the graph after applying filters
// This query filters cases by:
// 1. Moderna vaccine
// 2. Patients over the age of 45
// 3. From Germany, France or Italy
// And returns the sub-graph.

MATCH (v:Vaccine)-[:ADMINISTERED]-(c:Case)-[:REPORTED_FROM]-(co:Country)
MATCH (c)-[:REPORTED_AE]-(m:MeddraTerm)
WHERE toLower(v.TradeName) CONTAINS 'moderna'
    AND toLower(c.PatientOutcome) CONTAINS 'fatal'
    AND c.PatientAgeRangeMin >=45
    AND co.CountryCode IN ['DE', 'FR', 'IT']
RETURN * LIMIT 50