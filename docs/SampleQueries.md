# PSKG Sample Queries

This is a collection of sample queries to demonstrate the capabilities of the cypher query language to interrogate the graph.

**NOTICE**: If you edit or save query query text in tools like Microsoft Note, be aware that unicode whitespace may be introduced
if you paste the query text into the Neo4J browser interface.  Unicode white space will cause random syntax errors, or 
cause string matches on quoted items with spaces to fail.

## Identify COVID 19 vaccines

```
MATCH (v:Vaccine {VaxType:'COVID19'}) RETURN v
```

## Identify Revised EudraVigilance Cases
Show cases having at least two revisions.

```
MATCH p=((c:Case {DataSource:'EUDRAVIGILANCE'})-[:PREVIOUS_VERSION *2..]->(pc:Case))
RETURN c, p, pc 
```

## MedDRA Custom Query
Show EudraVigilance cases involving Guillain-Barré Syndrome, using an AZ defined set of Meddra PTs. 
```
MATCH (mcq:MeddraCq {Abbreviation:'GBS'})-[:MEDDRA_CQ_CONTAINS]->(m:MeddraPT)<-[:REPORTED_AE]-(c:Case {DataSource:'EUDRAVIGILANCE'})
WHERE apoc.coll.intersection(c.PatientOutcome,
        ['prolonged hospitalization',
        'birth defect',
        'disabled',
        'death',
        'life threatening',
        'other medically important condition']) <> []
MATCH (c)-[:ADMINISTERED]->(vx:Vaccine {VaxType:'COVID19'})
RETURN count(c) AS Cases,vx.GenericName As TradeName
```

## Fatal Cases involving COVID19 Vaccines Worldwide for TEE 
Use AZ's definition of thrombo-embolic event (TEE) to identify fatal cases by country

```
MATCH (v:Vaccine {VaxType:'COVID19'})-[:ADMINISTERED]-(c:Case)-[:REPORTED_FROM]-(co:Country)-[:HAS]-(e:ExposureData)
MATCH (c)-[:REPORTED_AE]-(m:MeddraPT)-[:MEDDRA_SMQ_CONTAINS]-(:MeddraSmq)-[:MEDDRA_SMQ_CONTAINS]-(:MeddraSmq {Name: 'Embolic and thrombotic events (SMQ)'})
WHERE 'death' in c.PatientOutcome
RETURN v.TradeName as Vaccine,  co.Name as Country, COUNT(DISTINCT c) as FatalCases
ORDER BY FatalCases DESC, v.TradeName, co.Name
```

## Determine Available Stratifications for Exposure data
Show stratifications for available exposure data, organized by Vaccine

```
MATCH (ex:ExposureData)<-[:HAS]-(c:Country)
MATCH (ex)<-[:HAS]-(v:Vaccine)
WITH c, ex, v, keys(ex) AS ats
UNWIND (ats) AS attributes
WITH c, ex, v, attributes
WHERE NOT attributes IN ['ExposureId','DataSource','StartDate','EndDate']
WITH c,ex,v, COLLECT(DISTINCT attributes) AS Stratifications
RETURN DISTINCT v.TradeName AS Vaccine, c.Name as Country, ex.DataSource as Source, Stratifications
```

## Rotavirus Query

Identify the number of cases involving the Meddra Preferred Term Intussusception and children under 2 years old.  Stratify by
data sources, sex, and summarize min/max vaccination dates and number of cases.

```
MATCH (v:Vaccine)<-[a:ADMINISTERED]-(c2:Case)-[:REPORTED_AE]->(m:MeddraPT {Name:'Intussusception'}) 
WHERE toLower(v.GenericName) =~ ".*rotashield.*" and c2.PatientAgeRangeMax <= 2
RETURN c2.DataSource, c2.PatientGender as Sex, MIN(a.VaccineDate) as MinVaxDate, MAX(a.VaccineDate) as MaxVaxDate, COUNT(c2) as Cases
```

## Multiple Vaccinations

Identify Cases receiving more than one vaccine dose, ordered by the number of suspect/concomittant drugs administered.
```
MATCH (c:Case {DataSource:'EUDRAVIGILANCE'})-[:ADMINISTERED]->(v:Vaccine {VaxType:'COVID19'}) 
WITH c,COUNT(v) as vaccinations 
MATCH (c)-[av:ADMINISTERED] -> (v2:Vaccine) 
WHERE vaccinations > 1 RETURN c.CaseId as CaseId, c.ReceivedDate as Received, av.Characterization AS Characterization, v2.GenericName AS GenericName
ORDER BY vaccinations DESC, c.CaseId 
LIMIT 50
```