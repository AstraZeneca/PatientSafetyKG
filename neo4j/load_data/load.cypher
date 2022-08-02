// Tested with Neo4J 4.4.4
// Node key unique attributes

RETURN "Creating constraints..." AS `Action:`;
CREATE CONSTRAINT caseIdConstraint          IF NOT EXISTS ON (c:Case)         ASSERT c.CaseId         IS UNIQUE;
CREATE CONSTRAINT vaxIdConstraint           IF NOT EXISTS ON (v:Vaccine)      ASSERT v.VaccineId      IS UNIQUE;
CREATE CONSTRAINT medIdConstraint           IF NOT EXISTS ON (m:Medication)   ASSERT m.MedicationId   IS UNIQUE;
CREATE CONSTRAINT expIdConstraint           IF NOT EXISTS ON (e:ExposureData) ASSERT e.ExposureId     IS UNIQUE;
CREATE CONSTRAINT countryCodeConstraint     IF NOT EXISTS ON (c:Country)      ASSERT c.CountryCode    IS UNIQUE;
CREATE CONSTRAINT continentCodeConstraint   IF NOT EXISTS ON (c:Continent)    ASSERT c.ContinentCode  IS UNIQUE;
CREATE CONSTRAINT meddraSMQConstraint       IF NOT EXISTS ON (m:MeddraSmq)    ASSERT m.MeddraSmqCode  IS UNIQUE;
CREATE CONSTRAINT meddraLLTIdConstraint     IF NOT EXISTS ON (m:MeddraLLT)    ASSERT m.MeddraId       IS UNIQUE;
CREATE CONSTRAINT meddraPTIdConstraint      IF NOT EXISTS ON (m:MeddraPT)     ASSERT m.MeddraId       IS UNIQUE;
CREATE CONSTRAINT meddraHLTIdConstraint     IF NOT EXISTS ON (m:MeddraHLT)    ASSERT m.MeddraId       IS UNIQUE;
CREATE CONSTRAINT meddraHLGTdConstraint     IF NOT EXISTS ON (m:MeddraHLGT)   ASSERT m.MeddraId       IS UNIQUE;
CREATE CONSTRAINT meddraSOCIdConstraint     IF NOT EXISTS ON (m:MeddraSOC)    ASSERT m.MeddraId       IS UNIQUE;
CREATE CONSTRAINT MeddraCqConstraint        IF NOT EXISTS ON (azm: MeddraCq)  ASSERT azm.Name         IS UNIQUE;

// Nodes

RETURN "Loading Case.tsv..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Case.tsv" AS r
FIELDTERMINATOR '\t'
MERGE (c:Case {
    CaseId: r.CaseId})
SET
    c.SourceCaseId       = r.SourceCaseId,
    c.DataSource         = r.DataSource,
    c.Tag                = r.Tag,
    c.ReportedDate       = datetime(r.ReportedDate),
    c.ReceivedDate       = datetime(r.ReceivedDate),
    c.PatientAgeRangeMin = toFloat(r.PatientAgeRangeMin),
    c.PatientAgeRangeMax = toFloat(r.PatientAgeRangeMax),
    c.PatientGender      = r.PatientGender,
    c.PatientOutcome     = split(r.PatientOutcome,","),
    c.PatientRecovered   = toBoolean(r.PatientRecovered),
    c.DeathDate          = datetime(r.DeathDate),
    c.HospitalizationLengthInDays = toInteger(r.HospitalizationLengthInDays),
    c.ReportType         = r.ReportType,
    c.Current            = true;

RETURN "Create Indexes on Case" AS `Action:`;
CREATE INDEX FOR (c:Case) on (c.DataSource);
CREATE INDEX FOR (c:Case) on (c.SourceCaseId);

// Previous Case Versions
// Some data sources, such as EudraVigilance, include the concept of updated cases.
// This statement identifies these cases and uses an APOC function to build 
// a linked list of starting from the current case through each prior case.
// Each previous version's Current flag is set to false.

RETURN "Build Links to Previous Case Versions" AS `Action:`;

MATCH (c:Case {DataSource:'EUDRAVIGILANCE'}) 
WITH c, c.SourceCaseId AS GroupId 
ORDER BY GroupId, c.CaseId DESC
WITH COLLECT(c) AS CaseNodes, COUNT(c) AS GroupSize, GroupId
WHERE GroupSize > 1
CALL apoc.nodes.link(CaseNodes,'PREVIOUS_VERSION')
RETURN COUNT(*) AS `Revised Cases`;

RETURN "Set flag on previous versions" as `Action:`;

MATCH (cc:Case {DataSource:'EUDRAVIGILANCE'})-[:PREVIOUS_VERSION *..]->(pc:Case)
SET pc.Current = False;

// Vaccine.tsv
// NOTE:
// VAERS data are used to set initial names for vaccines, as it is currently the most
// comprehensive source of vaccine names.  Thus a CASE statement with ON MATCH/ON CREATE
// clauses is used to not reset previously set values during import, and a NULL/not empty 
// check is needed for this.
// Moving forward names from sources will be validated and aligned to a common terminology,
// which will eliminate the need for this prioritization.

RETURN "Loading Vaccine.tsv..." AS `Action:`;
LOAD CSV WITH HEADERS FROM "file:///Vaccine.tsv" AS r
FIELDTERMINATOR '\t'
MERGE (v:Vaccine {
    VaccineId: r.VaccineId})
ON CREATE
    SET  v.VaxType      = TRIM(r.VaxType),
         v.RxNormCui    = TRIM(r.RxNormCui),
         v.GenericName  = TRIM(r.GenericName),
         v.TradeName    = TRIM(r.TradeName),
         v.Manufacturer = TRIM(r.Manufacturer),
         v.Description  = TRIM(r.Description)
ON MATCH
    SET  v.GenericName  =  CASE WHEN (v.GenericName IS NULL OR v.GenericName = '') 
                                     AND r.GenericName IS NOT NULL 
                                     AND TRIM(r.GenericName) <> '' 
                                THEN TRIM(r.GenericName) 
                                ELSE v.GenericName 
                           END,
         v.Manufacturer =  CASE WHEN (v.TradeName IS NULL OR v.TradeName = '') 
                                      AND r.TradeName IS NOT NULL 
                                      AND TRIM(r.TradeName) <> '' 
                                THEN TRIM(r.TradeName) 
                                ELSE v.TradeName 
                           END,
         v.Manufacturer =  CASE WHEN (v.Manufacturer IS NULL OR v.Manufacturer = '') 
                                      AND r.Manufacturer IS NOT NULL 
                                      AND TRIM(r.Manufacturer) <> '' 
                                THEN TRIM(r.Manufacturer) 
                                ELSE v.Manufacturer 
                           END,
         v.VaxType      =  CASE WHEN (v.VaxType IS NULL OR v.VaxType = '') 
                                      AND r.VaxType IS NOT NULL 
                                      AND TRIM(r.VaxType) <> '' 
                                THEN TRIM(r.VaxType) 
                                ELSE v.VaxType
                           END,
         v.RxNormCui     = CASE WHEN (v.RxNormCui IS NULL OR v.RxNormCui = '' ) 
                                      AND r.RxNormCui IS NOT NULL 
                                      AND TRIM(r.RxNormCui) <> '' 
                                THEN TRIM(r.RxNormCui) 
                                ELSE v.RxNormCui 
                           END,
         v.Description =   CASE WHEN (v.Description IS NULL OR v.Description = '') 
                                      AND r.Description IS NOT NULL AND TRIM(r.Description) <> '' 
                                THEN TRIM(r.Description) 
                                ELSE v.Description 
                           END;


RETURN "Loading Medication.tsv..." AS `Action:`;
LOAD CSV WITH HEADERS FROM "file:///Medication.tsv" AS r
FIELDTERMINATOR '\t'
MERGE (m:Medication {
    MedicationId: r.MedicationId})
SET
    m.RxNormCui   = r.RxNormCui,
    m.GenericName = r.GenericName,
    m.TradeName   = r.TradeName,
    m.Description = r.Description;


RETURN "Loading Country.tsv..." AS `Action:`;
LOAD CSV WITH HEADERS FROM "file:///Country.tsv" AS r
FIELDTERMINATOR '\t'
MERGE (c:Country {
    CountryCode: r.CountryCode})
SET
    c.Name                      = r.Name,
    c.Population                = toInteger(r.Population),
    c.AgeDistribution           = r.AgeDistribution,
    c.SocioEconomicDistribution = r.SocioEconomicDistribution,
    c.GenderDistribution        = r.GenderDistribution,
    c.RacialDistribution        = r.RacialDistribution,
    c.InformationDate           = datetime(r.InformationDate);

RETURN "Loading Continent.tsv..." AS `Action:`;
LOAD CSV WITH HEADERS FROM "file:///Continent.tsv" AS r
FIELDTERMINATOR '\t'
MERGE (c:Continent {
    ContinentCode: r.ContinentCode})
SET
    c.Name = r.Name;

RETURN "Loading ExposureData.tsv..." AS `Action:`;
LOAD CSV WITH HEADERS FROM "file:///ExposureData.tsv" AS r
FIELDTERMINATOR '\t'
MERGE (e:ExposureData {
    ExposureId: r.ExposureId})
SET
    e.DataSource        = r.DataSource,
    e.StartDate         = datetime(r.StartDate),
    e.EndDate           = datetime(r.EndDate),
    e.GroupAgeMin       = toFloat(r.GroupAgeMin),
    e.GroupAgeMax       = toFloat(r.GroupAgeMax),
    e.GroupGender       = r.GroupGender,
    e.GroupRace         = r.GroupRace,
    e.GroupCondition    = r.GroupCondition,
    e.Count             = toInteger(r.Count),
    e.DoseIdentifier    = r.DoseIdentifier,
    e.SubRegion         = r.SubRegion;

RETURN "Loading MeddraSmq.tsv..." AS `Action:`;
LOAD CSV WITH HEADERS FROM "file:///MeddraSmq.tsv" AS r
FIELDTERMINATOR '\t'
MERGE (m:MeddraSmq {
    MeddraSmqCode: toInteger(r.MeddraSmqCode)})
SET
    m.Name            = r.Name,
    m.SmqLevel        = r.MeddraSmqLevel,
    m.SmqDescription  = r.MeddraSmqDescription,
    m.SmqSource       = r.MeddraSmqSource,
    m.SmqNote         = r.MeddraSmqNote,
    m.SmqVersion      = r.MeddraSmqVersion,
    m.SmqAlgorithm    = r.MeddraSmqAlgorithm,
    m.SmqStatus       = r.MeddraSmqStatus;

// Create MedDRA nodes
// A single TSV file is created by the import process
// and contains information on all MedDRA object types (i.e.
// SOC, HGLT, HLT, PT, LLT). Cypher does not contain
// the concept of setting a label dyamically, so
// FOREACH statements are used to create each distinct
// MedDRA label 
// 
RETURN "Loading MeddraTerm.tsv..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///MeddraTerm.tsv" AS r
FIELDTERMINATOR '\t'
FOREACH ( ignoreMe IN
CASE
WHEN r.MeddraType = 'LLT' THEN [1] ELSE [] END | 
    MERGE (m:MeddraLLT {
        MeddraId: r.MeddraId})
    SET
        m.MeddraCode    = toInteger(r.MeddraCode),
        m.MeddraType    = r.MeddraType,
        m.Name          = r.Name,
        m.MeddraVersion = r.MeddraVersion
)
FOREACH ( ignoreMe IN
CASE
WHEN r.MeddraType = 'PT' THEN [1] ELSE [] END |
    MERGE (m:MeddraPT {
        MeddraId: r.MeddraId})
    SET
        m.MeddraCode    = toInteger(r.MeddraCode),
        m.MeddraType    = r.MeddraType,
        m.Name          = r.Name,
        m.MeddraVersion = r.MeddraVersion
)
FOREACH ( ignoreMe IN
CASE
WHEN r.MeddraType = 'HLT' THEN [1] ELSE [] END |
    MERGE (m:MeddraHLT {
        MeddraId: r.MeddraId})
    SET
        m.MeddraCode    = toInteger(r.MeddraCode),
        m.MeddraType    = r.MeddraType,
        m.Name          = r.Name,
        m.MeddraVersion = r.MeddraVersion
)
FOREACH ( ignoreMe IN
CASE
WHEN r.MeddraType = 'HLGT' THEN [1] ELSE [] END |
    MERGE (m:MeddraHLGT {
        MeddraId: r.MeddraId})
    SET
        m.MeddraCode    = toInteger(r.MeddraCode),
        m.MeddraType    = r.MeddraType,
        m.Name          = r.Name,
        m.MeddraVersion = r.MeddraVersion
)
FOREACH ( ignoreMe IN 
CASE
WHEN r.MeddraType = 'SOC' THEN [1] ELSE [] END |
    MERGE (m:MeddraSOC {
        MeddraId: r.MeddraId})
    SET
        m.MeddraCode    = toInteger(r.MeddraCode),
        m.MeddraType    = r.MeddraType,
        m.Name          = r.Name,
        m.MeddraVersion = r.MeddraVersion
);

RETURN "Creating Index on MeddraPT [Name,MeddraType]" AS `Action:`;

CREATE INDEX FOR (m:MeddraPT) on (m.Name, m.MeddraType);

RETURN "Loading CaseGroup.tsv..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///CaseGroup.tsv" AS r
FIELDTERMINATOR '\t'
MERGE (cg:CaseGroup {
    CaseGroupId: r.CaseGroupId})
SET
    cg.Name         = r.Name,
    cg.Abbreviation = r.Abbreviation,
    cg.Description  = r.Description;


// Edges

RETURN "Loading CasePrescribedMedication.tsv..." AS `Action:`;
LOAD CSV WITH HEADERS FROM "file:///CasePrescribedMedication.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (c:Case {
    CaseId: r.CaseId})
MATCH (m:Medication {
    MedicationId: r.MedicationId})
MERGE (c)-[p:PRESCRIBED]->(m)
SET
    p.StartDate         = datetime(r.StartDate),
    p.StopDate          = datetime(r.StopDate),
    p.Route             = r.Route,
    p.Duration          = CASE WHEN toFloat(r.Duration) > 0 THEN toFloat(r.Duration) ELSE NULL END,
    p.Dosage            = toFloat(r.Dosage),
    p.Units             = r.Units,
    p.Evidence          = r.Evidence,
    p.Characterization  = r.Characterization;

RETURN "Loading CasePrescribedMedication.tsv (for Indication)..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///CasePrescribedMedication.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (c:Case {
    CaseId: r.CaseId})
MATCH (m:MeddraPT { Name:r.Indication, MeddraType:'PT'})
MERGE (c)-[:MEDICATED_FOR_INDICATION]->(m);

RETURN "Loading CaseAdministeredVaccine.tsv..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///CaseAdministeredVaccine.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (c:Case {
    CaseId: r.CaseId})
MATCH (v:Vaccine {
    VaccineId: r.VaccineId})
MERGE (c)-[a:ADMINISTERED]->(v)
SET
    a.VaccineDate       = datetime(r.VaccineDate),
    a.VaccineLot        = r.VaccineLot,
    a.VaccineRoute      = r.VaccineRoute,
    a.VaccineSite       = r.VaccineSite,
    a.Dosage            = r.Dosage,
    a.Duration          = CASE WHEN toFloat(r.Duration) > 0 THEN toFloat(r.Duration) ELSE NULL END,
    a.Characterization  = r.Characterization;

RETURN "Loading CaseAdministeredVaccine.tsv (for Indication)..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///CaseAdministeredVaccine.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (c:Case {
    CaseId: r.CaseId})
MATCH (m:MeddraPT {Name:r.Indication, MeddraType:'PT'})
MERGE (c)-[:VACCINATED_FOR_INDICATION]->(m);

RETURN "Loading CaseReportedFromCountry.tsv..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///CaseReportedFromCountry.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (c:Case {
    CaseId: r.CaseId})
MATCH (co:Country {
    CountryCode: r.Country})
MERGE (c)-[e:REPORTED_FROM]->(co)
SET
    e.SubRegion = r.SubRegion;

RETURN "Loading ContainsCase.tsv..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///ContainsCase.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (c:Case {
    CaseId: r.CaseId})
MATCH (cg:CaseGroup {
    CaseGroupId: r.CaseGroupId})
MERGE (cg)-[:CONTAINS_CASE]->(c);

RETURN "Loading CaseReportedAEMeddraTerm.tsv..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///CaseReportedAEMeddraTerm.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (c:Case {
    CaseId: r.CaseId})
MATCH (m:MeddraPT {
    Name: r.MeddraTerm, MeddraType:'PT'})
MERGE (c)-[e:REPORTED_AE]->(m)
SET
    e.OnsetDate     = datetime(r.OnsetDate),
    e.LengthInDays  = toInteger(r.LengthInDays);

RETURN "Loading CountryInContinent.tsv..." AS `Action:`;
LOAD CSV WITH HEADERS FROM "file:///CountryInContinent.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (co:Country {
    CountryCode: r.CountryCode})
MATCH (c:Continent {
    ContinentCode: r.ContinentCode})
MERGE (co)-[:IN]->(c);

RETURN "Loading CountryHasExposureData.tsv..." AS `Action:`;
LOAD CSV WITH HEADERS FROM "file:///CountryHasExposureData.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (co:Country {
    CountryCode: r.CountryCode})
MATCH (e:ExposureData {
    ExposureId: r.ExposureId})
MERGE (co)-[:HAS]->(e);

RETURN "Loading VaccineHasExposureData.tsv..." AS `Action:`;
LOAD CSV WITH HEADERS FROM "file:///VaccineHasExposureData.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (v:Vaccine {
    VaccineId: r.VaccineId})
MATCH (e:ExposureData {
    ExposureId: r.ExposureId})
MERGE (v)-[:HAS]->(e);

RETURN "Loading MeddraOntology.tsv (LLTs -> PTs)..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///MeddraOntology.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (m_llt_f:MeddraLLT {
    MeddraId: r.MeddraIdFrom})
MATCH (m_pt_t:MeddraPT {
    MeddraId: r.MeddraIdTo})
MERGE (m_llt_f)-[:MEDDRA_LINK]->(m_pt_t);

RETURN "Loading MeddraOntology.tsv (PTs -> HLTs)..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///MeddraOntology.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (m_pt_f:MeddraPT {
    MeddraId: r.MeddraIdFrom})
MATCH (m_hlt_t:MeddraHLT {
    MeddraId: r.MeddraIdTo})
MERGE (m_pt_f)-[l:MEDDRA_LINK]->(m_hlt_t)
SET     l.PrimarySoc = r.PrimarySoc;

RETURN "Loading MeddraOntology.tsv (HLTs to HLGTs)..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///MeddraOntology.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (m_hlt_f:MeddraHLT {
    MeddraId: r.MeddraIdFrom})
MATCH (m_hlgt_t:MeddraHLGT {
    MeddraId: r.MeddraIdTo})
MERGE (m_hlt_f)-[:MEDDRA_LINK]->(m_hlgt_t);

RETURN "Loading MeddraOntology.tsv (HLGTs to SOCs)..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///MeddraOntology.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (m_hlgt_f:MeddraHLGT {
    MeddraId: r.MeddraIdFrom})
MATCH (m_soc_t:MeddraSOC {
    MeddraId: r.MeddraIdTo})
MERGE (m_hlgt_f)-[:MEDDRA_LINK]->(m_soc_t);

RETURN "Loading MeddraSmqContainsTerm.tsv..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///MeddraSmqContainsTerm.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (s:MeddraSmq {
    MeddraSmqCode: toInteger(r.MeddraSmqCode)})
MATCH (m:MeddraPT {
    MeddraId: r.MeddraId})
MERGE (s)-[l:MEDDRA_SMQ_CONTAINS]->(m)
SET
    l.Scope                 = r.Scope,
    l.Status                = r.Status,
    l.Category              = r.Category,
    l.Weight                = toFloat(r.Weight),
    l.AdditionVersion       = r.AdditionVersion,
    l.LastModifiedVersion   = r.LastModifiedVersion;

RETURN "Loading MeddraSmqContainsSmq.tsv..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///MeddraSmqContainsSmq.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (s:MeddraSmq {
    MeddraSmqCode: toInteger(r.MeddraSmqCode)})
MATCH (m:MeddraSmq {
    MeddraSmqCode: toInteger(r.MeddraId)})
MERGE (s)-[l:MEDDRA_SMQ_CONTAINS]->(m)
SET
    l.Scope                 = r.Scope,
    l.Status                = r.Status,
    l.Category              = r.Category,
    l.Weight                = toFloat(r.Weight),
    l.AdditionVersion       = r.AdditionVersion,
    l.LastModifiedVersion   = r.LastModifiedVersion;

RETURN "Loading MeddraCq.tsv..." AS `Action:`;
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///MeddraCq.tsv" AS r
FIELDTERMINATOR '\t'
MERGE (azm: MeddraCq {
    Name: r.Name})
SET
    azm.Abbreviation	 = r.Abbreviation,
    azm.Description	     = r.Description,
    azm.Authors	         = r.Authors,
    azm.CreatedDate	     = datetime(r.CreatedDate),
    azm.Version          = r.Version;

RETURN "Loading MeddraCqLinks.tsv..." AS `Action:`;
LOAD CSV WITH HEADERS FROM "file:///MeddraCqLinks.tsv" AS r
FIELDTERMINATOR '\t'
MATCH (azm:MeddraCq {
    Name: r.Name})
MATCH (m:MeddraPT {
    Name: r.PT, MeddraType: 'PT'})
MERGE (azm)-[:MEDDRA_CQ_CONTAINS]->(m);

RETURN "Loading Manifest.tsv..." AS `Action:`;
LOAD CSV WITH HEADERS FROM "file:///Manifest.tsv" AS r
FIELDTERMINATOR '\t'
MERGE (m:Manifest { Path: r.Path, 
                    Tag: CASE r.Tag WHEN NULL THEN '(no tag)' ELSE r.Tag END, 
                    Rows:toFloat(r.Rows), 
                    Size:toFloat(r.Size), 
                    LastModified:datetime(r.LastModified)})
SET m.Md5 = r.Md5;

RETURN "Load Complete..." AS `Action:`;

RETURN "Post Import Processing..." AS `Action:`;

RETURN "Setting VAERS Quality Attributes..." AS `Action:`;

// Set quality metrics on VAERS COVID19 vaccine data
// Specifically identify VAERS cases for COVID19 that:
//    Has a vaccination date, and it is not before 01-Dec-2021
//    Has a received date not prior to 01-Dec-2021
//    Has a TTO of less 100 days (this is an arbitrary parameter which
//        will be moved to a configuration file)
// These quality checks help eliminate COVID19 vaccines with incorrect
// vaccination dates (e.g. predating the availabilitiy of COVID19 vaccines)
// and questionable onset times.
MATCH (c:Case {DataSource:'VAERS'}) -[r:REPORTED_AE]->(md:MeddraPT) 
MATCH (c) -[:ADMINISTERED]->(v:Vaccine {VaxType:'COVID19'})
WHERE   r.OnsetDate >= datetime('2020-12-01')  
        AND r.LengthInDays < 100 
        AND c.ReceivedDate >= datetime('2020-12-01') 
        AND EXISTS(c.PatientAgeRangeMin) SET c.VaersQC = 1;

RETURN "Post Import Processing Complete." AS `Action:`;

RETURN "PSKG Graph Loaded." AS `Action:`;