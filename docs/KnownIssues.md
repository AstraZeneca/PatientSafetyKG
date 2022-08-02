## Known Issues/Enhancements

This is a working list of known issues and enhancements.

1. Add Dosage/Duration information to :ADMINISTERED/:PRESCRIBED edges when available (in-progress)
2. Capture VAERS MedDRA term versions.
2. Resolve all vaccine and medication names to appropriate controlled terminology
3. Differentiate meds and vaccines for EV (depends on controlled terminology)
4. Load unstructured VAERS data (e.g. LABS)

## Completed (from POC)
1. Need to process multiple years in VAERS. Currently a memory issue (resolved w/new architecture)
2. Need to standardise the outcomes across the two data sources (resolved with VAERS and EudraVigilance)
3. Need to add in the severity label (obsolete)
4. The MedDRA dictionary is versioned--this should be captured somewhere.  (MedDRA nodes are now versioned)
5. Capture the original VAERS file name (w/in the zip) and mod/date time, I'd like this functionality for all of our data sources--lineage information (even minimal) is essential for tracking down bugs. These data are projected in the SET analyses. (Resolved, see Manifest nodes)