# study_prep
Data preparation for the Study Service

### Install
```
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

#### Pre-step: Convert xpt files to json and reduced datasets
- CDISC pilot **_dm.xpt_**, **_vs.xpt_** and **_lb.xpt_** need to be available in **_tmp_** directory.
- Make sure to run **_utility.step-1-reduce-dm_** first. The subjects selected will be used when reducing other datasets.

#### Optional Pre-step: Debug mapping file agains db
Checks that bc, bc property, encounter and timing mappings in mappings.py exist in neo4j database :
- run **_utility.pre-step-check-mappings-against-db_**

#### Optional Pre-step: Data Contracts needed in study service database for ScheduledActivityInstance NOT on main timeline
If data contracts have not been created by study service for ScheduledActivityInstances that are not on the **main** timeline:
- run **_utility.pre-step-create-data-contracts-sub-timeline_**

### Steps
- Step-1: Create offline data-contract lookup file. Columns: BC_LABEL, BCP_NAME, ENCOUNTER_LABEL, TIMEPOINT_VALUE, DC_URI
- Step-2: Create enrolment file. Columns: STUDY_URI, SITEID, USUBJID
- Step-3: Create datapoint file. Columns: USUBJID, DC_URI, DATAPOINT_URI, VALUE

### Post-step: If not able to load via study service ui
- N.B! Copy **_data/output/enrolment.csv_** and **_datapoints.csv_** to Neo4j import library before running
- Run **_utility.load_datapoints.py_**

