# study_prep
Data preparation for the Study Service

### Install
```
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

#### Pre-step: Create reduced datasets (if needed)
Make sure to run _step-1-reduce-dm.py_ first. The subjects selected will be used when reducing other datasets.

#### Pre-step: Data Contracts needed in study service database
If data contracts have not been created by study service for ScheduledActivityInstances that are not on the main timeline:
- run _utility.pre-step-create-data-contracts-sub-timeline.py_

### Steps
- Step-1: Create offline data-contract lookup file: BC_LABEL, BCP_NAME, ENCOUNTER_LABEL, TIMEPOINT_VALUE, DC_URI
- Step-2: Create enrolment file: STUDY_URI, SITEID, USUBJID
- Step-3: Create datapoint file: USUBJID, DC_URI, DATAPOINT, VALUE

### Post-step: If not able to load via study service ui
- Run _utility.load_datapoints.py_

