
DATA_LABELS_TO_BC_LABELS = {
    'Temperature': 'Temperature',
    'Weight': 'Weight',
    'Height': 'Height',
    'Alanine Aminotransferase': 'Alanine Aminotransferase Concentration in Serum/Plasma',
    'Sodium': 'Sodium Measurement',
    'Aspartate Aminotransferase': 'Aspartate Aminotransferase in Serum/Plasma',
    'Potassium': 'Potassium Measurement',
    'Hemoglobin A1C' : 'Hemoglobin A1C Concentration in Blood',
    'Creatinine': 'Creatinine Measurement',
    'Alkaline Phosphatase': 'Alkaline Phosphatase Concentration in Serum/Plasma',
    'Diastolic Blood Pressure': 'Diastolic Blood Pressure',
    'Systolic Blood Pressure': 'Systolic Blood Pressure',
    'Pulse Rate': 'Heart Rate',
    'Sex': 'Sex',
    'Race': 'Race',
    'Informed Consent': 'Informed Consent',
    'Informed Consent': 'Informed Consent Obtained',
    'Date of Birth': 'Date of Birth',
}

# Unknown visits
# 'RETRIEVAL': 'CHECK', 
# 'AMBUL ECG PLACEMENT': 'CHECK', 
# 'AMBUL ECG REMOVAL': 'CHECK'
DATA_VISITS_TO_ENCOUNTER_LABELS = {
    'SCREENING 1': 'Screening 1', 
    'SCREENING 2': 'Screening 2', 
    'BASELINE': 'Baseline', 
    'WEEK 2': 'Week 2', 
    'WEEK 4': 'Week 4', 
    'WEEK 6': 'Week 6', 
    'WEEK 8': 'Week 8', 
    'WEEK 12': 'Week 12', 
    'WEEK 16': 'Week 16', 
    'WEEK 20': 'Week 20', 
    'WEEK 26': 'Week 24', 
    'WEEK 24': 'Week 26', 
}

DATA_TPT_TO_TIMING_LABELS = {
    "AFTER LYING DOWN FOR 5 MINUTES": 'PT5M',
    "AFTER STANDING FOR 1 MINUTE"   : 'PT1M',
    "AFTER STANDING FOR 3 MINUTES"  : 'PT2M'
}

VS_RESULT_NAME = 'VSORRES'
LB_RESULT_NAME = 'LBORRES'
VS_UNIT_NAME = 'VSORRESU'
LB_UNIT_NAME = 'LBORRESU'
TEST_ROW_VARIABLE_TO_BC_PROPERTY = {
    'Weight': {
        'VSORRES': VS_RESULT_NAME,
        'VSORRESU': VS_UNIT_NAME
    },
    'Height': {
        'VSORRES': VS_RESULT_NAME,
        'VSORRESU': VS_UNIT_NAME
    },
    'Temperature': {
        'VSORRES': VS_RESULT_NAME,
        'VSORRESU': VS_UNIT_NAME
    },
    'Diastolic Blood Pressure': {
        'VSORRES': VS_RESULT_NAME,
        'VSORRESU': VS_UNIT_NAME
    },
    'Systolic Blood Pressure': {
        'VSORRES': VS_RESULT_NAME,
        'VSORRESU': VS_UNIT_NAME
    },
    'Pulse Rate': {
        'VSORRES': VS_RESULT_NAME,
        'VSORRESU': VS_UNIT_NAME
    },
    'Aspartate Aminotransferase': {
        'LBORRES': LB_RESULT_NAME,
        'LBORRESU': LB_UNIT_NAME
    },
    'Alkaline Phosphatase': {
        'LBORRES': LB_RESULT_NAME,
        'LBORRESU': LB_UNIT_NAME,
        'LBFAST': 'LBFAST',
        'LBSPEC': 'LBSPEC'
    },
    'Alanine Aminotransferase': {
        'LBORRES': LB_RESULT_NAME,
        'LBORRESU': LB_UNIT_NAME,
        'LBFAST': 'LBFAST',
        'LBSPEC': 'LBSPEC'
    },
    'Sex': {
        'value': "Sex",
        'date': "Date Time",
    },
    'Race': {
        'value': "Race",
        'date': "Date Time",
    },
    'Informed Consent': {
        'value': "Observation Start Date Time",
        'date': "Date Time",
    },
    'Informed Consent Obtained 1': {
        'value': "Observation Start Date Time",
        'date': "Date Time",
    },
    'Informed Consent Obtained': {
        'value': "--DTC",
        'date': "Date Time",
    },
    'Date of Birth': {
        'value': "Date of Birth",
        'date': "Date Time",
    },
}
