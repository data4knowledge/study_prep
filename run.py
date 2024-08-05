from d4kms_service import Neo4jConnection
import time

# print("\033[H\033[J") # Clears terminal window in vs code

print("running")

# # Compare db
# from utility.compare_aura_local_crm import compare_crm
# from utility.compare.compare_aura_local_sdtm import compare_sdtm
from utility.compare.compare_aura_local_bc import compare_bc
from utility.compare.query_local import query_study_service
from utility.pre.pre_step_check_mappings_against_db import check_mappings_against_db

from utility.pre.pre_step_add_test_label import do

do()


exit()

# query_study_service()
# exit()

# print("\n== Compare db ")
# compare_sdtm()
# compare_bc()

print("\n== Check mappings against db")
check_mappings_against_db()

# exit()
# from utility.pre.pre_step_convert_surrogate_to_bc import convert_surrogate_to_bc
# from utility.pre.pre_step_link_informed_consent_to_dm import make_links
# convert_surrogate_to_bc()
# make_links()

# Steps
from step_0_create_data_contract_lookup import create_data_contracts_lookup
from step_1_create_subject_enrolment_load_file import create_subject_enrolment_load_file
from step_2_create_subject_data_load_file import create_subject_data_load_file

print("\n== Create data contracts")
create_data_contracts_lookup()
print("Sleep before continuing ....")
time.sleep(5)
print("\n== Create data enrolment")
create_subject_enrolment_load_file()
print("\n== Create data datapoints")
create_subject_data_load_file()

# # Post steps
# from utility.post.post_step_load_datapoints import load_datapoints
# # print("\033[H\033[J") # Clears terminal window in vs code

# print("\n== Load datapoint")
# load_datapoints()

# compare_crm()
# compare_sdtm()

print("done")

