print("running")

# # Pre-steps
from utility.step_1_reduce_dm import reduce_dm
# from utility.step_2_reduce_lb import reduce_lb
# from utility.step_2_reduce_vs import reduce_vs
# reduce_dm()
# reduce_lb()
# reduce_vs()

from utility.pre_step_convert_surrogate_to_bc import convert_surrogate_to_bc
convert_surrogate_to_bc()

# # Compare db
# from utility.compare_aura_local_crm import compare_crm
# from utility.compare_aura_local_sdtm import compare_sdtm

# Steps
from step_0_create_data_contract_lookup import create_data_contracts_lookup
from step_1_create_subject_enrolment_load_file import create_subject_enrolment_load_file
from step_2_create_subject_data_load_file import create_subject_data_load_file

# create_data_contracts_lookup()
# create_subject_enrolment_load_file()
create_subject_data_load_file()

# Post steps
from utility.post_step_load_datapoints import load_datapoints
# print("\033[H\033[J") # Clears terminal window in vs code


# load_datapoints()

# compare_crm()
# compare_sdtm()

print("done")

