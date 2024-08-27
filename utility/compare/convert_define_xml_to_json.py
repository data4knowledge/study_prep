import json
import xmltodict

define_xml = '/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/standards/define-xml/DefineV217_0/examples/DefineXML-2-1-SDTM/defineV21-SDTM.xml'
define_json = '/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/standards/define-xml/DefineV217_0/examples/DefineXML-2-1-SDTM/defineV21-SDTM.json'

with open(define_xml) as xml_file:
     
    data_dict = xmltodict.parse(xml_file.read())
    # xml_file.close()
     
    # generate the object using json.dumps() 
    # corresponding to json data
     
    json_data = json.dumps(data_dict, indent= 2)
     
    # Write the json data to output 
    # json file
    with open(define_json, "w") as json_file:
        json_file.write(json_data)
        # json_file.close()
