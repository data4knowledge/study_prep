import json
from pathlib import Path
from d4kms_service import Neo4jConnection
from model.configuration import Configuration, ConfigurationNode
from model.base_node import BaseNode
from utility.debug import write_debug, write_tmp, write_tmp_json
import xmltodict

def to_debug(*txts):
    global debug
    print("printilintar")
    list = []
    for x in txts:
        list.append(x)
    debug.append(" ".join(list))

# class Define(BaseNode):
#   name: str = ""
#   study_products_bcs: List[str]= []
#   disposition: List[str]= []
#   demography: List[str]= []

debug = []




def odm_properties():
  properties = {
    'xmlns':"http://www.cdisc.org/ns/odm/v1.3" ,
    'xmlns:xlink':"http://www.w3.org/1999/xlink",
    'xmlns:def':"http://www.cdisc.org/ns/def/v2.1",
    'ODMVersion':"1.3.2",
    'FileOID':"www.cdisc.org/StudyCDISC01_1/1/Define-XML_2.1.0",
    'FileType':"Snapshot",
    'CreationDateTime':"",
    'Originator':"Study Service",
    'SourceSystem':"Study service",
    'SourceSystemVersion':"Alpha1",
    'def:Context':"USDM",
  }
  return properties

def study(oid= 'tbd', study_name = 'tbd', description = 'tbd', protocol_name = 'tbd'):
  study = {
    '@OID': oid,
    'StudyName': study_name,
    'StudyDescription': description,
    'ProtocolName': protocol_name,

  }
  return study

def metadata_version(oid = 'tbd', name = 'tbd', description = 'tbd'):
  metadata = {
    "@OID": oid,
    "@Name": name,
    "@Description": description,
    "@def:DefineVersion": "2.1.7",
    # "def:Standards": ""      
  }
  return metadata



def main():

    define = {}
    define['odm'] = odm_properties()
    define['odm']['Study'] = study()
    metadata = metadata_version()

    define['odm']['Study']['MetaDataVersion'] = metadata




    # debug.append(define)
    # write_tmp("define-debug.txt",debug)
    write_tmp_json("define-debug",define)

if __name__ == "__main__":
    main()
