import os
import yaml
from d4kms_service import Node, Neo4jConnection
from d4kms_generic import ServiceEnvironment
from base_node import BaseNode
from typing import List, Literal

class ConfigurationNode(BaseNode):
  name: str = ""
  type: str = ""


class Configuration():
  uuid: str
  DIR = 'data/input'
  FILENAME = 'configuration.yaml'

  def __init__(self, *a, **kw):
    super().__init__(*a, **kw)
    self._configuration = self.read_yaml_file(os.path.join(self.DIR, self.FILENAME))

  @staticmethod
  def read_yaml_file(filepath):
    with open(filepath, "r") as f:
      data = yaml.load(f, Loader=yaml.FullLoader)
    return data

def main():
  c = Configuration()
  ConfigurationNode.create(c._configuration)
  for k,v in c._configuration.items():
    print("k",k)
    print("v",v)
  # print(c._configuration)

if __name__ == '__main__':
  main()

