import os
import yaml
from d4kms_service import Node, Neo4jConnection
from d4kms_generic import ServiceEnvironment
from uuid import uuid4
from base_node import BaseNode
from typing import List, Literal

class ConfigurationNode(BaseNode):
  name: str = ""
  study_products_bcs: List[str]= []
  disposition: List[str]= []
  demography: List[str]= []

  @classmethod
  def create(cls, params):
    db = Neo4jConnection()
    with db.session() as session:
      result = session.execute_write(cls._delete_configuration, cls)
      result = session.execute_write(cls._create, cls, params)
    db.close()
    return result

  @classmethod
  def get(cls):
    db = Neo4jConnection()
    with db.session() as session:
      result = session.execute_read(cls._get, cls)
    db.close()
    return result

  @staticmethod
  def _create(tx, cls, source_params):
    params = []
    for param in source_params:
      if isinstance(source_params[param], list):
        params.append(f"a.{param}={source_params[param]}")
      else:
        params.append(f"a.{param}='{source_params[param]}'")
    params_str = ", ".join(params)
    query = """
      CREATE (a:%s {uuid: $uuid1})
      SET %s 
      RETURN a
    """ % (cls.__name__, params_str)
    # print("query",query)
    result = tx.run(query, uuid1=str(uuid4()))
    for row in result:
      return cls.wrap(row['a'])
    return None

  @staticmethod
  def _get(tx, cls):
    query = """
      MATCH (a:%s) RETURN a
    """ % (cls.__name__)
    print("query",query)
    # result = tx.run(query, uuid1=str(uuid4()))
    result = tx.run(query)
    for row in result:
      return cls.wrap(row['a'])
    return None

  @staticmethod
  def _delete_configuration(tx, cls):
    query = """
      MATCH (a:%s) detach delete a
      RETURN count(a)
    """ % (cls.__name__)
    print("query",query)
    result = tx.run(query, uuid1=str(uuid4()))
    for row in result:
      return print("delete row",row)
    return None

class Configuration():
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
  # print(c._configuration)
  ConfigurationNode.create(c._configuration)
  # configuration = ConfigurationNode.get()

if __name__ == '__main__':
  main()

