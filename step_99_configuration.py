from pathlib import Path
from d4kms_service import Neo4jConnection
from model.configuration import Configuration, ConfigurationNode

def main():
  c = Configuration()
  # ConfigurationNode.delete()
  ConfigurationNode.create(c._configuration)


if __name__ == "__main__":
  print("Start")
  main()
  print("Done")
