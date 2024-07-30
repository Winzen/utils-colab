from client_graphql import MakeClient
from gql import gql


class RawData:
  
  def __init__(self, name: str, url: str):
    self.name = name
    self.url = url
    self.id_table = self.id_raw_source = self.id_coverage = None


def clear_id(string: str, place: str) -> str:

  return string.replace(place, "")


def get_response_tables_from_dataset(id_dataset: str) -> list:

  client = MakeClient()

  query = """
  query($id_dataset: ID!){
    allTable(dataset_Id: $id_dataset){
      edges{
        node{
          id
          slug
        }
      }

    }
  }
  """

  variable = {"id_dataset": id_dataset}

  response = client.query.execute(gql(query), variable_values=variable)
  response_tables = response["allTable"]["edges"]
  return response_tables


def get_create_raw_data(id_dataset: str, slot: RawData) -> None:
  client = MakeClient().mutation()

  query ="""
  mutation($input: CreateUpdateRawDataSourceInput!){
      CreateUpdateRawDataSource(input: $input){
    errors{
      messages
    }
    rawdatasource{
      id
    }
    }
  }
  """

  values = {
    "dataset": id_dataset,
    "name": slot.name,
    "url": slot.url,
    "areaIpAddressRequired": "5503dd29-4d9b-483b-ae09-63dc8ed28875",
    "availability": "ec7c1f35-7dda-41bf-84c5-74731fb685bd",
    "isFree": True,
    "status": "47208305-325a-4da9-9222-ac6849405b78"
      }

  variables = {
  "input": values
  }

  mutation_response = client.execute(gql(query), variable_values=variables)
  id_raw_source = mutation_response['CreateUpdateRawDataSource']['rawdatasource']['id']
  slot.id_raw_source = clear_id(id_raw_source, "RawDataSourceNode:")


def get_create_coverage(slot: RawData) -> None:
  
  client = MakeClient().mutation()

  query = """
  mutation($input: CreateUpdateCoverageInput!) {
    CreateUpdateCoverage(input: $input) {
      errors {
        messages
        field
      }
      coverage {
        id
      }
    }
  }
  """ 

  values = {
  "rawDataSource": slot.id_raw_source,
  "area": "5503dd29-4d9b-483b-ae09-63dc8ed28875"
  }

  variables = {
  "input": values
  }

  mutation_response = client.execute(gql(query), variable_values=variables)
  id_coverve = mutation_response['CreateUpdateCoverage']['coverage']['id']
  slot.id_coverge = clear_id(id_coverve, "CoverageNode:")


def get_create_date_time_range(slot: RawData) -> None:
  
  client = MakeClient().mutation()

  query = """
  mutation($input: CreateUpdateDateTimeRangeInput!){
    CreateUpdateDateTimeRange(input: $input) {
      errors {
        messages
        field
      }
      datetimerange
      {
        id
      }
      }
    }
  """

  values = {
  "coverage": slot.id_coverage, 
  "startYear":2000, 
  "startMonth":1, 
  "startDay":1, 
  "endYear":2010, 
  "endMonth":1, 
  "endDay":1, 
  "interval": 1 
  }

  variables = {
  "input": values
  }

  client.execute(gql(query), variable_values=variables)


def connect_raw_source_to_table(slot: RawData) -> None:
  
  client = MakeClient().mutation()

  query = """
  mutation($input: CreateUpdateTableInput!){
    CreateUpdateTable(input: $input) {
      errors {
        messages
        field
      }
      table
      {
        id
      }
      }
  }
  """

  values = {
      "id": slot.id_table, 
      "rawDataSource": slot.id_raw_source
      }

  variables = {
  "input": values
  }

  client.execute(gql(query), variable_values=variables)


def create_mult_raw_data_source(id_dataset: str, tables: dict) -> None:

  response_tables = get_response_tables_from_dataset(id_dataset)

  for table in response_tables:

    key = table["node"].get("slug")

    try:

      slot = tables[key]
      slot.id_table = clear_id(table["node"]["id"], "TableNode:")
      get_create_raw_data(id_dataset, slot)
      get_create_coverage(slot)
      get_create_date_time_range(slot)
      connect_raw_source_to_table(slot)
      print(f"{key} foi registrado com sucesso")
    
    except KeyError:
      
      print(f"{key} foi ignorada!")
      
      pass