from client_graphql import MakeClient
from utils import clear_id
from gql import gql


class RawData:
  
  def __init__(self, name: str, url: str, datatime: dict):
    self.name = name
    self.url = url
    self.datatime = datatime
    self.id_table = self.id_raw_source = self.id_coverage = None


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
 
  values = {
    "dataset": id_dataset,
    "name": slot.name,
    "url": slot.url,
    "areaIpAddressRequired": "5503dd29-4d9b-483b-ae09-63dc8ed28875",
    "availability": "ec7c1f35-7dda-41bf-84c5-74731fb685bd",
    "isFree": True,
    "status": "47208305-325a-4da9-9222-ac6849405b78"
      }
  
  slot.id_raw_source = MakeClient().query_mutation(mutation_class="RawDataSource", 
                                                   input_values=values, only_id=True)


def get_create_coverage(slot: RawData) -> None:
  
  values = {
  "rawDataSource": slot.id_raw_source,
  "area": "5503dd29-4d9b-483b-ae09-63dc8ed28875"
  }

  slot.id_coverage = MakeClient().query_mutation(mutation_class="Coverage", 
                                                  input_values=values, only_id=True)
  

def get_create_date_time_range(slot: RawData) -> None:
  
  slot.datatime["coverage"] = slot.id_coverage
  
  MakeClient().query_mutation(mutation_class="DateTimeRange", 
                              input_values=slot.datatime)


def connect_raw_source_to_table(slot: RawData) -> None:
  
  values = {
      "id": slot.id_table, 
      "rawDataSource": slot.id_raw_source
      }

  MakeClient().query_mutation(mutation_class="Table", 
                              input_values=values)


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
