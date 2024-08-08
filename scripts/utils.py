from client_graphql import MakeClient
from gql import gql
import os
import pandas as pd

def clear_id(string: str, place: str) -> str:

  return string.replace(place, "")

def extract_id_from_response(mutation_response: dict, mutation_class: str) -> str:

    key_mutation = f'CreateUpdate{mutation_class}'
    id_response = mutation_response[key_mutation][mutation_class.lower()]['id']
    return clear_id(id_response, f"{mutation_class}Node:")


def get_columns_from_api(dataset: str, table_id: str) -> None:
  query = """
  query($gcpDatasetId: String, $gcpTableId: String){
    allCloudtable(gcpDatasetId: $gcpDatasetId, gcpTableId: $gcpTableId){
      edges{
        node{
          table{
            id
            name
            slug
            columns{
              edges{
                node{
                  id
                  name
                  nameStaging
                  description
                }
              }
            }

          }
        }
      }
    }
  }
  """

  variable = {
      "gcpDatasetId": dataset,
      "gcpTableId": table_id
      }

  response = MakeClient().query.execute(gql(query), variable_values=variable)
  short_response = response["allCloudtable"]["edges"][0]["node"]["table"]["columns"]["edges"]
  linhas = [{"dataset_id": dataset, "table_id": table_id, "column": short["node"]["name"]} for short in short_response]

  df = pd.DataFrame(linhas)

  os.makedirs("arquiteturas", exist_ok=True)

  df.to_csv(f"arquiteturas/{dataset}__{table_id}.csv", index=False)
