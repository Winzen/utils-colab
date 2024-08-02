from gql import Client, gql
from google.colab import userdata
from gql.transport.requests import RequestsHTTPTransport
from google.colab import userdata
from typing import Dict
from utils import extract_id_from_response

class MakeClient:
  def __init__(self):

    self.graphql_url = "https://backend.basedosdados.org/graphql"
    self.query = self.make_client()

  def make_client(self, headers: Dict[str, str] = None) -> Client:

    transport = RequestsHTTPTransport(
                url=self.graphql_url, headers=headers, use_json=True
            )

    return Client(transport=transport, fetch_schema_from_transport=False)

  def mutation(self) -> Client:

    query = """
      mutation ($email: String!, $password: String!) {
          tokenAuth(email: $email, password: $password) {
              token
            }
        }
      """

    variables = {
          "email": userdata.get('email'),
          "password": userdata.get('senha'),
      }

    response = self.query.execute(gql(query), variable_values=variables)

    token = response["tokenAuth"]["token"]

    header_for_mutation_query = {"Authorization": f"Bearer {token}"}

    return self.make_client(header_for_mutation_query)


  def query_mutation(self, mutation_class: str, input_values: dict, only_id: bool = False) -> str|dict:

    client = self.mutation()

    query = f"""
                mutation($input:CreateUpdate{mutation_class}Input!){{
                    CreateUpdate{mutation_class}(input: $input){{
                    errors {{
                        field,
                        messages
                    }},
                    {mutation_class.lower()} {{
                        id,
                    }}
                }}
                }}
              """
    
    variables = {"input": input_values}

    mutation_response = client.execute(gql(query), variable_values=variables)
    
    if only_id:
      return extract_id_from_response(mutation_response, mutation_class)
    
    return mutation_response


# Funçoes para para trocar tirar "-dev" do nome dos projetos

def change_projectid_cloud_table(id_cloud: str, gcpProjectId: str) -> None:

  client = MakeClient().mutation()

  query = """
  mutation($input: CreateUpdateCloudTableInput!){
      CreateUpdateCloudTable(input: $input){
      errors{
        messages
        field
      }
        cloudtable{
        id
        gcpProjectId
      }
    }
  }
  """

  values = {
    "id": id_cloud,
    "gcpProjectId": gcpProjectId
  }

  variables = {
    "input": values
  }


  client.execute(gql(query), variable_values=variables)
  print(f"CloubTable com ID {id_cloud} Corrigido!")


def get_cloudtable_dev_list() -> list:
  client = MakeClient()

  query = """
  query{
    allCloudtable(gcpProjectId: "basedosdados-dev"){
      edges{
        node{
          id
          gcpProjectId
        }
      }
    }
  }
  """
  response = client.query.execute(gql(query))
  clear_id = lambda str_id: str_id.replace("CloudTableNode:", "")
  ids_cloudtable = [clear_id(id["node"]["id"]) for id in response["allCloudtable"]["edges"]]
  return ids_cloudtable
