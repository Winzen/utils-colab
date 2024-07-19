import pandas as pd
import ruamel.yaml as yaml
import os
from typing import List, Optional, Tuple
import requests
from io import StringIO
import re


def find_model_directory(directory: str)-> Optional[str]:
    # Check if 'model' is in the current directory
    if 'models' in os.listdir(directory):
        return os.path.join(directory, 'models')

    if 'queries-basedosdados-dev' in os.listdir(directory):
        return os.path.join(directory, 'queries-basedosdados-dev','models')

    # Get the parent directory
    parent_directory = os.path.dirname(directory)

    # If we've reached the root directory without finding 'model', return None
    if directory == parent_directory:
        return None

    # Otherwise, continue searching recursively in parent directories
    return find_model_directory(parent_directory)

def sheet_to_df(columns_config_url_or_path: str) -> pd.DataFrame:
    """
    Convert sheet to dataframe
    """
    pattern = r'\?pli=\d+'
    columns_config_url_or_path = re.sub(pattern, '', columns_config_url_or_path)

    url = columns_config_url_or_path.replace("edit#gid=", "export?format=csv&gid=")
    try:
        return pd.read_csv(StringIO(requests.get(url, timeout=10).content.decode("utf-8")), dtype= str, na_values= "")
    except:
        print(
            "Check if your google sheet Share are: Anyone on the internet with this link can view"
        )

def create_model_from_architecture(architecture_df: pd.DataFrame, output_dir: str, dataset_id: str, table_id: str, preprocessed_staging_column_names: bool = True) -> None:

        if preprocessed_staging_column_names:
            architecture_df['original_name'] = architecture_df['name']

        # Temporaria parte

        paths = [f"{output_dir}/{dataset_id}__{table_id}.sql",
                 f"{output_dir}-view/{dataset_id}__{table_id}.txt"]

        for path in paths:
            with open(path, 'w') as file:
                sql_config = "{{ config(alias=" + f"'{table_id}'," + "schema=" + f"'{dataset_id}'" + ") }}\n"
                file.write(sql_config)
                sql_first_line = "select\n"
                file.write(sql_first_line)

                for _, column in architecture_df.iterrows():
                    sql_line = f"safe_cast({column['original_name']} as {column['bigquery_type'].lower()}) {column['name']},\n"
                    file.write(sql_line)

                sql_last_line = f"from `basedosdados-dev.{dataset_id}_staging.{table_id}` as t\n\n"
                file.write(sql_last_line)

def extract_column_parts(input_string: str) -> str:
    pattern_1 = re.compile(r"(\w+)\.(\w+):(\w+)")
    pattern_2 = re.compile(r"\w+\.(\w+)\.(\w+):(\w+)")

    if pattern_1.match(input_string):
        return pattern_1.findall(input_string)[0]
    elif pattern_2.match(input_string):
        return pattern_2.findall(input_string)[0]
    else:
        raise ValueError(f"Invalid input format on `{input_string}`. Expected format: 'dataset.table:column'")

def extract_relationship_info(input_string: str) -> Tuple[str,str]:
    try:
        dataset, table, column = extract_column_parts(input_string)

        if column == table:
            column = f'{column}.{column}'

        field = column

        table_path = f"ref('{dataset}__{table}')"

        return table_path, field

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def create_relationships(directory_column: str) -> List:
        relationship_table, relationship_field  = extract_relationship_info(directory_column)
        yaml_relationship = yaml.comments.CommentedMap()
        yaml_relationship['relationships'] = {
            "to": relationship_table,
            "field": relationship_field
        }
        return [yaml_relationship]

def create_unique_combination(unique_keys: List[str]):
        combination = yaml.comments.CommentedMap()
        combination['dbt_utils.unique_combination_of_columns'] = {
            "combination_of_columns": unique_keys
        }
        return [combination]

def create_not_null_proportion(at_least:float) -> List:
        not_null = yaml.comments.CommentedMap()
        not_null['not_null_proportion_multiple_columns'] = {
            "at_least": at_least,
        }
        return [not_null]

def create_dict_coverage(dataset_id: str, list_covered_by_dict_columns: List[str])-> List:
        dict_coverage = yaml.comments.CommentedMap()
        dict_coverage['custom_dictionaries'] = {
            "columns_covered_by_dictionary": list_covered_by_dict_columns,
            "dictionary_model": f"ref('{dataset_id}__dicionario')"
        }
        return [dict_coverage]


def create_unique() -> List:
        return ["unique", "not_null"]


def update_dbt_project_yaml(dataset_id: str,models_path: str) -> None:
    dbt_project_path = models_path.replace('models','dbt_project.yml')

    yaml_obj = yaml.YAML(typ='rt')
    yaml_obj.explicit_start = True
    yaml_obj.indent(mapping=2, sequence=2, offset=2)

    with open(dbt_project_path, 'r') as file:
        data = yaml_obj.load(file)

    models = data['models']['basedosdados']
    models.update({dataset_id:{"+materialized":"table",
                              "+schema": dataset_id}})

    data['models']['basedosdados'] = {key: models[key] for key in sorted(models)}

    with open(dbt_project_path, 'w') as file:
        yaml_obj.dump(data, file)

    print(f"dbt_project successfully updated with {dataset_id}!")

    
def create_yaml_file(arch_url: str,
                     table_id: str,
                     dataset_id: str,
                     table_description: str = "Insert table description here",
                     at_least: float = 0.95,
                     unique_keys: List[str] = ["insert unique keys here"],
                     mkdir: bool = True,
                     preprocessed_staging_column_names=True) -> None:
    """
    Creates dbt models and schema.yaml files based on the architecture table, including data quality tests automatically.

    Args:
        arch_url (str or list): The URL(s) or file path(s) of the input file(s) containing the data.
        table_id (str or list): The table ID(s) or name(s) to use as the YAML model name(s).
        dataset_id (str): The ID or name of the dataset to be used in the dbt models.
        at_least (float): The proportion of non-null values accepted in the columns.
        unique_keys (list, optional): A list of column names for which the 'dbt_utils.unique_combination_of_columns' test should be applied.
                                      Defaults to ["insert unique keys here"].
        mkdir (bool, optional): If True, creates a directory for the new model(s). Defaults to True.
        preprocessed_staging_column_names (bool, optional):  If True, builds SQL file renaming from 'original_name' to 'name' using the architecture file. Defaults to True.

    Raises:
        TypeError: If the table_id is not a string or a list.
        ValueError: If the number of URLs or file paths does not match the number of table IDs.

    Notes:
        The function generates dbt models in YAML format based on the input data and saves them to the specified output file.
        The generated YAML file includes information about the dataset, model names, descriptions, and column details.

    Example:
        ```python
        create_yaml_file(arch_url='input_data.csv', table_id='example_table', dataset_id='example_dataset')
        ```

    """
    
        
    output_path = f"/content/{dataset_id}"
    
    os.makedirs(output_path, exist_ok=True)
    os.makedirs(output_path + "-view", exist_ok=True)
    
  
    schema_path = f"{output_path}/schema.yml"
    print(schema_path, output_path)
    yaml_obj = yaml.YAML(typ='rt')
    yaml_obj.indent(mapping=4, sequence=4, offset=2)

    if os.path.exists(schema_path):
        with open(schema_path, 'r') as file:
            data = yaml_obj.load(file)
    else:
        data = yaml.comments.CommentedMap()
        data['version'] = 2
        data.yaml_set_comment_before_after_key('models', before='\n\n')
        data['models'] = []

    exclude = ['(excluded)', '(erased)', '(deleted)','(excluido)', '(excluir)']

    if isinstance(table_id, str):
        table_id = [table_id]
        arch_url = [arch_url]

    # If table_id is a list, assume multiple input files
    if not isinstance(arch_url, list) or len(arch_url) != len(table_id):
        raise ValueError("The number of URLs or file paths must match the number of table IDs.")

    for url, id in zip(arch_url, table_id):

        unique_keys_copy = unique_keys.copy()
        architecture_df = sheet_to_df(url)
        architecture_df.dropna(subset = ['bigquery_type'], inplace= True)
        architecture_df = architecture_df[~architecture_df['bigquery_type'].apply(lambda x: any(word in x.lower() for word in exclude))]

        table = yaml.comments.CommentedMap()
        table['name'] = f"{dataset_id}__{id}"

        # If model is already in the schema.yaml, delete old model from schema and create a new one
        for model in data['models']:
            if id == model['name'] or table['name'] == model['name'] :
                data['models'].remove(model)
                break

        table['description'] = table_description
        table['tests'] = create_unique_combination(unique_keys_copy)
        table['tests'] += create_not_null_proportion(at_least)

        covered_by_dict_columns = architecture_df['covered_by_dictionary']=='yes'
        if covered_by_dict_columns.sum():
            list_covered_by_dict_columns = architecture_df[covered_by_dict_columns]['name'].tolist()
            table['tests'] += create_dict_coverage(dataset_id, list_covered_by_dict_columns)

        table['columns'] = []

        for _, row in architecture_df.iterrows():
            column = yaml.comments.CommentedMap()
            column['name'] = row['name']
            column['description'] = row['description']
            if pd.notna(row["directory_column"]):
                tests = []
                directory = row["directory_column"]
                tests = create_relationships(directory)
                column['tests'] = tests
            table['columns'].append(column)


        data['models'].append(table)

        create_model_from_architecture(architecture_df,
                                        output_path,
                                        dataset_id,
                                        id,
                                        preprocessed_staging_column_names)

    with open(schema_path, 'w') as file:
        yaml_obj.dump(data, file)

    print(f"Files successfully created for {dataset_id}!")

    # update_dbt_project_yaml(dataset_id,models_path)
