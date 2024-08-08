def clear_id(string: str, place: str) -> str:

  return string.replace(place, "")

def extract_id_from_response(mutation_response: dict, mutation_class: str) -> str:

    key_mutation = f'CreateUpdate{mutation_class}'
    id_response = mutation_response[key_mutation][mutation_class.lower()]['id']
    return clear_id(id_response, f"{mutation_class}Node:")
