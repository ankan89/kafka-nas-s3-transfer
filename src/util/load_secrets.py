import hvac
import logging
import json
import re
# from logging_config import app_logger as logger

def load_vault_secrets(secret_paths, vault_addr, role_id, secret_id, ns):

    if not (vault_addr and role_id and secret_id and ns):
        raise ValueError("Vault address, Role ID, Secret ID, and namespace must be provided in the vault-secrets directory.")
    
    client = hvac.Client(url=vault_addr, namespace=ns, verify="./src/root.crt")
    client.auth.approle.login(role_id=role_id, secret_id=secret_id)

    # kv_secret_paths_list = []
    # keys_list=[]
    all_secrets = {}

    for secret_path in secret_paths:
        mount, path = secret_path.split('/', 1)

        # key = path.split('/')[-1]
        # kv_secret_path = '/'.join(secret_path.split('/')[:-1])
        # kv_secret_paths_list.append(kv_secret_path)
        # unique_paths = set(kv_secret_paths_list)
        # keys_list.append(key)

    # for unique_path in unique_paths:
        #print(f"reading vault: {secret_path}")
        secrets = client.secrets.kv.v2.read_secret_version(mount_point=mount, path=path)
        # append secrets['data']['data'] json to all_secrets
        all_secrets.update(secrets['data']['data'])

    client.auth.token.revoke_self()
    
    return all_secrets

def add_secrets_from_vault(configs, vault_addr, role_id, secret_id, ns):

    # Find all placeholders in the JSON data
    kv_paths = set(re.findall(r'{{(.*?)}}', json.dumps(configs)))

    # if kv_paths list is empty return configs (means there are not vault secrets need to fetched)
    if not kv_paths:
        return configs

    # Create a set to store the unique base paths
    base_paths = set(get_base_path(path) for path in kv_paths)

    # get secrets for all base paths from vault
    secrets = load_vault_secrets(base_paths, vault_addr, role_id, secret_id, ns)

    # Create a dictionary to store the placeholder and its corresponding value
    placeholders_dict = {}
    for placeholder in kv_paths:
            placeholders_dict[placeholder] = secrets[placeholder.split('/')[-1]]

    return replace_placeholders(configs, placeholders_dict)

def get_base_path(path):
    # Find the position of the last '/' and slice the string to get the base path
    last_slash_index = path.rfind('/')
    if last_slash_index != -1:
        return path[:last_slash_index + 1]  # Include the '/' to keep the trailing slash
    return path  # Return the original path if no '/' is found

def replace_placeholders(data, mapping):
    """
    Recursively replace placeholders in the JSON data with values from the mapping.
    
    Args:
        data (dict or list): The JSON data to process.
        mapping (dict): The dictionary with placeholder mappings.
    
    Returns:
        dict or list: The updated JSON data with placeholders replaced.
    """
    if isinstance(data, dict):
        return {k: replace_placeholders(v, mapping) for k, v in data.items()}
    elif isinstance(data, list):
        return [replace_placeholders(item, mapping) for item in data]
    elif isinstance(data, str):
        # Replace placeholders in the string
        return re.sub(r'\{\{([^}]+)\}\}', lambda m: mapping.get(m.group(1), m.group(0)), data)
    else:
        return data