"""
Adobe Audience Manager Python wrapper to manage audience manager data.
It supports JWT integration.
"""

from .audiencemanager import *
from .__version__ import __version__
from audiencemanager import config
from audiencemanager import connector
import json, os
from pathlib import Path


def createConfigFile(verbose: object = False, **kwargs)->None:
    """
    This function will create a 'config_aam.json' file where you can store your access data.
    Arguments:
        sandbox : OPTIONAL : consider to add a parameter for sandboxes
        verbose : OPTIONAL : set to true, gives you a print stateent where is the location.
    kwargs: 
        filename : filename to your config file.
    """
    json_data = {
        'org_id': '<orgID>',
        'client_id': "<client_id>",
        'tech_id': "<something>@techacct.adobe.com",
        'secret': "<YourSecret>",
        'pathToKey': '<path/to/your/privatekey.key>',
    }
    filename = f"{kwargs.get('filename', 'config_aam')}"
    if '.json' not in filename:
        filename = filename + '.json'
    with open(filename, 'w') as cf:
        cf.write(json.dumps(json_data, indent=4))
    if verbose:
        print(f'File created at this location :{os.getcwd()}{os.sep}{filename}')


def _find_path(path: str) -> object:
    """Checks if the file denoted by the specified `path` exists and returns the Path object
    for the file.

    If the file under the `path` does not exist and the path denotes an absolute path, tries
    to find the file by converting the absolute path to a relative path.

    If the file does not exist with either the absolute and the relative path, returns `None`.
    """
    if Path(path).exists():
        return Path(path)
    elif path.startswith('/') and Path('.' + path).exists():
        return Path('.' + path)
    elif path.startswith('\\') and Path('.' + path).exists():
        return Path('.' + path)
    else:
        return None


def importConfigFile(path: str)-> None:
    """
    This function will read the 'config_admin.json' to retrieve the information to be used by this module.
    """
    config_file_path = _find_path(path)
    if config_file_path is None:
        raise FileNotFoundError(
            f"Unable to find the configuration file under path `{path}`.")
    with open(Path(config_file_path), 'r') as file:
        f = json.load(file)
        config.config_object['org_id'] = f['org_id']
        config.header["x-gw-ims-org-id"] = config.config_object['org_id']
        if 'api_key' in f.keys():
            config.config_object['client_id'] = f['api_key']
        elif 'client_id' in f.keys():
            config.config_object['client_id'] = f['client_id']
        config.header["X-Api-Key"] = config.config_object['client_id']
        config.header['Authorization'] = ''
        config.config_object['tech_id'] = f['tech_id']
        config.config_object['secret'] = f['secret']
        config.config_object['pathToKey'] = f['pathToKey']
        config.config_object['date_limit'] = 0


