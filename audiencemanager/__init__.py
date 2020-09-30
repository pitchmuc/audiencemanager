"""
Adobe Audience Manager Python wrapper to manage audience manager data.
It supports JWT integration.
"""

__version__ = "0.0.1"
from audiencemanager import *
from audiencemanager import modules
from audiencemanager import config
from audiencemanager import connector


def createConfigFile(sandbox: bool = False, verbose: object = False, **kwargs)->None:
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
    if sandbox:
        json_data['sandbox-name'] = "<your_sandbox_name>"
    filename = f"{kwargs.get('filename', 'config_aam')}.json"
    with open(filename, 'w') as cf:
        cf.write(modules.json.dumps(json_data, indent=4))
    if verbose:
        print(f'File created at this location :{config._cwd}/{filename}')


def _find_path(path: str) -> object:
    """Checks if the file denoted by the specified `path` exists and returns the Path object
    for the file.

    If the file under the `path` does not exist and the path denotes an absolute path, tries
    to find the file by converting the absolute path to a relative path.

    If the file does not exist with either the absolute and the relative path, returns `None`.
    """
    if modules.Path(path).exists():
        return modules.Path(path)
    elif path.startswith('/') and modules.Path('.' + path).exists():
        return modules.Path('.' + path)
    elif path.startswith('\\') and modules.Path('.' + path).exists():
        return modules.Path('.' + path)
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
    with open(modules.Path(config_file_path), 'r') as file:
        f = modules.json.load(file)
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


class AudienceManager:
    """
    Class that will enable you to request information on your Audience Manager data.
    """

    def __init__(self, config_object: dict = config.config_object)->None:
        """
        Instantiate the Audience Manager class.
        """
        self.config = modules.deepcopy(config_object)
        self.connector = connector.AdobeRequest(
            config_object=config_object)
        self.endpoint = "https://aam.adobe.io/v1"
        self.header = self.connector.header

    def getTraits(self, folderId: int = None, integrationCode: str = None, dataSourceIds: list = None, includeDetails: bool = False):
        """
        Return traits following the parameters provided.
        Arguments:
            folderId : OPTIONAL : Only return traits from the selected folder.
            integrationCode : OPTIONAL : Returns traits that contain this integration code.
            dataSourceId : OPTIONAL : List of dataSourceIds. Returns traits that belong to the selected data sources.  
            includeDetails : OPTIONAL : For True, returns additional details for the traits. Additional returned values include ttl,integrationCode, comments, traitRule, traitRuleVersion, and type.
        """
        path = "/traits/"
        params = {}
        if folderId is not None:
            params["folderId"] = folderId
        if integrationCode is not None:
            params["integrationCode"] = integrationCode
        if includeDetails:
            params["includeDetails"] = True
        if type(dataSourceIds) == list:
            if len(dataSourceIds) == 1:
                params["dataSourceId"] = dataSourceIds[0]
            else:
                list_dsids = [str(dsid) for dsid in dataSourceIds]
                string = "&dataSourceId=".join(list_dsids)
                params["dataSourceId"] = string
        res = self.connector.getData(
            self.endpoint+path, params=params, headers=self.header)
        return res
