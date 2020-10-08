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

    def _loop_folders(self, obj: dict, ids: list = None, names=None, parentids: list = None, folderCounts: list = None, paths: list = None)->tuple:
        """Loop function to retrieve id, names, ParentFolderID, FolderID, folderCount, path.
        Returns the tuple containing the elements in that order.
        """
        if ids is None:
            ids = list()
        else:
            ids = ids
        if names is None:
            names = list()
        else:
            names = names
        if parentids is None:
            parentids = list()
        else:
            parentids = parentids
        if folderCounts is None:
            folderCounts = list()
        else:
            folderCounts = folderCounts
        if paths is None:
            paths = list()
        else:
            paths = paths
        if type(obj) == list:
            for o in obj:
                self._loop_folders(o, ids=ids, names=names, parentids=parentids,
                                   folderCounts=folderCounts, paths=paths)  # recursive loop
        if type(obj) == dict:
            if 'folderId' in obj.keys():
                ids.append(obj['folderId'])
                names.append(obj['name'])
                parentids.append(obj['parentFolderId'])
                folderCounts.append(obj.get('folderCount', 0))
                paths.append(obj.get('path', ''))
            if 'subFolders' in obj.keys():
                self._loop_folders(obj['subFolders'], ids=ids, names=names,
                                   parentids=parentids, folderCounts=folderCounts, paths=paths)  # recursion
        return ids, names, parentids, folderCounts, paths

    def getTraits(self, folderId: int = None, integrationCode: str = None, dataSourceIds: list = None, includeDetails: bool = False, format: str = 'df')->object:
        """
        Return traits following the parameters provided.
        Can return 2 type of result, a dataframe or a list. 
        Arguments:
            folderId : OPTIONAL : Only return traits from the selected folder.
            integrationCode : OPTIONAL : Returns traits that contain this integration code.
            dataSourceId : OPTIONAL : List of dataSourceIds. Returns traits that belong to the selected data sources.  
            includeDetails : OPTIONAL : For True, returns additional details for the traits. Additional returned values include ttl,integrationCode, comments, traitRule, traitRuleVersion, and type.
            format : OPTIONAL : default "df" that returns a dataframe, you can also return the raw format ("raw")
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
        if format == "raw":
            return res
        elif format == "df":
            df = modules.pd.DataFrame(res)
            return df

    def getTrait(self, traitId: str = None, intCode: str = None)->dict:
        """
        Return a trait by its id or by integrationCode. Require one of the following arguments.
        Arguments:
            traitId : REQUIRED : Trait ID
            intCode : REQUIRED : integration code.
        """
        if traitId is None and intCode is None:
            raise Exception("Must provide either traitId or intCode")
        if intCode is not None:
            path = f"/traits/ic:{intCode}"
            res = self.connector.getData(
                self.endpoint+path, headers=self.header)
            return res
        elif traitId is not None:
            path = f"/traits/{traitId}"
            res = self.connector.getData(
                self.endpoint+path, headers=self.header)
            return res

    def deleteTraits(self, traitId: str = None, intCode: str = None)->str:
        """
        Delete a trait based on its Trait ID or its integration Code.
        It requires one of the 2 elements.
        Arguments:
            traitId : OPTIONAL : Trait ID to be deleted
            intCode : OPTIONAL : Integraton code of the trait to be deleted.
        """
        if traitId is None and intCode is None:
            raise Exception("traitId or intCode must be specified")
        if intCode is not None:
            path = f"/traits/ic:{intCode}"
        if traitId is not None:
            path = f"/traits/{traitId}"
        res = self.connector.delete(self.endpoint+path, headers=self.header)
        return res

    def deleteBulkTraits(self, traitIds: list = None)->str:
        """
        Delete the traits passed in the traitIds list parameter.
        Arguments:
            traitIds : REQUIRED : list of trait Ids to be deleted
        """
        path = "/traits/bulk-delete"
        if traitIds is None or type(traitIds) != list:
            raise Exception(
                'Require a list of ids to be deleted in traitIds parameter')
        data = [str(el) for el in traitIds]
        res = self.connector.postData(
            self.endpoint + path, data=data, headers=self.header)
        return res

    def getTraitLimit(self)->dict:
        """
        Returns a count of the maximum number of traits you can create for each trait type.
        """
        path = "/traits/limits"
        res = self.connector.getData(self.endpoint+path, headers=self.header)
        return res

    def getTraitVersion(self, traitId: str = None)->dict:
        """
        See the change history of a trait rule.
        """
        if traitId is None:
            raise Exception("require a traitId to be entered")
        path = f"/traits/{traitId}/trait-rule/versions"
        res = self.connector.getData(self.endpoint+path, headers=self.header)
        return res

    def validateTraitRule(self, rule: str = None)->dict:
        """
        Validate a rule logic.
        Arguments:
            rule : REQUIRED : string representing your trait rule
        """
        path = "/traits/validate"
        obj = {
            "code": rule
        }
        res = self.connector.postData(
            self.endpoint+path, data=obj, headers=self.header)
        return res

    def createTrait(self, name: str = None, traitType: str = None, dataSourceId: int = None, folderId: int = None, traitRule: str = None, ttl: int = 120, **kwargs)->dict:
        """
        Create Traits based on the information passed.
        Information : https://bank.demdex.com/portal/swagger/index.html#/Traits%20API/post_traits_
        Arguments:
            name : REQUIRED : name of the Trait.
            traitType : REQUIRED : Type of trait following possibilities: RULE_BASED_TRAIT, ON_BOARDED_TRAIT, ALGO_TRAIT
            dataSourceId : REQUIRED : Associates the trait with a specific data provider
            folderId : REQUIRED : Determines which storage folder the trait belongs to. 
            traitRule : OPTIONAL : rule string for your trait.
            ttl : OPTIONAL : Trait Expiration in days. Default 120 days.
        Possible kwargs and respective type:
            description : str
            comments : str
            status : str (Active)
            type : str
            categoryId : int
            ttl : int
            algoModelId : int
            thresholdValue : int
        see https://bank.demdex.com/portal/swagger/index.html#/Traits%20API/post_traits_
        """
        if name is None or dataSourceId is None or folderId is None or traitType is None:
            raise Exception(
                'Require a name, a dataSourceId, a traitType and a folderId')
        if traitType not in ["RULE_BASED_TRAIT", "ON_BOARDED_TRAIT", "ALGO_TRAIT"]:
            raise ValueError(
                "traitType should be one of the following value [RULE_BASED_TRAIT, ON_BOARDED_TRAIT, ALGO_TRAIT]")
        path = "​/traits​/"
        obj = {
            "name": name,
            "traitType": traitType,
            "dataSourceId": str(dataSourceId),
            "folderId": str(folderId),
            "traitRule": traitRule,
            "ttl": str(ttl),
        }
        for key in kwargs:
            if type(kwargs[key]) is not None and str(kwargs[key]) != "nan":
                obj.update({key: str(kwargs[key])})
        res = self.connector.postData(
            self.endpoint + path, data=obj, headers=self.header)
        return res

    def updateTrait(self, name: str = None, traitId: str = None, traitType: int = None, folderId: str = None, dataSourceId: int = None, ** kwargs):
        """
        Update the trait based on its ID.
        Pass the key value pair for which you want to update the trait.
        Arguments:
            name : REQUIRED : name of the Trait.
            traitId : REQUIRED : traitId to be updated.
            traitType : REQUIRED : Type of trait following possibilities: RULE_BASED_TRAIT, ON_BOARDED_TRAIT, ALGO_TRAIT
            dataSourceId : REQUIRED : Associates the trait with a specific data provider
            folderId : REQUIRED : Determines which storage folder the trait belongs to. 
        Possible kwargs and respective type:
            description : str
            comments : str
            status : str (Active)
            traitRule : str
            type : str
            categoryId : int
            ttl : int
            algoModelId : int
            thresholdValue : int
        """
        if traitId is None:
            raise Exception("Require a traitId as parameter")
        path = f"/traits/{traitId}"
        obj = {
            "name": name,
            "traitType": traitType,
            "dataSourceId": str(dataSourceId),
            "folderId": str(folderId),
        }
        for key in kwargs:
            if type(kwargs[key]) is not None and str(kwargs[key]) != "nan":
                obj.update({key: str(kwargs[key])})
        res = self.connector.putData(
            self.endpoint + path, data=obj, headers=self.header)
        return res

    def getTraitFolders(self, includeThirdParty: bool = None, format: str = 'df')->dict:
        """
        Returns the Trait folder information. 
        Arguments:
            includeThirdParty : OPTIONAL : For True, returns folders that store third-party traits.
            format : OPTIONAL : return a dataframe by default ("df"), but can return raw response ("raw")
            by default the dataframe returns the first level in the dataframe.
        """
        params = {}
        if includeThirdParty:
            params['includeThirdParty'] = includeThirdParty
        path = "/folders/traits/"
        res = self.connector.getData(self.endpoint+path,
                                     params=params, headers=self.header)
        if format == "raw":
            return res
        elif format == "df":
            ids, names, parentids, folderCounts, paths = self._loop_folders(
                res)
            dict_folders = {
                'folderId': ids,
                'name': names,
                'parentFolderId': parentids,
                'path': paths,
                'folderCounts': folderCounts
            }
            df = modules.pd.DataFrame(dict_folders)
            return df

    def createTraitFolder(self, name: str = None, parentFolderId: int = 0)->dict:
        """
        Create a Folder Trait.
        Arguments:
            parentFolderId : REQUIRED : ID of the Parent folder. Default root folder (0)
            name : REQUIRED : name of the folder.
        """
        path = "/folders/traits/"
        if name is None:
            raise Exception("require a name for the folder")
        obj = {
            "parentFolderId": str(parentFolderId),
            "name": name
        }
        res = self.connector.postData(
            self.endpoint+path, data=obj, headers=self.header)
        return res

    def deleteTraitFolder(self, folderId: str)->str:
        """
        Delete a specific folder by its ID.
        Arguments:
            folderId : REQUIRED : Folder ID to be deleted.
        """
        if folderId is None:
            raise Exception("require folderId to be specified")
        path = f"/folders/traits/{folderId}"
        res = self.connector.deleteData(
            self.endpoint + path, headers=self.header)
        return res

    def updateTraitFolder(self, folderId: str = None, name: str = None, parentFolderId: int = 0, **kwargs)->dict:
        """
        Update the folder ID specified with new information.
        Arguments:
            folderId : REQUIRED : Folder ID to be updated
            name : REQUIRED : name to be updated.
            parentFolderId : OPTIONAL : parentFolderId used to attach this folder (default root folder (0))
        kwargs possible : see https://bank.demdex.com/portal/swagger/index.html#/Trait%20Folder%20API/put_folders_traits__folderId_
        """
        if folderId is None:
            raise Exception("require folderId to be specified")
        path = f"/folders/traits/{folderId}"
        obj = {
            "parentFolderId": str(parentFolderId),
            "name": name
        }
        if len(kwargs) > 0:
            for key in kwargs:
                obj[key] = str(kwargs[key])
        res = self.connector.putData(
            self.endpoint+path, data=obj, headers=self.header)
        return res

    def getSegments(self, includeInUseStatus: bool = None, status: str = None, containsTrait: int = None, dataSourceId: int = None, mergeRuleDataSourceId: int = None, includeMetrics: bool = True, includeTraitDataSourceIds: bool = False, includeAddressableAudienceMetrics: bool = False, format: str = 'df')->object:
        """
        Returns either a list or a dataframe of segments depending the type of output you select.
        Arguments:
            includeInUseStatus : OPTIONAL : include only segment in use (having a destination).
            status : OPTIONAL : Returns segments that have the selected status. Accepted values are ACTIVE and INACTIVE
            containsTrait : OPTIONAL : if a trait is set, it will return segments containing a specific trait.
            dataSourceId : OPTIONAL : if a dataSourceId is set, it return return segments containing that dataSource.
            mergeRuleDataSourceId : OPTIONAL : Returns segments that follow the selected Profile Merge Rule ID.
            includeMetrics : OPTIONAL : For true, returns segment population metrics in the API response. (default True)
            includeTraitDataSourceIds : OPTIONAL : For true, returns the data source IDs of the traits that build up this segment. (default False)
            includeAddressableAudienceMetrics : OPTIONAL : For true, returns addressable audience metrics in the API response (default False)
            format : OPTIONAL : by default returns a dataframe ("df"), can return the list by putting "raw"
        """
        path = "/segments/"
        params = {}
        if includeMetrics:
            params['includeMetrics'] = True
        if includeTraitDataSourceIds:
            params['includeTraitDataSourceIds'] = True
        if includeAddressableAudienceMetrics:
            params['includeAddressableAudienceMetrics'] = True
        if status is None:
            if status == "ACTIVE" or status == "INACTIVE":
                params['status'] = status
        if includeInUseStatus:
            params['includeInUseStatus'] = True
        if containsTrait is not None:
            params['containsTrait'] = containsTrait
        if mergeRuleDataSourceId is not None:
            params["mergeRuleDataSourceId"] = mergeRuleDataSourceId
        res = self.connector.getData(
            self.endpoint+path, params=params, headers=self.header)
        if format == "raw":
            return res
        elif format == "df":
            df = modules.pd.DataFrame(res)
            return df

    def getSegment(self, segId: str)->dict:
        """
        Retrieve information about a specific segment.
        Arguments:
            segId : REQUIRED : Segment ID to be retrieved.
        """
        if segId is None:
            raise Exception("Expected a segment ID to be passed")
        path = f"segments/{segId}"
        res = self.connector.getData(self.endpoint+path, headers=self.header)
        return res

    def getSegmentLimits(self)->dict:
        """
        Get Segement limitation
        """
        path: str = "/segments/limits"
        res = self.connector.getData(self.endpoint+path, headers=self.header)
        return res

    def deleteSegment(self, segId: str = None, intCode: str = None)->str:
        """
        Delete the segment based on either segment ID or integration code.
        Arguments:
            segId : OPTIONAL : Segment Id to be deleted.
            intCode : OPTIONAL : integration code of the segment to be deleted.
        """
        if segId is None and intCode is None:
            raise Exception('Expecting a segment Id or an integration code')
        if segId is not None:
            path = f"/segments/{segId}"
            res = self.connector.deleteData(
                self.endpoint+path, headers=self.header)
            return res
        elif intCode is not None:
            path = f"/segments/ic:{intCode}"
            res = self.connector.deleteData(
                self.endpoint+path, headers=self.header)
            return res

    def deleteBulkSegments(self, segmentIds: list = None):
        """
        Delete the list of segments passed in the parameter.
        Arguments:
            segmentIds : REQUIRED : list of the segment id to be deleted.
        """
        path = "/segments/bulk-delete"
        if segmentIds is None or type(segmentIds) != list:
            raise Exception(
                'Require a list of ids to be deleted in traitIds parameter')
        data = [str(el) for el in segmentIds]
        res = self.connector.postData(
            self.endpoint + path, data=data, headers=self.header)
        return res

    def createSegment(self, name: str = None, segmentRule: str = None, folderId: int = None, dataSourceId: int = None, mergeRuleDataSourceId: int = None, integrationCode: str = None, **kwargs)->dict:
        """
        Create a segment based on the information provided.
        Arguments:
            name : REQUIRED : name of the segment
            segmentRule : REQUIRED : rule of the segment
            folderId : REQUIRED : Where segment is going to be located.
            dataSourceId : REQUIRED : DataSource associated with the segment.
            mergeRuleDataSourceId : REQUIRED : Profile merge rule associated with the segment.
            integrationCode : OPTIONAL : integration code
        """
        if name is None or segmentRule is None or folderId is None or dataSourceId is None or mergeRuleDataSourceId is None:
            raise Exception(
                "Some REQUIRED elements were not passed. Refer to the docstring")
        path = "/segments/"
        obj = {
            "name": name,
            "segmentRule": segmentRule,
            "folderId": str(folderId),
            "dataSourceId": str(dataSourceId),
            "mergeRuleDataSourceId": str(mergeRuleDataSourceId)
        }
        if integrationCode is not None:
            obj["integrationCode"] = str(integrationCode)
        for kwarg in kwargs:
            obj[kwarg] = str(kwargs[kwarg])
        res = self.connector.postData(
            self.endpoint+path, data=obj, headers=self.header)
        return res

    def updateSegment(self, segId: str = None, name: str = None, segmentRule: str = None, folderId: int = None, dataSourceId: int = None, mergeRuleDataSourceId: int = None, integrationCode: str = None, **kwargs)->dict:
        """
        update a segment based on the information provided.
        Arguments:
            segId : REQUIRED : segment Id to be updated
            name : REQUIRED : name of the segment
            segmentRule : REQUIRED : rule of the segment
            folderId : REQUIRED : Where segment is going to be located.
            dataSourceId : REQUIRED : DataSource associated with the segment.
            mergeRuleDataSourceId : REQUIRED : Profile merge rule associated with the segment.
            integrationCode : OPTIONAL : integration code
        """
        if segId is None:
            raise Exception("Missing segId as parameter")
        if name is None or segmentRule is None or folderId is None or dataSourceId is None or mergeRuleDataSourceId is None:
            raise Exception(
                "Some REQUIRED elements were not passed. Refer to the docstring")
        path = f"/segments/{segId}"
        obj = {
            "name": name,
            "segmentRule": segmentRule,
            "folderId": str(folderId),
            "dataSourceId": str(dataSourceId),
            "mergeRuleDataSourceId": str(mergeRuleDataSourceId)
        }
        if integrationCode is not None:
            obj["integrationCode"] = str(integrationCode)
        for kwarg in kwargs:
            obj[kwarg] = str(kwargs[kwarg])
        res = self.connector.putData(
            self.endpoint+path, data=obj, headers=self.header)
        return res

    def updateSegmentsMergeRuleBulk(self, oldMergeRuleId: str = None, newMergeRuleId: str = None)->str:
        """
        Update all segments from one merge rule id to another.
        Arguments:
            oldMergeRuleId : REQUIRED : old merge rule ID to be replaced.
            newMergeRuleId : REQUIRED : new merge rule ID to be set.
        """
        if oldMergeRuleId is None or newMergeRuleId is None:
            raise Exception(
                "oldMergeRuleId and newMergeRuleId are required as parameters")
        path: str = "/segments/bulk-merge-rule-replace"
        obj = {
            "existingMergeRuleDataSourceId": oldMergeRuleId,
            "replaceWithMergeRuleDataSourceId": newMergeRuleId
        }
        res = self.connector.postData(
            self.endpoint+path, data=obj, headers=self.header)
        return res

    def getSegmentFolders(self, format: str = 'df')->dict:
        """
        Returns the Segments folder information. 
        Arguments:
            includeThirdParty : OPTIONAL : For True, returns folders that store third-party traits.
            format : OPTIONAL : return a dataframe by default ("df"), but can return raw response ("raw")
        """
        path = "/folders/segments/"
        res = self.connector.getData(self.endpoint+path, headers=self.header)
        if format == "raw":
            return res
        elif format == "df":
            ids, names, parentids, folderCounts, paths = self._loop_folders(
                res)
            dict_folders = {
                'folderId': ids,
                'name': names,
                'parentFolderId': parentids,
                'path': paths,
                'folderCounts': folderCounts
            }
            df = modules.pd.DataFrame(dict_folders)
            return df

    def createSegmentFolder(self, name: str = None, parentFolderId: int = 0)->dict:
        """
        Create a Folder Segment.
        Arguments:
            parentFolderId : REQUIRED : ID of the Parent folder. Default root folder (0)
            name : REQUIRED : name of the folder.
        """
        path = "/folders/segments/"
        if name is None:
            raise Exception("require a name for the folder")
        obj = {
            "parentFolderId": str(parentFolderId),
            "name": name
        }
        res = self.connector.postData(
            self.endpoint+path, data=obj, headers=self.header)
        return res

    def deleteSegmentFolder(self, folderId: str)->str:
        """
        Delete a specific folder by its ID.
        Arguments:
            folderId : REQUIRED : Folder ID to be deleted.
        """
        if folderId is None:
            raise Exception("require folderId to be specified")
        path = f"/folders/segments/{folderId}"
        res = self.connector.deleteData(
            self.endpoint + path, headers=self.header)
        return res

    def updateSegmentFolder(self, folderId: str = None, name: str = None, parentFolderId: int = 0, **kwargs)->dict:
        """
        Update the folder ID specified with new information.
        Arguments:
            folderId : REQUIRED : Folder ID to be updated
            name : REQUIRED : name to be updated.
            parentFolderId : OPTIONAL : parentFolderId used to attach this folder (default root folder (0))
        kwargs possible : https://bank.demdex.com/portal/swagger/index.html#/Segment%20Folder%20API/put_folders_segments__folderId_
        """
        if folderId is None:
            raise Exception("require folderId to be specified")
        path = f"/folders/segments/{folderId}"
        obj = {
            "parentFolderId": str(parentFolderId),
            "name": name
        }
        if len(kwargs) > 0:
            for key in kwargs:
                obj[key] = str(kwargs[key])
        res = self.connector.putData(
            self.endpoint+path, data=obj, headers=self.header)
        return res

    def getDataSources(self, inboundOnly: bool = None, outboundOnly: bool = None, integrationCode: str = None, includeThirdParty: bool = None, modelingEnabled: bool = None,
                       availableForContainersOnly: bool = None, excludeReportSuites: bool = None, format: str = 'df')->dict:
        """ 
        Returns the datasources for that instances.
        Arguments:
            inboundOnly : OPTIONAL : Filter data sources with Inbound = true.
            outboundOnly : OPTIONAL : Filter data sources with Outbound = true.
            integrationCode : OPTIONAL : Filter on input integration code.
            includeThirdParty : OPTIONAL : set to True to include datasources from other companies
            modelingEnabled : OPTIONAL : set to True to only return datasources with modeling enabled.
            availableForContainersOnly : OPTIONAL : Filter data sources that is available for creating containers.
            excludeReportSuites : OPTIONAL : Exclude Report Suite DataSources in the result.
            format : OPTIONAL : return a dataframe by default ("df"), but can return raw response ("raw")
        """
        path = "/datasources/"
        params = {}
        if inboundOnly:
            params["inboundOnly"] = inboundOnly
        if outboundOnly:
            params["outboundOnly"] = outboundOnly
        if integrationCode is not None:
            params["integrationCode"] = integrationCode
        if includeThirdParty:
            params["includeThirdParty"] = includeThirdParty
        if modelingEnabled:
            params["modelingEnabled"] = modelingEnabled
        if availableForContainersOnly:
            params["availableForContainersOnly"] = availableForContainersOnly
        if excludeReportSuites:
            params["excludeReportSuites"] = excludeReportSuites
        res = self.connector.getData(
            self.endpoint+path, params=params, headers=self.header)
        if format == "raw":
            return res
        elif format == "df":
            df = modules.pd.DataFrame(res)
            return df

    def deleteDataSource(self, dataSourceId: str = None)->str:
        """
        Delete a specific Data Source based on its ID.
        Arguments:
            dataSourceId : REQUIRED : dataSource ID to be deleted
        """
        if dataSourceId is None:
            raise Exception("require a datasource ID as parameter")
        path = f"/datasources/{dataSourceId}"
        res = self.connector.deleteData(
            self.endpoint+path, headers=self.header)
        return res

    def deleteDataSourceBulk(self, dataSourceIds: list = None)->str:
        """
        Delete several data sources based on id passed. 
        Arguments:
            dataSourceIds : REQUIRED : list of dataSource ID to be deleted
        """
        if dataSourceIds is None or type(dataSourceIds) is not list:
            raise Exception("require a list of Data Source ID as a parameter")
        path = "/datasources/bulk-delete"
        res = self.connector.postData(
            self.endpoint+path, data=dataSourceIds, headers=self.header)
        return res

    def getDataSourceIdTypes(self)->dict:
        """
        Returns all available data source ID types
        """
        path = "/datasources/configurations/available-id-types"
        res = self.connector.getData(self.endpoint+path, headers=self.header)
        return res

    def getDataSourceInboundHistory(self, dataSourceId: str = None, includeSamplingDetails: bool = None)->dict:
        """
        Returns Onboarding Status Report with success and failure rates for inbound data source files.
        Arguments:
            dataSourceId : REQUIRED : Data Source to look for onboarding status.
            includeSamplingDetails : OPTIONAL : Specifies whether sampling details should be included in the response
        """
        if dataSourceId is None:
            raise Exception("require data source ID")
        path = f"/datasources/{dataSourceId}/history/inbound"
        params = {}
        if includeSamplingDetails:
            params["includeSamplingDetails"] = includeSamplingDetails
        res = self.connector.getData(
            self.endpoint+path, params=params, headers=self.header)
        return res

    def getDataSourceInboundHistorySample(self, dataSourceId: str = None, dataFileName: str = None)->dict:
        """
        Returns sampling data for a datasource for onboarding (inbound) files.
        Arguments:
            dataSourceId : REQUIRED : Data Source to look for onboarding status.
            dataFileName : REQUIRED : Inbound Data Filename
        """
        if dataSourceId is None:
            raise Exception("require data source ID")
        path = f"/datasources/{dataSourceId}/history/inbound"
        params = {}
        if dataFileName is not None:
            params["dataFileName"] = dataFileName
        res = self.connector.getData(
            self.endpoint+path, params=params, headers=self.header)
        return res

    def getMostChangedTraits(self, interval: str = "1D", cutOff: int = 0, restrictType: str = None, format: str = 'raw')->dict:
        """
        returns information about the most changed traits for a given interval. 
        The response include compacted trait information, along with the trait metrics and deltas. 
        Pagination, and Sorting supported. Also, the response would be cached for a day if reporting data is available for the current date.
        Arguments:
            interval : OPTIONAL : Interval for which most changed traits are computed. Valid values are (1D/7D/14D/30D/60D). Default value set to 1D.
            cutOff : OPTIONAL : specifies cutOff for total uniques needed in order for the trait to be qualified for consideration. Default is set to 0
            restrictType : OPTIONAL : The trait type this list should be restricted to. Valid values are RULE_BASED_TRAIT, ON_BOARDED_TRAIT, and ALGO_TRAIT.
            By default, the response would be computed over all trait types.
            format : OPTIONAL : return raw response by default ("raw"), but can return a dataframe ("df")
        """
        path = "/reports/most-changed-traits"
        params = {'interval': interval, "cutOff": 0}
        if restrictType is not None:
            if restrictType in ["RULE_BASED_TRAIT", "ON_BOARDED_TRAIT", "ALGO_TRAIT"]:
                params['restrictType'] = restrictType
        res = self.connector.getData(
            self.endpoint+path, params=params, headers=self.header)
        if format == "raw":
            return res
        elif format == "df":
            df = modules.pd.DataFrame(res)
            return df

    def getMostChangedSegments(self, interval: str = "1D", cutOff: int = 0, format: str = 'raw')->dict:
        """
        returns information about the most changed segments for a given interval. 
        The response include segments, along with the segment metrics and deltas. Pagination, and Sorting supported. 
        Also, the response would be cached for a day if reporting data is available for the current date.
        Arguments:
            interval : OPTIONAL : Interval for which most changed segments are computed. Valid values are (1D/7D/14D/30D/60D). Default value set to 1D.
            cutOff : OPTIONAL : specifies cutOff for total uniques needed in order for the segment to be qualified for consideration. Default is set to 0
            format : OPTIONAL : return raw response by default ("raw"), but can return a dataframe ("df")
        """
        path = "/reports//most-changed-segments"
        params = {'interval': interval, "cutOff": 0}
        res = self.connector.getData(
            self.endpoint+path, params=params, headers=self.header)
        if format == "raw":
            return res
        elif format == "df":
            df = modules.pd.DataFrame(res)
            return df

    def getLargestTraits(self, interval: str = "1D", cutOff: int = 0, restrictType: str = None, format: str = 'raw')->dict:
        """
        Returns information about the largest traits for a given interval. The response include compacted trait information, along with the trait metrics. 
        Pagination, and Sorting supported. 
        Also, the response would be cached for a day if reporting data is available for the current date.
        Arguments:
            interval : OPTIONAL : Interval for which most largest traits are computed. Valid values are (1D/7D/14D/30D/60D). Default value set to 1D.
            cutOff : OPTIONAL : specifies cutOff for total uniques needed in order for the trait to be qualified for consideration. Default is set to 0
            restrictType : OPTIONAL : The trait type this list should be restricted to. Valid values are RULE_BASED_TRAIT, ON_BOARDED_TRAIT, and ALGO_TRAIT.
            By default, the response would be computed over all trait types.
            format : OPTIONAL : return raw response by default ("raw"), but can return a dataframe ("df")
        """
        path = "/reports/largest-traits"
        params = {'interval': interval, "cutOff": 0}
        if restrictType is not None:
            if restrictType in ["RULE_BASED_TRAIT", "ON_BOARDED_TRAIT", "ALGO_TRAIT"]:
                params['restrictType'] = restrictType
        res = self.connector.getData(
            self.endpoint+path, params=params, headers=self.header)
        if format == "raw":
            return res
        elif format == "df":
            df = modules.pd.DataFrame(res)
            return df

    def getLargestSegments(self, interval: str = "1D", cutOff: int = 0, format: str = 'raw')->dict:
        """
        Returns information about the largest segments for a given interval. The response include segments, along with the segment metrics and deltas. 
        Pagination, and Sorting supported. 
        Also, the response would be cached for a day if reporting data is available for the current date.
        Arguments:
            interval : OPTIONAL : Interval for which most largest traits are computed. Valid values are (1D/7D/14D/30D/60D). Default value set to 1D.
            cutOff : OPTIONAL : specifies cutOff for total uniques needed in order for the trait to be qualified for consideration. Default is set to 0
            restrictType : OPTIONAL : The trait type this list should be restricted to. Valid values are RULE_BASED_TRAIT, ON_BOARDED_TRAIT, and ALGO_TRAIT.
            By default, the response would be computed over all trait types.
            format : OPTIONAL : return raw response by default ("raw"), but can return a dataframe ("df")
        """
        path = "/reports/largest-segments"
        params = {'interval': interval, "cutOff": 0}
        res = self.connector.getData(
            self.endpoint+path, params=params, headers=self.header)
        if format == "raw":
            return res
        elif format == "df":
            df = modules.pd.DataFrame(res)
            return df
