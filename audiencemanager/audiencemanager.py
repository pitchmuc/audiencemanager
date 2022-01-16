from audiencemanager import config
from audiencemanager import connector
from copy import deepcopy
import json
import pandas as pd

class AudienceManager:
    """
    Class that will enable you to request information on your Audience Manager data.
    Calling this class will generate automatically a token to request the API later on.
    """

    def __init__(self, config_object: dict = config.config_object)->None:
        """
        Instantiate the Audience Manager class.
        """
        self.config = deepcopy(config_object)
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

    def getTraits(self, folderId: int = None, includeMetrics:bool=True,integrationCode: str = None, dataSourceIds: list = None, includeDetails: bool = False, format: str = 'df',save:bool=False)->object:
        """
        Return traits following the parameters provided.
        Can return 2 type of result, a dataframe or a list. 
        Arguments:
            folderId : OPTIONAL : Only return traits from the selected folder.
            includeMetrics : OPTIONAL : Include the trait population (default True)
            integrationCode : OPTIONAL : Returns traits that contain this integration code.
            dataSourceId : OPTIONAL : List of dataSourceIds. Returns traits that belong to the selected data sources.  
            includeDetails : OPTIONAL : For True, returns additional details for the traits. Additional returned values include ttl,integrationCode, comments, traitRule, traitRuleVersion, and type.
            format : OPTIONAL : default "df" that returns a dataframe, you can also return the raw format ("raw")
            save : OPTIONAL : if set to true, create a file to save the data.
        """
        path = "/traits/"
        params = {}
        if includeMetrics:
            params["includeMetrics"] = includeMetrics
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
            if save:
                with open('traits.json', "w") as f:
                    f.write(json.dumps(res,indent=2))
            return res
        elif format == "df":
            df = pd.DataFrame(res)
            if save:
                df.to_csv('traits.csv',index=False)
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

    def deleteTrait(self, traitId: str = None, intCode: str = None)->str:
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
        res = self.connector.deleteData(self.endpoint+path, headers=self.header)
        return res

    def deleteBulkTraits(self, traitIds: list = None, verbose:bool=False) -> str:
        """
        Delete the traits passed in the traitIds list parameter.
        Careful old endpoint used here and inconsistent result returned.
        Error can be return but the job has been processed.
        Arguments:
            traitIds : REQUIRED : list of trait Ids to be deleted
            verbose : OPTIONAL : print information if set to true. 
        """
        path = "/traits/bulk-delete"
        if traitIds is None or type(traitIds) != list:
            raise Exception(
                'Require a list of ids to be deleted in traitIds parameter')
        data = [el for el in traitIds]
        if verbose:
            print(f"Deleting {len(data)} traits")
        res = self.connector.postData(
            f"https://api.demdex.com/v1{path}", data=data, headers=self.header,verbose=verbose)
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
            df = pd.DataFrame(dict_folders)
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

    def getSegments(self, includeInUseStatus: bool = None, status: str = None, containsTrait: int = None, dataSourceId: int = None, mergeRuleDataSourceId: int = None, includeMetrics: bool = True, includeTraitDataSourceIds: bool = False, includeAddressableAudienceMetrics: bool = False, format: str = 'df',save:bool=False)->object:
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
            save : OPTIONAL : if set to True will save the data in a file.
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
            if save:
                with open("segment.json", 'w') as f:
                    f.write(json.dumps(res,indent=2))
            return res
        elif format == "df":
            df = pd.DataFrame(res)
            if save:
                df.to_csv('segments.csv',index=False)
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

    def deleteBulkSegments(self, segmentIds: list = None,verbose:bool=False):
        """
        Delete the list of segments passed in the parameter.
        Arguments:
            segmentIds : REQUIRED : list of the segment id to be deleted.
            verbose : OPTIONAL : Prn information if set to True.
        """
        path = "/segments/bulk-delete"
        if segmentIds is None or type(segmentIds) != list:
            raise Exception(
                'Require a list of ids to be deleted in traitIds parameter')
        data = [el for el in segmentIds]
        if verbose:
            print(f"Deleting {len(data)} Segments")
        res = self.connector.postData(
            self.endpoint + path, data=data, headers=self.header,verbose=True)
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
            df = pd.DataFrame(dict_folders)
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
                       availableForContainersOnly: bool = None, excludeReportSuites: bool = None, format: str = 'df',save:bool=False)->dict:
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
            save : OPTIONAL : if set to True, save in a file.(default False)
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
            if save: 
                with open('datasources.json', 'w') as f:
                    f.write(json.dumps(res))
            return res
        elif format == "df":
            df = pd.DataFrame(res)
            if save:
                df.to_csv('datasources.csv',index=False)
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

    def getMostChangedTraits(self, interval: str = "1D", cutOff: int = 0, restrictType: str = None, format: str = 'raw',save:bool=False)->dict:
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
            save : OPTIONAL : If set to True, will save the data in a file. (default False)
        """
        path = "/reports/most-changed-traits"
        params = {'interval': interval, "cutOff": 0}
        if restrictType is not None:
            if restrictType in ["RULE_BASED_TRAIT", "ON_BOARDED_TRAIT", "ALGO_TRAIT"]:
                params['restrictType'] = restrictType
        res = self.connector.getData(
            self.endpoint+path, params=params, headers=self.header)
        if format == "raw":
            if save:
                with open('mostChangedTraits.json', 'w') as f:
                    f.write(json.dumps(res,indent=2))
            return res
        elif format == "df":
            df = pd.DataFrame(res)
            if save:
                df.to_csv('mostChangedTraits.csv')
            return df

    def getMostChangedSegments(self, interval: str = "1D", cutOff: int = 0, format: str = 'raw',save:bool=False)->dict:
        """
        returns information about the most changed segments for a given interval. 
        The response include segments, along with the segment metrics and deltas. Pagination, and Sorting supported. 
        Also, the response would be cached for a day if reporting data is available for the current date.
        Arguments:
            interval : OPTIONAL : Interval for which most changed segments are computed. Valid values are (1D/7D/14D/30D/60D). Default value set to 1D.
            cutOff : OPTIONAL : specifies cutOff for total uniques needed in order for the segment to be qualified for consideration. Default is set to 0
            format : OPTIONAL : return raw response by default ("raw"), but can return a dataframe ("df")
            save : OPTIONAL : If set to True, will save the data in a file. (default False)
        """
        path = "/reports//most-changed-segments"
        params = {'interval': interval, "cutOff": 0}
        res = self.connector.getData(
            self.endpoint+path, params=params, headers=self.header)
        if format == "raw":
            if save:
                with open('mostChangedSegments.json', 'w') as f:
                    f.write(json.dumps(res,indent=2))
            return res
        elif format == "df":
            df = pd.DataFrame(res)
            if save:
                df.to_csv('mostChangedSegments.csv')
            return df

    def getLargestTraits(self, interval: str = "1D", cutOff: int = 0, restrictType: str = None, format: str = 'raw',save:bool=False)->dict:
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
            save : OPTIONAL : If set to True, will save the data in a file. (default False)
        """
        path = "/reports/largest-traits"
        params = {'interval': interval, "cutOff": 0}
        if restrictType is not None:
            if restrictType in ["RULE_BASED_TRAIT", "ON_BOARDED_TRAIT", "ALGO_TRAIT"]:
                params['restrictType'] = restrictType
        res = self.connector.getData(
            self.endpoint+path, params=params, headers=self.header)
        if format == "raw":
            if save:
                with open('LargestTraits.json', 'w') as f:
                    f.write(json.dumps(res,indent=2))
            return res
        elif format == "df":
            df = pd.DataFrame(res)
            if save:
                df.to_csv('LargestTraits.csv')
            return df

    def getLargestSegments(self, interval: str = "1D", cutOff: int = 0, format: str = 'raw',save:bool=False)->dict:
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
            save : OPTIONAL : If set to True, will save the data in a file. (default False)
        """
        path = "/reports/largest-segments"
        params = {'interval': interval, "cutOff": 0}
        res = self.connector.getData(
            self.endpoint+path, params=params, headers=self.header)
        if format == "raw":
            if save:
                with open('LargestSegments.json', 'w') as f:
                    f.write(json.dumps(res,indent=2))
            return res
        elif format == "df":
            df = pd.DataFrame(res)
            if save:
                df.to_csv('LargestSegments.csv')
            return df

    def getDestinations(self,containsSegment:str=None,includeMasterDataSourceIdType:bool=None,includeMetrics:bool=True,includeAddressableAudienceMetrics:bool=False,format:str='df',save:bool=False) -> object:
        """
        By default return a dataframe of the different destinations used.
        Arguments:
            containsSegment : OPTIONAL : Segment Id that has to be used in the destinations.
            includeMasterDataSourceIdType : OPTIONAL : If set to true, it includes the Master Data Source ID
            includeMetrics : OPTIONAL : returns metrics for the destinations (default True)
            includeAddressableAudienceMetrics : OPTIONAL : returns the addressable audience information (default False)
            format : OPTIONAL : by default (df) returning a dataframe of the information. Can return raw answer by setting "raw".
            save : OPTIONAL : If set to True, will save the data in a file. (default False)
        """
        path = "/destinations/"
        params = {}
        if includeMasterDataSourceIdType:
            params["includeMasterDataSourceIdType"] = includeMasterDataSourceIdType
        if includeMetrics:
            params["includeMetrics"] = includeMetrics
        if includeAddressableAudienceMetrics:
            params["includeAddressableAudienceMetrics"] = includeAddressableAudienceMetrics
        if containsSegment is not None:
            params["containsSegment"] = containsSegment
        res = self.connector.getData(self.endpoint + path, params=params, headers=self.header)
        if format == "raw":
            if save:
                with open('destinations.json', 'w') as f:
                    f.write(json.dumps(res,indent=2))
            return res
        elif format == "df":
            df = pd.DataFrame(res)
            if save:
                df.to_csv('destinations.csv')
            return df
    
    def getDestinationsLimits(self)->dict:
        """
        Return the limit of the destination.
        """
        path = "/destinations/limits"
        res = self.connector.getData(self.endpoint + path, headers=self.header)
        return res
    
    def getDestinationHistory(self, destinationId: str = None) -> dict:
        """
        Return the outbound batch job history information for a specified destination and time period.
        Arguments:
            destinationId : REQUIRED : destination ID to be used.
        """
        if destinationId is None:
            raise Exception("require a destination ID")
        path = f"/destinations/{destinationId}/history/outbound/"
        res = self.connector.getData(self.endpoint + path, headers=self.header)
        return res
    
    def getDestination(self, destinationId: str = None)->dict:
        """
        Return a destination information based on its ID.
        Arguments:
            destinationId : REQUIRED : destination ID to be used.
        """
        if destinationId is None:
            raise Exception("require a destination ID")
        path = f"/destinations/{destinationId}"
        res = self.connector.getData(self.endpoint + path, headers=self.header)
        return res

    def deleteDestination(self, destinationId: str = None)->str:
        """
        Delete a destination based on its ID.
        Arguments:
            destinationId : REQUIRED : destination ID to be deleted.
        """
        if destinationId is None:
            raise Exception("require a destination ID")
        path = f"/destinations/{destinationId}"
        res = self.connector.deleteData(self.endpoint + path, headers=self.header)
        return res
    
    def createDestination(self, data: dict = None)->dict:
        """
        Create a destination based on the data passed on the argument.
        Argument:
            data : REQUIRED : Dictionary providing the required information to create a destination.
        """
        if data is None or type(data) != dict:
            raise Exception("Require a dictionary that contains the information to create the connection")
        if 'name' not in data.keys():
            raise ValueError("Requires a name for the connection")
        path = "/destinations/"
        res = self.connector.postData(self.endpoint+path, data=data,headers=self.header)
        return res
    
    def updateDestination(self, destinationId: str = None, data: dict = None) -> dict:
        """
        Update a destination based on the data passed on the argument.
        Argument:
            destinationId : REQUIRED : ID of the destination to be created
            data : REQUIRED : Dictionary providing the required information to create a destination.
        """
        if destinationId is None:
            raise ValueError("destinationId is required ")
        if data is None or type(data) != dict:
            raise Exception("Require a dictionary that contains the information to create the connection")
        if 'name' not in data.keys():
            raise ValueError("Requires a name for the connection")
        path = f"/destinations/{destinationId}"
        res = self.connector.putData(self.endpoint+path, data=data,headers=self.header)
        return res
    
    def getDestinationMappings(self, destinationId: str = None, includeMetrics: bool = True, includeAddressableAudienceMetrics: bool = None, includeDeletedEntities: bool = None) -> dict:
        """
        Returns all the destination mappings for a specific destination by 'destinationId'.
        Arguments:
            destinationId : REQUIRED : destination ID to look for.
            includeMetrics : OPTIONAL : returns the metrics for the destination. (default True)
            includeAddressableAudienceMetrics : OPTIONAL : if set to True returns the addressable audience metrics.(default None)
            includeDeletedEntities : OPTIONAL : if set to True, return the information with deleted entities. (default None)
        """
        if destinationId is None:
            raise Exception("Requires a destinationId parameter")
        params = {}
        if includeMetrics:
            params["includeMetrics"] = includeMetrics
        if includeAddressableAudienceMetrics:
            params["includeAddressableAudienceMetrics"] = includeAddressableAudienceMetrics
        if includeDeletedEntities:
            params["includeDeletedEntities"] = includeDeletedEntities
        path = f"/destinations/{destinationId}/mappings/"
        res = self.connector.getData(self.endpoint+path, params=params, headers=self.header)
        return res

    def getDestinationMapping(self,destinationId : str = None, mappingId : str = None)->dict:
        """
        Return a single destination mapping by combination of destinationId and destinationMappingId.
        Arguments:
            destinationId : REQUIRED : destination ID to be used.
            mappingId : REQUIRED : destination mapping ID to be used
        """
        if destinationId is None or mappingId is None:
            raise Exception("destinationId and mappingId are required")
        path = f"/destinations/{destinationId}/mappings/{mappingId}"
        res = self.connector.getData(self.endpoint+path, headers=self.header)
        return res
    
    def deleteDestinationMapping(self,destinationId : str = None, mappingId : str = None)->str:
        """
        Delete a single destination mapping by combination of destinationId and destinationMappingId.
        Arguments:
            destinationId : REQUIRED : destination ID to be deleted.
            mappingId : REQUIRED : destination mapping ID to be deleted.
        """
        if destinationId is None or mappingId is None:
            raise Exception("destinationId and mappingId are required")
        path = f"/destinations/{destinationId}/mappings/{mappingId}"
        res = self.connector.deleteData(self.endpoint+path, headers=self.header)
        return res
    
    def getDestinationSegmentImpressions(self,includeDestinationName:bool=True,includeSegmentName:bool=True,includeFeedIds:bool=None)->list:
        """
        Returns the impressions for the segment on a destination.
        Arguments:
            includeDestinationName : OPTIONAL : Includes destination name in the response (default True)
            includeSegmentName : OPTIONAL : Includes segment name in the response
            includeFeedIds : OPTIONAL : Includes feed IDs in use with segment impressions
        """
        path = "/destinations/segment-impressions/"
        params={}
        if includeDestinationName:
            params["includeDestinationName"] = includeDestinationName
        if includeSegmentName:
            params["includeSegmentName"] = includeSegmentName
        if includeFeedIds:
            params["includeFeedIds"] = includeFeedIds
        res = self.connector.getData(self.endpoint + path, params=params, headers=self.header)
        return res

    def getDerivedSignals(self,format:str='df',save:bool=False)->object:
        """
        Get the derived signals associated with this AAM instance.
        Arguments:
            format : OPTIONAL : return a dataframe ("df") by default , but can return raw response ("raw")
            save : OPTIONAL : if set to True, save the data in a file (default False)
        """
        path = "/signals/derived"
        res = self.connector.getData(self.endpoint + path, headers=self.header)
        if format == "raw":
            if save:
                with open('derivedSignals.json', 'w') as f:
                    f.write(json.dumps(res,indent=2))
            return res
        elif format == "df":
            df = pd.DataFrame(res)
            if save:
                df.to_csv('derivedSignals.csv',index=False)
            return df
    
    def getDerivedSignal(self, signalId: str = None) -> dict:
        """
        Retrieve a single derived ID.
        Arguments:
            signalId : REQUIRED : Derived signal ID to be retrieved. 
        """
        if signalId is None:
            raise Exception("signalId argument is required")
        path = f"/signals/derived/{signalId}"
        res = self.connector.getData(self.endpoint + path, headers=self.header)
        return res
    
    def deleteDerivedSignal(self, signalId: str = None) -> str:
        """
        Delete a single derived ID.
        Arguments:
            signalId : REQUIRED : Derived signal ID to be deleted. 
        """
        if signalId is None:
            raise Exception("signalId argument is required")
        path = f"/signals/derived/{signalId}"
        res = self.connector.deleteData(self.endpoint + path, headers=self.header)
        return res
    
    def createDerivedSignal(self, sourceKey: str = None, sourceValue: str = None, targetKey: str = None, targetValue: str = None, integrationCode: str = "") -> dict:
        """
        Create a derived signal based on the information passed.
        Arguments:
            sourceKey : REQUIRED : signal key that the rule will be based on.
            sourceValue : REQUIRED : signal value that the rule will look for.
            targetKey : REQUIRED : target key that the signal will create.
            targetValue : REQUIRED : target value that the signal will create.
            integrationCode : OPTIONAL : integration code for the signal.
        """
        if sourceKey is None or sourceValue is None:
            raise ValueError("sourceKey and sourceValue are required to build the derived signal")
        if targetKey is None or targetValue is None:
            raise ValueError("targetKey and targetValue are required to build the derived signal")
        path = "/signals/derived"
        obj = {
            "sourceKey": sourceKey,
            "sourceValue": sourceValue,
            "targetKey": targetKey,
            "targetValue": targetValue,
            "integrationCode":integrationCode
        }
        res = self.connector.postData(self.endpoint + path, data=obj, headers=self.header)
        return res
    
    def updateDerivedSignal(self, signalId:str=None,sourceKey: str = None, sourceValue: str = None, targetKey: str = None, targetValue: str = None, integrationCode: str = "") -> dict:
        """
        Update a derived signal based on the information passed.
        Arguments:
            signalId : REQUIRED : derived signal ID to be updated.
            sourceKey : REQUIRED : signal key that the rule will be based on.
            sourceValue : REQUIRED : signal value that the rule will look for.
            targetKey : REQUIRED : target key that the signal will create.
            targetValue : REQUIRED : target value that the signal will create.
            integrationCode : OPTIONAL : integration code for the signal.
        """
        if signalId is None:
            raise Exception("requires a signal ID to be specified")
        if sourceKey is None or sourceValue is None:
            raise ValueError("sourceKey and sourceValue are required to build the derived signal")
        if targetKey is None or targetValue is None:
            raise ValueError("targetKey and targetValue are required to build the derived signal")
        path = f"/signals/derived/{signalId}"
        obj = {
            "sourceKey": sourceKey,
            "sourceValue": sourceValue,
            "targetKey": targetKey,
            "targetValue": targetValue,
            "integrationCode":integrationCode
        }
        res = self.connector.putData(self.endpoint + path, data=obj, headers=self.header)
        return res
    
    def getModels(self, search: str = None,includeDataSources:bool=False,usesDataSource:bool=False,containsSeedFromDataSource:bool=False,save:bool=False,**kwargs)->object:
        """
        This returns the algorithmic traits and their summary.
        Arguments:
            search : OPTIONAL : Returns results based on the specified string you want to use as a search parameter. Looks in any fields.
            includeDataSources : OPTIONAL : Select true to return information about data sources in the model information.
            usesDataSource : OPTIONAL : Returns models that use this data source ID.
            containsSeedFromDataSource : OPTIONAL : Returns information about the models that uses a trait or segment from this data source ID as a baseline seed.
            save : OPTIONAL : if set to True, create a file to save the result.
        """
        path = "/models"
        params = {"pageSize":kwargs.get("pageSize",100)}
        if search:
            params["search"] = search
        if includeDataSources:
            params["includeDataSources"] = includeDataSources
        if usesDataSource:
            params["usesDataSource"] = usesDataSource
        if containsSeedFromDataSource:
            params["containsSeedFromDataSource"] = containsSeedFromDataSource
        res = self.connector.getData(self.endpoint + path, headers=self.header, params=params)
        df = pd.DataFrame(res)
        if save:
            df.to_csv('models.csv',index=False)
        return df
    
    def deleteModel(self, modelId: str) -> str:
        """
        Delete the model ID specified in the parameter.
        Arguments: 
            modelId : REQUIRED : model ID to be deleted.
        """
        if modelId is None:
            raise Exception("Expected a model ID as parameter")
        path = f"/models/{modelId}"
        res = self.connector.deleteData(self.endpoint + path, headers=self.header)
        return res
    
    def getModel(self, modelId: str = None) -> dict:
        """
        Return a dictionary of the model details.
        Arguments:
            modelId : REQUIRED : the model ID to be retrieved.
        """
        if modelId is None:
            raise Exception("Expected a model ID as parameter")
        path = f"/models/{modelId}"
        res = self.connector.getData(self.endpoint + path, headers=self.header)
        return res
    
    def getModelTraits(self, modelId: str = None, format:str='df') -> object:
        """
        Returns the most influencial traits used in a specific models.
        Arguments:
            modelId : REQUIRE : the model ID to be retrieved
            format : OPTIONAL : by default returns a dataframe ("df"), can return the default list "raw".
        """
        if modelId is None:
            raise Exception("Expected a model ID as parameter")
        path = f"/models/{modelId}/runs/latest/traits"
        res = self.connector.getData(self.endpoint + path, headers=self.header)
        if format == "raw":
            return res
        elif format == "df":
            df = pd.DataFrame(res)
            return df

    def getModelStats(self, modelId: str = None) -> dict:
        """
        Returns accuracy and reach values for your algorithmic model.
        Arguments:
            modelId : REQUIRE : the model ID to be retrieved
        """
        if modelId is None:
            raise Exception("Expected a model ID as parameter")
        path = f"/models/{modelId}/runs/latest/stats"
        res = self.connector.getData(self.endpoint + path, headers=self.header)
        return res