

class AudienceManager:
    """
    Class that will enable you to request information on your Audience Manager data.
    """

    def __init__(self, config_object: dict = audiencemanager.config.config_object)->None:
        """
        Instantiate the Audience Manager class.
        """
        self.config = aam.modules.deepcopy(config_object)
        self.connector = aam.connector.AdobeRequest(
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
