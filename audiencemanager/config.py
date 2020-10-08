from audiencemanager import modules

config_object = {
    "org_id": "",
    "client_id": "",
    "tech_id": "",
    "pathToKey": "",
    "secret": "",
    "companyid": "",
    "date_limit": 0,
    "tokenEndpoint": "https://ims-na1.adobelogin.com/ims/exchange/jwt",
    "location": modules.Path.as_posix(modules.Path.cwd()),
    "token": ''
}

header = {"Accept": "application/json",
          "Content-Type": "application/json",
          "Authorization": "",
          "X-Api-Key": config_object["client_id"],
          "x-gw-ims-org-id": config_object["org_id"],
          }
