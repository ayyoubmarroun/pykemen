import json
import webbrowser
import httplib2
from googleapiclient.discovery import build
from oauth2client import client
import os
from builtins import input

def saveJson(filename, object):
    with open(filename, 'w') as f:
        json.dump(object, f)


def openJson(filename):
    with open(filename, 'r') as f:
        object = json.load(f)
    return object


def getCredentials(secrets, credentials, scopes):
    if not os.path.isfile(credentials):
        flow = client.flow_from_clientsecrets(
                secrets,
                scope=scopes,
                redirect_uri='urn:ietf:wg:oauth:2.0:oob')
        auth_uri = flow.step1_get_authorize_url()
        print("Auth url: {}".format(auth_uri))
        webbrowser.open(auth_uri)
        auth_code = input('Enter the auth code: ')
        cre = flow.step2_exchange(auth_code)
        saveJson(credentials,cre.to_json())
    else:
        cre = client.Credentials.new_from_json(openJson(credentials))
    return cre


def create_api(api_name, api_version, scopes=None, secrets=None, credentials=None):
    if None in (secrets, credentials, scopes):
        return build(api_name, api_version)
    # else:
    #     raise ValueError("The variables {}, {} and {} should not be empty if there is no SA available!".format(scopes, secrets, credentials))
    http_auth = getCredentials(secrets, credentials, scopes).authorize(httplib2.Http())
    return build(api_name, api_version, http=http_auth)
