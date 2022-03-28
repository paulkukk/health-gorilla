import AWS as aws
import datetime
import requests ##requests must be installed to use this script
import json
import jwt
import csv
import HealthGorillaHelper as hgh
from urllib.parse import urlparse, urlsplit
import time


baseUrl = "https://sandbox.healthgorilla.com"
url = baseUrl+"/oauth/token"
client_id = 'rTAySqHFrHf7NQQPQDfcHE4esqmgKrqGySHtbCNC'
BUCKET_NAME = 'db-fhir'

def getBearerToken():
    '''
    Function handles creating the token to Health Gorilla to make API calls
    :return:
    '''
    bearerToken = ''
    assertion = ''
    # We need to set the variables to waht we need to create the Token
    tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
    audience = 'https://sandbox.healthgorilla.com/oauth/token'
    issuer = 'https://daybreak.health/'
    sub = 'paul.kukk'
    now = datetime.datetime.now()

    # Build the dict for the encoding
    data = {"aud": audience,  "iss": issuer, "sub": sub, "iat": now, "exp": tomorrow};
    # Need to grab the JWT toekn assign it to the assertion variable
    assertion = jwt.encode(data, "QBPGbU5/ZqjAmKt1jQc/2baDSZxwZS+uknh2xGJ7j9c=", algorithm="HS256")

    # Create the payload
    payload = 'grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&client_id=' + client_id + '&assertion='\
              + assertion + '&scope=user%2F*.*%20patient360%20'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    # make the request
    response = requests.request("POST", url, headers=headers, data=payload)

    # convert string to a dict so user can pull access token into a variable
    resDict = json.loads(response.text)
    bearerToken = resDict['access_token']
    print('bearerToken', bearerToken)
    return bearerToken


def createPatient(pat, bearerToken):
    '''
    Function creates a patient inside Health Gorilla
    :param pat:
    :param bearerToken:
    :return:
    '''
    url = baseUrl + "/fhir/R4/Patient"
    headers = {
        'Authorization': 'Bearer ' + bearerToken,
        'Content-Type': 'application/json',
    }
    payload = pat
    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code == 201:
        # get the response header location and split it into elements
        fhirPatientUrl = urlsplit(response.headers['location'])
        # grab the path from the location url
        fhirPath = fhirPatientUrl[2]
        # strip the path from the patient ID
        patientId = fhirPath[14:]

        print("Patient created in HG with patient ID " + patientId)
        return patientId
    else:
        print("Failure to create a new HG patient for patient")
        return False


def searchForPatient(patient, bearerToken):
    '''
    Function will search for a patient based on last anem and first name.
    :param patient:
    :param bearerToken:
    :return:
    '''
    firstName = patient['First Name']
    lastName = patient['Last Name']
    print("Looking for Health Gorilla record for", firstName, lastName)
    # url = "https://sandbox.healthgorilla.com/fhir/Patient?given=" + firstName + "&family=" + lastName + "&birthdate=" + birthdate
    url = baseUrl+"/fhir/R4/Patient?given="+lastName+"&family="+firstName

    payload = ""
    headers = {
        'Authorization': 'Bearer ' + bearerToken,
        'Content-Type': 'application/json',
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    resDict = json.loads(response.text)
    # print('-->', resDict)
    if resDict['total'] == 0:
        print("No records Found")
        return resDict
    elif resDict['total'] == 1:
        print("1 record Found")

        # next line returns the HG ID from the JSON response
        # return resDict['entry'][0]['resource']['id']
        return resDict
    else:
        print("Multiple records Found")
        # return "multiple"
        return resDict


def getPatientEverything(conn, patientid, bearerToken):
    print('Getting the everything reference patient ', patientid)
    url = baseUrl + "/fhir/R4/Patient/" + patientid + "/$everything"
    payload = ""
    headers = {
        'Authorization': 'Bearer ' + bearerToken,
        'Content-Type': 'application/json',
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    resDict = json.loads(response.text)

    # Move full file to S3 bucket
    json_obj = a_bytes = bytes(response.text, "UTF-8")
    bucket_path = 'patient/' + patientid + '_patient.jsonc'
    aws.put_in_object(json_obj, BUCKET_NAME, bucket_path)

    print('HG_ID', resDict['entry'][0]['resource']['id'])
    print('Last Updated', resDict['entry'][0]['resource']['meta']['lastUpdated'])
    # print(resDict['entry'][0]['resource']['extension'][0]['url'])
    # print(resDict)
    # print('EnrollmentDate', resDict['entry'][0]['resource']['extension'][0]['extension'][1]['valueDateTime'])

    # We need to loop thru the Record and process the data into the ODS
    for _resource in resDict["entry"]:
        # print(_resource['resource']['resourceType'])
        if _resource['resource']['resourceType'] == 'Observation':
            # Pass that section of the dict, the HG Patient Id and conneciton to Process
            hgh.processObservation(_resource, resDict['entry'][0]['resource']['id'], conn)

        elif _resource['resource']['resourceType'] == 'DiagnosticReport':
            # Pass that section of the dict, the HG Patient Id and conneciton to Process
            hgh.processDiagnosticReport(_resource, resDict['entry'][0]['resource']['id'], conn)  # JSON, HG Id conn

        elif _resource['resource']['resourceType'] == 'Coverage':
            # Pass that section of the dict, the HG Patient Id and conneciton to Process
            hgh.processCoverage(_resource, resDict['entry'][0]['resource']['id'], conn)  # JSON, HG Id conn

        elif _resource['resource']['resourceType'] == 'Patient':
            # Pass that section of the dict, the HG Patient Id and conneciton to Process
            hgh.processPatient(_resource, resDict['entry'][0]['resource']['id'], conn)  # JSON, HG Id conn

        elif _resource['resource']['resourceType'] == 'Organization':
            # Pass that section of the dict, the HG Patient Id and conneciton to Process
            hgh.processOrganization(_resource, resDict['entry'][0]['resource']['id'], conn)  # JSON, HG Id conn

        elif _resource['resource']['resourceType'] == 'Encounter':
            # Pass that section of the dict, the HG Patient Id and conneciton to Process
            hgh.processEncounter(_resource, resDict['entry'][0]['resource']['id'], conn)  # JSON, HG Id conn


def getPatientImmunization(conn, patientid, bearerToken):
    # print('Getting the immunization reference patient ', patientid)
    url = baseUrl + "/fhir/R4/Immunization?patient=" + patientid
    payload = ""
    headers = {
        'Authorization': 'Bearer ' + bearerToken,
        'Content-Type': 'application/json',
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    resDict = json.loads(response.text)
    # print(response.text)

    # Move full file to S3 bucket
    json_obj = a_bytes = bytes(response.text, "UTF-8")
    bucket_path = 'immunization/' + patientid + '_immunization.jsonc'
    aws.put_in_object(json_obj, BUCKET_NAME, bucket_path)
    print('HG_ID', patientid)
    print('Last Updated', resDict['meta']['lastUpdated'])
    # print(resDict['entry'][0]['resource']['extension'][0]['url'])
    # print('EnrollmentDate', resDict['entry'][0]['resource']['extension'][0]['extension'][1]['valueDateTime'])

    # Need to see if anything came back from the search result
    search_result = resDict['total']

    # If search+result is not 0 then we need to loop thru the Record and process the data into the ODS
    if search_result != 0:
        for _resource in resDict["entry"]:
            if _resource['resource']['resourceType'] == 'Immunization':
                hgh.processImmunization(_resource, patientid, conn)
