import AWS as aws
import json
import PostgresHelper as ph


def processEncounter(hgDict, hg_id, conn):
    '''
    Inserts the records into the Encounter table fhir schema.
    :param hgDict:
    :param hg_id:
    :param conn:
    :return:
    '''
    resp = json.dumps(hgDict)
    resource_id = hgDict['resource']['id']
    resource_type = hgDict['resource']['resourceType']
    # j_map = a_bytes = bytes(row[0], "ascii")

    try:
        status = hgDict['resource']['status']
    except:
        status = None

    lastUpdated = hgDict['resource']['meta']['lastUpdated']
    # print('Patient Id', hg_id, 'Resource Id', resource_id, 'status', status)
    # print('Resource Type', resource_type)
    sql = f'''INSERT INTO fhir.encounter (id, patient_id, resource_type, status, response,
                    response_datetime ) VALUES ('{resource_id}', '{hg_id}', '{resource_type}', '{status}', '{resp}', 
                    '{lastUpdated}')  ON CONFLICT (id) 
                    DO UPDATE SET patient_id = EXCLUDED.patient_id, resource_type =  EXCLUDED.resource_type,
                    status = EXCLUDED.status, response = EXCLUDED.response, response_datetime = EXCLUDED.response_datetime
                    '''

    ph.execute_sql(conn, sql)


def processImmunization(hgDict, hg_id, conn):
    '''
    Inserts records into the Immunization table fhir schema
    :param hgDict:
    :param hg_id:
    :param conn:
    :return:
    '''
    resp = json.dumps(hgDict)
    resource_id = hgDict['resource']['id']
    resource_type = hgDict['resource']['resourceType']
    # j_map = a_bytes = bytes(row[0], "ascii")

    try:
        status = hgDict['resource']['status']
    except:
        status = None

    lastUpdated = hgDict['resource']['meta']['lastUpdated']
    # print('Patient Id', hg_id, 'Resource Id', resource_id, 'status', status)
    # print('Resource Type', resource_type)
    sql = f'''INSERT INTO fhir.immunization (id, patient_id, resource_type, status, response,
                    response_datetime ) VALUES ('{resource_id}', '{hg_id}', '{resource_type}', '{status}', '{resp}', 
                    '{lastUpdated}')  ON CONFLICT (id) 
                    DO UPDATE SET patient_id = EXCLUDED.patient_id, resource_type =  EXCLUDED.resource_type,
                    status = EXCLUDED.status, response = EXCLUDED.response, response_datetime = EXCLUDED.response_datetime
                    '''

    ph.execute_sql(conn, sql)


def processPatient(hgDict, hg_id, conn):
    '''
    Inserts records into the Patient table fhir schema
    :param hgDict:
    :param hg_id:
    :param conn:
    :return:
    '''
    resp = json.dumps(hgDict)
    resource_id = hgDict['resource']['id']
    resource_type = hgDict['resource']['resourceType']
    status = hgDict['resource']['text']['status']
    lastUpdated = hgDict['resource']['meta']['lastUpdated']
    # print('Patient Id', hg_id, 'Resource Id', resource_id, 'status', status)
    # print('Resource Type', resource_type)
    # Push to S3 Bucket
    sql = f'''INSERT INTO fhir.patient (id, patient_id, resource_type, status, response,
                response_datetime ) VALUES ('{resource_id}', '{hg_id}', '{resource_type}', '{status}', '{resp}', 
                '{lastUpdated}')  ON CONFLICT (id) 
                DO UPDATE SET patient_id = EXCLUDED.patient_id, resource_type =  EXCLUDED.resource_type,
                status = EXCLUDED.status, response = EXCLUDED.response, response_datetime = EXCLUDED.response_datetime
                '''

    ph.execute_sql(conn, sql)


def processOrganization(hgDict, hg_id, conn):
    '''
    Insert records into the organization table fhir schema
    :param hgDict:
    :param hg_id:
    :param conn:
    :return:
    '''
    resp = json.dumps(hgDict)
    resource_id = hgDict['resource']['id']
    resource_type = hgDict['resource']['resourceType']
    status = hgDict['resource']['text']['status']
    lastUpdated = hgDict['resource']['meta']['lastUpdated']
    print('Patient Id', hg_id, 'Resource Id', resource_id, 'status', status)
    # print('Resource Type', resource_type)
    sql = f'''INSERT INTO fhir.organization (id, patient_id, resource_type, status, response,
                    response_datetime ) VALUES ('{resource_id}', '{hg_id}', '{resource_type}', '{status}', '{resp}', 
                    '{lastUpdated}')  ON CONFLICT (id) 
                    DO UPDATE SET patient_id = EXCLUDED.patient_id, resource_type =  EXCLUDED.resource_type,
                    status = EXCLUDED.status, response = EXCLUDED.response, response_datetime = EXCLUDED.response_datetime
                    '''

    ph.execute_sql(conn, sql)


def processCoverage(hgDict, hg_id, conn):
    '''
    Inserts records into the covergae table fhir schema
    :param hgDict:
    :param hg_id:
    :param conn:
    :return:
    '''
    resp = json.dumps(hgDict)
    resource_id = hgDict['resource']['id']
    resource_type = hgDict['resource']['resourceType']
    status = hgDict['resource']['status']
    lastUpdated = hgDict['resource']['meta']['lastUpdated']
    # print('Patient Id', hg_id, 'Resource Id', resource_id, 'status', status)
    # print('Resource Type', resource_type)
    sql = f'''INSERT INTO fhir.coverage (id, patient_id, resource_type, status, response,
                    response_datetime ) VALUES ('{resource_id}', '{hg_id}', '{resource_type}', '{status}', '{resp}', 
                    '{lastUpdated}')  ON CONFLICT (id) 
                    DO UPDATE SET patient_id = EXCLUDED.patient_id, resource_type =  EXCLUDED.resource_type,
                    status = EXCLUDED.status, response = EXCLUDED.response, response_datetime = EXCLUDED.response_datetime
                    '''

    ph.execute_sql(conn, sql)


def processDiagnosticReport(hgDict, hg_id, conn):
    '''
    Inserts records into the diagnostic report table fhir schema
    :param hgDict:
    :param hg_id:
    :param conn:
    :return:
    '''
    # print('==>', json.dumps(hgDict))
    resp = json.dumps(hgDict)
    resource_id = hgDict['resource']['id']
    resource_type = hgDict['resource']['resourceType']
    status = hgDict['resource']['text']['status']
    lastUpdated = hgDict['resource']['meta']['lastUpdated']
    # print('Patient Id', hg_id, 'Resource Id', resource_id, 'status', status)
    # print('Resource Type', resource_type)
    sql = f'''INSERT INTO fhir.diagonstic_report (id, patient_id, resource_type, status, response,
            response_datetime ) VALUES ('{resource_id}', '{hg_id}', '{resource_type}', '{status}', '{resp}', 
            '{lastUpdated}')  ON CONFLICT (id) 
            DO UPDATE SET patient_id = EXCLUDED.patient_id, resource_type =  EXCLUDED.resource_type,
            status = EXCLUDED.status, response = EXCLUDED.response, response_datetime = EXCLUDED.response_datetime
            '''

    ph.execute_sql(conn, sql)
    # print('===========================================')


def processObservation(hgDict, hg_id, conn):
    '''
    Inserts records into the observation table fhir schema
    :param hgDict:
    :param hg_id:
    :param conn:
    :return:
    '''
    # print('==>', json.dumps(hgDict))
    resp = json.dumps(hgDict)
    resource_id = hgDict['resource']['id']
    resource_type = hgDict['resource']['resourceType']
    status = hgDict['resource']['status']
    lastUpdated = hgDict['resource']['meta']['lastUpdated']
    # print('Patient Id', hg_id, 'Resource Id', resource_id, 'status', status)
    # print('Resource Type', resource_type)
    sql = f'''INSERT INTO fhir.observation (id, patient_id, resource_type, status, response,
                response_datetime ) VALUES ('{resource_id}', '{hg_id}', '{resource_type}', '{status}', 
                '{resp.replace("'","''")}', 
                '{lastUpdated}')  ON CONFLICT (id) 
                DO UPDATE SET patient_id = EXCLUDED.patient_id, resource_type =  EXCLUDED.resource_type,
                status = EXCLUDED.status, response = EXCLUDED.response, response_datetime = EXCLUDED.response_datetime
                '''

    ph.execute_sql(conn, sql)
    # print('===========================================')
