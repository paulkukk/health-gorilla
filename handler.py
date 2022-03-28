import json
from AWS import session, serviceRegion, DB, DB_ENDPOINT, DB_PORT, DB_USER, get_secret
import FHIRPatientHandler
import GorillaHandler as gh
import HealthGorilla as hg
import time
import psycopg2
import PostgresHelper


# boto3 client
awsclient = session.client(
    service_name='rds'
)
# generate session token
token = awsclient.generate_db_auth_token(
    DBHostname=DB_ENDPOINT,
    Port=DB_PORT,
    DBUsername=DB_USER,
    Region=serviceRegion
)
# print('Token::', token)
# db connection
conn = psycopg2.connect(
    dbname=DB,
    user=DB_USER,
    password=token,
    # host=DB_ENDPOINT,
    host='localhost',
    sslmode='require'
)


def process_data(event, context):
    print('Starting fhir creation')
    # Created a Record in Health Gorilla
    FHIRPatientHandler.run_fhir(conn, 'Daybreak', 'Organization/f001')
    bearerToken = hg.getBearerToken()

    # Creates a Record of Medical History of patient
    # gh.run_getEverything(conn, bearerToken)

    # hg.getPatientEverything(conn, '420e106291454d05d19b2029', bearerToken)
    # hg.getPatientEverything(conn, '775522624b7e209c1e45b786', bearerToken)
    # hg.getPatientEverything(conn, '818b0f62a2998115db187dda', bearerToken)
    # hg.getPatientEverything(conn, '420e106291454d05d19b2029', bearerToken)
    # hg.getPatientEverything(conn, '9b1f156292928c6e7750e9ae', bearerToken)

    # Creates a record of immunization for patient
    # gh.run_getImmunization(conn, bearerToken)

    # hg.getPatientImmunization(conn, '818b0f62a2998115db187dda', bearerToken)
    # hg.getPatientImmunization(conn, '0c241562c6213166906f9cbd', bearerToken)
    # hg.getPatientImmunization(conn, '420e106291454d05d19b2029', bearerToken)
    # hg.getPatientImmunization(conn, '9b1f156292928c6e7750e9ae', bearerToken)


if __name__ == "__main__":
    start = time.time()
    process_data('', '')

    print('Total time to run: ', time.time() - start)
