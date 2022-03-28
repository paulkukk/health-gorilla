import AWS as aws
import fhirclient.models.patient as p
import fhirclient.models.humanname as hn
import fhirclient.models.address as addr
import fhirclient.models.contactpoint as cp
import fhirclient.models.fhirreference as fmf
import fhirclient.models.fhirdate as fdate
import fhirclient.models.meta as meta
import HealthGorilla as hg
import PostgresHelper as ph
import psycopg2
import json
import ValidationHandler


BUCKET_NAME = 'db-fhir'


def run_fhir(conn, organization, reference):
    '''
    Function handles the checking and building of a patient
    :param conn:
    :param organization:
    :param reference:
    :return:
    '''

    print("Starting to build FHIR document")
    cursor = conn.cursor()

    # new_sql = f''' SELECT * FROM salesforce.get_patient('test') '''
    # new_sql = f''' SELECT * FROM salesforce.get_patient('Specific','0015w00002YeCK0AAN;0015w00002YeBj0AAF;0015w00002dZRxaAAG;0015w00002aHrafAAC;0015w00002dZdZlAAK;0015w00002dZRw3AAG;0015w00002dahlqAAA') '''
    new_sql = f''' SELECT * FROM salesforce.get_patient('Specific','0018L000001mulJQAQ')'''
    # 0015w00002dZ77EAAS
    #new_sql = f''' SELECT * FROM salesforce.get_patient('ALL','') '''

    try:
        cursor.execute(new_sql)
        temp = cursor.fetchall()
        rows = []
        for row in temp:
            rows.append(row[0])

        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1
    finally:
        cursor.close()

    print(len(rows))
    # We need to loop thru all the rows retrieved to build FHIR Patient record
    for x in rows:
        print(x)
        if x != None:
            j = json.loads(x)

            # LOOP THRU THE LIST
            for i in range(len(j)):
                temp_json = json.dumps(j[i])
                j_object = json.loads(temp_json)

                patient = p.Patient({'id': j_object["id"]})
                # NAME
                name = hn.HumanName()
                name.family = j_object["family"]
                name.given = [j_object["given"]]
                if j_object["suffix"] != '':
                    name.suffix = [j_object["suffix"]]
                patient.name = [name]
                patient.gender = j_object["gender"]
                patient.active = bool(j_object["active"])
                # BIRTHDATE
                birth_date = fdate.FHIRDate(j_object["birthDate"])
                patient.birthDate = birth_date


                #TELECOM
                commun = []
                # MOBILE PHONE
                if j_object["mobilephone"] != '':
                    phone = cp.ContactPoint()
                    phone.system = 'phone'
                    phone.use = 'mobile'
                    phone.value = j_object["mobilephone"]
                    commun.append(phone)
                # print(phone.as_json())

                # HOME PHONE
                if j_object["phone"] != '':
                    homephone = cp.ContactPoint()
                    homephone.system = 'phone'
                    homephone.use = 'home'
                    homephone.value = j_object["phone"]
                    commun.append(homephone)
                # EMAIL
                if j_object["email"] != '':
                    pat_email = cp.ContactPoint()
                    pat_email.system = "email"
                    pat_email.use = 'home'
                    pat_email.value = j_object["email"]
                    commun.append(pat_email)
                # print(homephone.as_json())
                # ADD MULTIPLE ITEMS TO THE TELECOM BASED ON WHAT VALUES HAVE ACTUAL BEEN COLLECTED
                # patient.telecom = [homephone, phone, pat_email]
                patient.telecom = commun

                # ADDRESS
                if j_object["line"] != '':
                    address = addr.Address()
                    address.use = "home"
                    address.line = [j_object["line"]]
                    address.city =j_object["city"]
                    address.postalCode = j_object["postalCode"]
                    if j_object["country"] != '':
                        address.country = j_object["country"]
                    patient.address = [address]

                #Managing Organization
                pat_org = fmf.FHIRReference()
                pat_org.display = organization
                pat_org.reference = reference
                patient.managingOrganization = pat_org

                #META
                #f_meta = meta.Meta()
                #last_date = fdate.FHIRDate(j_object["lastUpdated"])
                #f_meta.lastUpdated = last_date
                #patient.meta = f_meta

                # print(patient.as_json())

                print("FHIR::", json.dumps(patient.as_json()))

                criteria = {"First Name": j_object["family"], "Last Name": j_object["given"]}
                beartoken = hg.getBearerToken()
                # print(beartoken)
                # See if the patient exists Need to pass in  a Dict with First Name, Last Name
                result = hg.searchForPatient(criteria, beartoken)

                salesforce_id = j_object["id"]
                # Id result is None then there was no patient returned in the search
                if result['total'] == 0:
                    print('Need to create patient record in Health Gorilla for', j_object["family"], j_object["given"])
                    # Create the Patient in Health Gorilla
                    create_result = hg.createPatient(json.dumps(patient.as_json()), beartoken)
                    # If there is a nt/ in the result lets remove it before storing the id in the Database
                    create_result = str(create_result).replace("nt/", "")
                    print("SF Id", salesforce_id, "HG Id", create_result)

                    # Then we need to upsert a row into the Master Schema linking the Salesforce Id and Health Gorilla Id
                    sql = f'''INSERT INTO master.master_identity (salesforce_id, health_gorilla_id)
                    VALUES ('{salesforce_id}', '{create_result}') ON CONFLICT (salesforce_id) DO UPDATE SET 
                    health_gorilla_id = EXCLUDED.health_gorilla_id'''
                    ph.execute_sql(conn, sql)
                elif result['total'] == 1:
                    print('1 record found for Salesforce Id', salesforce_id, ' Health Gorilla Id',
                          result['entry'][0]['resource']['id'])
                    sql = f'''INSERT INTO master.master_identity (salesforce_id, health_gorilla_id, updated_datetime)
                          VALUES ('{salesforce_id}', '{result['entry'][0]['resource']['id']}', now()) 
                          ON CONFLICT (salesforce_id) 
                          DO UPDATE SET health_gorilla_id = EXCLUDED.health_gorilla_id, 
                          updated_datetime = EXCLUDED.updated_datetime'''
                    ph.execute_sql(conn, sql)
                else:
                    print('Multiple records found')
                    # If multiple records are found Need to check the date of birth against what is stored in the
                    # salesforce schema


                print('------------------------------------------------------------')



