import HealthGorilla as hg
import psycopg2


def run_getImmunization(conn, bearerToken):
    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    cursor = conn.cursor()
    new_sql = f''' SELECT health_gorilla_id FROM master.master_identity '''

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

    # We need to loop thru all the rows retrieved to build FHIR Patient record
    for x in rows:
        print('Getting immunization for Health Gorilla Id', x)
        hg.getPatientImmunization(conn, x, bearerToken)


def run_getEverything(conn, bearerToken):
    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    cursor = conn.cursor()
    new_sql = f''' SELECT health_gorilla_id FROM master.master_identity '''

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

    # We need to loop thru all the rows retrieved to build FHIR Patient record
    for x in rows:
        print('Getting everything for Health Gorilla Id', x)
        hg.getPatientEverything(conn, x, bearerToken)
