

def validate_patient_data(j_object):
    print('Starting to validate patient data')
    msg = ''
    if j_object["family"] == '':
        msg = msg + 'No Family Name'
    if j_object["given"] == '':
        msg = msg + 'No Given Name'
    if j_object["birthDate"] == '':
        msg = msg + 'No Birth Date'
    if j_object["gender"] == '':
        msg = msg + 'No Gender'

