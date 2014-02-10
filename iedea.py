import csv
import sys
import MySQLdb
import MySQLdb.cursors
import pandas
import logging
import numpy as np
import datetime

log = logging.getLogger('iedea')
log.setLevel(logging.WARN)

db=MySQLdb.connect(user="**************", passwd="*****************",db="openmrs",
    cursorclass=MySQLdb.cursors.DictCursor)

class AttrDict(dict):
    "http://stackoverflow.com/questions/4984647/accessing-dict-keys-like-an-attribute-in-python"
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

#
# Map/Reduce/Stat Functions
#
# def has_pid_reduce(count, patient, 0):
#     if len(patient['pids']) > 0:
#         return count + 1
#     else:
#         return count

# def number_with_pids(plist):
#     "Given the patient list get the number that have exactly 1 pid."
#     pass

#
# End Map/Reduce/Stat Functions
#

def has_facesid(l):
    for item in l:
        if item['identifier_type'] == 3:
            return True
    return False


def pident_from_facesnum(search):
    "Returns a list of matching pid's"
    c=db.cursor()
    c.execute("""
select patient_identifier_id, patient_id, identifier, identifier_type, preferred, voided
from openmrs.patient_identifier
where identifier_type = 3 and
upper(identifier) like upper('%%%s%%')""" % (search,))
    res = c.fetchall()
    return res


def find_pid(patient):
    # First try the CCSP-ID
    # This looks like XYZ-c1234-00  Location, number, check digit
    parts = patient.ccsp_id.split('-')
    if len(parts) != 3:
        print "Could not match %s" % (patient.ccsp_id)
        return []
    patient.ccsp_search = "%s%s" % (parts[1].strip('c').strip().zfill(5),parts[0].upper())
    res = pident_from_facesnum(patient.ccsp_search)
    if len(res) > 0:
        return res

    # Second try Faces ID
    res = pident_from_facesnum(patient.faces_id)

    # Try malformed
    # Try the faces faces_id? 01234-XYZ-00  Number, location, check digit
    #parts = patient.faces_id.split('-')
    #if len(parts) != 3:
    #    print "Could not match %s" % (patient.faces_id)
    #else:
    #    patient.faces_search = "%s%s" % (parts[0].strip().zfill(5),parts[1].upper())
    #    res = pident_from_facesnum(patient.faces_search)


    return res

def no_pids(patients):
    "Get lists of folks with no pids"
    return [i for i in patients if len(i.pids) == 0]

def process(filename='vJuly13_18nov13.csv'):
    patients = []
    odk_mapping = build_odk_mapping()
    with open(filename, 'rb') as csvfile:
        reader = csv.DictReader(csvfile)
        row_count = 0
        for row in reader:
            next_patient = AttrDict({
                "ccsp_id": row['l206'],
                "faces_id": row['l207'],
                "moh_id": row['l205'],
                "row": row,
            })
            pid = row['l206']
            print "checking %s  %s  %s" % (row['l205'],row['l206'],row['l207'])
            next_patient.pids = find_pid(next_patient)
            patients.append(next_patient)
            # Now that we've contructed the raw patient ID, lets add the observations
            # using our mappings and rows.
            #add_obs(next_patient, odk_mapping)

            # For testing and dev
            if row_count > 100:
                break
            row_count += 1

    return patients

def is_odk_junk(val):
    "Returns true if this is a skipped or bad ODK value."
    junk = ['-999','null']
    return val in junk

def get_obs_for_patient_row(patient, odk_mapping):
    """
    Using the patient row from the ODK csv export and the dictionary of
    ODK Mappings, construct and add in the observations.
    """
    for key, val in patient.row.iteritems():
        if odk_mapping.has_key(key):
            if is_odk_junk(val):
                log.info("Skipping junk value: %s %s %s" %
                         (patient.pids, key, val))
                continue
            # Get rid of nan concept types from empty cells
            if type(odk_mapping[key]['openmrs:type']) == np.float and \
               np.isnan(odk_mapping[key]['openmrs:type']):
                continue
            # Calculate the actual value
            concept = odk_mapping[key]
            concept_type = odk_mapping[key]['openmrs:type']
            concept_mapping = odk_mapping[key]['openmrs:mapping']
            obs_val = None
            if concept_type == 'numeric':
                # TODO Correct for floats
                obs_val = int(val)
            elif concept_type == 'coded':
                print "Looking up concept: ", val, " -> ", concept_mapping
            elif concept_type == 'text':
                obs_val = val
            elif concept_type == 'date':
                obs_val = datetime.datetime.strptime(val.strip().split(' ')[0], "%Y-%m-%d").date()
            elif concept_type == 'encounter.encounter_datetime':
                pass
            else:
                print ("Type of concept type: %s" %  (type(concept_type)))
                log.warn("Unhandled concept type: %s" % (concept_type))
            print "We have an obs %s %s %s %s" % (key, concept_type, obs_val, val)


def get_cur_faces_mapping_filename():
    return 'OpenMRS_concept_mapping_July2013_form_24oct13.xls'

def build_odk_mapping(filename=None):
    if filename is None:
        filename = get_cur_faces_mapping_filename()
    odk_df = pandas.read_excel(filename, 'survey')
    map = {}
    for i in range(len(odk_df.values)):
        row = odk_df.loc[i,['openmrs:type','openmrs:mapping','name']]
        map[row[2]] = {
            'openmrs:type': row[0],
            'openmrs:mapping': row[1],
            'name': row[2]
        }
    return map

if __name__ == "__main__":
    #process() #(sys.argv[1:])
    pass
