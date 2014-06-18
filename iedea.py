import csv
import sys
import uuid
import pandas
import logging
import numpy as np
import datetime
from attrdict import AttrDict

log = logging.getLogger('iedea')
log.setLevel(logging.WARN)

from secure import db

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
from patient_identifier
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

class OdkObs(object):
    CE = 'CE' # Coded
    NM = 'NM' # Numeric
    DT = 'DT' # DateTime

    def __init__(self, datatype, concept, value, set_id):
        self.datatype = datatype 
        self.concept = concept
        self.value = value
        self.set_id = set_id

    def hl7_OBX(self,obsdate):
        msg = "OBX|%s|%s|%s^placeholder^99DCT||%s|||||||||%s" % \
            (self.set_id,
            self.datatype,
            self.concept,
            self.value,
            obsdate.strftime('%Y%m%d'))
        return msg
         

class OdkPatient(AttrDict):
    def __init__(self, *args, **kwargs):
        super(OdkPatient, self).__init__(*args, **kwargs)

    @staticmethod
    def hl7_MSH():
        "Build the HL7 Message Header"
        msg = """MSH|^~\\&|FORMENTRY|AMRS.ELD|HL7LISTENER|AMRS.ELD|%s||ORU^R01|%s|P|2.5|1||||||||1^AMRS.ELD.FORMID""" % \
                (datetime.datetime.now().strftime('%Y%m%d%H%M%S'), str(uuid.uuid4()).replace('-',''))
        return msg

    @property
    def pid(self):
        return self.pids[0].patient_id     

    def hl7_PID(self):
        "Build the HL7 Patient ID Message"
        msg = "PID|||%s^^^||||" % (self.pid,)
        return msg

    def hl7(self):
        "Generate entire HL7 Encounter"
        segs = [] 
        segs.append(OdkPatient.hl7_MSH())
        segs.append(self.hl7_PID())
        segs.append("""PV1||O|1^Unknown Location||||1^Super User (1-8)|||||||||||||||||||||||||||||||||||||20080212|||||||V""")
        segs.append("""OBR|1|||1238^MEDICAL RECORD OBSERVATIONS^99DCT""")
        for ob in self.obs:
            segs.append(ob.hl7_OBX(self.encounter_datetime))
        msg = "\r\n".join(segs)
        return msg
        

class OdkImport(object):
    def __init__(self, odkexport_file, odkmapping_file):
        patients = []
        odk_mapping = build_odk_mapping(odkmapping_file)
        with open(odkexport_file, 'rb') as csvfile:
            reader = csv.DictReader(csvfile)
            row_count = 0
            for row in reader:
                next_patient = OdkPatient({
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
                get_obs_for_patient_row(next_patient, odk_mapping)
                # For testing and dev
                if row_count > 100:
                    break
                row_count += 1
        self.patients = patients
        self.odk_mapping = odk_mapping    

    @staticmethod
    def is_odk_junk(val):
        "Returns true if this is a skipped or bad ODK value."
        junk = ['-999','null']
        return val in junk

def get_obs_for_patient_row(patient, odk_mapping):
    """
    Using the patient row from the ODK csv export and the dictionary of
    ODK Mappings, construct and add in the observations.
    """
    obs = []
    set_id = 1
    for key, val in patient.row.iteritems():
        if odk_mapping.has_key(key):
            if OdkImport.is_odk_junk(val):
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
                obs.append(OdkObs(OdkObs.NM, concept['openmrs:mapping'], obs_val, set_id))
                set_id += 1   
            elif concept_type == 'coded':
                print "Looking up concept: ", val, " -> ", concept_mapping
            elif concept_type == 'text':
                obs_val = val
            elif concept_type == 'date':
                #obs_val = datetime.datetime.strptime(val.strip().split(' ')[0], "%Y-%m-%d").date()
                pass
            elif concept_type == 'encounter.encounter_datetime':
                patient.encounter_datetime = datetime.datetime.strptime(val, '%d-%m-%y').date()
            else:
                print ("Type of concept type: %s" %  (type(concept_type)))
                log.warn("Unhandled concept type: %s" % (concept_type))
            print "We have an obs %s %s %s %s" % (key, concept_type, obs_val, val)
    patient.obs = obs


def build_odk_mapping(filename=None):
    if filename is None:
        raise
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


def test():
    return OdkImport('../faces-scratch-data/vJuly13_03feb14.csv', '../faces-scratch-data/OpenMRS_concept_mapping_July2013_form_26july13.xls')


if __name__ == "__main__":
    #process() #(sys.argv[1:])
    pass
