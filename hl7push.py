import httplib2
from urllib import urlencode

"""


PID|||3||HornBlower^Horatio^L||
"""

msg = """MSH|^~\\&|FORMENTRY|AMRS.ELD|HL7LISTENER|AMRS.ELD|20080226102656||ORU^R01|JqnfhKKtouEz8kzTk6Zo|P|2.5|1||||||||16^AMRS.ELD.FORMID\r
PID|||13^^^||Rapondi^Jarus^Agemba||\r
PV1||O|1^Unknown Location||||1^Super User (1-8)|||||||||||||||||||||||||||||||||||||20080212|||||||V\r
ORC|RE||||||||20080226102537|1^Super User\r
OBR|1|||1238^MEDICAL RECORD OBSERVATIONS^99DCT\r
OBX|1|NM|5497^CD4, BY FACS^99DCT||450|||||||||20080206\r
OBX|2|DT|5096^RETURN VISIT DATE^99DCT||20080229|||||||||20080212
"""

omrs_url = "http://localhost:8081/openmrs-standalone"

hl7_form = "/remotecommunication/postHl7.form"

endpoint = omrs_url + hl7_form

headers = {'Content-type': 'application/x-www-form-urlencoded'}

h = httplib2.Http()
data = dict(username="admin", password="test", source="LOCAL", hl7Message=msg)
resp, content = h.request(endpoint, "POST", headers=headers, 
                          body=urlencode(data))
print resp
print content
