from __future__ import division
from __future__ import print_function

from string import Template

import requests
from requests.auth import HTTPBasicAuth
from requests_toolbelt.multipart import decoder
import xmltodict

#------------------------------------------------------------------------------

def _call(spec, xml):
  '''Makes the actual HTTP call to the OnCore SOAP endpoint.
  Args:
    o spec: see README
    o xml: the outgoing XML blob as a unicode string
  Returns a map with three keys:
    o 'status-code': the response's HTTP status code
    o 'xml': the response's XML blob as a string
    o 'structured-data': data from the XML reformatted as a
       nested OrderedDict.
  '''
  if type(xml) != unicode:
    raise TypeError
  headers = {'content-type': 'text/xml'}
  auth = HTTPBasicAuth(spec['user'], spec['password'])
  result = requests.post(spec['service-url'], data=xml.encode('utf_8'),
                         headers=headers, auth=auth)
  multipart_data = decoder.MultipartDecoder.from_response(result) 
  response_xml = multipart_data.parts[0].content
  structured_data = xmltodict.parse(response_xml)
  return {'status-code': result.status_code,
          'xml': response_xml,
          'structured-data': structured_data}

#------------------------------------------------------------------------------

def get_protocol(spec, protocol_num):
  xml = (u'''
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:data="http://data.service.opas.percipenz.com">
   <soapenv:Header/>
   <soapenv:Body>
      <data:ProtocolSearchCriteria>
         <!--type: string-->
         <protocolNo>{PROTOCOL_NUM}</protocolNo>
      </data:ProtocolSearchCriteria>
   </soapenv:Body>
</soapenv:Envelope>
  ''').format(PROTOCOL_NUM=protocol_num)
  return _call(spec, xml)

def get_subject_data(spec, primary_id):
  xml = (u'''
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:ser="http://service.opas.percipenz.com">
   <soapenv:Header/>
   <soapenv:Body>
      <ser:SubjectSearchData>
         <!--type: string-->
         <PrimaryIdentifier>{PRIMARY_ID}</PrimaryIdentifier>
      </ser:SubjectSearchData>
   </soapenv:Body>
</soapenv:Envelope>
  ''').format(PRIMARY_ID=primary_id)
  return _call(spec, xml)

#------------------------------------------------------------------------------
# subject data convenience functions

def _extract_subj_data_elem(subject_data, element):
  '''Normally returns a unicode string but might return 
  a list in some cases (e.g., Race).'''
  return subject_data['structured-data']['soap:Envelope']['soap:Body'
                      ]['ns7:Subject'][element]

def subject_record_exists(subject_data):
  '''Takes the result of get_subject_data and returns True
  if the subject already has a record in OnCore.'''
  # If no record, the ns7:Subject element will have an attribute
  # key-value pair of xsi:nil="true"; it won't be there otherwise.
  try:
    nil_flag = _extract_subj_data_elem(subject_data, '@xsi:nil')
    return nil_flag != u'true'
  except KeyError:
    return True

def extract_primary_identifier(subject_data):
  return _extract_subj_data_elem(subject_data, 'PrimaryIdentifier')

def extract_subject_num(subject_data):
  return _extract_subj_data_elem(subject_data, 'SubjectNo')

def extract_first_name(subject_data):
  return _extract_subj_data_elem(subject_data, 'FirstName')

def extract_last_name(subject_data):
  return _extract_subj_data_elem(subject_data, 'LastName')

def extract_birthdate(subject_data):
  return _extract_subj_data_elem(subject_data, 'BirthDate')

def extract_gender(subject_data):
  return _extract_subj_data_elem(subject_data, 'Gender')

def extract_races(subject_data):
  '''Returns a list.'''
  rslt = _extract_subj_data_elem(subject_data, 'Race')
  if type(rslt) == unicode: return [rslt]
  else: return rslt

def extract_ethnicity(subject_data):
  return _extract_subj_data_elem(subject_data, 'Ethnicity')

#------------------------------------------------------------------------------
# registration

def verify_reg_data(reg_data):
  '''Returns True if reg_data contains all required
  key-value pairs needed to subsequently call 
  register_subject_to_protocol.'''
  keys = ['primary-identifier', 'context', 'study-site', 'protocol-num',
          'last-name', 'first-name', 'birthdate', 'gender', 'races',
          'ethnicity']
  return all(map(lambda k: reg_data.get(k), keys))

def prep_subject_data(subject_data):
  '''Returns a map having a standardized format that can be
  used elsewhere when working with demographics (e.g., comparison
  with EHR or REDCap.)'''
  keys = ['primary-identifier',
          'last-name', 'first-name', 'birthdate', 'gender', 'races',
          'ethnicity']
  return dict(
           zip(keys,
             [extract_primary_identifier(subject_data),
              extract_last_name(subject_data),
              extract_first_name(subject_data),
              extract_birthdate(subject_data),
              extract_gender(subject_data),
              extract_races(subject_data),
              extract_ethnicity(subject_data)]))

def register_subject_to_protocol(spec, reg_data, subject_num=None, xml_only=False):
  '''Registers a subject to a protocol. If subject_num
  is included, assumption is this subject already has 
  a record in OnCore and registerExistingSubjectToProtocol
  will be called. Otherwise, registerNewSubjectToProtocol
  will be called; note that in the latter case, this will result
  in the creation of a new subject record in OnCore for 
  this individual. 
  Returns result of _call.
  If xml_only is True, instead of calling API, the prepared XML payload 
  is returned.
  '''
  if not verify_reg_data(reg_data):
    raise ValueError
  op_element = (u'ProtocolExistingSubjectRegistrationData' if subject_num 
                else u'ProtocolNewSubjectRegistrationData')

  subject_num_element = (u'<SubjectNo>' + unicode(subject_num) + u'</SubjectNo>'
                         if subject_num else '')
  race_elements = reduce(lambda xml, race: xml + u'<Race>' + race + u'</Race>',
                         reg_data['races'],
                         '')
  xml = Template(u'''
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:sub="http://data.service.opas.percipenz.com/subject">
   <soapenv:Header/>
   <soapenv:Body>
      <sub:$OP_ELEMENT>
         <sub:ProtocolSubjectRegistrationData>
            <Context>$CONTEXT</Context>
            <ProtocolSubject>
               <ProtocolNo>$PROTOCOL_NUM</ProtocolNo>
               <StudySite>$STUDY_SITE</StudySite>
               <Subject>
                  <PrimaryIdentifier>$PRIMARY_IDENTIFIER</PrimaryIdentifier>
                  $SUBJECT_NUM_ELEMENT
                  $RACE_ELEMENTS
                  <LastName>$LAST_NAME</LastName>
                  <FirstName>$FIRST_NAME</FirstName>
                  <BirthDate>$BIRTHDATE</BirthDate>
                  <Gender>$GENDER</Gender>
                  <Ethnicity>$ETHNICITY</Ethnicity>
               </Subject>
            </ProtocolSubject>
         </sub:ProtocolSubjectRegistrationData>
      </sub:$OP_ELEMENT>
   </soapenv:Body>
</soapenv:Envelope>
      ''')
  prepped = xml.substitute(OP_ELEMENT=op_element,
                           CONTEXT=reg_data['context'],
                           PROTOCOL_NUM=reg_data['protocol-num'],
                           STUDY_SITE=reg_data['study-site'],
                           PRIMARY_IDENTIFIER=reg_data['primary-identifier'],
                           SUBJECT_NUM_ELEMENT=subject_num_element,
                           RACE_ELEMENTS=race_elements,
                           LAST_NAME=reg_data['last-name'],
                           FIRST_NAME=reg_data['first-name'],
                           BIRTHDATE=reg_data['birthdate'],
                           GENDER=reg_data['gender'],
                           ETHNICITY=reg_data['ethnicity'],)
  if xml_only: return prepped
  else: return _call(spec, prepped)

