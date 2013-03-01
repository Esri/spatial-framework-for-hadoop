import os
from xml.etree import ElementTree as etree
import requests

######################################################################
class Configuration():

    def __init__(self, jobPropFile):
        '''create an xml file from jobProperties'''
        if (os.path.exists(jobPropFile)):
            config = etree.Element("configuration")
            with open(jobPropFile, 'r') as f:
                for line in iter(f):
                    (k,v) = line.strip().split("=")
                    prop = etree.Element('property')
                    name = etree.Element('name')
                    name.text = k
                    prop.append(name)
                    value = etree.Element('value')
                    value.text = v
                    prop.append(value)
                    config.append(prop)
            self.xmldata = etree.tostring(config)
        else:
            arcpy.AddError("File not found {0}".format(jobPropFile))                

######################################################################
class Oozie():
    
    def __init__(self,oozieurl):        
        '''verify oozie url and create a client '''
        self.version = 'v1'
        response = requests.get("/".join([oozieurl.rstrip('/'),self.version,"admin/status"]))
        resp_out = self.verifyResponse(response,200,"Unable to connect to Oozie {0}".format(oozieurl),
                       ["systemMode"])
        if resp_out[0] == "NORMAL":
            self.url = oozieurl.rstrip("/")
        else:
            arcpy.AddError("The status of oozie interface is not NORMAL")
    
    def submit(self, xmldata):
        '''Submit a job '''
        response = requests.post(
            url     = '/'.join([self.url, self.version, 'jobs']),
            data    = xmldata,
            headers = {'content-type': 'application/xml'}
        )        
        resp_out = self.verifyResponse(response,201,"Unable to submit job to Oozie {0}".format(self.url),
                                       ["id"])
        return resp_out[0]
    
    def run(self, id):
        '''Runs the oozie job'''
        response = requests.put(
            url    = '/'.join([self.url, self.version, 'job', id]),
            params = {'action': 'start'},
        )        
        return self.verifyResponse(response,200,"Unable to run job {0}".format(id))
    
    def status(self, jobId):
        '''verify the current status of job '''
        response = requests.get(
            url = '/'.join([self.url, self.version, 'job', jobId]),
        )
        resp_out = self.verifyResponse(response,200,"Unable to determine job status {0}".format(id),
                                       ["status"])
        return resp_out[0]
    
    @staticmethod
    def verifyResponse(response, status_code, errorMessage, returnFields=None):
        '''verify status code and return required fields from response'''
        if response.status_code == status_code:            
            if returnFields:
                res_json = response.json()
                return [res_json[flds] for flds in returnFields]
            else:
                return True
        else :
            arcpy.AddError(errorMessage)
