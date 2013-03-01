import os, sys
import arcpy
from webhdfs import WebHDFS, WebHDFSError
from OozieUtil import Oozie, OozieError, Configuration
import JSONUtil

######################################################################
class Toolbox(object):
    def __init__(self):
        self.label = "Hadoop Tools"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [CopyToHDFS, CopyFromHDFS, FeaturesToJSON, JSONToFeatures, ExecuteWorkflow] #, HDFSCommand]

######################################################################
def AddExceptionError(messages, message = '') :
    for ei in sys.exc_info() :
        if isinstance(ei, Exception) :
            messages.addErrorMessage('%s : %s' % (message if (message!= None and len(message) > 0) else 'Unexpected error', str(ei)))
    
######################################################################
def SetExceptionError(parameter, message = '') :
    for ei in sys.exc_info() :
        if isinstance(ei, Exception) :
            parameter.setErrorMessage('%s : %s' % (message if (message!= None and len(message) > 0) else 'Unexpected error', str(ei)))

######################################################################
class CopyToHDFS(object):
    def __init__(self):
        self.label = "Copy To HDFS"
        self.description = "Copies file to Hadoop File System"
        self.canRunInBackground = False

    def getParameterInfo(self):
        in_file = arcpy.Parameter(
            name="in_local_file",
            displayName="Input local file",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        
        host = arcpy.Parameter(
            name="host_name",
            displayName="HDFS server hostname",
            datatype="String",
            parameterType="Required",
            direction="Input")

        port = arcpy.Parameter(
            name="port_number",
            displayName="HDFS TCP port number",
            datatype="Long",
            parameterType="Required",
            direction="Input")
        port.value = 50070

        user = arcpy.Parameter(
            name="user_name",
            displayName="HDFS username",
            datatype="String",
            parameterType="Required",
            direction="Input")

        in_remote_file = arcpy.Parameter(
            name="in_remote_file",
            displayName="HDFS remote file",
            datatype="String",
            parameterType="Required",
            direction="Input")

        b_append = arcpy.Parameter(
            name="append_file",
            displayName="Append file",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        
        out_remote_file = arcpy.Parameter(
            name="out_remote_file",
            displayName="Output HDFS file",
            datatype="String",
            parameterType="Derived",
            direction="Output")

        b_append.filter.type = "ValueList"
        b_append.filter.list = ["CREATE", "APPEND"]
        b_append.value = False
        
        parameters = [in_file, host, port, user, in_remote_file, b_append, out_remote_file]
        return parameters

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        if parameters[4].altered == False :
            in_file      = parameters[0].value        
            webhdfs_host = parameters[1].value
            webhdfs_port = parameters[2].value
            webhdfs_user = parameters[3].value
                    
            if in_file != None and webhdfs_port != None and webhdfs_host != None and webhdfs_user != None and len(unicode(in_file)) and len(webhdfs_host) and len(webhdfs_user) :
                homeDir = ''
                try :
                    wh = WebHDFS(webhdfs_host, webhdfs_port, webhdfs_user)
                    homeDir = wh.getHomeDir()
                except :
                    parameters[4].value = ''
                else :
                    parameters[4].value = homeDir + '/' + arcpy.Describe(in_file).name
        return
                
    def updateMessages(self, parameters):
        webhdfs_host = parameters[1].value
        webhdfs_port = int(parameters[2].value)
        webhdfs_user = parameters[3].value
        webhdfs_file = parameters[4].value

        webhdfs_path = ''
        webhdfs_name = ''
        files = []
        if webhdfs_file and len(unicode(webhdfs_file)) :
            (webhdfs_path, webhdfs_name) = os.path.split(webhdfs_file)
        
        try :
            wh = WebHDFS(webhdfs_host, webhdfs_port, webhdfs_user)
            files = wh.listDirEx(webhdfs_path)
        except WebHDFSError as whe:
            parameters[4].setErrorMessage(str(whe))
        except :
            SetExceptionError(parameters[4])
            return
        
        for f in files :
            if f['type'] == 'FILE' and f['pathSuffix'] == webhdfs_name :
                if arcpy.gp.overwriteOutput:
                    parameters[4].setWarningMessage("Remote file '" + webhdfs_file + "' already exists.")
                else :
                    parameters[4].setErrorMessage("Remote file '" + webhdfs_file + "' already exists.")
                break

        return

    def execute(self, parameters, messages):
        #'''
        input_file = parameters[0].value
        webhdfs_host = parameters[1].value
        webhdfs_port = int(parameters[2].value)
        webhdfs_user = parameters[3].value
        webhdfs_file = parameters[4].value
        b_append     = parameters[5].value
        
        try :
            wh = WebHDFS(webhdfs_host, webhdfs_port, webhdfs_user)
            if b_append :
                wh.appendToHDFS(unicode(input_file), unicode(webhdfs_file))
            else:
                wh.copyToHDFS(unicode(input_file), unicode(webhdfs_file), overwrite = bool(arcpy.gp.overwriteOutput))
        except WebHDFSError as whe:
            messages.addErrorMessage(str(whe))
        except:
            AddExceptionError(messages)
            
        return

######################################################################
class CopyFromHDFS(object):
    def __init__(self):
        self.label = "Copy From HDFS"
        self.description = "Copies file from Hadoop File System"
        self.canRunInBackground = False

    def getParameterInfo(self):
        host = arcpy.Parameter(
            name="host_name",
            displayName="HDFS server hostname",
            datatype="String",
            parameterType="Required",
            direction="Input")

        port = arcpy.Parameter(
            name="port_number",
            displayName="HDFS TCP port number",
            datatype="Long",
            parameterType="Required",
            direction="Input")
        port.value = 50070

        user = arcpy.Parameter(
            name="user_name",
            displayName="HDFS username",
            datatype="String",
            parameterType="Required",
            direction="Input")

        in_remote_file = arcpy.Parameter(
            name="in_remote_file",
            displayName="HDFS remote file",
            datatype="String",
            parameterType="Required",
            direction="Input")

        out_local_file = arcpy.Parameter(
            name="out_local_file",
            displayName="Output local file",
            datatype="DEFile",
            parameterType="Required",
            direction="Output")

        parameters = [host, port, user, in_remote_file, out_local_file]
        return parameters

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        return
                
    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        webhdfs_host = parameters[0].value
        webhdfs_port = int(parameters[1].value)
        webhdfs_user = parameters[2].value
        webhdfs_file = parameters[3].value
        out_local_file = unicode(parameters[4].value)
        
        if os.path.isfile(out_local_file):
            os.remove(out_local_file)
        if os.path.isfile(out_local_file):
            arcpy.gp.addError("Cannot delete: " + out_local_file)
            sys.exit()
        
        try :
            wh = WebHDFS(webhdfs_host, webhdfs_port, webhdfs_user)
            wh.copyFromHDFS(unicode(webhdfs_file), unicode(out_local_file), overwrite = bool(arcpy.gp.overwriteOutput))
        except WebHDFSError as whe:
            messages.addErrorMessage(str(whe))
        except:
            AddExceptionError(messages)

        return

######################################################################
class FeaturesToJSON(object):
    _esrijsonCollection = 'ENCLOSED_JSON'
    _esrijsonSimple = 'UNENCLOSED_JSON'
    
    def __init__(self):
        self.label = "Features To JSON"
        self.description = "Converts features to Esri JSON file"
        self.canRunInBackground = False

    def getParameterInfo(self):
        in_features = arcpy.Parameter(
            displayName="Input features",
            name="in_features",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input")
        
        in_features.filter.list = ["Point", "Multipoint", "Polyline", "Polygon"]

        out_json_file = arcpy.Parameter(
            name="out_json_file",
            displayName="Output JSON",
            datatype="DEFile",
            parameterType="Required",
            direction="Output")
        
        out_json_file.filter.list = ["json"]
        out_json_file.parameterDependencies = [in_features.name]

        json_type = arcpy.Parameter(
            name="json_type",
            displayName="JSON type",
            datatype="String",
            parameterType="Optional",
            direction="Input")
        
        json_type.filter.type = "ValueList"
        json_type.filter.list = [FeaturesToJSON._esrijsonCollection, FeaturesToJSON._esrijsonSimple]
        json_type.value = FeaturesToJSON._esrijsonCollection

        pjson = arcpy.Parameter(
            name="format_json",
            displayName="Formatted JSON",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        
        pjson.filter.type = "ValueList"
        pjson.filter.list = ["FORMATTED", "NOT_FORMATTED"]
        pjson.value = False

        parameters = [in_features, out_json_file, json_type, pjson]
        return parameters

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        return
                
    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        in_features = parameters[0].value
        out_json_file = parameters[1].value
        json_type = parameters[2].value
        b_pjson = parameters[3].value
        with open(unicode(out_json_file), 'wb') as json_file :
            if json_type == FeaturesToJSON._esrijsonCollection :
                JSONUtil.DumpFC2JSON(in_features, json_file, pjson = bool(b_pjson))
            elif json_type == FeaturesToJSON._esrijsonSimple :
                JSONUtil.DumpFC2JSONSimple(in_features, json_file, pjson = bool(b_pjson))
        return

######################################################################
class JSONToFeatures(object):
     
    def __init__(self):
        self.label = "JSON To Features"
        self.description = "Converts Esri JSON file to features"
        self.canRunInBackground = False

    def getParameterInfo(self):
        in_json_file = arcpy.Parameter(
            displayName="Input JSON",
            name="in_json_file",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        
        in_json_file.filter.list = ["json"]

        out_features = arcpy.Parameter(
            name="out_features",
            displayName="Output feature class",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Output")
        
        out_features.parameterDependencies = [in_json_file.name]

        parameters = [in_json_file, out_features]
        return parameters

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        return
                
    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        in_json_file = parameters[0].value
        out_features = parameters[1].value
        
        if arcpy.Exists(out_features):
            arcpy.Delete_management(out_features)
        if arcpy.Exists(out_features):
            messages.addErrorMessage("Cannot delete: " + unicode(out_features))
            return
        
        with open(unicode(in_json_file), 'rb') as json_fc_file:
            JSONUtil.ImportFromJSON(json_fc_file, unicode(out_features))
        return

######################################################################
class ExecuteWorkflow(object):

    def __init__(self):
        self.label = "Execute Workflow"
        self.description = "Executes Oozie workwlow"
        self.canRunInBackground = False

    def getParameterInfo(self):
        in_oozie_url = arcpy.Parameter(
            displayName="Oozie URL",
            name="in_oozie_url",
            datatype="String",
            parameterType="Required",
            direction="Input")
        
        in_jobprops_file = arcpy.Parameter(
            name="in_jobprops_file",
            displayName="Job properties",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        
        track_status = arcpy.Parameter(
            name="track_status",
            displayName="Track status",
            datatype="Boolean",
            parameterType="Optional",
            direction="Input")
        
        track_status.filter.type = "ValueList"
        track_status.filter.list = ["TRACK_STATUS", "NO_TRACK_STATUS"]
        track_status.value = True
        
        job_succseeded = arcpy.Parameter(
            name="job_succseeded",
            displayName="Job succseeded",
            datatype="Boolean",
            parameterType="Derived",
            direction="Output")

        job_succseeded.value = False

        parameters = [in_oozie_url, in_jobprops_file, track_status, job_succseeded]
        return parameters

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        return
                
    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        # Get parameters
        oozie_url = parameters[0].value
        jobprops_file = unicode(parameters[1].value)
        b_track_status = parameters[2].value
        b_job_succeeded = False    
        
        try :
            #prepare and run a job
            conf = Configuration(jobprops_file)
            oozie_client = Oozie(oozie_url)
            job_id = oozie_client.submit(conf.xmldata)
            messages.addMessage("Oozie job id {0}".format(job_id))
            oozie_client.run(job_id)
            
            # track progress if requested
            if b_track_status:    
                curr_status = oozie_client.status(job_id)                
                messages.addMessage("Status : {0}".format(curr_status))
                while curr_status in ['PENDING','RUNNING']:
                    time.sleep(3)
                    status = oozie_client.status(job_id)        
                    if status != curr_status:
                        messages.addMessage("Status : {0}".format(status))
                    curr_status = status   
    
                if curr_status == 'SUCCEEDED' :
                    b_job_succeeded = True
                    messages.addMessage("Job {0} succeeded".format(job_id))
                    # TODO: Retrieve log information
                else:
                    messages.addErrorMessage("Job {0} failed".format(job_id))       
                    
            else:
                b_job_succeeded = True
                messages.addMessage("Job {0} successfully submitted".format(job_id))

        except OozieError as err:
            messages.addErrorMessage(str(err))
        except:
            AddExceptionError(messages)
            
        parameters[3].value = b_job_succeeded
        return

######################################################################
#class HDFSCommand(object):
    #_cmdCreateFolder = 'CREATE_FOLDER'
    #_cmdDeleteFile = 'DELETE_FILE'
    #_cmdDeleteFolderRecursively = 'DELETE_FOLDER_RECURSIVELY'
    
    #def __init__(self):
        #self.label = "Execute HDFS command"
        #self.description = "Executes HDFS command"
        #self.canRunInBackground = False

    #def getParameterInfo(self):
        #host = arcpy.Parameter(
            #name="host_name",
            #displayName="HDFS server hostname",
            #datatype="String",
            #parameterType="Required",
            #direction="Input")

        #port = arcpy.Parameter(
            #name="port_number",
            #displayName="HDFS TCP port number",
            #datatype="Long",
            #parameterType="Required",
            #direction="Input")
        #port.value = 50070

        #user = arcpy.Parameter(
            #name="user_name",
            #displayName="HDFS username",
            #datatype="String",
            #parameterType="Required",
            #direction="Input")

        #command = arcpy.Parameter(
            #name="hdfs_command",
            #displayName="HDFS command",
            #datatype="String",
            #parameterType="Required",
            #direction="Input")
        #command.filter.list = [HDFSCommand._cmdCreateFolder, HDFSCommand._cmdDeleteFile, HDFSCommand._cmdDeleteFolderRecursively]
            
        #in_remote_path = arcpy.Parameter(
            #name="in_remote_path",
            #displayName="HDFS remote path",
            #datatype="String",
            #parameterType="Required",
            #direction="Input")

        #command_output = arcpy.Parameter(
            #name="command_output",
            #displayName="Command output",
            #datatype="String",
            #parameterType="Derived",
            #direction="Output",
            #multiValue = True)

        #parameters = [host, port, user, command, in_remote_path, command_output]
        #return parameters

    #def isLicensed(self):
        #"""Set whether tool is licensed to execute."""
        #return True

    #def updateParameters(self, parameters):
        #return
                
    #def updateMessages(self, parameters):
        ##TODO:
        ##remote_paths = wh.listDir(webhdfs_path)
        ##if (len(remote_path) == 0):
        ##    messages.addMessage("Remote HDFS entity /" + webhdfs_path + "does notexists!")
        #return

    #def execute(self, parameters, messages):
        #webhdfs_host = parameters[0].value
        #webhdfs_port = int(parameters[1].value)
        #webhdfs_user = parameters[2].value
        #command = parameters[3].value
        #in_remote_path = parameters[4].value
        #try:
            #wh = WebHDFS(webhdfs_host, webhdfs_port, webhdfs_user)
        
            #if command == HDFSCommand._cmdCreateFolder :
                #wh.mkDir(in_remote_path)
            
            #elif command == HDFSCommand._cmdDeleteFile :
                #wh.delete(in_remote_path)
            
            #elif command == HDFSCommand._cmdDeleteFolderRecursively :
                #wh.rmDir(in_remote_path)
        #except WebHDFSError as whe:
            #messages.addMessage(str(whe))
        #except :
            #AddExceptionError(messages)
        #return