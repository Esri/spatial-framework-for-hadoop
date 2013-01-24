import os
import sys
import arcpy
from webhdfs import WebHDFS


######################################################################
######################################################################
######################################################################
class Toolbox(object):
    def __init__(self):
        self.label = "Hadoop Tools"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [CopyToHDFS, CopyFromHDFS, FeaturesToJSON] #, HDFSCommand]


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
            datatype="File",
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

        out_remote_file = arcpy.Parameter(
            name="out_remote_file",
            displayName="Output HDFS file",
            datatype="String",
            parameterType="Derived",
            direction="Output")

        parameters = [in_file, host, port, user, in_remote_file, out_remote_file]
        return parameters

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        if parameters[4].altered == False :
            in_file = parameters[0].value        
            webhdfs_host = parameters[1].value
            webhdfs_port = parameters[2].value
            webhdfs_user = parameters[3].value
            if in_file != None and webhdfs_port != None and webhdfs_host!= None and webhdfs_user != None and\
              len(unicode(in_file)) and len(webhdfs_host) and len(webhdfs_user) :
                homeDir = ''
                try :
                    wh = HDFS(webhdfs_host, webhdfs_port, webhdfs_user)
                    homeDir = wh.getHomeDir()
                except :
                    parameters[0].setErrorMessage('Error while trying to connect to : ' + str(webhdfs_host) + ':' + str(webhdfs_port))
                    parameters[4].value = ''
                else :
                    parameters[4].clearMessage()
                    parameters[4].value = homeDir + '/' + arcpy.Describe(in_file).name
        return
                
    def updateMessages(self, parameters):
        #TODO: webhdfs.py missing support for file-specific attribute/exists information
        #remote_paths = webhdfs.listdir(webhdfs_path)
        #if (len(remote_path) > 0):
        #    messages.addMessage("Remote HDFS entity /" + webhdfs_path + " already exists!")
        return

    def execute(self, parameters, messages):
        #'''
        input_file = parameters[0].value
        webhdfs_host = parameters[1].value
        webhdfs_port = int(parameters[2].value)
        webhdfs_user = parameters[3].value
        webhdfs_file = parameters[4].value
        
        response = None
        
        try :
            webhdfs = WebHDFS(webhdfs_host, webhdfs_port, webhdfs_user)
            response = webhdfs.copyFromLocal(unicode(input_file), unicode(webhdfs_file), overwrite = bool(arcpy.gp.overwriteOutput))
        except :
            messages.addErrorMessage('Unexpected error: "{0}"'.format(sys.exc_info()[0]))
            
        if response != None :
            if response.status >= 400 :
                messages.addErrorMessage('HTTP ERROR {0}. Reason: {1}'.format(response.status, response.reason))
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
            datatype="File",
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
        
        response = None

        try :
            webhdfs = WebHDFS(webhdfs_host, webhdfs_port, webhdfs_user)
            response = webhdfs.copyToLocal(unicode(webhdfs_file), unicode(out_local_file), overwrite = bool(arcpy.gp.overwriteOutput))
        except :
            messages.addErrorMessage('Unexpected error: "{0}"'.format(sys.exc_info()[0]))
            
        if response != None :
            if response.status >= 400 :
                messages.addErrorMessage('HTTP ERROR {0}. Reason: {1}'.format(response.status, response.reason))

        return

######################################################################
import JSONUtil

class FeaturesToJSON(object):
     
    def __init__(self):
        self.label = "Features To JSON"
        self.description = "Converts features to Esri JSON"
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

        pjson = arcpy.Parameter(
            name="format_json",
            displayName="Formatted JSON",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        
        pjson.filter.type = "ValueList"
        pjson.filter.list = ["FORMATTED", "NOT_FORMATTED"]
        pjson.value = False

        parameters = [in_features, out_json_file, pjson]
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
        b_pjson = parameters[2].value
        with open(unicode(out_json_file), 'wb') as json_file :
            JSONUtil.DumpFC2JSON(in_features, json_file, pjson = bool(b_pjson))
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
        ##TODO: webhdfs.py missing support for file-specific attribute/exists information
        ##remote_paths = webhdfs.listdir(webhdfs_path)
        ##if (len(remote_path) == 0):
        ##    messages.addMessage("Remote HDFS entity /" + webhdfs_path + "does notexists!")
        #return

    #def execute(self, parameters, messages):
        #webhdfs_host = parameters[0].value
        #webhdfs_port = int(parameters[1].value)
        #webhdfs_user = parameters[2].value
        #command = parameters[3].value
        #in_remote_path = parameters[4].value
                
        #webhdfs = WebHDFS(webhdfs_host, webhdfs_port, webhdfs_user)
        
        #if command == HDFSCommand._cmdCreateFolder :
            #response = webhdfs.mkdir(in_remote_path)
            #parameters[4].value = response
            
        #elif command == HDFSCommand._cmdDeleteFile :
            #response = webhdfs.delete(in_remote_path)
            #parameters[4].value = response
            
        #elif command == HDFSCommand._cmdDeleteFolderRecursively :
            #response = webhdfs.rmdir(in_remote_path)
            #parameters[4].value = response
        
        #if response != None :
            #messages.addMessage('Response is "{0}"'.format(response))
        #return
