import os, sys, json, datetime, uuid
import arcpy

##################################################
def CreateDataset(output_fc, json_fc, geomType = None, geomFieldName = None) :
    #create in_memory table/FC first to add fields faster then regular fgdb
    in_mem = 'in_memory'
    temp_name = 'tmp' + str(uuid.uuid1()).replace('-', '_')
    in_mem_fc = in_mem + '/' + temp_name
    
    if geomType :
        hasM = 'DISABLED'
        if json_fc.has_key(u'hasM') :
            hasM = ('ENABLED' if json_fc[u'hasM'] else 'DISABLED')
          
        hasZ = 'DISABLED'
        if json_fc.has_key(u'hasZ') :
            hasZ = ('ENABLED' if json_fc[u'hasZ'] else 'DISABLED')

        spRef = None
        if json_fc.has_key('spatialReference') :
            spRef = json_fc['spatialReference']
            if type(spRef) == type({}) :
                spRef = spRef[u'wkid']
            
        arcpy.CreateFeatureclass_management(in_mem, temp_name, geomType, '', hasM, hasZ, spRef)
    else :
        arcpy.CreateTable_management(in_mem, temp_name)
        
    #add fields
    attributeFieldList = []
    for field in json_fc[u'fields'] :
        field_type = field[u'type'][len('esriFieldType'):]
        if field_type != 'OID' and field[u'name'] not in ['Shape_Length', 'Shape_Area', geomFieldName]:
            if field_type == u'String' :
                field_type = 'TEXT'
            arcpy.AddField_management(in_mem_fc, field[u'name'], field_type, "", "", (field[u'length'] if field.has_key(u'length') else ""), field[u'alias'], True)
            #keep original field list to access json attributes while inserting into new table (new table can have different field names after validation)
            attributeFieldList.append(field[u'name'])
            
    #copy table/fc to the destination workspace (helps with fields validation too. Validation is done inside Copy...)
    if geomType :
        arcpy.CopyFeatures_management(in_mem_fc, output_fc)
    else :
        arcpy.CopyRows_management(in_mem_fc, output_fc)

    arcpy.Delete_management(in_mem_fc)
    return attributeFieldList

##################################################
def ImportFromJSON(json_file, output_fc) :
    json_fc = json.load(json_file)
    
    workspace = os.path.dirname(output_fc)
    fc_name = os.path.basename(output_fc)
    
    geomType = None
    if len(json_fc[u'features']) > 0 and json_fc[u'features'][0].has_key(u'geometry') :
        geom = json_fc[u'features'][0][u'geometry']
        if geom.has_key(u'rings') :
            geomType = 'POLYGON'
        elif geom.has_key(u'paths') :
            geomType = 'POLYLINE'
        elif geom.has_key(u'points') :
            geomType = 'MULTIPOINT'
        elif geom.has_key(u'x') :
            geomType = 'POINT'
        else :
            arcpy.gp.addError('Unknown geometry type')
            sys.exit()
    
    #create output dataset
    attributeFieldList = CreateDataset(output_fc, json_fc, geomType)
    
    if arcpy.Exists(output_fc) == False :
        arcpy.gp.addError("Cannot create: " + output_fc)
        sys.exit()
    
    #prepare new field list for insert cursor
    field_list = []
    desc_output_fc = arcpy.Describe(output_fc)
    output_fields = desc_output_fc.fields
    
    for field in output_fields :
        if field.type not in ['Geometry', 'OID'] and field.name not in ['Shape_Length', 'Shape_Area']:
            field_list.append(unicode(field.name))
    
    #insert features
    try :
        with arcpy.da.InsertCursor(output_fc, field_list + ([u'shape@json'] if geomType else [])) as cursor:
            for feature in json_fc[u'features'] :
                row = []
                for field in attributeFieldList :
                    row.append(feature[u'attributes'][field])
                    
                if geomType :
                    geom = unicode(json.dumps(feature[u'geometry']))
                    row.append(geom)
                    
                cursor.insertRow(row)
    except :    
        arcpy.gp.addError("Cannot save: " + output_fc)

##################################################
def DumpFields2JSONStr(fields, pjson = False) :
    fields_json = []
    for field in fields :
        field_type = field.type
        if field_type not in ['Geometry', 'OID'] and field.name not in ['Shape_Length', 'Shape_Area']:
            field_json = {}
            field_json[u'alias'] = unicode(field.aliasName)
            field_json[u'name'] = unicode(field.name)
            field_json[u'type'] = unicode('esriFieldType' + field_type)
            if field_type in ['String', 'Blob'] :
                field_json[u'length'] = field.length          
            
            fields_json.append(field_json)
        
    return unicode(json.dumps(fields_json, indent = (4 if pjson else None)))

##################################################
def DumpFC2JSON(fc, ftmp, pjson = False) :
    desc_fc = arcpy.Describe(fc)
    feature_type = None
    try :
        feature_type = desc_fc.featureType
    except :
        pass

    NL = u''
    if pjson == True:
        NL = u'\n'
        
    ftmp.write(u'{' + NL)
    
    #add fields
    fields = desc_fc.fields
    fields_json_string = DumpFields2JSONStr(fields, pjson)
    ftmp.write(u'"fields": ' + fields_json_string + u',' + NL)
    
    #add Z, M info
    if feature_type :
        ftmp.write(u'"hasZ": {0},'.format(u'true' if desc_fc.hasZ else u'false') + NL)
        ftmp.write(u'"hasM": {0},'.format(u'true' if desc_fc.hasM else u'false') + NL)
        ftmp.write(u'"spatialReference": {{"wkid":"{0}"}},'.format(desc_fc.spatialReference.factoryCode) + NL)
               
    #prepare field list
    field_list = []
    shape_field = None
    if feature_type :
        shape_field = unicode(desc_fc.shapeFieldName)
    
    for field in fields :
        if field.type not in ['Geometry', 'OID'] and field.name not in ['Shape_Length', 'Shape_Area']:
            field_list.append(unicode(field.name))

    if feature_type :
        field_list.append(u'shape@json')

    #add fieatures
    ftmp.write(u'"features": [' + NL)
    with arcpy.da.SearchCursor(fc, field_list) as cursor:
        add_comma = False
        row_len_no_geom = len(field_list) - (1 if feature_type else 0) #process geometry separately
        attributes_json = {}

        for row in cursor :
            attributes_json.clear()
            i = 0
            for attr in row :
                if i < row_len_no_geom :
                    attributes_json[field_list[i]] = (attr if type(attr) != datetime.datetime else unicode(attr))
                    i += 1

            if add_comma :
                ftmp.write(u',' + NL)
            else:
                add_comma = True

            attributes_str = unicode(json.dumps(attributes_json, indent = (4 if pjson else None)))
            if feature_type :    
                geometry_str = unicode(row[len(row) - 1]) if pjson != True else unicode(json.dumps(json.loads(row[len(row) - 1]), indent=4))
                row_json_str = u'{{%s"attributes": {0},%s"geometry": {1}%s}}'.format(attributes_str, geometry_str)
                row_json_str = row_json_str % (NL, NL, NL)
            else:
                row_json_str = u'{{%s"attributes": {0}%s}}'.format(attributes_str)
                row_json_str = row_json_str % (NL, NL)

            ftmp.write(row_json_str)
            
    ftmp.write(u']' + NL)

    ftmp.write(u'}')
    
##################################################
def DumpFC2JSONSimple(fc, ftmp, pjson = False) :
    desc_fc = arcpy.Describe(fc)
    feature_type = None
    try :
        feature_type = desc_fc.featureType
    except :
        pass

    NL = u''
    if pjson == True:
        NL = u'\n'
    
    #prepare field list
    field_list = []
    shape_field = None
    if feature_type :
        shape_field = unicode(desc_fc.shapeFieldName)
    
    for field in desc_fc.fields :
        if field.type not in ['Geometry', 'OID'] and field.name not in ['Shape_Length', 'Shape_Area']:
            field_list.append(unicode(field.name))

    if feature_type :
        field_list.append(u'shape@json')

    #add fieatures
    with arcpy.da.SearchCursor(fc, field_list) as cursor:
        row_len_no_geom = len(field_list) - (1 if feature_type else 0) #process geometry separately
        attributes_json = {}

        for row in cursor :
            attributes_json.clear()
            i = 0
            for attr in row :
                if i < row_len_no_geom :
                    attributes_json[field_list[i]] = (attr if type(attr) != datetime.datetime else unicode(attr))
                    i += 1

            attributes_str = unicode(json.dumps(attributes_json, indent = (4 if pjson else None)))
            if feature_type :    
                geometry_str = unicode(row[len(row) - 1]) if pjson != True else unicode(json.dumps(json.loads(row[len(row) - 1]), indent=4))
                row_json_str = u'{{%s"attributes": {0},%s"geometry": {1}%s}}%s'.format(attributes_str, geometry_str)
                row_json_str = row_json_str % (NL, NL, NL, NL)
            else:
                row_json_str = u'{{%s"attributes": {0}%s}}%s'.format(attributes_str)
                row_json_str = row_json_str % (NL, NL, NL)

            ftmp.write(row_json_str)
