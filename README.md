# hadoop-tools

These tools provide Geoprocessing methods for ArcGIS to use with an external Hadoop data-processing system.  The tools, written in Python, are available in the attached Hadoop Tools toolbox file, or can be imported into an existing custom toolbox.  The tools make use of the Hadoop [HDFS filesystem](http://hadoop.apache.org/docs/stable/#HDFS), and Hadoop [Oozie workflow management system](http://oozie.apache.org/).

## Features
* Tools that export features from ArcGIS to Hadoop, and import features from Hadoop to ArcGIS
* Tools to submit a workflow to process data in Hadoop, and to check the status of a workflow
* Tools to convert features to JSON (Will eventually use JSON Conversion tools in the ArcGIS Geoprocessing Data Management Toolbox)

## Instructions

1. Download and unzip the .zip file to a suitable location or clone the repository with a git tool.
2. In the ‘ArcToolbox’ pane of ArcGIS desktop, use the ‘Add Toolbox…’ command to add the Hadoop Tools toolbox (.tbx) file into ArcGIS Desktop.
3. Use the tools individually, in models or in scripts.

## Requirements

* Hadoop data-processing system with WebHDFS and Oozie workflow engine enabled
* [Esri Geometry API for Java](https://github.com/Esri/geometry-api-java) jar file installed on the Hadoop system
* Workflows calling MapReduce jobs require the location of the custom job to be run
* Custom MapReduce jobs that use the Esri Geometry API require that the developer has authored the job, (referencing the com.esri.geometry.\* classes), and deployed the job Jar file to the Hadoop system, prior to the ArcGIS user submitting the workflow file. 

## Dependencies
* For WebHDFS support, a Python library webhdfs-py is bundled in.  Source is located at: https://github.com/Esri/webhdfs-py

## Resources

* [ArcGIS Geodata Resource Center]( http://resources.arcgis.com/en/communities/geodata/)
* [ArcGIS Blog](http://blogs.esri.com/esri/arcgis/)
* [twitter@esri](http://twitter.com/esri)

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an issue.

## Contributing

Anyone and everyone is welcome to contribute. 

## Licensing
Copyright 2012 Esri

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

A copy of the license is available in the repository's [license.txt]( https://raw.github.com/Esri/hadoop-tools/master/license.txt) file.
