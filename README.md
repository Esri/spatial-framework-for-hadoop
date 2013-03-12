# hadoop-tools

The Hadoop tools for ArcGIS allow the user to make use of an externally (and customer-provided) Hadoop data processing system from within the ArcGIS GeoProcessing framework.


## Features

* Java developer utilities for interacting with data exported from ArcGIS
* Submodule links to the components that form the Hadoop toolkit
 * **Java Geometry API** - Enables spatial operations in Java
 * **Hive Spatial UDFs** - User defined functions for Hive that wrap methods in the Geometry API
 * **Geoprocessing Tools** - Geoprocessing tools that enable ArcGIS users to interact with a Hadoop system

## Requirements

* [Esri Geometry API for Java](https://github.com/Esri/geometry-api-java) jar file installed on the Hadoop system
* Workflows calling MapReduce jobs require the location of the custom job to be run
* Custom MapReduce jobs that use the Esri Geometry API require that the developer has authored the job, (referencing the com.esri.geometry.\* classes), and deployed the job Jar file to the Hadoop system, prior to the ArcGIS user submitting the workflow file. 

## Resources

* [ArcGIS Geodata Resource Center]( http://resources.arcgis.com/en/communities/geodata/)
* [ArcGIS Blog](http://blogs.esri.com/esri/arcgis/)
* [twitter@esri](http://twitter.com/esri)

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an issue.

## Contributing

Anyone and everyone is welcome to contribute. 

## Licensing
Copyright 2013 Esri

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

[](Esri Tags: ArcGIS, Hadoop, Big-Data, GeoProcessing, JSON, Oozie, Workflow, Java)
[](Esri Language: Python)

