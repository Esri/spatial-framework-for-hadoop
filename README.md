# spatial-framework-for-hadoop

The __Spatial Framework for Hadoop__ allows developers and data scientists to use the Hadoop data processing system 
for spatial data analysis.

For tools, [samples](https://github.com/Esri/gis-tools-for-hadoop/tree/master/samples), and [tutorials](https://github.com/Esri/gis-tools-for-hadoop/wiki) that use this framework, head over 
to [GIS Tools for Hadoop](https://github.com/Esri/gis-tools-for-hadoop).

## What's New

* Hive v4 compatibility (as of Y2023).
* ST_Centroid now returns the geometry centroid rather than the center of its envelope (as of v2.1).

## Features

* **[JSON Utilities](https://github.com/Esri/spatial-framework-for-hadoop/wiki/JSON-Utilities)** -
  Utilities for interacting with JSON exported from ArcGIS
  - [Javadoc](http://esri.github.io/spatial-framework-for-hadoop/json/)
* **[Hive Spatial](https://github.com/Esri/spatial-framework-for-hadoop/wiki/Hive-Spatial)** -
  User-Defined Functions and SerDes for spatial analysis in Hive and SparkSQL
  - [UDF Documentation](https://github.com/Esri/spatial-framework-for-hadoop/wiki/UDF-Documentation)
  - [JSON SerDe](https://github.com/Esri/spatial-framework-for-hadoop/wiki/Hive-JSON-SerDe)

## Getting Started

### Pre-Built

Up-to-date releases may be available [on Github](https://github.com/Esri/spatial-framework-for-hadoop/releases) but
[may not be on Maven Central](https://github.com/Esri/spatial-framework-for-hadoop/issues/123).

### Maven

Build as you would any other Mavenized repository.  All dependencies are pulled automatically. 

### Ant

Ant build files are also available,
but are considered legacy, and may likely be removed in a future release.

At the root level of this repository, you can build a single jar with everything in the framework 
using [Apache Ant](http://ant.apache.org/).  Alternatively, you can build a jar at the root level of each 
framework component (i.e., `hive/build.xml`).

The build files use [Maven Ant Tasks](http://maven.apache.org/ant-tasks/download.html) for dependency 
management. You will need the jar in a place Ant can find it (i.e., `~/.ant/lib/maven-ant-tasks-2.1.3.jar`).


## Dependencies

* [Esri Geometry API for Java](https://github.com/Esri/geometry-api-java) - Java geometry library for spatial data 
processing.

## Requirements

* Geometry v2.2
* Hive v1 and above or SparkSQL ([compatibility details](https://github.com/Esri/spatial-framework-for-hadoop/wiki/ST_Geometry-for-Hive-Compatibility-with-Hive-Versions))
* Hadoop v2 and above
* Workflows calling MapReduce jobs require the location of the custom job to be run.
* Custom MapReduce jobs that use the Esri Geometry API require that the developer has authored the job, 
  (referencing the com.esri.geometry.\* classes), and deployed the job Jar file to the Hadoop system,
  prior to the ArcGIS user submitting the workflow file. 
* System administrators should always deploy an appropriate version of dependencies into production,
  that include important and especially security patches,
  even if a JAR file has been built with a lower version.


## Resources

* [GeoData Blog on the ArcGIS Blogs](http://blogs.esri.com/esri/arcgis/author/jonmurphy/)
* [Big Data Place on GeoNet](https://geonet.esri.com/groups/big-data)
* [ArcGIS Geodata Resource Center](http://resources.arcgis.com/en/communities/geodata/)
* [ArcGIS Blog](http://blogs.esri.com/esri/arcgis/)
* [twitter@esri](http://twitter.com/esri)


## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an [issue](https://github.com/Esri/spatial-framework-for-hadoop/issues).

## Contributing

Esri welcomes contributions from anyone and everyone. Please see our [guidelines for contributing](https://github.com/esri/contributing).

## Licensing
Copyright 2013-2024 Esri

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

A copy of the license is available in the 
repository's [license.txt](https://raw.github.com/Esri/spatial-framework-for-hadoop/master/license.txt) file.


## See Also

* [Apache Sedona](https://github.com/apache/incubator-sedona/)
* [ArcGIS GeoAnalytics Engine](https://developers.arcgis.com/geoanalytics/)
