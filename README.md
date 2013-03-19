# spatial-framework-hadoop

The Spatial Framework for Hadoop allows developers and data scientists to use the Hadoop data processing system for spatial data analysis.

For applications and samples that use this framework, head over to [Spatial Applications for Hadoop](https://github.com/Esri/spatial-tools-hadoop).

## Features

* **[JSON Utilities](https://github.com/Esri/spatial-framework-hadoop/wiki/JSON-Utilities)** - Utilities for interacting with JSON exported from ArcGIS
 * [Javadoc](http://esri.github.com/spatial-framework-hadoop/json/)
* **[Hive Spatial](https://github.com/Esri/spatial-framework-hadoop/wiki/Hive-Spatial)** - User-Defined Functions and SerDes for spatial analysis in Hive
 * [UDF Documentation](https://github.com/Esri/spatial-framework-hadoop/wiki/UDF-Documentation)
 * [JSON Serde](https://github.com/Esri/spatial-framework-hadoop/wiki/Hive-JSON-SerDe)

## Getting Started

At the root level of this repository, you can build a single jar with everything in the framework using [Apache Ant](http://ant.apache.org/).  Alternatively, you can build a jar at the root level of each framework component (i.e. `hive\build.xml`).

You will need to update `build.properties` to point to your local distributions of Hadoop and Hive.  All build files will use this properties file.

> For older versions of Hadoop, `dir.hadoop.lib` may point to $HADOOP_HOME/share.  This is fine as the build file searches the lib path recursively.

```bash
# hadoop library base path
dir.hadoop.lib=/path/to/hadoop/lib

# hive library base path
dir.hive.lib=/path/to/hive/lib

# esri libraries path (esri-geometry-api.jar, ...)
dir.esri.lib=/path/to/esri/lib
```

## Dependencies

* [Esri Geometry API Java](https://github.com/Esri/geometry-api-java) - Java geometry library for spatial data processing 

## Requirements

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

A copy of the license is available in the repository's [license.txt]( https://raw.github.com/Esri/spatial-framework-hadoop/master/license.txt) file.

[](Esri Tags: ArcGIS, Hadoop, Big-Data, GeoProcessing, JSON, Oozie, Workflow, Java)
[](Esri Language: Python)

