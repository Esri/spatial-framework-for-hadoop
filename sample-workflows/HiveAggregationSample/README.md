# Aggregation Sample for Hive

First start the Hive Command line (Hive CLI).  If you do not have Hive installed, see [Hive Installation](https://cwiki.apache.org/Hive/adminmanual-installation.html)

```bash
# use '-S' for silent mode
hive -S
```

Add the required external libraries.
```bash
add jar
  ${env:HOME}/esri-git/hadoop-tools/sample-workflows/Java/esri-geometry-api.jar
  ${env:HOME}/esri-git/hadoop-tools/sample-workflows/Java/hadoop-utilities.jar;
```


Define a schema for the earthquake data.

```sql
CREATE EXTERNAL TABLE earthquakes (earthquake_date STRING, latitude DOUBLE, longitude DOUBLE, magnitude DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '${env:HOME}/esri-git/hadoop-tools/sample-workflows/data/earthquake-data';
```

Define a schema for the California counties data.

```sql
CREATE EXTERNAL TABLE counties (Area string, Perimeter string, State string, County string, Name string, BoundaryShape binary)                                         
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.hadoop.hive.serde.EnclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '${env:HOME}/esri-git/hadoop-tools/sample-workflows/data/counties-data'; 
```

Now run a select statement to aggregate earthquake counts accross the California counties.

```sql
SELECT counties.name, count(*) cnt FROM counties
JOIN earthquakes
WHERE ST_Contains(counties.boundaryshape, ST_Point(earthquakes.longitude, earthquakes.latitude))
GROUP BY counties.name
ORDER BY cnt desc;
```

Your results should look like this:

```
Kern  36
San Bernardino	35
Imperial	28
Inyo	20
Los Angeles	18
Riverside	14
Monterey	14
Santa Clara	12
Fresno	11
San Benito	11
San Diego	7
Santa Cruz	5
San Luis Obispo	3
Ventura	3
Orange	2
San Mateo	1
```
