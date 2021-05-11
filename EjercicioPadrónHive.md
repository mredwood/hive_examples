DROP TABLE padron_txt_String;
DROP TABLE padron_txt;
DROP TABLE padron_txt_2;


## Creamos una tabla padron_txt_String. Cuidado, OpenCSVSerde lo transforma todo en string.

CREATE TABLE padron_txt_String(
COD_DISTRITO int,
DESC_DISTRITO string,
COD_DIST_BARRIO int,
DESC_BARRIO string,
COD_BARRIO int,
COD_DIST_SECCION int,
COD_SECCION int,
COD_EDAD_INT int,
EspanolesHombres int,
EspanolesMujeres int,
ExtranjerosHombres int,
ExtranjerosMujeres int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = '\u0059',
   "quoteChar"     = '"',
   "escapeChar"    = '\\'
)
STORED AS TEXTFILE;

ALTER TABLE padron_txt_String SET TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/cloudera/Downloads/Rango_Edades_Seccion_202104.csv' INTO TABLE padron_txt_String;



## Creamos una tabla que transforme los strings en int.


CREATE TABLE padron_txt AS
SELECT CAST(COD_DISTRITO AS INT) AS COD_DISTRITO, DESC_DISTRITO, CAST(COD_DIST_BARRIO AS INT) AS COD_DIST_BARRIO, DESC_BARRIO, CAST(COD_BARRIO AS INT) AS COD_BARRIO, CAST(COD_DIST_SECCION AS INT) AS COD_DIST_SECCION, CAST(COD_SECCION AS INT) AS COD_SECCION, CAST(COD_EDAD_INT AS INT) AS COD_EDAD_INT, CAST(EspanolesHombres AS INT) AS EspanolesHombres, CAST(EspanolesMujeres AS INT) AS EspanolesMujeres, CAST(ExtranjerosHombres AS INT) AS ExtranjerosHombres, CAST(ExtranjerosMujeres AS INT) AS ExtranjerosMujeres FROM padron_txt_String;



## Creamos una tabla padron_txt_2, trimming los espacios.


CREATE TABLE padron_txt_2 AS
SELECT COD_DISTRITO, TRIM(DESC_DISTRITO) AS DESC_DISTRITO, COD_DIST_BARRIO, TRIM(DESC_BARRIO) AS DESC_BARRIO, COD_BARRIO, COD_DIST_SECCION, COD_SECCION, COD_EDAD_INT, (CASE WHEN EspanolesHombres IS NULL THEN 0 ELSE EspanolesHombres END) AS EspanolesHombres, (CASE WHEN EspanolesMujeres IS NULL THEN 0 ELSE EspanolesMujeres END) AS EspanolesMujeres, (CASE WHEN ExtranjerosHombres IS NULL THEN 0 ELSE ExtranjerosHombres END) AS ExtranjerosHombres, (CASE WHEN ExtranjerosMujeres IS NULL THEN 0 ELSE ExtranjerosMujeres END) AS ExtranjerosMujeres FROM padron_txt;


SELECT * FROM padron_txt where COD_DIST_BARRIO = 101 LIMIT 10;
SELECT * FROM padron_txt_2 where COD_DIST_BARRIO = 101 LIMIT 10;




## Parquet


DROP TABLE padron_parquet;
DROP TABLE padron_parquet_2;


### Creamos una tabla llamada padron_parquet directamente desde padron_txt.

CREATE TABLE padron_parquet STORED AS PARQUET AS SELECT COD_DISTRITO, TRIM(DESC_DISTRITO) AS DESC_DISTRITO, COD_DIST_BARRIO, TRIM(DESC_BARRIO) AS DESC_BARRIO, COD_BARRIO, COD_DIST_SECCION, COD_SECCION, COD_EDAD_INT, (CASE WHEN EspanolesHombres IS NULL THEN 0 ELSE EspanolesHombres END) AS EspanolesHombres, (CASE WHEN EspanolesMujeres IS NULL THEN 0 ELSE EspanolesMujeres END) AS EspanolesMujeres, (CASE WHEN ExtranjerosHombres IS NULL THEN 0 ELSE ExtranjerosHombres END) AS ExtranjerosHombres, (CASE WHEN ExtranjerosMujeres IS NULL THEN 0 ELSE ExtranjerosMujeres END) AS ExtranjerosMujeres FROM padron_txt;



### Creamos una segunda tabla, padron_parquet_2, desde padron_txt_2


CREATE TABLE padron_parquet_2 STORED AS PARQUET AS SELECT * FROM padron_txt_2;

SELECT * FROM padron_parquet LIMIT 15;
SELECT * FROM padron_parquet_2 LIMIT 15;


SELECT sum(EspanolesHombres), sum(EspanolesMujeres), sum(ExtranjerosHombres), sum(ExtranjerosMujeres), DESC_DISTRITO, DESC_BARRIO FROM padron_parquet_2 GROUP BY DESC_DISTRITO, DESC_BARRIO ORDER BY DESC_DISTRITO, DESC_BARRIO;

SELECT sum(EspanolesHombres), sum(EspanolesMujeres), sum(ExtranjerosHombres), sum(ExtranjerosMujeres), DESC_DISTRITO, DESC_BARRIO FROM padron_txt_2 GROUP BY DESC_DISTRITO, DESC_BARRIO ORDER BY DESC_DISTRITO, DESC_BARRIO;



## Creamos una tabla particionada, padron_particionado


DROP TABLE padron_particionado;

CREATE TABLE padron_particionado(
COD_DISTRITO int,
COD_DIST_BARRIO int,
COD_BARRIO int,
COD_DIST_SECCION int,
COD_SECCION int,
COD_EDAD_INT int,
EspanolesHombres int,
EspanolesMujeres int,
ExtranjerosHombres int,
ExtranjerosMujeres int,
DESC_BARRIO string)
PARTITIONED BY(DESC_DISTRITO string) STORED AS PARQUET
LOCATION '/user/hive/warehouse/anotherLocation';

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=1000;

INSERT OVERWRITE TABLE padron_particionado
PARTITION(DESC_DISTRITO)
SELECT COD_DISTRITO,
COD_DIST_BARRIO,
COD_BARRIO,
COD_DIST_SECCION,
COD_SECCION,
COD_EDAD_INT,
EspanolesHombres,
EspanolesMujeres,
ExtranjerosHombres,
ExtranjerosMujeres, DESC_BARRIO, DESC_DISTRITO FROM padron_parquet_2;

## Particionado también por desc_barrio da error, ya que alguno tiene Ñ.


SELECT sum(EspanolesHombres), sum(EspanolesMujeres), sum(ExtranjerosHombres), sum(ExtranjerosMujeres), DESC_DISTRITO, DESC_BARRIO FROM padron_particionado WHERE DESC_DISTRITO IN ('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS') GROUP BY DESC_DISTRITO, DESC_BARRIO ORDER BY DESC_DISTRITO, DESC_BARRIO;



## HDFS


CREATE DATABASE numeros;
USE numeros;

DROP TABLE numeros_tbl;

CREATE EXTERNAL TABLE numeros_tbl(
num1 int,
num2 int,
num3 int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://quickstart.cloudera:8020/user/cloudera/test1';


### Esto no es necesario si ponemos "location"
LOAD DATA INPATH 'test1/datos1.txt' INTO TABLE numeros_tbl;


