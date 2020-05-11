!erase zip.msg;

DROP TABLE cse532.uszip;

!erase zip.msg;
 
!db2se import_shape cse532
-fileName         D:\stonybrook\homework\database\hm3\Tianao Wang\tl_2019_us_zcta510.shp
-srsName          nad83_srs_1
-tableSchema      cse532
-tableName        uszip
-spatialColumn    shape
-client           1
-messagesFile     zip.msg
;
 
!db2se register_spatial_column cse532
-tableSchema      cse532
-tableName        uszip
-columnName       shape
-srsName          nad83_srs_1
;
 
--describe table cse532.uszip;
 
 

