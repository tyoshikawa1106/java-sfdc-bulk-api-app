# java-sfdc-bulk-api-app
## Create Properties
```
src → main → resources → conf → userInfo.properties
```

```
userId = <Your UserID>
password = <Your Password>
filePath = <Your File Path>/accounts.csv
```

##Run
```
$ cd SalesforceBulkDI
$ mvn spring-boot:run
```

##Create JAR File
```
$ mvn package
$ java -jar target/SalesforceBulkDI-1.0.0-SNAPSHOT.jar
```
