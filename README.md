# java-sfdc-bulk-api-app
## Create Properties File
###userInfo.properties
```
userId = <Your UserID>
password = <Your Password>
apiVersion = 34.0
authEndpoint = https://<login or test>.salesforce.com/services/Soap/u/
filePath = <Your File Path>/accounts.csv
```

## Properties File Path
###Debug
```
java-sfdc-bulk-api-app
├── conf
│   └── userInfo.properties
│   └── spec.csv
├── pom.xml
├── src
```

###Product
```
java-sfdc-bulk-api-app
├── SalesforceBulkDI-1.0.0-SNAPSHOT.jar
├── conf
│   └── userInfo.properties
│   └── spec.csv
└── data
    └── accounts.csv
```

##Run
```
$ mvn spring-boot:run
```

##Package
```
$ mvn package
$ java -jar SalesforceBulkDI-1.0.0-SNAPSHOT.jar
```
