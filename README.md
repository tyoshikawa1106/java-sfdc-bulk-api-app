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
###spec.csv
Salesforce Field,Csv Header,Value,Hint
NAME,NAME,,
ACCOUNTNUMBER,NUMBER,,
###

## Properties File Path
###Debug
```
SalesforceBulkDI
├── conf
│   └── userInfo.properties
│   └── spec.csv
├── pom.xml
├── src
```

###Product
```
ProductFolder
├── SalesforceBulkDI-1.0.0-SNAPSHOT.jar
├── conf
│   └── userInfo.properties
│   └── spec.csv
└── data
    └── accounts.csv
```

##Run
```
$ cd SalesforceBulkDI
$ mvn spring-boot:run
```

##Package
```
$ mvn package
$ java -jar SalesforceBulkDI-1.0.0-SNAPSHOT.jar
```
