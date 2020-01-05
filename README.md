# Demo Introduce 

Web service demo based on spring boot 2 and Elasticsearch bboss client 5.3.8，Verify the following elasticsearch functions:

1. Determine whether the indice exists
2. Delete existing indice
3. Create indice by dsl
4. Get the indice definition structure
5. Create/modify/delete/query index documents
6. search documents pararrel

# Demo build 

Modify the es address in es_bboss_web\src\main\resources\application.properties file:

```java
spring.elasticsearch.bboss.elasticsearch.rest.hostNames=192.168.137.1:9200
```

Then run the maven build packaging directive:

```
mvn clean package
```



# Demo run 

First run elasticsearch 5 or elasticsearch 6.Then run the demo:

```
cd target

java -jar es_bboss_web-0.0.1-SNAPSHOT.jar
```



# Run the service

Enter the following address in the browser to perform the add, delete, modify and search operations:

http://localhost:808/testBBossIndexCrud

Return the following search results in the browser to indicate successful execution:

```json
{
    "demos": [
        {
            "type": "demo",
            "id": "3",
            "fields": null,
            "version": 0,
            "index": "demo",
            "highlight": null,
            "sort": null,
            "score": 0,
            "parent": null,
            "routing": null,
            "found": false,
            "nested": null,
            "innerHits": null,
            "dynamicPriceTemplate": null,
            "demoId": 3,
            "contentbody": "this is content body3",
            "agentStarttime": "2019-02-21T05:08:19.724+0000",
            "applicationName": "blackcatdemo3",
            "orderId": "NFZF15045871807281445364228",
            "contrastStatus": 3,
            "name": "zhangxueyou"
        },
        {
            "type": "demo",
            "id": "2",
            "fields": null,
            "version": 0,
            "index": "demo",
            "highlight": null,
            "sort": null,
            "score": 0,
            "parent": null,
            "routing": null,
            "found": false,
            "nested": null,
            "innerHits": null,
            "dynamicPriceTemplate": null,
            "demoId": 2,
            "contentbody": "this is modify content body2",
            "agentStarttime": "2019-02-21T05:08:19.871+0000",
            "applicationName": "blackcatdemo2",
            "orderId": "NFZF15045871807281445364228",
            "contrastStatus": 2,
            "name": "刘德华modify\t"
        }
    ],
    "totalSize": 2
}
```

# run the db-elasticsearch data tran job
Enter the following address in the browser to perform the add, delete, modify and search operations:

http://localhost:808/scheduleDB2ESJob

Return the following search results in the browser to indicate successful execution:

```json
db2ESImportBuilder started.
```

# development document：

https://esdoc.bbossgroups.com/#/development