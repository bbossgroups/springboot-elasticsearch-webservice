# Demo Introduce 

Web service demo based on spring boot 1.x,2.x and Elasticsearch bboss client，the following elasticsearch operation:

1. Determine whether the indice exists
2. Delete existing indice
3. Create indice by dsl
4. Get the indice definition structure
5. Create/modify/delete/query index documents
6. search documents pararrel
7. database-elasticsearch数据同步
8. hbase-elasticsearch数据同步

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

First run elasticsearch 5 or elasticsearch 6 or elasticsearch 7，run hbase and database for hbase-elasticsearch and database-elasticsearch. 

Then run the demo:

```
mvn clean install
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
# 本demo还包含两个数据同步的案例：
- DB-Elasticsearch数据同步
- Hbase-Elasticsearch数据同步（基于hbase 1.3.0开发，如果需要对接其他版本，需要调整pom.xml中的hbase-shaded-client maven坐标版本号）
```xml
	    <dependency>
			<groupId>org.apache.hbase</groupId>
			<artifactId>hbase-shaded-client</artifactId>
			<version>1.2.4</version>
		</dependency>
```
使用方法如下：
# run the db-elasticsearch data tran job
Enter the following address in the browser to run the db-elasticsearch data tran job:

http://localhost:808/scheduleDB2ESJob

Return the following results in the browser to show successful execution:

作业启动成功
```json
db2ESImport job started.
```

作业已经启动
```json
db2ESImport job has started.
```
# stop the db-elasticsearch data tran job
Enter the following address in the browser to stop the db-elasticsearch data tran job:

http://localhost:808/stopDB2ESJob

Return the following search results in the browser to show successful execution:
作业停止成功
```json
db2ESImport job started.
```
作业已经停止
```json
db2ESImport job has started.
```

# run the hbase-elasticsearch data tran job
Enter the following address in the browser to run the hbase-elasticsearch data tran job:

http://localhost:808/scheduleHBase2ESJob

Return the following results in the browser to show successful execution:

作业启动成功
```json
HBase2ES job started.
```

作业已经启动
```json
HBase2ES job has started.
```
# stop the db-elasticsearch data tran job
Enter the following address in the browser to stop the hbase-elasticsearch data tran job:

http://localhost:808/stopHBase2ESJob

Return the following search results in the browser to show successful execution:
作业停止成功
```json
HBase2ES job started.
```
作业已经停止
```json
HBase2ES job has started.
```

# development document：

https://esdoc.bbossgroups.com/#/development