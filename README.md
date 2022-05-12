# Demo Introduce 

Web service demo based on spring boot 1.x,2.x and Elasticsearch bboss client，include following elasticsearch operations:

1. check indice exists
2. Delete existing indice
3. Create indice by dsl
4. Get the indice definition structure
5. Create/modify/delete/query index documents
6. search documents pararrel
7. database-elasticsearch数据同步
8. hbase-elasticsearch数据同步
9. file-elasticsearch数据同步

# Demo build 

Modify the es address in springboot-elasticsearch\src\main\resources\application.properties file:

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

Elasticsearch 7以下版本

http://localhost:808/testBBossIndexCrud

Elasticsearch 7及以上版本

http://localhost:808/testBBossIndexCrud7

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
- Hbase-Elasticsearch数据同步（基于hbase 2.2.3开发，如果需要对接其他版本，需要调整pom.xml中的hbase-shaded-client maven坐标版本号）
```xml
	    <dependency>
			<groupId>org.apache.hbase</groupId>
			<artifactId>hbase-shaded-client</artifactId>
			<version>2.2.3</version>
		</dependency>
```
使用方法如下：
# 1.运行基本db-elasticsearch作业
## 1.1 run the db-elasticsearch data tran job
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
## 1.2 stop the db-elasticsearch data tran job
Enter the following address in the browser to stop the db-elasticsearch data tran job:

http://localhost:808/stopDB2ESJob

Return the following search results in the browser to show successful execution:
作业停止成功
```json
db2ESImport job started.
```
作业已经停止
```json
db2ESImport job has been stopped.
```
# 2.运行基本hbase-elasticsearch作业
## 2.1 run the hbase-elasticsearch data tran job
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
## 2.2 stop the db-elasticsearch data tran job
Enter the following address in the browser to stop the hbase-elasticsearch data tran job:

http://localhost:808/stopHBase2ESJob

Return the following search results in the browser to show successful execution:
作业停止成功
```json
HBase2ES job started.
```
作业已经停止
```json
HBase2ES job has been stopped.
```

# 3.运行控制作业调度的db-elasticsearch作业
## 3.1 run the db-elasticsearch data tran job
Enter the following address in the browser to run the db-elasticsearch data tran job:
创建需要人工手动暂停才能暂停作业，作业启动后持续运行，当暂停后需执行resume作业才能继续调度执行
http://localhost:808/schedulecontrol/scheduleDB2ESJob?autoPause=false

创建具备暂停功能的数据同步作业，调度执行后将作业自动标记为暂停状态，等待下一个resumeShedule指令才继续允许作业调度执行，执行后再次自动暂停
http://localhost:808/schedulecontrol/scheduleDB2ESJob?autoPause=true

Return the following results in the browser to show successful execution:

作业启动成功
```json
db2ESImport job started.
```

作业已经启动
```json
db2ESImport job has started.
```
## 3.2 stop the db-elasticsearch data tran job
Enter the following address in the browser to stop the db-elasticsearch data tran job:

http://localhost:808/schedulecontrol/stopDB2ESJob

Return the following search results in the browser to show successful execution:
作业停止成功
```json
db2ESImport job stopped.
```
作业已经停止
```json
db2ESImport job has been stopped.
```
## 3.3 Pause schedule the db-elasticsearch data tran job
Enter the following address in the browser to Pause the db-elasticsearch data tran job:

http://localhost:808/schedulecontrol/pauseScheduleDB2ESJob

Return the following search results in the browser to show successful execution:
作业暂停成功
```json
db2ESImport job schedule paused.
```
作业已经暂停
```json
b2ESImport job schedule is not scheduled, Ignore pauseScheduleJob command.
```
作业已经停止
```json
db2ESImport job has been stopped.
```

## 3.4 Resume schedule the db-elasticsearch data tran job
Enter the following address in the browser to Resume the db-elasticsearch data tran job:

http://localhost:808/schedulecontrol/resumeScheduleDB2ESJob

Return the following search results in the browser to show successful execution:
作业继续调度成功
```json
db2ESImport job schedule resume to continue.
```
作业已经在调度执行提示
```json
db2ESImport job schedule is not paused, Ignore resumeScheduleJob command.
```
作业已经停止
```json
db2ESImport job has been stopped.
```

# 4.运行控制作业调度的基本hbase-elasticsearch作业
## 4.1 run the hbase-elasticsearch data tran job
Enter the following address in the browser to run the hbase-elasticsearch data tran job:

http://localhost:808/schedulecontrol/scheduleHBase2ESJob

Return the following results in the browser to show successful execution:

作业启动成功
```json
HBase2ES job started.
```

作业已经启动
```json
HBase2ES job has started.
```
## 4.2 stop the hbase-elasticsearch data tran job
Enter the following address in the browser to stop the hbase-elasticsearch data tran job:

http://localhost:808/schedulecontrol/stopHBase2ESJob

Return the following search results in the browser to show successful execution:
作业停止成功
```json
HBase2ES job started.
```
作业已经停止
```json
HBase2ES job has been stopped.
```
## 4.3 Pause schedule the hbase-elasticsearch data tran job
Enter the following address in the browser to Pause the hbase-elasticsearch data tran job:

http://localhost:808/schedulecontrol/pauseScheduleHBase2ESJob

Return the following search results in the browser to show successful execution:
作业暂停成功
```json
HBase2ES job schedule paused.
```
作业已经暂停
```json
HBase2ES job schedule is not scheduled, Ignore pauseScheduleJob command.
```
作业已经停止
```json
HBase2ES job has been stopped.
```

## 4.4 Resume schedule the hbase-elasticsearch data tran job
Enter the following address in the browser to Resume the hbase-elasticsearch data tran job:

http://localhost:808/schedulecontrol/resumeScheduleHBase2ESJob

Return the following search results in the browser to show successful execution:
作业继续调度成功
```json
HBase2ES job schedule resume to continue.
```
作业已经在调度执行
```json
HBase2ES job schedule is not paused, Ignore resumeScheduleJob command.
```
作业已经停止
```json
HBase2ES job has been stopped.
```

# 5.运行控制作业调度的基本file-elasticsearch作业
## 5.1 run the file-elasticsearch data tran job
Enter the following address in the browser to run the file-elasticsearch data tran job:

http://localhost:808/schedulecontrol/startfile2es

或者创建带有自动暂停的作业

Return the following results in the browser to show successful execution:

作业启动成功
```json
file2ES job started.
```

作业已经启动
```json
file2ES job has started.
```
## 5.2 stop the file-elasticsearch data tran job
Enter the following address in the browser to stop the file-elasticsearch data tran job:

http://localhost:808/schedulecontrol/stopfile2es

Return the following search results in the browser to show successful execution:
作业停止成功
```json
file2ES job started.
```
作业已经停止
```json
file2ES job has been stopped.
```
## 5.3 Pause schedule the file-elasticsearch data tran job
Enter the following address in the browser to Pause the file-elasticsearch data tran job:

http://localhost:808/schedulecontrol/pauseFile2es

Return the following search results in the browser to show successful execution:
作业暂停成功
```json
file2ES job schedule paused.
```
作业已经暂停
```json
file2ES job schedule is not scheduled, Ignore pauseScheduleJob command.
```
作业已经停止
```json
file2ES job has been stopped.
```

## 5.4 Resume schedule the file-elasticsearch data tran job
Enter the following address in the browser to Resume the file-elasticsearch data tran job:

http://localhost:808/schedulecontrol/resumeFile2es

Return the following search results in the browser to show successful execution:
作业继续调度成功
```json
file2ES job schedule resume to continue.
```
作业已经在调度执行
```json
file2ES job schedule is not paused, Ignore resumeScheduleJob command.
```
作业已经停止
```json
file2ES job has been stopped.
```

# Development document：

https://esdoc.bbossgroups.com/#/development