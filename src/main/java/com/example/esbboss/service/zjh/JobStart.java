package com.example.esbboss.service.zjh;
/**
 * Copyright 2023 bboss
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.frameworkset.common.poolman.util.DBConf;
import com.frameworkset.common.poolman.util.SQLManager;
import org.frameworkset.elasticsearch.boot.ElasticSearchBoot;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/7/28
 * @author biaoping.yin
 * @version 1.0
 */
public class JobStart {
    public static void main(String[] args){
        //初始化Elasticsearch数据源：
        Map properties = new HashMap();
/**
 * 这里只设置必须的配置项，其他的属性参考配置文件：resources/application.properties
 *
 */
////认证账号和口令配置，如果启用了安全认证才需要，支持xpack和searchguard
//        properties.put("elasticUser","elastic");
//        properties.put("elasticPassword","changeme");
////es服务器地址和端口，多个用逗号分隔
//        properties.put("elasticsearch.rest.hostNames","127.0.0.1:9200");
////是否在控制台打印dsl语句，log4j组件日志级别为INFO或者DEBUG
//        properties.put("elasticsearch.showTemplate","true");
////集群节点自动发现
//        properties.put("elasticsearch.discoverHost","true");
//        properties.put("http.timeoutSocket","60000");
//        properties.put("http.timeoutConnection","40000");
//        properties.put("http.connectionRequestTimeout","70000");
        
        properties.put("elasticsearch.serverNames", "hemiao_es");
        properties.put("hemiao_es.elasticsearch.rest.hostNames", "es-cn-x0r3bkhai000dw7t4.elasticsearch.aliyuncs.com:9200");
        properties.put("hemiao_es.elasticsearch.showTemplate", "true");
        properties.put("hemiao_es.elasticUser", "elastic");
        properties.put("hemiao_es.elasticPassword", "axsMFaGASJwDTOh3");
        properties.put("hemiao_es.elasticsearch.failAllContinue", "true");
        properties.put("hemiao_es.http.timeoutSocket", "60000");
        properties.put("hemiao_es.http.timeoutConnection", "40000");
        properties.put("hemiao_es.http.connectionRequestTimeout", "70000");
        properties.put("hemiao_es.http.maxTotal", "200");
        properties.put("hemiao_es.http.defaultMaxPerRoute", "100");
        ElasticSearchBoot.boot(properties);


        /**
        //从配置文件加载配置：初始化数据库数据源
        PropertiesContainer propertiesContainer = PropertiesUtil.getPropertiesContainer("application.properties");
        String dbName  = propertiesContainer.getProperty("db.name");
        String dbUser  = propertiesContainer.getProperty("db.user");
        String dbPassword  = propertiesContainer.getProperty("db.password");
        String dbDriver  = propertiesContainer.getProperty("db.driver");
        String dbUrl  = propertiesContainer.getProperty("db.url");

        String showsql  = propertiesContainer.getProperty("db.showsql");
        String validateSQL  = propertiesContainer.getProperty("db.validateSQL");
        String dbInfoEncryptClass = propertiesContainer.getProperty("db.dbInfoEncryptClass");

        DBConf tempConf = new DBConf();
        tempConf.setPoolname(dbName);
        tempConf.setDriver(dbDriver);
        tempConf.setJdbcurl(dbUrl);
        tempConf.setUsername(dbUser);
        tempConf.setPassword(dbPassword);
        tempConf.setValidationQuery(validateSQL);
        tempConf.setShowsql(showsql != null && showsql.equals("true"));
        //tempConf.setTxIsolationLevel("READ_COMMITTED");
        tempConf.setJndiName("jndi-"+dbName);
        tempConf.setDbInfoEncryptClass(dbInfoEncryptClass);
        String initialConnections  = propertiesContainer.getProperty("db.initialSize");
        int _initialConnections = 10;
        if(initialConnections != null && !initialConnections.equals("")){
            _initialConnections = Integer.parseInt(initialConnections);
        }
        String minimumSize  = propertiesContainer.getProperty("db.minimumSize");
        int _minimumSize = 10;
        if(minimumSize != null && !minimumSize.equals("")){
            _minimumSize = Integer.parseInt(minimumSize);
        }
        String maximumSize  = propertiesContainer.getProperty("db.maximumSize");
        int _maximumSize = 20;
        if(maximumSize != null && !maximumSize.equals("")){
            _maximumSize = Integer.parseInt(maximumSize);
        }
        tempConf.setInitialConnections(_initialConnections);
        tempConf.setMinimumSize(_minimumSize);
        tempConf.setMaximumSize(_maximumSize);
        tempConf.setUsepool(false);
        tempConf.setExternal(false);
        tempConf.setEncryptdbinfo(false);
        if(showsql != null && showsql.equalsIgnoreCase("true"))
            tempConf.setShowsql(true);
        else{
            tempConf.setShowsql(false);
        }
//# 控制map中的列名采用小写，默认为大写
        tempConf.setColumnLableUpperCase(false);
        //启动数据源
        SQLManager.startPool(tempConf);
         */

//        .setDbName("middle_platformsecond")
//                .setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
//
//
//                .setDbUrl(jdbcUrl) //通过useCursorFetch=true启用mysql的游标fetch机制，否则会有严重的性能隐患，useCursorFetch必须和jdbcFetchSize参数配合使用，否则不会生效
//                .setJdbcFetchSize(-2147483648)
//                .setDbUser("sync")
//                .setDbPassword("zFfBu21vvfuUzkEE")
//                .setValidateSQL("select 1")
//                .setUsePool(false)
//                .setDbInitSize(5)
//                .setDbMinIdleSize(5)
//                .setDbMaxSize(10)
//                .setShowSql(true);//是否使用连接池;
        String jdbcUrl = "jdbc:mysql://192.168.88.10:3306/middle_platform?rewriteBatchedStatements=true&useServerPrepStmts=false&useCompression=true&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&serverTimezone=Asia/Shanghai";

        DBConf tempConf = new DBConf();
        tempConf.setPoolname("middle_platform");
        tempConf.setDriver("com.mysql.cj.jdbc.Driver");
        tempConf.setJdbcurl(jdbcUrl);
        tempConf.setUsername("sync");
        tempConf.setPassword("zFfBu21vvfuUzkEE");
        tempConf.setValidationQuery("select 1");
        tempConf.setShowsql(true);
        //tempConf.setTxIsolationLevel("READ_COMMITTED");
        tempConf.setJndiName("jndi-middle_platform");
//        String initialConnections  = propertiesContainer.getProperty("db.initialSize");
//        int _initialConnections = 10;
//        if(initialConnections != null && !initialConnections.equals("")){
//            _initialConnections = Integer.parseInt(initialConnections);
//        }
//        String minimumSize  = propertiesContainer.getProperty("db.minimumSize");
//        int _minimumSize = 10;
//        if(minimumSize != null && !minimumSize.equals("")){
//            _minimumSize = Integer.parseInt(minimumSize);
//        }
//        String maximumSize  = propertiesContainer.getProperty("db.maximumSize");
//        int _maximumSize = 20;
//        if(maximumSize != null && !maximumSize.equals("")){
//            _maximumSize = Integer.parseInt(maximumSize);
//        }
//        tempConf.setInitialConnections(10);
//        tempConf.setMinimumSize(20);
//        tempConf.setMaximumSize(20);
        tempConf.setUsepool(false);

//# 控制map中的列名采用小写，默认为大写
        tempConf.setColumnLableUpperCase(false);
        //启动数据源
        SQLManager.startPool(tempConf);

        //运行作业，可以不同的作业在不同的线程里面运行
        Db2EleasticsearchFullRunOncestore_order_detail_pos.main(args);
        Db2EleasticsearchFullRunOncestore_order_pos.main(args);

    }
}
