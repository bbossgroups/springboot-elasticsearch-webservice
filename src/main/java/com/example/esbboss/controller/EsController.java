/*
 *  Copyright 2008-2019 bboss
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.example.esbboss.controller;


import com.example.esbboss.entity.DemoSearchResult;
import com.example.esbboss.entity.PositionUrl;
import com.example.esbboss.service.DocumentCRUD;
import com.example.esbboss.service.DocumentCRUD7;
import com.frameworkset.common.poolman.BatchHandler;
import com.frameworkset.common.poolman.ConfigSQLExecutor;
import com.frameworkset.common.poolman.util.SQLUtil;
import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.frameworkset.bulk.*;
import org.frameworkset.elasticsearch.bulk.*;
import org.frameworkset.elasticsearch.client.ClientOptions;
import org.frameworkset.util.shutdown.ShutdownUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author yinbp[yin-bp@163.com]
 */
@RestController
public class EsController {
    private Logger logger = LoggerFactory.getLogger(EsController.class);

    @Autowired
    private DocumentCRUD documentCRUD;
    @Autowired
    private DocumentCRUD7 documentCRUD7;
    @RequestMapping("/health")
    public @ResponseBody String health() {
        return "ok";
    }
    @RequestMapping("/testBBossIndexCrud7")
    public @ResponseBody
    DemoSearchResult testBBossIndexCrud7()  {
        documentCRUD7.dropAndCreateAndGetIndice();
        documentCRUD7.addAndUpdateDocument();
        DemoSearchResult demoSearchResult = documentCRUD7.search();
        documentCRUD7.searchAllPararrel();
//        documentCRUD.deleteDocuments();
        return demoSearchResult;
    }

    @RequestMapping("/testBBossSearch7")
    public @ResponseBody
    DemoSearchResult testBBossSearch7()  {
//        documentCRUD.dropAndCreateAndGetIndice();
//        documentCRUD.addAndUpdateDocument();
        try {
            DemoSearchResult demoSearchResult = documentCRUD7.search();
//        documentCRUD.searchAllPararrel();
//        documentCRUD.deleteDocuments();
            return demoSearchResult;
        }
        catch (Exception e){
            logger.error("",e);
            throw  e;
        }
    }


    @RequestMapping("/testBBossIndexCrud")
    public @ResponseBody
    DemoSearchResult testBBossIndexCrud()  {
    	documentCRUD7.dropAndCreateAndGetIndice();
    	documentCRUD7.addAndUpdateDocument();
        DemoSearchResult demoSearchResult = documentCRUD.search();
        documentCRUD7.searchAllPararrel();
//        documentCRUD.deleteDocuments();
        return demoSearchResult;
    }

    @RequestMapping("/testBBossSearch")
    public @ResponseBody
    DemoSearchResult testBBossSearch()  {
//        documentCRUD.dropAndCreateAndGetIndice();
//        documentCRUD.addAndUpdateDocument();
        try {
            DemoSearchResult demoSearchResult = documentCRUD7.search();
//        documentCRUD.searchAllPararrel();
//        documentCRUD.deleteDocuments();
            return demoSearchResult;
        }
        catch (Exception e){
            logger.error("",e);
            throw  e;
        }
    }
    @RequestMapping("/shutdownBBossBulk")
    public @ResponseBody synchronized
    void shutdownBBossBulk()  {
        if(bulkProcessor != null){
            bulkProcessor.shutDown();
            bulkProcessor = null;
        }
    }
    @RequestMapping("/testBBossBulk")
    public @ResponseBody
    void testBBossBulk()  {
        buildBulkProcessor(10*1024*1024);
        ClientOptions clientOptions = new ClientOptions();
        clientOptions.setIdField("id")//通过clientOptions指定map中的key为id的字段值作为文档_id，
//		          .setEsRetryOnConflict(1)
//							.setPipeline("1")

//				.setOpType("index")
//				.setIfPrimaryTerm(2l)
//				.setIfSeqNo(3l)
        ;
        Map<String,Object> data = new HashMap<String,Object>();
        data.put("name","duoduo1");
        data.put("id",1);
        bulkProcessor.insertData("bulkdemo",data,clientOptions);
        data = new HashMap<String,Object>();
        data.put("name","duoduo2");
        data.put("id",2);
        bulkProcessor.insertData("bulkdemo",data,clientOptions);
        data = new HashMap<String,Object>();
        data.put("name","duoduo3");
        data.put("id",3);
        bulkProcessor.insertData("bulkdemo",data,clientOptions);
        data = new HashMap<String,Object>();
        data.put("name","duoduo4");
        data.put("id",4);
        bulkProcessor.insertData("bulkdemo",data,clientOptions);
        data = new HashMap<String,Object>();
        data.put("name","duoduo5");
        data.put("id",5);

        bulkProcessor.insertData("bulkdemo",data,clientOptions);
//		ClientOptions deleteclientOptions = new ClientOptions();

//		deleteclientOptions.setEsRetryOnConflict(1);
        //.setPipeline("1")
//		bulkProcessor.deleteDataWithClientOptions("bulkdemo","1",deleteclientOptions);
        bulkProcessor.deleteData("bulkdemo","1");//验证error回调方法
        List<Object> datas = new ArrayList<Object>();
        for(int i = 6; i < 106; i ++) {
            data = new HashMap<String,Object>();
            data.put("name","duoduo"+i);
            data.put("id",i);
            datas.add(data);
        }
        bulkProcessor.insertDatas("bulkdemo",datas);
        data = new HashMap<String,Object>();
        data.put("name",5);//error data
        data.put("id",5);
        ClientOptions updateOptions = new ClientOptions();
//		List<String> sourceUpdateExcludes = new ArrayList<String>();
//		sourceUpdateExcludes.add("name");
        //					updateOptions.setSourceUpdateExcludes(sourceUpdateExcludes); //es 7不起作用
//		List<String> sourceUpdateIncludes = new ArrayList<String>();
//		sourceUpdateIncludes.add("name");
//
//		/**
//		 * ersion typesedit
//		 * In addition to the external version type, Elasticsearch also supports other types for specific use cases:
//		 *
//		 * internal
//		 * Only index the document if the given version is identical to the version of the stored document.
//		 * external or external_gt
//		 * Only index the document if the given version is strictly higher than the version of the stored document or if there is no existing document. The given version will be used as the new version and will be stored with the new document. The supplied version must be a non-negative long number.
//		 * external_gte
//		 * Only index the document if the given version is equal or higher than the version of the stored document. If there is no existing document the operation will succeed as well. The given version will be used as the new version and will be stored with the new document. The supplied version must be a non-negative long number.
//		 * The external_gte version type is meant for special use cases and should be used with care. If used incorrectly, it can result in loss of data. There is another option, force, which is deprecated because it can cause primary and replica shards to diverge.
//		 */
//		updateOptions.setSourceUpdateIncludes(sourceUpdateIncludes);//es 7不起作用
        updateOptions
//				.setDetectNoop(false)
//				.setDocasupsert(false)
//				.setReturnSource(true)
//				.setEsRetryOnConflict(1)
                .setIdField("id") //验证exception回调方法
        //elasticsearch7不能同时指定EsRetryOnConflict和IfPrimaryTerm/IfSeqNo
        //.setVersion(10).setVersionType("internal") elasticsearch 7x必须使用IfPrimaryTerm和IfSeqNo代替version
//						.setIfPrimaryTerm(2l)
//				.setIfSeqNo(3l).setPipeline("1")
        ;
        bulkProcessor.updateData("bulkdemo",data,updateOptions);


        data = new HashMap<String,Object>();
        data.put("id",1000);
        data.put("script","{\"name\":\"duoduo104\",\"goodsid\":104}");
        clientOptions = new ClientOptions();
        clientOptions.setIdField("id");
        clientOptions.setScriptField("script");
        bulkProcessor.insertData("bulkdemo",data,clientOptions);

        data = new HashMap<String,Object>();
        data.put("id",1000);
        data.put("script","{\"name\":\"updateduoduo104\",\"goodsid\":1104}");
        clientOptions = new ClientOptions();
        clientOptions.setIdField("id");
        clientOptions.setScriptField("script");
        bulkProcessor.updateData("bulkdemo",data,clientOptions);

    }

    @RequestMapping("/shutdownCommonBBossBulk")
    public @ResponseBody synchronized
    void shutdownCommonBBossBulk()  {
        if(commonBulkProcessor != null){
            commonBulkProcessor.shutDown();
            commonBulkProcessor = null;
        }
    }
    @RequestMapping("/testCommonBBossBulk")
    public @ResponseBody
    void testCommonBBossBulk()  {
        buildCommonBulkProcessor();
        int i = 0;
        do{
            if(commonBulkProcessor.isShutdown())//如果已经关闭异步批处理器，中断插入数据
                break;
            PositionUrl entity = new PositionUrl();
            entity.setId(UUID.randomUUID().toString());
            entity.setPositionUrl("positionUrl"+ i);
            entity.setPositionName("position.getPostionName()"+i);
            Timestamp time = new Timestamp(System.currentTimeMillis());
            entity.setCreatetime(time);
//            logger.info(SimpleStringUtil.object2json(entity));
            commonBulkProcessor.insertData(entity);//添加一条记录
//            try {
//                sleep(10);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            i ++;
        }while(i < 50);

    }

    private synchronized void buildBulkProcessor(int maxMemSize){
        if(bulkProcessor != null)
            return ;
        //定义BulkProcessor批处理组件构建器
        BulkProcessorBuilder bulkProcessorBuilder = new BulkProcessorBuilder();
        bulkProcessorBuilder.setBlockedWaitTimeout(0)//指定bulk数据缓冲队列已满时后续添加的bulk数据排队等待时间，如果超过指定的时候数据将被拒绝处理，单位：毫秒，默认为0，不拒绝并一直等待成功为止
                .setBulkSizes(10)//按批处理数据记录数，达到BulkSizes对应的值时，执行一次bulk操作
                //设置批量记录占用内存最大值，以字节为单位，达到最大值时，执行一次bulk操作
                // 可以根据实际情况调整maxMemSize参数，如果不设置maxMemSize，则按照按批处理数据记录数BulkSizes来判别是否执行执行一次bulk操作
                //maxMemSize参数默认值为0，不起作用，只有>0才起作用
                .setMaxMemSize(maxMemSize)
                .setFlushInterval(5000)//强制bulk操作时间，单位毫秒，如果自上次bulk操作flushInterval毫秒后，数据量没有满足BulkSizes对应的记录数，或者没有满足maxMemSize，但是有记录，那么强制进行bulk处理

                .setWarnMultsRejects(1000)//bulk处理操作被每被拒绝WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息
                .setWorkThreads(2)//bulk处理工作线程数
                .setWorkThreadQueue(2)//bulk处理工作线程池缓冲队列大小
                .setBulkProcessorName("test_bulkprocessor")//工作线程名称，实际名称为BulkProcessorName-+线程编号
                .setBulkRejectMessage("Reject test bulkprocessor")//bulk处理操作被每被拒绝WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息提示前缀
                .setElasticsearch("default")//指定Elasticsearch集群数据源名称，bboss可以支持多数据源
                .addBulkInterceptor(new BulkInterceptor() {
                    public void beforeBulk(BulkCommand bulkCommand) {
                        logger.debug("beforeBulk");
                    }

                    public void afterBulk(BulkCommand bulkCommand, String result) {
                        logger.info("afterBulk："+result);
                        logger.info("appendSize:"+bulkCommand.getAppendRecords());
                        logger.info("totalSize:"+bulkCommand.getTotalSize());
                        logger.info("totalFailedSize:"+bulkCommand.getTotalFailedSize());
                    }

                    public void exceptionBulk(BulkCommand bulkCommand, Throwable exception) {
                        logger.error("exceptionBulk：",exception);
                        logger.info("appendSize:"+bulkCommand.getAppendRecords());
                        logger.info("totalSize:"+bulkCommand.getTotalSize());
                        logger.info("totalFailedSize:"+bulkCommand.getTotalFailedSize());
                    }
                    public void errorBulk(BulkCommand bulkCommand, String result) {
                        logger.warn("errorBulk："+result);
                        logger.info("appendSize:"+bulkCommand.getAppendRecords());
                        logger.info("totalSize:"+bulkCommand.getTotalSize());
                        logger.info("totalFailedSize:"+bulkCommand.getTotalFailedSize());
                    }
                })
                //为了提升性能，并没有把所有响应数据都返回，过滤掉了部分数据，可以自行设置FilterPath进行控制
                .setFilterPath("took,errors,items.*.error")

                // 重试配置
                .setBulkRetryHandler(new BulkRetryHandler() { //设置重试判断策略，哪些异常需要重试
                    public boolean neadRetry(Exception exception, BulkCommand bulkCommand) { //判断哪些异常需要进行重试
                        if (exception instanceof HttpHostConnectException     //NoHttpResponseException 重试
                                || exception instanceof ConnectTimeoutException //连接超时重试
                                || exception instanceof UnknownHostException
                                || exception instanceof NoHttpResponseException
//              				|| exception instanceof SocketTimeoutException    //响应超时不重试，避免造成业务数据不一致
                        ) {

                            return true;//需要重试
                        }

                        if(exception instanceof SocketException){
                            String message = exception.getMessage();
                            if(message != null && message.trim().equals("Connection reset")) {
                                return true;//需要重试
                            }
                        }

                        return false;//不需要重试
                    }
                })
                .setRetryTimes(3) // 设置重试次数，默认为0，设置 > 0的数值，会重试给定的次数，否则不会重试
                .setRetryInterval(1000l) // 可选，默认为0，不等待直接进行重试，否则等待给定的时间再重试

        // https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
        //下面的参数都是bulk url请求的参数：RefreshOption和其他参数只能二选一，配置了RefreshOption（类似于refresh=true&&aaaa=bb&cc=dd&zz=ee这种形式，将相关参数拼接成合法的url参数格式）就不能配置其他参数，
        // 其中的refresh参数控制bulk操作结果强制refresh入elasticsearch，便于实时查看数据，测试环境可以打开，生产不要设置
//				.setRefreshOption("refresh")
//				.setTimeout("100s")
//				.setMasterTimeout("50s")
//				.setRefresh("true")
//				.setWaitForActiveShards(2)
//				.setRouting("1") //(Optional, string) Target the specified primary shard.
//				.setPipeline("1") // (Optional, string) ID of the pipeline to use to preprocess incoming documents.
        ;
        /**
         * 构建BulkProcessor批处理组件，一般作为单实例使用，单实例多线程安全，可放心使用
         */
        bulkProcessor = bulkProcessorBuilder.build();//构建批处理作业组件
        ShutdownUtil.addShutdownHook(new Runnable() {
            @Override
            public void run() {
                bulkProcessor.shutDown();
            }
        });
    }

    private synchronized void buildCommonBulkProcessor(){
        if(commonBulkProcessor != null)
            return ;
        int bulkSize = 59;
        int workThreads = 5;
        int workThreadQueue = 100;
        final ConfigSQLExecutor executor = new ConfigSQLExecutor("dbbulktest.xml");//加载sql配置文件，初始化一个db dao组件
//		DBInit.startDatasource(""); //初始化bboss数据源方法，参考文档：https://doc.bbossgroups.com/#/persistent/PersistenceLayer1
        SQLUtil.startPool("default",//数据源名称
                "com.mysql.jdbc.Driver",//oracle驱动
                "jdbc:mysql://localhost:3306/bboss",//mysql链接串
                "root","123456",//数据库账号和口令
                "select 1 " //数据库连接校验sql
        );
        //定义BulkProcessor批处理组件构建器
        CommonBulkProcessorBuilder bulkProcessorBuilder = new CommonBulkProcessorBuilder();
        bulkProcessorBuilder.setBlockedWaitTimeout(-1)//指定bulk工作线程缓冲队列已满时后续添加的bulk处理排队等待时间，如果超过指定的时候bulk将被拒绝处理，单位：毫秒，默认为0，不拒绝并一直等待成功为止

                .setBulkSizes(bulkSize)//按批处理数据记录数
                .setFlushInterval(50000)//强制bulk操作时间，单位毫秒，如果自上次bulk操作flushInterval毫秒后，数据量没有满足BulkSizes对应的记录数，但是有记录，那么强制进行bulk处理

                .setWarnMultsRejects(1000)//由于没有空闲批量处理工作线程，导致bulk处理操作出于阻塞等待排队中，BulkProcessor会对阻塞等待排队次数进行计数统计，bulk处理操作被每被阻塞排队WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息
                .setWorkThreads(workThreads)//bulk处理工作线程数
                .setWorkThreadQueue(workThreadQueue)//bulk处理工作线程池缓冲队列大小
                .setBulkProcessorName("db_bulkprocessor")//工作线程名称，实际名称为BulkProcessorName-+线程编号
                .setBulkRejectMessage("db bulkprocessor ")//bulk处理操作被每被拒绝WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息提示前缀
                .addBulkInterceptor(new CommonBulkInterceptor() {// 添加异步处理结果回调函数
                    /**
                     * 执行前回调方法
                     * @param bulkCommand
                     */
                    public void beforeBulk(CommonBulkCommand bulkCommand) {

                    }

                    /**
                     * 执行成功回调方法
                     * @param bulkCommand
                     * @param result
                     */
                    public void afterBulk(CommonBulkCommand bulkCommand, BulkResult result) {
                        if(logger.isDebugEnabled()){
//                           logger.debug(result.getResult());
                        }

                        //查看队列中追加的总记录数
                        logger.info("appendSize:"+bulkCommand.getAppendRecords());
                        //查看已经被处理成功的总记录数
                        logger.info("totalSize:"+bulkCommand.getTotalSize());
                        //查看处理失败的记录数
                        logger.info("totalFailedSize:"+bulkCommand.getTotalFailedSize());
                    }

                    /**
                     * 执行异常回调方法
                     * @param bulkCommand
                     * @param exception
                     */
                    public void exceptionBulk(CommonBulkCommand bulkCommand, Throwable exception) {
                        if(logger.isErrorEnabled()){
                            logger.error("exceptionBulk",exception);
                        }
                    }

                    /**
                     * 执行过程中部分数据有问题回调方法
                     * @param bulkCommand
                     * @param result
                     */
                    public void errorBulk(CommonBulkCommand bulkCommand, BulkResult result) {
                        if(logger.isWarnEnabled()){
//                            logger.warn(result);
                        }
                    }
                })//添加批量处理执行拦截器，可以通过addBulkInterceptor方法添加多个拦截器
                /**
                 * 设置执行数据批处理接口，实现对数据的异步批处理功能逻辑
                 */
                .setBulkAction(new BulkAction() {
                    public BulkResult execute(CommonBulkCommand command) {
                        List<CommonBulkData> bulkDataList = command.getBatchBulkDatas();//拿出要进行批处理操作的数据
                        List<PositionUrl> positionUrls = new ArrayList<PositionUrl>();
                        for(int i = 0; i < bulkDataList.size(); i ++){
                            CommonBulkData commonBulkData = bulkDataList.get(i);
                            /**
                             * 可以根据操作类型，对数据进行相应处理
                             */
//							if(commonBulkData.getType() == CommonBulkData.INSERT) 新增记录
//							if(commonBulkData.getType() == CommonBulkData.UPDATE) 修改记录
//							if(commonBulkData.getType() == CommonBulkData.DELETE) 删除记录
                            positionUrls.add((PositionUrl)commonBulkData.getData());

                        }
                        BulkResult bulkResult = new BulkResult();//构建批处理操作结果对象
                        try {
                            //调用数据库dao executor，将数据批量写入数据库，对应的sql语句addPositionUrl在xml配置文件dbbulktest.xml中定义
                            executor.executeBatch("addPositionUrl", positionUrls, 150, new BatchHandler<PositionUrl>() {
                                public void handler(PreparedStatement stmt, PositionUrl record, int i) throws SQLException {
                                    //id,positionUrl,positionName,createTime
                                    stmt.setString(1, record.getId());
                                    stmt.setString(2, record.getPositionUrl());
                                    stmt.setString(3, record.getPositionName());
                                    stmt.setTimestamp(4, record.getCreatetime());
                                }
                            });

                        }
                        catch (Exception e){
                            //如果执行出错，则将错误信息设置到结果中
                            logger.error("",e);
                            bulkResult.setError(true);
                            bulkResult.setErrorInfo(e.getMessage());
//                            bulkResult.setMarkStopBulk(true);
                        }
                        return bulkResult;
                    }
                })
        ;
        /**
         * 构建BulkProcessor批处理组件，一般作为单实例使用，单实例多线程安全，可放心使用
         */
        commonBulkProcessor = bulkProcessorBuilder.build();//构建批处理作业组件
        ShutdownUtil.addShutdownHook(new Runnable() {
            @Override
            public void run() {
                commonBulkProcessor.shutDown();
            }
        });
    }


    /**
     * BulkProcessor批处理组件，一般作为单实例使用，单实例多线程安全，可放心使用
     */
    private BulkProcessor bulkProcessor;

    private CommonBulkProcessor commonBulkProcessor;

}
