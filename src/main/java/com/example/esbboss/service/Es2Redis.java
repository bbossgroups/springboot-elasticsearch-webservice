//package com.example.esbboss.service;
//
//import com.frameworkset.util.SimpleStringUtil;
//import com.hzwq.schedule.utils.common.TaskStatusEnum;
//import com.hzwq.schedule.pojo.entity.ScheduleTask;
//import com.hzwq.schedule.pojo.model.IncrementInfo;
//import com.hzwq.schedule.pojo.model.Task;
//import com.hzwq.schedule.pojo.repository.ScheduleTaskRepository;
//import com.hzwq.schedule.scheduled.DataTransfer;
//import com.hzwq.schedule.utils.SqliteIncrement;
//import org.apache.kafka.clients.producer.RecordMetadata;
//import org.frameworkset.elasticsearch.boot.BBossESStarter;
//import org.frameworkset.nosql.redis.RedisConfig;
//import org.frameworkset.nosql.redis.RedisFactory;
//import org.frameworkset.tran.CommonRecord;
//import org.frameworkset.tran.DataStream;
//import org.frameworkset.tran.ExportResultHandler;
//import org.frameworkset.tran.es.input.dummy.ES2DummyExportBuilder;
//import org.frameworkset.tran.schedule.CallInterceptor;
//import org.frameworkset.tran.schedule.ImportIncreamentConfig;
//import org.frameworkset.tran.schedule.TaskContext;
//import org.frameworkset.tran.task.TaskCommand;
//import redis.clients.jedis.JedisPool;
//import redis.clients.jedis.Pipeline;
//
//import java.io.PrintWriter;
//import java.io.StringWriter;
//import java.sql.Timestamp;
//import java.text.DateFormat;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.LinkedHashMap;
//import java.util.Map;
//import java.util.Properties;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//
//public class Es2Redis implements DataTransfer {
//
//    public Es2Redis(ScheduleTaskRepository scheduleTaskRepository) {
//        this.scheduleTaskRepository = scheduleTaskRepository;
//    }
//private  BBossESStarter es;
//    private final ScheduleTaskRepository scheduleTaskRepository;
//
//    private JedisPool currentJedisPool;
//
//    private JedisPool getCurrent(Properties properties) {
//        if (currentJedisPool == null) {
//            String servers = properties.getProperty("redis.servers");
//            Pattern p = Pattern.compile("^\\s*(.*?):(\\d+)\\s*$");
//            Matcher m = p.matcher(servers);
//            if (m.matches()) {
//                String host = m.group(1);
//                int port = Integer.parseInt(m.group(2));
//                currentJedisPool = new JedisPool(host, port);
//            }
//        }
//
//        return currentJedisPool;
//    }
//
//    private void initRedisConnection(Properties properties, String name) {
//
//        RedisConfig redisConfig = new RedisConfig();
//
//        String auth = properties.getProperty("redis.auth");
//        String servers = properties.getProperty("redis.servers");
//        String mode = properties.getProperty("redis.mode");
//        String connectionTimeout = properties.getProperty("redis.connectionTimeout");
//        int cto = Integer.parseInt(connectionTimeout);
//        String socketTimeout = properties.getProperty("redis.socketTimeout");
//        int sto = Integer.parseInt(socketTimeout);
//
//        String poolMaxWaitMillis = properties.getProperty("redis.poolMaxWaitMillis");
//        int pmw = Integer.parseInt(poolMaxWaitMillis);
//
//        String poolMaxTotal = properties.getProperty("redis.poolMaxTotal");
//        int pmt = Integer.parseInt(poolMaxTotal);
//
//        redisConfig.setName(name)
//                .setAuth(auth)
//                //集群节点可以通过逗号分隔，也可以通过\n符分隔
//                //.setServers("10.13.4.15:6359\n10.13.4.15:6369\n10.13.4.15:6379\n10.13.4.15:6389")
//                .setServers(servers)
//                .setMaxRedirections(5)
//                .setMode(mode)
//                .setConnectionTimeout(cto)
//                .setSocketTimeout(sto)
//                //.setPoolMaxWaitMillis(pmw)
//                .setPoolMaxTotal(pmt)
//                .setPoolTimeoutRetry(3)
//                .setPoolTimeoutRetryInterval(500L)
//                .setMaxIdle(-1)
//                .setMinIdle(-1)
//                .setTestOnBorrow(true)
//                .setTestOnReturn(false)
//                .setTestWhileIdle(false)
//                .setProperties(new LinkedHashMap<>());
//        RedisFactory.builRedisDB(redisConfig);
//    }
//
//
//    private DataStream currentDataStream;
//
//    @Override
//    public DataStream getCurrentDataStream() {
//        return currentDataStream;
//    }
//
//    @Override
//    public void scheduleIncrementIdImportData(Task task) {
//        //ES2DummyExportBuilder
//        ES2DummyExportBuilder importBuilder = new ES2DummyExportBuilder();
//        Properties properties = task.getNacosConfig();
//
//        Integer fetchSize = Integer.parseInt(properties.getProperty("fetchSize"));
//        importBuilder.setFetchSize(fetchSize);
//        importBuilder.setBatchSize(1000);
//
//        String hostName = properties.getProperty("elasticsearch.rest.hostNames");
//        importBuilder.addElasticsearchProperty("elasticsearch.rest.hostNames", hostName);
//        String dateFormat = properties.getProperty("elasticsearch.dateFormat");
//        importBuilder.addElasticsearchProperty("elasticsearch.dateFormat", dateFormat);
//        String timeZone = properties.getProperty("elasticsearch.timeZone");
//        importBuilder.addElasticsearchProperty("elasticsearch.timeZone", timeZone);
//        String showTemplate = properties.getProperty("elasticsearch.showTemplate");
//        importBuilder.addElasticsearchProperty("elasticsearch.showTemplate", showTemplate);
//        String discoverHost = properties.getProperty("elasticsearch.discoverHost");
//        importBuilder.addElasticsearchProperty("elasticsearch.discoverHost", discoverHost);
//
//        String user = properties.getProperty("elasticUser");
//        importBuilder.addElasticsearchProperty("elasticUser", user);
//        String elasticPassword = properties.getProperty("elasticPassword");
//        importBuilder.addElasticsearchProperty("elasticPassword", elasticPassword);
//        importBuilder.addElasticsearchProperty("dslfile.dslMappingDir", dslMappingDir);
//
//
//
//        //初始化redis连接
//        //initRedisConnection(properties, task.getDataId());
//
//        String esQuery = properties.getProperty("queryDslName");
//        String searchIndex = properties.getProperty("searchIndex");
//        importBuilder
//                .setDsl2ndSqlFile(task.getDslFileName())
//                .setDslName(esQuery)
//                .setScrollLiveTime("10m")
//                //.setSliceQuery(true)
//                //.setSliceSize(5)
//                .setQueryUrl(searchIndex + "/_search")
//                .setIncreamentEndOffset(5000);
//        //添加dsl中需要用到的参数及参数值
//        //.addParam("var1","v1")
//        //.addParam("var2","v2")
//        //.addParam("var3","v3");
//
//        //定时任务配置，
//        String delayStr = properties.getProperty("delay");
//        Long delay = Long.parseLong(delayStr);
//        String periodStr = properties.getProperty("period");
//        Long period = Long.parseLong(periodStr);
//        importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//                .setDeyLay(delay) // 任务延迟执行delay毫秒后执行
//                .setPeriod(period); //每隔period毫秒执行，如果不设置，只执行一次
//        //定时任务配置结束
//
//        String startTimeStr = properties.getProperty("startTime");
//        String startTimeFormat = properties.getProperty("startTimeFormat");
//
//        try {
//            DateFormat formatter = new SimpleDateFormat(startTimeFormat);
//            Date date = formatter.parse(startTimeStr);
//            importBuilder.setScheduleDate(date);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//
//        //自己处理数据
//        String transferMode = properties.getProperty("transferMode").toUpperCase();
//        String list = properties.getProperty("list");
//        String channel = properties.getProperty("channel");
//
//        importBuilder.setCustomOutPut((taskContext, data) -> {
//            //You can do anything here for data
//            //批量处理
//            try {
//                //Pipeline rh = RedisFactory.getRedisHelper(task.getDataId()).pipelined();
//                clusterPipeline = RedisTool.getInstance().getClusterPipelined();
//                Pipeline rh = getCurrent(properties).getResource().pipelined();
//
//                for (CommonRecord record : data) {
//                    Map<String, Object> rowData = record.getDatas();
//                    //String LOG_ID =String.valueOf(data.get("LOG_ID"));
//                    String valueData = SimpleStringUtil.object2json(rowData);
//                    switch (transferMode) {
//                        case "PUBSUB":
//                            rh.publish(channel, valueData);
//                            break;
//                        case "LIST":
//                            rh.lpush(list, valueData);
//                    }
//                }
//                //batch insert
//                rh.sync();
//
//            } catch (Exception ex) {
//                ex.printStackTrace();
//                //抛出异常，不然会默认成功传递数据
//                throw ex;
//            }
//        }).addCallInterceptor(new CallInterceptor() {
//            @Override
//            public void preCall(TaskContext taskContext) {
//                System.out.println("preCall");
//            }
//
//            @Override
//            public void afterCall(TaskContext taskContext) {
//                System.out.println("afterCall");
//            }
//
//            @Override
//            public void throwException(TaskContext taskContext, Throwable e) {
//                System.out.println("数据传输前throwException");
//                IncrementInfo incrementInfo = SqliteIncrement.getCurrentIncrement(task.getTaskId());
//
//                ScheduleTask scheduleTask = new ScheduleTask();
//                scheduleTask.setScheduleType(task.getScheduleTypeEnum());
//                scheduleTask.setEsIndex(searchIndex);
//                scheduleTask.setTaskId(task.getTaskId());
//
//                if (incrementInfo == null) {
//                    String message = "取增量失败";
//                    System.out.println(message);
//
//                } else {
//                    scheduleTask.setCurrentIncrementId(incrementInfo.getLastValue());
//                }
//                scheduleTask.setCreateTime(new Timestamp(System.currentTimeMillis()));
//
//                StringWriter sw = new StringWriter();
//                PrintWriter pw = new PrintWriter(sw);
//                e.printStackTrace(pw);
//                String sStackTrace = sw.toString();
//                if (sStackTrace.length() > 1000) {
//                    sStackTrace = sStackTrace.substring(0, 999);
//                }
//                scheduleTask.setMessage(e.getMessage() + System.lineSeparator() + sStackTrace);
//                scheduleTask.setTaskStatus(TaskStatusEnum.EXCEPTION.getCode());
//                scheduleTaskRepository.save(scheduleTask);
//            }
//        });
//
//
//        //设置任务执行拦截器，可以添加多个
//
//        //增量配置开始
//        String incrementId = properties.getProperty("incrementId");
//        importBuilder.setLastValueColumn(incrementId);//指定数字增量查询字段变量名称
//        importBuilder.setFromFirst(false);//setFromFirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，//setFromFirst(true) 如果作业停了，作业重启后，重新开始采集数据
//        String storePath = task.getTaskId();
//        importBuilder.setLastValueStorePath(storePath);//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//        //importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increment_tab
//        String incrementType = properties.getProperty("incrementType");
//        if (incrementType.equals("time_stamp")) {
//            importBuilder.setLastValueType(ImportIncreamentConfig.TIMESTAMP_TYPE);
//            String dateTimeFormat = properties.getProperty("dateTimeFormat");
//            importBuilder.setLastValueDateformat(dateTimeFormat);
//        } else {
//            importBuilder.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncrementConfig.NUMBER_TYPE 数字类型
//        }
//
//        //映射和转换配置结束
//        importBuilder.setExportResultHandler(new ExportResultHandler<Object, RecordMetadata>() {
//            @Override
//            public void success(TaskCommand<Object, RecordMetadata> taskCommand, RecordMetadata result) {
//                System.out.println("处理耗时：" + taskCommand.getElapsed() + "毫秒");
//                System.out.println(taskCommand.getTaskMetrics());
//
//                IncrementInfo incrementInfo = SqliteIncrement.getCurrentIncrement(task.getTaskId());
//
//                ScheduleTask scheduleTask = new ScheduleTask();
//                scheduleTask.setScheduleType(task.getScheduleTypeEnum());
//                scheduleTask.setEsIndex(searchIndex);
//                scheduleTask.setTaskId(task.getTaskId());
//                scheduleTask.setSize(taskCommand.getDataSize());
//                scheduleTask.setElapsedTime(taskCommand.getElapsed());
//                scheduleTask.setMetrics(taskCommand.getTaskMetrics().toString());
//                if (incrementInfo == null) {
//                    String message = "数据已发送，但获取增量失败";
//                    System.out.println(message);
//                    scheduleTask.setMessage(message);
//                } else {
//                    scheduleTask.setCurrentIncrementId(incrementInfo.getLastValue());
//                }
//
//                scheduleTask.setCreateTime(new Timestamp(System.currentTimeMillis()));
//                scheduleTask.setTaskStatus(TaskStatusEnum.RUNNING.getCode());
//                scheduleTaskRepository.save(scheduleTask);
//            }
//
//            @Override
//            public void error(TaskCommand<Object, RecordMetadata> taskCommand, RecordMetadata result) {
//                System.out.println(taskCommand.getTaskMetrics());
//                IncrementInfo incrementInfo = SqliteIncrement.getCurrentIncrement(task.getTaskId());
//
//                ScheduleTask scheduleTask = new ScheduleTask();
//                scheduleTask.setScheduleType(task.getScheduleTypeEnum());
//                scheduleTask.setEsIndex(searchIndex);
//                scheduleTask.setTaskId(task.getTaskId());
//
//                scheduleTask.setMetrics(taskCommand.getTaskMetrics().toString());
//                if (incrementInfo == null) {
//                    String message = "出现错误，获取增量失败";
//                    System.out.println(message);
//
//                } else {
//                    scheduleTask.setCurrentIncrementId(incrementInfo.getLastValue());
//                }
//                scheduleTask.setTaskStatus(TaskStatusEnum.EXCEPTION.getCode());
//                scheduleTask.setCreateTime(new Timestamp(System.currentTimeMillis()));
//                scheduleTask.setMessage("任务运行出现错误");
//                scheduleTaskRepository.save(scheduleTask);
//            }
//
//            @Override
//            public void exception(TaskCommand<Object, RecordMetadata> taskCommand, Throwable exception) {
//                System.out.println(taskCommand.getTaskMetrics());
//                IncrementInfo incrementInfo = SqliteIncrement.getCurrentIncrement(task.getTaskId());
//
//                ScheduleTask scheduleTask = new ScheduleTask();
//                scheduleTask.setScheduleType(task.getScheduleTypeEnum());
//                scheduleTask.setEsIndex(searchIndex);
//                scheduleTask.setTaskId(task.getTaskId());
//                scheduleTask.setSize(taskCommand.getDataSize());
//                scheduleTask.setElapsedTime(taskCommand.getElapsed());
//                scheduleTask.setMetrics(taskCommand.getTaskMetrics().toString());
//                if (incrementInfo == null) {
//                    String message = "取增量失败";
//                    System.out.println(message);
//
//                } else {
//                    scheduleTask.setCurrentIncrementId(incrementInfo.getLastValue());
//
//                }
//                scheduleTask.setCreateTime(new Timestamp(System.currentTimeMillis()));
//
//                StringWriter sw = new StringWriter();
//                PrintWriter pw = new PrintWriter(sw);
//                exception.printStackTrace(pw);
//                String sStackTrace = sw.toString();
//                if (sStackTrace.length() > 1000) {
//                    sStackTrace = sStackTrace.substring(0, 999);
//                }
//                scheduleTask.setMessage(exception.getMessage() + System.lineSeparator() + sStackTrace);
//                scheduleTask.setTaskStatus(TaskStatusEnum.EXCEPTION.getCode());
//                scheduleTaskRepository.save(scheduleTask);
//            }
//
//            @Override
//            public int getMaxRetry() {
//                return 0;
//            }
//        });
//
//        /*
//          内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
//         */
//        importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
//        importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
//        importBuilder.setThreadCount(5);//设置批量导入线程池工作线程数量
//        importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
//        importBuilder.setAsyn(false);//true 异步方式执行，不等待所有导入作业任务结束，方法快速返回；false（默认值） 同步方式执行，等待所有导入作业任务结束，所有作业结束后方法才返回
//        importBuilder.setPrintTaskLog(false);
//
//        /*
//          启动es数据导入redis作业
//         */
//        DataStream dataStream = importBuilder.builder();
//        dataStream.execute();//启动同步作业
//
//        currentDataStream = dataStream;
//        task.setCurrentDataStream(currentDataStream);
//    }
//}
