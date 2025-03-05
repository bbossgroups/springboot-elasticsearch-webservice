package com.example.esbboss.service;
/**
 * Copyright 2008 biaoping.yin
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

import com.example.esbboss.agent.AgentInfoBo;
import com.example.esbboss.agent.Buffer;
import com.example.esbboss.agent.FixedBuffer;
import com.example.esbboss.entity.LoginModuleMetric;
import com.example.esbboss.entity.LoginUserMetric;
import com.frameworkset.common.poolman.util.DBConf;
import com.frameworkset.common.poolman.util.DBStartResult;
import com.frameworkset.common.poolman.util.SQLManager;
import com.frameworkset.util.SimpleStringUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.boot.BBossESStarter;
import org.frameworkset.elasticsearch.bulk.*;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.kafka.KafkaMapRecord;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.entity.MetricKey;
import org.frameworkset.tran.metrics.job.KeyMetricBuilder;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.metrics.job.MetricsConfig;
import org.frameworkset.tran.metrics.job.builder.MetricBuilder;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.plugin.hbase.input.HBaseInputConfig;
import org.frameworkset.tran.plugin.kafka.input.Kafka2InputConfig;
import org.frameworkset.tran.plugin.kafka.output.Kafka2OutputConfig;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.util.RecordGenerator;
import org.frameworkset.util.ResourceEnd;
import org.frameworkset.util.ResourceStart;
import org.frameworkset.util.ResourceStartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.frameworkset.tran.plugin.kafka.input.KafkaInputConfig.CODEC_JSON;
import static org.frameworkset.tran.plugin.kafka.input.KafkaInputConfig.CODEC_LONG;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2020/1/5 12:03
 * @author biaoping.yin
 * @version 1.0
 */
@Service
public class DataTran {
	private Logger logger = LoggerFactory.getLogger(DataTran.class);
	@Autowired
	private BBossESStarter bbossESStarter;
	private ImportBuilder db2ESImportBuilder;
	private DataStream dataStream;
	private ImportBuilder db2kafkaImportBuilder;
	private DataStream dataKafkaStream;
    private DataStream db2EleasticsearchMetricsStream;


	private ImportBuilder kafka2esImportBuilder;
	private DataStream kafka2esStream;

	public  String scheduleKafka2esJob(){
		if (kafka2esImportBuilder == null) {
			synchronized (this) {
				if (kafka2esImportBuilder == null) {
					ImportBuilder importBuilder = ImportBuilder.newInstance();
					//kafka相关配置参数
					/**
					 *
					 <property name="value.deserializer" value="org.apache.kafka.common.serialization.StringDeserializer">
					 <description> <![CDATA[ Deserializer class for value that implements the <code>org.apache.kafka.common.serialization.Deserializer</code> interface.]]></description>
					 </property>
					 <property name="key.deserializer" value="org.apache.kafka.common.serialization.LongDeserializer">
					 <description> <![CDATA[ Deserializer class for key that implements the <code>org.apache.kafka.common.serialization.Deserializer</code> interface.]]></description>
					 </property>
					 <property name="group.id" value="test">
					 <description> <![CDATA[ A unique string that identifies the consumer group this consumer belongs to. This property is required if the consumer uses either the group management functionality by using <code>subscribe(topic)</code> or the Kafka-based offset management strategy.]]></description>
					 </property>
					 <property name="session.timeout.ms" value="30000">
					 <description> <![CDATA[ The timeout used to detect client failures when using "
					 + "Kafka's group management facility. The client sends periodic heartbeats to indicate its liveness "
					 + "to the broker. If no heartbeats are received by the broker before the expiration of this session timeout, "
					 + "then the broker will remove this client from the group and initiate a rebalance. Note that the value "
					 + "must be in the allowable range as configured in the broker configuration by <code>group.min.session.timeout.ms</code> "
					 + "and <code>group.max.session.timeout.ms</code>.]]></description>
					 </property>
					 <property name="auto.commit.interval.ms" value="1000">
					 <description> <![CDATA[ The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if <code>enable.auto.commit</code> is set to <code>true</code>.]]></description>
					 </property>



					 <property name="auto.offset.reset" value="latest">
					 <description> <![CDATA[ What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted): <ul><li>earliest: automatically reset the offset to the earliest offset<li>latest: automatically reset the offset to the latest offset</li><li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li><li>anything else: throw exception to the consumer.</li></ul>]]></description>
					 </property>
					 <property name="bootstrap.servers" value="192.168.137.133:9093">
					 <description> <![CDATA[ A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping&mdash;this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form "
					 + "<code>host1:port1,host2:port2,...</code>. Since these servers are just used for the initial connection to "
					 + "discover the full cluster membership (which may change dynamically), this list need not contain the full set of "
					 + "servers (you may want more than one, though, in case a server is down).]]></description>
					 </property>
					 <property name="enable.auto.commit" value="true">
					 <description> <![CDATA[If true the consumer's offset will be periodically committed in the background.]]></description>
					 </property>
					 */

					// kafka服务器参数配置
					// kafka 2x 客户端参数项及说明类：org.apache.kafka.clients.consumer.ConsumerConfig
					Kafka2InputConfig kafka2InputConfig = new Kafka2InputConfig();

					kafka2InputConfig//.addKafkaConfig("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
							//.addKafkaConfig("key.deserializer","org.apache.kafka.common.serialization.LongDeserializer")
							.addKafkaConfig("group.id","trantest") // 消费组ID
							.addKafkaConfig("session.timeout.ms","30000")
							.addKafkaConfig("auto.commit.interval.ms","5000")
							.addKafkaConfig("auto.offset.reset","latest")
//				.addKafkaConfig("bootstrap.servers","192.168.137.133:9093")
//				.addKafkaConfig("bootstrap.servers","127.0.0.1:9092")
							.addKafkaConfig("bootstrap.servers","192.168.137.133:9092")
							.addKafkaConfig("enable.auto.commit","true")
							.addKafkaConfig("max.poll.records","500") // The maximum number of records returned in a single call to poll().
//				.setKafkaTopic("xinkonglog") // kafka topic
							.setKafkaTopic("db2kafka") // kafka topic
							.setConsumerThreads(5) // 并行消费线程数，建议与topic partitions数一致
							.setKafkaWorkQueue(10)
							.setKafkaWorkThreads(2)

							.setPollTimeOut(1000) // 从kafka consumer poll(timeout)参数
							.setValueCodec(CODEC_JSON)
							.setKeyCodec(CODEC_LONG)
					;

					importBuilder.setInputConfig(kafka2InputConfig);

//		importBuilder.addIgnoreFieldMapping("remark1");
//		importBuilder.setSql("select * from td_sm_log ");
					/**
					 * es相关配置
					 */
					ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();
					elasticsearchOutputConfig
							.setIndex("db2kafkademo") //必填项，索引名称
							.setIndexType("db2kafkademo") //es 7以后的版本不需要设置indexType，es7以前的版本必需设置indexType
							.setRefreshOption("refresh");//可选项，null表示不实时刷新，importBuilder.setRefreshOption("refresh");表示实时刷新
					importBuilder.setOutputConfig(elasticsearchOutputConfig)
							.setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
							.setBatchSize(100) ; //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理
//				.setFetchSize(100); //按批从kafka拉取数据的大小，设置了max.poll.records就不要设施FetchSize
					//设置强制刷新检测空闲时间间隔，单位：毫秒，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作，默认8秒,为0时关闭本机制
					importBuilder.setFlushInterval(10000l);

//		//设置任务执行拦截器，可以添加多个，定时任务每次执行的拦截器
//		importBuilder.addCallInterceptor(new CallInterceptor() {
//			@Override
//			public void preCall(TaskContext taskContext) {
//				System.out.println("preCall");
//			}
//
//			@Override
//			public void afterCall(TaskContext taskContext) {
//				System.out.println("afterCall");
//			}
//
//			@Override
//			public void throwException(TaskContext taskContext, Throwable e) {
//				System.out.println("throwException");
//			}
//		}).addCallInterceptor(new CallInterceptor() {
//			@Override
//			public void preCall(TaskContext taskContext) {
//				System.out.println("preCall 1");
//			}
//
//			@Override
//			public void afterCall(TaskContext taskContext) {
//				System.out.println("afterCall 1");
//			}
//
//			@Override
//			public void throwException(TaskContext taskContext, Throwable e) {
//				System.out.println("throwException 1");
//			}
//		});
//		//设置任务执行拦截器结束，可以添加多个

					//映射和转换配置开始
//		/**
//		 * db-es mapping 表字段名称到es 文档字段的映射：比如document_id -> docId
//		 *
//		 */
//		importBuilder.addFieldMapping("document_id","docId")
//				.addFieldMapping("docwtime","docwTime")
//				.addIgnoreFieldMapping("channel_id");//添加忽略字段
//
//
//		/**
//		 * 为每条记录添加额外的字段和值
//		 * 可以为基本数据类型，也可以是复杂的对象
//		 */
//		importBuilder.addFieldValue("testF1","f1value");
//		importBuilder.addFieldValue("testInt",0);
//		importBuilder.addFieldValue("testDate",new Date());
//		importBuilder.addFieldValue("testFormateDate","yyyy-MM-dd HH",new Date());
//		TestObject testObject = new TestObject();
//		testObject.setId("testid");
//		testObject.setName("jackson");
//		importBuilder.addFieldValue("testObject",testObject);
//
//		/**
//		 * 重新设置es数据结构
//		 */
					importBuilder.setDataRefactor(new DataRefactor() {
						public void refactor(Context context) throws Exception  {
							//添加字段extfiled到记录中，值为1
							context.addFieldValue("extfiled",1);
							// 将long类型字段值转换为Date类型
							long birthDay = context.getLongValue("birthDay");
							context.addFieldValue("birthDay",new Date(birthDay));
							// 获取原始的Kafka记录
							KafkaMapRecord record = (KafkaMapRecord) context.getCurrentRecord().getRecord();
							if(record.getKey() == null)
								System.out.println("key is null!");
							//上述三个属性已经放置到docInfo中，如果无需再放置到索引文档中，可以忽略掉这些属性
//				context.addIgnoreFieldMapping("author");
//				context.addIgnoreFieldMapping("title");
//				context.addIgnoreFieldMapping("subtitle");
						}
					});
					//映射和转换配置结束

					/**
					 * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
					 */
					importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
					importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
					importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
					importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
					importBuilder.setExportResultHandler(new ExportResultHandler<String>() {
						@Override
						public void success(TaskCommand<String> taskCommand, String result) {
							TaskMetrics taskMetric = taskCommand.getTaskMetrics();
							System.out.println("处理耗时："+taskCommand.getElapsed() +"毫秒");
							System.out.println(taskCommand.getTaskMetrics());
						}

						@Override
						public void error(TaskCommand<String> taskCommand, String result) {
							System.out.println(taskCommand.getTaskMetrics());
						}

						@Override
						public void exception(TaskCommand<String> taskCommand, Throwable exception) {
							System.out.println(taskCommand.getTaskMetrics());
						}


					});
					/**
					 importBuilder.setEsIdGenerator(new EsIdGenerator() {
					 //如果指定EsIdGenerator，则根据下面的方法生成文档id，
					 // 否则根据setEsIdField方法设置的字段值作为文档id，
					 // 如果默认没有配置EsIdField和如果指定EsIdGenerator，则由es自动生成文档id

					 @Override
					 public Object genId(Context context) throws Exception {
					 return SimpleStringUtil.getUUID();//返回null，则由es自动生成文档id
					 }
					 });
					 */
					/**
					 * 构建和启动导出关系数据库数据并发送kafka同步作业
					 */
					DataStream dataStream = importBuilder.builder();
					dataStream.execute();//执行导入操作
					kafka2esImportBuilder = importBuilder;
					this.kafka2esStream = dataStream;
					return "kafka2esImportBuilder job started.";
				}
				else{
					return "kafka2esImportBuilder job has started.";
				}
			}
		}
		else{
			return "kafka2esImportBuilder job has started.";
		}

	}
	public String stopKafka2esJob(){
		if(kafka2esStream != null) {
			synchronized (this) {
				if (kafka2esStream != null) {
					kafka2esStream.destroy(true);
					kafka2esStream = null;
					kafka2esImportBuilder = null;
					return "kafka2esImportBuilder job stopped.";
				} else {
					return "kafka2esImportBuilder job has stopped.";
				}
			}
		}
		else {
			return "kafka2esImportBuilder job has stopped.";
		}
	}
    public String stopDb2EleasticsearchMetricsDemo(){
        if(db2EleasticsearchMetricsStream == null){
            return "db2EleasticsearchMetricsStream 未启动";

        }
        db2EleasticsearchMetricsStream.destroy();
        db2EleasticsearchMetricsStream = null;
        return "db2EleasticsearchMetricsStream 停止完毕";
    }
    public  String scheduleDb2EleasticsearchMetricsDemo(){
        if(db2EleasticsearchMetricsStream != null){
            return "db2EleasticsearchMetricsStream 已经启动";
        }
        ImportBuilder importBuilder = new ImportBuilder() ;
        //在任务数据抽取之前做一些初始化处理，例如：通过删表来做初始化操作


        importBuilder.setImportStartAction(new ImportStartAction() {
            /**
             * 初始化之前执行的处理操作，比如后续初始化操作、数据处理过程中依赖的资源初始化
             * @param importContext
             */
            @Override
            public void startAction(ImportContext importContext) {


                importContext.addResourceStart(new ResourceStart() {
                    @Override
                    public ResourceStartResult startResource() {
                        DBConf tempConf = new DBConf();
                        tempConf.setPoolname("testStatus");
                        tempConf.setDriver("com.mysql.cj.jdbc.Driver");
                        tempConf.setJdbcurl("jdbc:mysql://192.168.137.1:3306/bboss?useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true");

                        tempConf.setUsername("root");
                        tempConf.setPassword("123456");
                        tempConf.setValidationQuery("select 1");

                        tempConf.setInitialConnections(5);
                        tempConf.setMinimumSize(10);
                        tempConf.setMaximumSize(10);
                        tempConf.setUsepool(true);
                        tempConf.setShowsql(true);
                        tempConf.setJndiName("testStatus-jndi");
                        //# 控制map中的列名采用小写，默认为大写
                        tempConf.setColumnLableUpperCase(false);
                        //启动数据源
                        boolean result = SQLManager.startPool(tempConf);
                        ResourceStartResult resourceStartResult = null;
                        //记录启动的数据源信息，用户作业停止时释放数据源
                        if(result){
                            resourceStartResult = new DBStartResult();
                            resourceStartResult.addResourceStartResult("testStatus");
                        }
                        return resourceStartResult;
                    }
                });

            }

            /**
             * 所有初始化操作完成后，导出数据之前执行的操作
             * @param importContext
             */
            @Override
            public void afterStartAction(ImportContext importContext) {

                try {
                    //清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
                    ElasticSearchHelper.getRestClientUtil().dropIndice("dbdemo");
                } catch (Exception e) {
                    logger.error("Drop indice dbdemo failed:",e);
                }
                try {
                    //清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
                    ElasticSearchHelper.getRestClientUtil().dropIndice("vops-loginmodulemetrics");
                } catch (Exception e) {
                    logger.error("Drop indice  vops-loginmodulemetrics failed:",e);
                }
                try {
                    //清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
                    ElasticSearchHelper.getRestClientUtil().dropIndice("vops-loginusermetrics");
                } catch (Exception e) {
                    logger.error("Drop indice  vops-loginusermetrics failed:",e);
                }

            }
        });


        DBInputConfig dbInputConfig = new DBInputConfig();
        //指定导入数据的sql语句，必填项，可以设置自己的提取逻辑，
        // 设置增量变量log_id，增量变量名称#[log_id]可以多次出现在sql语句的不同位置中，例如：
        // select * from td_sm_log where log_id > #[log_id] and parent_id = #[log_id]
        // 需要设置setLastValueColumn信息log_id，
        // 通过setLastValueType方法告诉工具增量字段的类型，默认是数字类型

//		importBuilder.setSql("select * from td_sm_log where LOG_OPERTIME > #[LOG_OPERTIME]");
        dbInputConfig.setSql("select * from td_sm_log where log_id > #[log_id]")
                .setDbName("test")
                .setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
                .setDbUrl("jdbc:mysql://192.168.137.1:3306/bboss?useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true") //通过useCursorFetch=true启用mysql的游标fetch机制，否则会有严重的性能隐患，useCursorFetch必须和jdbcFetchSize参数配合使用，否则不会生效
                .setDbUser("root")
                .setDbPassword("123456")
                .setValidateSQL("select 1")
                .setUsePool(true)
                .setDbInitSize(5)
                .setDbMinIdleSize(5)
                .setDbMaxSize(10)
                .setShowSql(true);//是否使用连接池;
        importBuilder.setInputConfig(dbInputConfig);




        BulkProcessorBuilder bulkProcessorBuilder = new BulkProcessorBuilder();
        bulkProcessorBuilder.setBlockedWaitTimeout(-1)//指定bulk工作线程缓冲队列已满时后续添加的bulk处理排队等待时间，如果超过指定的时候bulk将被拒绝处理，单位：毫秒，默认为0，不拒绝并一直等待成功为止

                .setBulkSizes(200)//按批处理数据记录数
                .setFlushInterval(5000)//强制bulk操作时间，单位毫秒，如果自上次bulk操作flushInterval毫秒后，数据量没有满足BulkSizes对应的记录数，但是有记录，那么强制进行bulk处理

                .setWarnMultsRejects(1000)//由于没有空闲批量处理工作线程，导致bulk处理操作出于阻塞等待排队中，BulkProcessor会对阻塞等待排队次数进行计数统计，bulk处理操作被每被阻塞排队WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息
                .setWorkThreads(10)//bulk处理工作线程数
                .setWorkThreadQueue(50)//bulk处理工作线程池缓冲队列大小
                .setBulkProcessorName("detail_bulkprocessor")//工作线程名称，实际名称为BulkProcessorName-+线程编号
                .setBulkRejectMessage("detail bulkprocessor")//bulk处理操作被每被拒绝WarnMultsRejects次（1000次），在日志文件中输出拒绝告警信息提示前缀
                .setElasticsearch("default")//指定明细Elasticsearch集群数据源名称，bboss可以支持多数据源
                .setFilterPath(BulkConfig.ERROR_FILTER_PATH)
                .addBulkInterceptor(new BulkInterceptor() {
                    public void beforeBulk(BulkCommand bulkCommand) {

                    }

                    public void afterBulk(BulkCommand bulkCommand, String result) {
                        if(logger.isDebugEnabled()){
                            logger.debug(result);
                        }
                    }

                    public void exceptionBulk(BulkCommand bulkCommand, Throwable exception) {
                        if(logger.isErrorEnabled()){
                            logger.error("exceptionBulk",exception);
                        }
                    }
                    public void errorBulk(BulkCommand bulkCommand, String result) {
                        if(logger.isWarnEnabled()){
                            logger.warn(result);
                        }
                    }
                })//添加批量处理执行拦截器，可以通过addBulkInterceptor方法添加多个拦截器
        ;
        /**
         * 构建BulkProcessor批处理组件，一般作为单实例使用，单实例多线程安全，可放心使用
         */
        BulkProcessor bulkProcessor = bulkProcessorBuilder.build();//构建批处理作业组件
        ETLMetrics keyMetrics = new ETLMetrics(Metrics.MetricsType_KeyTimeMetircs){
            @Override
            public void builderMetrics(){
                //指标1 按操作模块统计模块登录次数
                addMetricBuilder(new MetricBuilder() {
                    @Override
                    public MetricKey buildMetricKey(MapData mapData){
                        CommonRecord data = (CommonRecord) mapData.getData();
                        String operModule = (String) data.getData("operModule");
                        if(operModule == null || operModule.equals("")){
                            operModule = "未知模块";
                        }
                        return new MetricKey(operModule);
                    }
                    @Override
                    public KeyMetricBuilder metricBuilder(){
                        return new KeyMetricBuilder() {
                            @Override
                            public KeyMetric build() {
                                return new LoginModuleMetric();
                            }
                        };
                    }
                });

                //指标2 按照用户统计登录次数
                addMetricBuilder(new MetricBuilder() {
                    @Override
                    public MetricKey buildMetricKey(MapData mapData){
                        CommonRecord data = (CommonRecord) mapData.getData();
                        String logUser = (String) data.getData("logOperuser");//
                        if(logUser == null || logUser.equals("")){
                            logUser = "未知用户";
                        }
                        return new MetricKey(logUser);
                    }
                    @Override
                    public KeyMetricBuilder metricBuilder(){
                        return new KeyMetricBuilder() {
                            @Override
                            public KeyMetric build() {
                                return new LoginUserMetric();
                            }
                        };
                    }
                });
                // key metrics中包含两个segment(S0,S1)
                setSegmentBoundSize(5000000);
                setTimeWindows(60 );//统计时间窗口
                this.setTimeWindowType(MetricsConfig.TIME_WINDOW_TYPE_MINUTE);
            }

            /**
             * 存储指标计算结果
             * @param metrics
             */
            @Override
            public void persistent(Collection< KeyMetric> metrics) {
                metrics.forEach(keyMetric->{
                    if(keyMetric instanceof LoginModuleMetric) {
                        LoginModuleMetric testKeyMetric = (LoginModuleMetric) keyMetric;
                        Map esData = new HashMap();
                        esData.put("dataTime", testKeyMetric.getDataTime());
                        esData.put("metricTime", new Date());
                        esData.put("hour", testKeyMetric.getDayHour());
                        esData.put("minute", testKeyMetric.getMinute());
                        esData.put("day", testKeyMetric.getDay());
                        esData.put("metric", testKeyMetric.getMetric());
                        esData.put("operModule", testKeyMetric.getOperModule());
                        esData.put("count", testKeyMetric.getCount());
                        bulkProcessor.insertData("vops-loginmodulemetrics", esData);
                    }
                    else if(keyMetric instanceof LoginUserMetric) {
                        LoginUserMetric testKeyMetric = (LoginUserMetric) keyMetric;
                        Map esData = new HashMap();
                        esData.put("dataTime", testKeyMetric.getDataTime());
                        esData.put("metricTime", new Date());
                        esData.put("hour", testKeyMetric.getDayHour());
                        esData.put("minute", testKeyMetric.getMinute());
                        esData.put("day", testKeyMetric.getDay());
                        esData.put("metric", testKeyMetric.getMetric());
                        esData.put("logUser", testKeyMetric.getLogUser());
                        esData.put("count", testKeyMetric.getCount());
                        bulkProcessor.insertData("vops-loginusermetrics", esData);
                    }

                });

            }
        };
        //作业结束后销毁初始化阶段自定义的http数据源
        importBuilder.setImportEndAction(new ImportEndAction() {
            @Override
            public void endAction(ImportContext importContext, Exception e) {
                //销毁初始化阶段自定义的数据源
                importContext.destroyResources(new ResourceEnd() {
                    @Override
                    public void endResource(ResourceStartResult resourceStartResult) {
                        if(resourceStartResult instanceof DBStartResult) { //作业停止时，释放db数据源
                            DataTranPluginImpl.stopDatasources((DBStartResult) resourceStartResult);
                        }
                    }
                });
                bulkProcessor.shutDown();

            }
        });

        importBuilder.setDataTimeField("logOpertime");
        importBuilder.addMetrics(keyMetrics);

//		importBuilder.addFieldMapping("LOG_CONTENT","message");
//		importBuilder.addIgnoreFieldMapping("remark1");
//		importBuilder.setSql("select * from td_sm_log ");
        ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();
        elasticsearchOutputConfig
                .setTargetElasticsearch("default")
                .setIndex("dbdemo")
                .setEsIdField("log_id")//设置文档主键，不设置，则自动产生文档id
                .setDebugResponse(false)//设置是否将每次处理的reponse打印到日志文件中，默认false
                .setDiscardBulkResponse(false);//设置是否需要批量处理的响应报文，不需要设置为false，true为需要，默认false
        /**
         elasticsearchOutputConfig.setEsIdGenerator(new EsIdGenerator() {
         //如果指定EsIdGenerator，则根据下面的方法生成文档id，
         // 否则根据setEsIdField方法设置的字段值作为文档id，
         // 如果默认没有配置EsIdField和如果指定EsIdGenerator，则由es自动生成文档id

         @Override
         public Object genId(Context context) throws Exception {
         return SimpleStringUtil.getUUID();//返回null，则由es自动生成文档id
         }
         });
         */
//				.setIndexType("dbdemo") ;//es 7以后的版本不需要设置indexType，es7以前的版本必需设置indexType;
//				.setRefreshOption("refresh")//可选项，null表示不实时刷新，importBuilder.setRefreshOption("refresh");表示实时刷新
        /**
         * es相关配置
         */
//		elasticsearchOutputConfig.setTargetElasticsearch("default,test");//同步数据到两个es集群

        importBuilder.setOutputConfig(elasticsearchOutputConfig);

        /**
         * 设置IP地址信息库
         */
        importBuilder.setGeoipDatabase("d:/geolite2/GeoLite2-City.mmdb");
        importBuilder.setGeoipAsnDatabase("d:/geolite2/GeoLite2-ASN.mmdb");
        importBuilder.setGeoip2regionDatabase("d:/geolite2/ip2region.db");

        importBuilder
                .setUseLowcase(false)
                .setUseJavaName(true) //可选项,将数据库字段名称转换为java驼峰规范的名称，true转换，false不转换，默认false，例如:doc_id -> docId
                .setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
                .setBatchSize(10);  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理

        //定时任务配置，
        importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
                .setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
                .setPeriod(5000L); //每隔period毫秒执行，如果不设置，只执行一次
        //定时任务配置结束
//
//		//设置任务执行拦截器，可以添加多个，定时任务每次执行的拦截器
        importBuilder.addCallInterceptor(new CallInterceptor() {
            @Override
            public void preCall(TaskContext taskContext) {
                System.out.println("preCall");
            }

            @Override
            public void afterCall(TaskContext taskContext) {
                System.out.println("afterCall");
            }

            @Override
            public void throwException(TaskContext taskContext, Throwable e) {
                System.out.println("throwException");
            }
        });
//		//设置任务执行拦截器结束，可以添加多个
        //增量配置开始
//		importBuilder.setStatusDbname("test");//设置增量状态数据源名称
        importBuilder.setLastValueColumn("log_id");//手动指定数字增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
        importBuilder.setFromFirst(true);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
//		setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
        importBuilder.setStatusDbname("testStatus");//指定增量状态数据源名称
//		importBuilder.setLastValueStorePath("logtable_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
        importBuilder.setLastValueStoreTableName("logstable");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
        importBuilder.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
//		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
//		try {
//			Date date = format.parse("2000-01-01");
//			importBuilder.setLastValue(date);//增量起始值配置
//		}
//		catch (Exception e){
//			e.printStackTrace();
//		}
        // 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
        //增量配置结束

        //映射和转换配置开始
//		/**
//		 * db-es mapping 表字段名称到es 文档字段的映射：比如document_id -> docId
//		 * 可以配置mapping，也可以不配置，默认基于java 驼峰规则进行db field-es field的映射和转换
//		 */
//		importBuilder.addFieldMapping("document_id","docId")
//				.addFieldMapping("docwtime","docwTime")
//				.addIgnoreFieldMapping("channel_id");//添加忽略字段
//
//
//		/**
//		 * 为每条记录添加额外的字段和值
//		 * 可以为基本数据类型，也可以是复杂的对象
//		 */
//		importBuilder.addFieldValue("testF1","f1value");
//		importBuilder.addFieldValue("testInt",0);
//		importBuilder.addFieldValue("testDate",new Date());
//		importBuilder.addFieldValue("testFormateDate","yyyy-MM-dd HH",new Date());
//		TestObject testObject = new TestObject();
//		testObject.setId("testid");
//		testObject.setName("jackson");
//		importBuilder.addFieldValue("testObject",testObject);
//
        /**
         * 重新设置es数据结构
         */
        importBuilder.setDataRefactor(new DataRefactor() {
            public void refactor(Context context) throws Exception  {
//				Date date = context.getDateValue("LOG_OPERTIME");
                context.addFieldValue("collecttime",new Date());
                IpInfo ipInfo = context.getIpInfoByIp("219.133.80.136");
                if(ipInfo != null)
                    context.addFieldValue("ipInfo", SimpleStringUtil.object2json(ipInfo));
            }
        });
        //映射和转换配置结束

        /**
         * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
         */
        importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
        importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
        importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
        importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行

        importBuilder.setExportResultHandler(new ExportResultHandler<String>() {
            @Override
            public void success(TaskCommand<String> taskCommand, String result) {
                TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
                logger.info(taskMetrics.toString());
                logger.debug(result);
            }

            @Override
            public void error(TaskCommand<String> taskCommand, String result) {
                TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
                logger.info(taskMetrics.toString());
                logger.debug(result);
            }

            @Override
            public void exception(TaskCommand<String> taskCommand, Throwable exception) {
                TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
                logger.debug(taskMetrics.toString());
            }


        });


        /**
         * 构建和执行数据库表数据导入es和指标统计作业
         */
        DataStream dataStream = importBuilder.builder();
        dataStream.execute();
        db2EleasticsearchMetricsStream = dataStream;
        return "db2EleasticsearchMetricsStream 启动成功";
    }
//	@Autowired
//	private BBossStarter bbossStarter;
	public  String scheduleDB2KafkaJob(){
		if (db2kafkaImportBuilder == null) {
			synchronized (this) {
				if (db2kafkaImportBuilder == null) {
					ImportBuilder importBuilder = ImportBuilder.newInstance();
					//kafka相关配置参数
					/**
					 *
					 <property name="productorPropes">
					 <propes>

					 <property name="value.serializer" value="org.apache.kafka.common.serialization.StringSerializer">
					 <description> <![CDATA[ 指定序列化处理类，默认为kafka.serializer.DefaultEncoder,即byte[] ]]></description>
					 </property>
					 <property name="key.serializer" value="org.apache.kafka.common.serialization.LongSerializer">
					 <description> <![CDATA[ 指定序列化处理类，默认为kafka.serializer.DefaultEncoder,即byte[] ]]></description>
					 </property>

					 <property name="compression.type" value="gzip">
					 <description> <![CDATA[ 是否压缩，默认0表示不压缩，1表示用gzip压缩，2表示用snappy压缩。压缩后消息中会有头来指明消息压缩类型，故在消费者端消息解压是透明的无需指定]]></description>
					 </property>
					 <property name="bootstrap.servers" value="192.168.137.133:9093">
					 <description> <![CDATA[ 指定kafka节点列表，用于获取metadata(元数据)，不必全部指定]]></description>
					 </property>
					 <property name="batch.size" value="10000">
					 <description> <![CDATA[ 批处理消息大小：
					 the producer will attempt to batch records together into fewer requests whenever multiple records are being sent to the same partition. This helps performance on both the client and the server. This configuration controls the default batch size in bytes.
					 No attempt will be made to batch records larger than this size.

					 Requests sent to brokers will contain multiple batches, one for each partition with data available to be sent.

					 A small batch size will make batching less common and may reduce throughput (a batch size of zero will disable batching entirely). A very large batch size may use memory a bit more wastefully as we will always allocate a buffer of the specified batch size in anticipation of additional records.
					 ]]></description>
					 </property>

					 <property name="linger.ms" value="10000">
					 <description> <![CDATA[
					 <p>
					 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by
					 * the <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we will
					 * generally have one of these buffers for each active partition).
					 * <p>
					 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However if you
					 * want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This will
					 * instruct the producer to wait up to that number of milliseconds before sending a request in hope that more records will
					 * arrive to fill up the same batch. This is analogous to Nagle's algorithm in TCP. For example, in the code snippet above,
					 * likely all 100 records would be sent in a single request since we set our linger time to 1 millisecond. However this setting
					 * would add 1 millisecond of latency to our request waiting for more records to arrive if we didn't fill up the buffer. Note that
					 * records that arrive close together in time will generally batch together even with <code>linger.ms=0</code> so under heavy load
					 * batching will occur regardless of the linger configuration; however setting this to something larger than 0 can lead to fewer, more
					 * efficient requests when not under maximal load at the cost of a small amount of latency.
					 * <p>
					 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
					 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
					 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it throws
					 * a TimeoutException.
					 * <p>]]></description>
					 </property>
					 <property name="buffer.memory" value="10000">
					 <description> <![CDATA[ 批处理消息大小：
					 The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
					 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
					 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it throws
					 * a TimeoutException.]]></description>
					 </property>

					 </propes>
					 </property>
					 */

					// kafka服务器参数配置
					// kafka 2x 客户端参数项及说明类：org.apache.kafka.clients.consumer.ConsumerConfig
					Kafka2OutputConfig kafkaOutputConfig = new Kafka2OutputConfig();
					kafkaOutputConfig.setTopic("db2kafka");
					kafkaOutputConfig.addKafkaProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
					kafkaOutputConfig.addKafkaProperty("key.serializer","org.apache.kafka.common.serialization.LongSerializer");
					kafkaOutputConfig.addKafkaProperty("compression.type","gzip");
					kafkaOutputConfig.addKafkaProperty("bootstrap.servers","192.168.137.133:9092");
					kafkaOutputConfig.addKafkaProperty("batch.size","10");
	//		kafkaOutputConfig.addKafkaProperty("linger.ms","10000");
	//		kafkaOutputConfig.addKafkaProperty("buffer.memory","10000");
					kafkaOutputConfig.setKafkaAsynSend(true);
	//指定文件中每条记录格式，不指定默认为json格式输出
					kafkaOutputConfig.setRecordGenerator(new RecordGenerator() {
						@Override
						public void buildRecord(TaskContext taskContext, CommonRecord record, Writer builder) {
							//直接将记录按照json格式输出到文本文件中
							SerialUtil.normalObject2json(record.getDatas(),//获取记录中的字段数据
									builder);
							String data = (String)taskContext.getTaskData("data");//从任务上下文中获取本次任务执行前设置时间戳
	//          System.out.println(data);

						}
					});
					importBuilder.setOutputConfig(kafkaOutputConfig);
	//		importBuilder.setIncreamentEndOffset(300);//单位秒，同步从上次同步截止时间当前时间前5分钟的数据，下次继续从上次截止时间开始同步数据,对增量时间戳数据同步起作用
					DBInputConfig dbInputConfig = new DBInputConfig();
					dbInputConfig.setSqlFilepath("sqlFile.xml")
							.setSqlName("demoexport")
							.setDbName("test");
					importBuilder.setInputConfig(dbInputConfig);
					//定时任务配置，
					importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
	//					 .setScheduleDate(date) //指定任务开始执行时间：日期
							.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
							.setPeriod(30000L); //每隔period毫秒执行，如果不设置，只执行一次
					//定时任务配置结束

					//设置任务执行拦截器，可以添加多个
					importBuilder.addCallInterceptor(new CallInterceptor() {
						@Override
						public void preCall(TaskContext taskContext) {

							String formate = "yyyyMMddHHmmss";
							//HN_BOSS_TRADE00001_YYYYMMDDHHMM_000001.txt
							SimpleDateFormat dateFormat = new SimpleDateFormat(formate);
							String time = dateFormat.format(new Date());
							//可以在preCall方法中设置任务级别全局变量，然后在其他任务级别和记录级别接口中通过taskContext.getTaskData("time");方法获取time参数
							taskContext.addTaskData("time",time);

						}

						@Override
						public void afterCall(TaskContext taskContext) {
							System.out.println("afterCall 1");
						}

						@Override
						public void throwException(TaskContext taskContext, Throwable e) {
							System.out.println("throwException 1");
						}
					});
	//		//设置任务执行拦截器结束，可以添加多个
					//增量配置开始
					importBuilder.setLastValueColumn("log_id");//手动指定日期增量查询字段变量名称
					importBuilder.setFromFirst(true);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
					//setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
					importBuilder.setLastValueStorePath("db2kafka");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
	//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
					importBuilder.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
					// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
					//指定增量同步的起始时间
	//		importBuilder.setLastValue(new Date());
					//增量配置结束

					//映射和转换配置开始
	//		/**
	//		 * db-es mapping 表字段名称到es 文档字段的映射：比如document_id -> docId
	//		 * 可以配置mapping，也可以不配置，默认基于java 驼峰规则进行db field-es field的映射和转换
	//		 */
	//		importBuilder.addFieldMapping("document_id","docId")
	//				.addFieldMapping("docwtime","docwTime")
	//				.addIgnoreFieldMapping("channel_id");//添加忽略字段
	//
	//
	//		/**
	//		 * 为每条记录添加额外的字段和值
	//		 * 可以为基本数据类型，也可以是复杂的对象
	//		 */
	//		importBuilder.addFieldValue("testF1","f1value");
	//		importBuilder.addFieldValue("testInt",0);
	//		importBuilder.addFieldValue("testDate",new Date());
	//		importBuilder.addFieldValue("testFormateDate","yyyy-MM-dd HH",new Date());
	//		TestObject testObject = new TestObject();
	//		testObject.setId("testid");
	//		testObject.setName("jackson");
	//		importBuilder.addFieldValue("testObject",testObject);
					importBuilder.addFieldValue("author","张无忌");
	//		importBuilder.addFieldMapping("operModule","OPER_MODULE");
	//		importBuilder.addFieldMapping("logContent","LOG_CONTENT");
	//		importBuilder.addFieldMapping("logOperuser","LOG_OPERUSER");
					//设置ip地址信息库地址
					importBuilder.setGeoipDatabase("d:/geolite2/GeoLite2-City.mmdb");
					importBuilder.setGeoipAsnDatabase("d:/geolite2/GeoLite2-ASN.mmdb");
					importBuilder.setGeoip2regionDatabase("d:/geolite2/ip2region.db");
					/**
					 * 重新设置es数据结构
					 */
					importBuilder.setDataRefactor(new DataRefactor() {
						public void refactor(Context context) throws Exception  {
							//可以根据条件定义是否丢弃当前记录
							//context.setDrop(true);return;
	//				if(s.incrementAndGet() % 2 == 0) {
	//					context.setDrop(true);
	//					return;
	//				}
							String data = (String)context.getTaskContext().getTaskData("data");
	//				System.out.println(data);

	//				context.addFieldValue("author","duoduo");//将会覆盖全局设置的author变量
							context.addFieldValue("title","解放");
							context.addFieldValue("subtitle","小康");

	//				context.addIgnoreFieldMapping("title");
							//上述三个属性已经放置到docInfo中，如果无需再放置到索引文档中，可以忽略掉这些属性
	//				context.addIgnoreFieldMapping("author");

	//				//修改字段名称title为新名称newTitle，并且修改字段的值
	//				context.newName2ndData("title","newTitle",(String)context.getValue("title")+" append new Value");
							/**
							 * 获取ip对应的运营商和区域信息
							 */
							IpInfo ipInfo = (IpInfo)context.getIpInfo("LOG_VISITORIAL");
							if(ipInfo != null)
								context.addFieldValue("ipinfo", ipInfo);
							else{
								context.addFieldValue("ipinfo", "");
							}

							context.addFieldValue("newcollecttime",new Date());

							/**
							 //关联查询数据,单值查询
							 Map headdata = SQLExecutor.queryObjectWithDBName(Map.class,"test",
							 "select * from head where billid = ? and othercondition= ?",
							 context.getIntegerValue("billid"),"otherconditionvalue");//多个条件用逗号分隔追加
							 //将headdata中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
							 context.addFieldValue("headdata",headdata);
							 //关联查询数据,多值查询
							 List<Map> facedatas = SQLExecutor.queryListWithDBName(Map.class,"test",
							 "select * from facedata where billid = ?",
							 context.getIntegerValue("billid"));
							 //将facedatas中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
							 context.addFieldValue("facedatas",facedatas);
							 */
						}
					});
					//映射和转换配置结束
					importBuilder.setExportResultHandler(new ExportResultHandler<RecordMetadata>() {
						@Override
						public void success(TaskCommand<RecordMetadata> taskCommand, RecordMetadata result) {
							TaskMetrics taskMetric = taskCommand.getTaskMetrics();
							System.out.println("处理耗时："+taskCommand.getElapsed() +"毫秒");
							System.out.println(taskCommand.getTaskMetrics());
						}

						@Override
						public void error(TaskCommand<RecordMetadata> taskCommand, RecordMetadata result) {
							System.out.println(taskCommand.getTaskMetrics());
						}

						@Override
						public void exception(TaskCommand<RecordMetadata> taskCommand, Throwable exception) {
							System.out.println(taskCommand.getTaskMetrics());
						}

					});

					importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
					importBuilder.setPrintTaskLog(true);

					/**
					 * 构建和启动导出关系数据库数据并发送kafka同步作业
					 */
					DataStream dataStream = importBuilder.builder();
					dataStream.execute();//执行导入操作
					db2kafkaImportBuilder = importBuilder;
					this.dataKafkaStream = dataStream;
					return "db2kafkaImportBuilder job started.";
				}
				else{
					return "db2kafkaImportBuilder job has started.";
				}
			}
		}
		else{
			return "db2kafkaImportBuilder job has started.";
		}

	}
	public String stopDB2kafkaJob(){
		if(dataKafkaStream != null) {
			synchronized (this) {
				if (dataKafkaStream != null) {
					dataKafkaStream.destroy(true);
					dataKafkaStream = null;
					db2kafkaImportBuilder = null;
					return "db2kafkaImportBuilder job stopped.";
				} else {
					return "db2kafkaImportBuilder job has stopped.";
				}
			}
		}
		else {
			return "db2kafkaImportBuilder job has stopped.";
		}
	}

	public String stopDB2ESJob(){
		if(dataStream != null) {
			synchronized (this) {
				if (dataStream != null) {
					dataStream.destroy(true);
					dataStream = null;
					db2ESImportBuilder = null;
					return "db2ESImport job stopped.";
				} else {
					return "db2ESImport job has stopped.";
				}
			}
		}
		else {
			return "db2ESImport job has stopped.";
		}
	}

	private AgentInfoBo.Builder createBuilderFromValue(byte[] serializedAgentInfo) {
		final Buffer buffer = new FixedBuffer(serializedAgentInfo);
		final AgentInfoBo.Builder builder = new AgentInfoBo.Builder();
		builder.setHostName(buffer.readPrefixedString());
		builder.setIp(buffer.readPrefixedString());
		builder.setPorts(buffer.readPrefixedString());
		builder.setApplicationName(buffer.readPrefixedString());
		builder.setServiceTypeCode(buffer.readShort());
		builder.setPid(buffer.readInt());
		builder.setAgentVersion(buffer.readPrefixedString());
		builder.setStartTime(buffer.readLong());
		builder.setEndTimeStamp(buffer.readLong());
		builder.setEndStatus(buffer.readInt());
		// FIXME - 2015.09 v1.5.0 added vmVersion (check for compatibility)
		if (buffer.hasRemaining()) {
			builder.setVmVersion(buffer.readPrefixedString());
		}
		return builder;
	}
	public  String scheduleDB2ESJob(){
		if (db2ESImportBuilder == null) {
			synchronized (this) {
				if (db2ESImportBuilder == null) {
					ImportBuilder importBuilder = ImportBuilder.newInstance();
					//增量定时任务不要删表，但是可以通过删表来做初始化操作
//			if(dropIndice) {
//				try {
//					//清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
//					String repsonse = ElasticSearchHelper.getRestClientUtil().dropIndice("dbdemo");
//					System.out.println(repsonse);
//				} catch (Exception e) {
//				}
//			}
//					//数据源相关配置，可选项，可以在外部启动数据源
//					importBuilder.setDbName("test")
//							.setDbDriver("com.mysql.cj.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
//							//mysql stream机制一 通过useCursorFetch=true启用mysql的游标fetch机制，否则会有严重的性能隐患，useCursorFetch必须和jdbcFetchSize参数配合使用，否则不会生效
////					.setDbUrl("jdbc:mysql://localhost:3306/bboss?useCursorFetch=true&useUnicode=true&characterEncoding=utf-8&useSSL=false")
////					.setJdbcFetchSize(3000)//启用mysql stream机制1，设置jdbcfetchsize大小为3000
//							//mysql stream机制二  jdbcFetchSize为Integer.MIN_VALUE即可，url中不需要设置useCursorFetch=true参数，这里我们使用机制二
//							.setDbUrl("jdbc:mysql://localhost:3306/bboss?useUnicode=true&characterEncoding=utf-8&useSSL=false")
//							.setJdbcFetchSize(Integer.MIN_VALUE)//启用mysql stream机制二,设置jdbcfetchsize大小为Integer.MIN_VALUE
//							.setDbUser("root")
//							.setDbPassword("123456")
//							.setValidateSQL("select 1")
//							.setUsePool(false);//是否使用连接池
					DBInputConfig dbInputConfig = new DBInputConfig();
					dbInputConfig.setDbName("test");//这里只需要指定dbname，具体的数据源配置在application.properties文件中指定

					//指定导入数据的sql语句，必填项，可以设置自己的提取逻辑，
					// 设置增量变量log_id，增量变量名称#[log_id]可以多次出现在sql语句的不同位置中，例如：
					// select * from td_sm_log where log_id > #[log_id] and parent_id = #[log_id]
					// log_id和数据库对应的字段一致,就不需要设置setLastValueColumn信息，
					// 但是需要设置setLastValueType告诉工具增量字段的类型

					dbInputConfig.setSql("select * from td_sm_log where LOG_OPERTIME > #[LOG_OPERTIME]");
					importBuilder.setInputConfig(dbInputConfig);
					/**
					 * es相关配置
					 */
					ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();
					elasticsearchOutputConfig.setTargetElasticsearch("default");
					elasticsearchOutputConfig
							.setIndex("dbdemo") //必填项
							.setEsIdField("log_id");//设置文档主键，不设置，则自动产生文档id

					/**
					 elasticsearchOutputConfig.setEsIdGenerator(new EsIdGenerator() {
					 //如果指定EsIdGenerator，则根据下面的方法生成文档id，
					 // 否则根据setEsIdField方法设置的字段值作为文档id，
					 // 如果默认没有配置EsIdField和如果指定EsIdGenerator，则由es自动生成文档id

					 @Override public Object genId(Context context) throws Exception {
					 return SimpleStringUtil.getUUID();//返回null，则由es自动生成文档id
					 }
					 });
					 */
//					.setIndexType("dbdemo") //es 7以后的版本不需要设置indexType，es7以前的版本必需设置indexType
//				.setRefreshOption("refresh")//可选项，null表示不实时刷新，importBuilder.setRefreshOption("refresh");表示实时刷新
					importBuilder.setOutputConfig(elasticsearchOutputConfig)
							.setUseJavaName(false) //可选项,将数据库字段名称转换为java驼峰规范的名称，true转换，false不转换，默认false，例如:doc_id -> docId
							.setUseLowcase(false)  //可选项，true 列名称转小写，false列名称不转换小写，默认false，只要在UseJavaName为false的情况下，配置才起作用
							.setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
							.setBatchSize(10);  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理

					//定时任务配置，
					importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
							.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
							.setPeriod(5000L); //每隔period毫秒执行，如果不设置，只执行一次
					//定时任务配置结束
//
//		//设置任务执行拦截器，可以添加多个，定时任务每次执行的拦截器
		importBuilder.addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {
			}

			@Override
			public void afterCall(TaskContext taskContext) {
				if(taskContext != null)
					logger.info(taskContext.getJobTaskMetrics().toString());
			}

			@Override
			public void throwException(TaskContext taskContext, Throwable e) {
				if(taskContext != null)
					logger.info(taskContext.getJobTaskMetrics().toString(),e);
			}
		});
//		.addCallInterceptor(new CallInterceptor() {
//			@Override
//			public void preCall(TaskContext taskContext) {
//				System.out.println("preCall 1");
//			}
//
//			@Override
//			public void afterCall(TaskContext taskContext) {
//				System.out.println("afterCall 1");
//			}
//
//			@Override
//			public void throwException(TaskContext taskContext, Throwable e) {
//				System.out.println("throwException 1");
//			}
//		});
//		//设置任务执行拦截器结束，可以添加多个
					//增量配置开始
//		importBuilder.setLastValueColumn("log_id");//手动指定数字增量查询字段，默认采用上面设置的sql语句中的增量变量名称作为增量查询字段的名称，指定以后就用指定的字段
					importBuilder.setFromFirst(true);//任务重启时，重新开始采集数据，true 重新开始，false不重新开始，适合于每次全量导入数据的情况，如果是全量导入，可以先删除原来的索引数据
					importBuilder.setStatusDbname("logtable");
					importBuilder.setLastValueStorePath("logtable_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
					importBuilder.setLastValueType(ImportIncreamentConfig.TIMESTAMP_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
//					importBuilder.setStatusDbname("default");//default是一个数据库datasource的名称，具体配置参考application.properties文件内容：
					importBuilder.setAsynFlushStatusInterval(10000);
					SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
					try {
						Date date = format.parse("2000-01-01");
						importBuilder.setLastValue(date);//增量起始值配置
					} catch (Exception e) {
						e.printStackTrace();
					}
					// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
					//增量配置结束

					//映射和转换配置开始
//		/**
//		 * db-es mapping 表字段名称到es 文档字段的映射：比如document_id -> docId
//		 * 可以配置mapping，也可以不配置，默认基于java 驼峰规则进行db field-es field的映射和转换
//		 */
//		importBuilder.addFieldMapping("document_id","docId")
//				.addFieldMapping("docwtime","docwTime")
//				.addIgnoreFieldMapping("channel_id");//添加忽略字段
//
//
//		/**
//		 * 为每条记录添加额外的字段和值
//		 * 可以为基本数据类型，也可以是复杂的对象
//		 */
//		importBuilder.addFieldValue("testF1","f1value");
//		importBuilder.addFieldValue("testInt",0);
//		importBuilder.addFieldValue("testDate",new Date());
//		importBuilder.addFieldValue("testFormateDate","yyyy-MM-dd HH",new Date());
//		TestObject testObject = new TestObject();
//		testObject.setId("testid");
//		testObject.setName("jackson");
//		importBuilder.addFieldValue("testObject",testObject);
//
//		/**
//		 * 重新设置es数据结构
//		 */
//		importBuilder.setDataRefactor(new DataRefactor() {
//			public void refactor(Context context) throws Exception  {
//				CustomObject customObject = new CustomObject();
//				customObject.setAuthor((String)context.getValue("author"));
//				customObject.setTitle((String)context.getValue("title"));
//				customObject.setSubtitle((String)context.getValue("subtitle"));
//				customObject.setIds(new int[]{1,2,3});
//				context.addFieldValue("docInfo",customObject);//如果还需要构建更多的内部对象，可以继续构建
//
//				//上述三个属性已经放置到docInfo中，如果无需再放置到索引文档中，可以忽略掉这些属性
//				context.addIgnoreFieldMapping("author");
//				context.addIgnoreFieldMapping("title");
//				context.addIgnoreFieldMapping("subtitle");
//			}
//		});
					//映射和转换配置结束

					/**
					 * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
					 */
					importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
					importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
					importBuilder.setThreadCount(6);//设置批量导入线程池工作线程数量
					importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行

					importBuilder.setExportResultHandler(new ExportResultHandler<String>() {
						@Override
						public void success(TaskCommand<String> taskCommand, String result) {
							TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
							logger.info(taskMetrics.toString());
							logger.info(result);
						}

						@Override
						public void error(TaskCommand<String> taskCommand, String result) {
							TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
							logger.info(taskMetrics.toString());
							logger.info(result);
						}

						@Override
						public void exception(TaskCommand<String> taskCommand, Throwable exception) {
							TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
							logger.info(taskMetrics.toString());
						}


					});
					/**
					 * 执行数据库表数据导入es操作
					 */
					DataStream dataStream = importBuilder.builder();
					dataStream.execute();//执行导入操作
					db2ESImportBuilder = importBuilder;
					this.dataStream = dataStream;
					return "db2ESImport job started.";
				}
				else{
					return "db2ESImport job has started.";
				}
			}
		}
		else{
			return "db2ESImport job has started.";
		}

	}
	private ImportBuilder hBaseExportBuilder;
	private DataStream hbase2esDataStream;
	public String stopHBase2ESJob() {
		if(hbase2esDataStream != null) {
			synchronized (this) {
				if (hbase2esDataStream != null) {
					hbase2esDataStream.destroy(true);
					hbase2esDataStream = null;
					hBaseExportBuilder = null;
					return "HBase2ES job stopped.";
				} else {
					return "HBase2ES job has stopped.";
				}
			}
		}
		else {
			return "HBase2ES job has stopped.";
		}
	}

	public String scheduleHBase2ESJob() {
		if (hBaseExportBuilder == null) {
			synchronized (this) {
				if (hBaseExportBuilder == null) {
					ImportBuilder importBuilder = new ImportBuilder();
					importBuilder.setBatchSize(1000) //设置批量写入目标Elasticsearch记录数
							.setFetchSize(10000); //设置批量从源Hbase中拉取的记录数,HBase-0.98 默认值为为 100，HBase-1.2 默认值为 2147483647，即 Integer.MAX_VALUE。Scan.next() 的一次 RPC 请求 fetch 的记录条数。配置建议：这个参数与下面的setMaxResultSize配合使用，在网络状况良好的情况下，自定义设置不宜太小， 可以直接采用默认值，不配置。

//		importBuilder.setHbaseBatch(100) //配置获取的列数，假如表有两个列簇 cf，info，每个列簇5个列。这样每行可能有10列了，setBatch() 可以控制每次获取的最大列数，进一步从列级别控制流量。配置建议：当列数很多，数据量大时考虑配置此参数，例如100列每次只获取50列。一般情况可以默认值（-1 不受限）
//				.setMaxResultSize(10000l);//客户端缓存的最大字节数，HBase-0.98 无该项配置，HBase-1.2 默认值为 210241024，即 2M。Scan.next() 的一次 RPC 请求 fetch 的数据量大小，目前 HBase-1.2 在 Caching 为默认值(Integer Max)的时候，实际使用这个参数控制 RPC 次数和流量。配置建议：如果网络状况较好（万兆网卡），scan 的数据量非常大，可以将这个值配置高一点。如果配置过高：则可能 loadCache 速度比较慢，导致 scan timeout 异常
					// 参考文档：https://blog.csdn.net/kangkangwanwan/article/details/89332536


					/**
					 * hbase参数配置
					 */
					HBaseInputConfig hBaseInputConfig = new HBaseInputConfig();
//					hBaseInputConfig.addHbaseClientProperty("hbase.zookeeper.quorum","192.168.137.133")  //hbase客户端连接参数设置，参数含义参考hbase官方客户端文档
//							.addHbaseClientProperty("hbase.zookeeper.property.clientPort","2183")
					hBaseInputConfig.addHbaseClientProperty("hbase.zookeeper.quorum","127.0.0.1")  //hbase客户端连接参数设置，参数含义参考hbase官方客户端文档
							.addHbaseClientProperty("hbase.zookeeper.property.clientPort","2185")
							.addHbaseClientProperty("zookeeper.znode.parent","/hbase")
							.addHbaseClientProperty("hbase.ipc.client.tcpnodelay","true")
							.addHbaseClientProperty("hbase.rpc.timeout","1000000")
							.addHbaseClientProperty("hbase.client.operation.timeout","1000000")
							.addHbaseClientProperty("hbase.ipc.client.socket.timeout.read","2000000")
							.addHbaseClientProperty("hbase.ipc.client.socket.timeout.write","3000000")

							.setHbaseClientThreadCount(100)  //hbase客户端连接线程池参数设置
							.setHbaseClientThreadQueue(100)
							.setHbaseClientKeepAliveTime(10000l)
							.setHbaseClientBlockedWaitTimeout(10000l)
							.setHbaseClientWarnMultsRejects(1000)
							.setHbaseClientPreStartAllCoreThreads(true)
							.setHbaseClientThreadDaemon(true)

							.setHbaseTable("AgentInfo") //指定需要同步数据的hbase表名称
					;
					//FilterList和filter二选一，只需要设置一种
//		/**
//		 * 设置hbase检索filter
//		 */
//		SingleColumnValueFilter scvf= new SingleColumnValueFilter(Bytes.toBytes("Info"), Bytes.toBytes("i"),
//
//				CompareOperator.EQUAL,"wap".getBytes());
//
//		scvf.setFilterIfMissing(true); //默认为false， 没有此列的数据也会返回 ，为true则只返回name=lisi的数据
//
//		hBaseInputConfig.setFilter(scvf);

					/**
					 * 设置hbase组合条件FilterList
					 * FilterList 代表一个过滤器链，它可以包含一组即将应用于目标数据集的过滤器，过滤器间具有“与” FilterList.Operator.MUST_PASS_ALL 和“或” FilterList.Operator.MUST_PASS_ONE 关系
					 */

//		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE); //数据只要满足一组过滤器中的一个就可以
//
//		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("Info"), Bytes.toBytes("i"),
//
//				CompareOperator.EQUAL,"wap".getBytes());
//
//		list.addFilter(filter1);
//
//		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("Info"), Bytes.toBytes("i"),
//
//				CompareOperator.EQUAL,Bytes.toBytes("my other value"));
//
//		list.addFilter(filter2);
//		hBaseInputConfig.setFilterList(list);

//		//设置同步起始行和终止行key条件
//		hBaseInputConfig.setStartRow(startRow);
//		hBaseInputConfig.setEndRow(endRow);
					//设置记录起始时间搓（>=）和截止时间搓(<),如果是基于时间范围的增量同步，则不需要指定下面两个参数
//		hBaseInputConfig.setStartTimestamp(startTimestam);
//		hBaseInputConfig.setEndTimestamp(endTimestamp);
					importBuilder.setInputConfig(hBaseInputConfig);
					/**
					 * es相关配置
					 * 可以通过addElasticsearchProperty方法添加Elasticsearch客户端配置，
					 * 也可以直接读取application.properties文件中设置的es配置,两种方式都可以，案例中采用application.properties的方式
					 */
					ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();
//		elasticsearchOutputConfig.addElasticsearchProperty("elasticsearch.rest.hostNames","192.168.137.1:9200");//设置es服务器地址，更多配置参数文档：https://esdoc.bbossgroups.com/#/mongodb-elasticsearch?id=_5242-elasticsearch%e5%8f%82%e6%95%b0%e9%85%8d%e7%bd%ae
					elasticsearchOutputConfig.setTargetElasticsearch("default");//设置目标Elasticsearch集群数据源名称，和源elasticsearch集群一样都在application.properties文件中配置

					elasticsearchOutputConfig.setIndex("hbase233esdemo") ;//全局设置要目标elasticsearch索引名称
//							.setIndexType("hbase233esdemo"); //全局设值目标elasticsearch索引类型名称，如果是Elasticsearch 7以后的版本不需要配置
// 设置Elasticsearch索引文档_id
					/**
					 * 如果指定rowkey为文档_id,那么需要指定前缀meta:，如果是其他数据字段就不需要
					 * 例如：
					 * meta:rowkey 行key byte[]
					 * meta:timestamp  记录时间戳
					 */
					elasticsearchOutputConfig.setEsIdField("meta:rowkey");
					// 设置自定义id生成机制
					//如果指定EsIdGenerator，则根据下面的方法生成文档id，
					// 否则根据setEsIdField方法设置的字段值作为文档id，
					// 如果默认没有配置EsIdField和如果指定EsIdGenerator，则由es自动生成文档id
//		elasticsearchOutputConfig.setEsIdGenerator(new EsIdGenerator(){
//
//			@Override
//			public Object genId(Context context) throws Exception {
//					Object id = context.getMetaValue("rowkey");
//					String agentId = BytesUtils.safeTrim(BytesUtils.toString((byte[]) id, 0, PinpointConstants.AGENT_NAME_MAX_LEN));
//					return agentId;
//			}
//		});
				  importBuilder.setOutputConfig(elasticsearchOutputConfig);


					//定时任务配置，
					importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
							.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
							.setPeriod(10000L); //每隔period毫秒执行，如果不设置，只执行一次
					//定时任务配置结束

					//hbase表中列名，由"列族:列名"组成
//		//设置任务执行拦截器结束，可以添加多个
//		//增量配置开始
////		importBuilder.setLastValueColumn("Info:id");//指定数字增量查询字段变量名称
					importBuilder.setFromFirst(true);//任务重启时，重新开始采集数据，true 重新开始，false不重新开始，适合于每次全量导入数据的情况，如果是全量导入，可以先删除原来的索引数据
					importBuilder.setStatusDbname("hbase233esdemo");
					importBuilder.setLastValueStorePath("hbase233esdemo_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
					//指定增量字段类型为日期类型，如果没有指定增量字段名称,则按照hbase记录时间戳进行timerange增量检索
					importBuilder.setLastValueType(ImportIncreamentConfig.TIMESTAMP_TYPE);
					// ImportIncreamentConfig.NUMBER_TYPE 数字类型
//		// ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
					//设置增量查询的起始值时间起始时间
					SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
					try {

						Date date = format.parse("2000-01-01");
						importBuilder.setLastValue(date);
					}
					catch (Exception e){
						e.printStackTrace();
					}
					//增量配置结束




					//映射和转换配置开始
//		/**
//		 * db-es mapping 表字段名称到es 文档字段的映射：比如document_id -> docId
//		 * 可以配置mapping，也可以不配置，默认基于java 驼峰规则进行db field-es field的映射和转换
//		 */
//		importBuilder.addFieldMapping("document_id","docId")
//				.addFieldMapping("docwtime","docwTime")
//				.addIgnoreFieldMapping("channel_id");//添加忽略字段
//
//
//		/**
//		 * 为每条记录添加额外的字段和值
//		 * 可以为基本数据类型，也可以是复杂的对象
//		 */
//		importBuilder.addFieldValue("testF1","f1value");
//		importBuilder.addFieldValue("testInt",0);
//		importBuilder.addFieldValue("testDate",new Date());
//		importBuilder.addFieldValue("testFormateDate","yyyy-MM-dd HH",new Date());
//		TestObject testObject = new TestObject();
//		testObject.setId("testid");
//		testObject.setName("jackson");
//		importBuilder.addFieldValue("testObject",testObject);
//					importBuilder.addFieldValue("author","作者");

					/**
					 * 设置es数据结构
					 */
					importBuilder.setDataRefactor(new DataRefactor() {
						public void refactor(Context context) throws Exception  {
							//可以根据条件定义是否丢弃当前记录
							//context.setDrop(true);return;
//				if(s.incrementAndGet() % 2 == 0) {
//					context.setDrop(true);
//					return;
//				}
							//获取原始的hbase记录Result对象
//				Result result = (Result)  context.getRecord();

							// 直接获取行key，对应byte[]类型，自行提取和分析保存在其中的数据
							String agentId = Bytes.toString((byte[])context.getMetaValue("rowkey"));
							context.addFieldValue("agentId",agentId);
							Date startTime = (Date)context.getMetaValue("timestamp");
							context.addFieldValue("startTime",startTime);
							// 通过context.getValue方法获取hbase 列的原始值byte[],方法参数对应hbase表中列名，由"列族:列名"组成
							String serializedAgentInfo =  context.getStringValue("Info:i");
							String serializedServerMetaData =  context.getStringValue("Info:m");
							String serializedJvmInfo =  context.getStringValue("Info:j");

							context.addFieldValue("serializedAgentInfo",serializedAgentInfo);
							context.addFieldValue("serializedServerMetaData",serializedServerMetaData);
							context.addFieldValue("serializedJvmInfo",serializedJvmInfo);
							context.addFieldValue("subtitle","小康");
							context.addFieldValue("collectTime",new Date());


//				/**
//				 * 获取ip对应的运营商和区域信息
//				 */
//				IpInfo ipInfo = context.getIpInfo("Info:agentIp");
//				if(ipInfo != null)
//					context.addFieldValue("ipinfo", SimpleStringUtil.object2json(ipInfo));
//				else{
//					context.addFieldValue("ipinfo", "");
//				}
//				DateFormat dateFormat = SerialUtil.getDateFormateMeta().toDateFormat();
//				Date optime = context.getDateValue("logOpertime",dateFormat);
//				context.addFieldValue("logOpertime",optime);
//				context.addFieldValue("collecttime",new Date());

						}
					});
					//映射和转换配置结束

					/**
					 * 作业创建一个内置的线程池，实现多线程并行数据导入elasticsearch功能
					 */
					importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
					importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
					importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
					importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
					importBuilder.setPrintTaskLog(true); //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false

					/**
					 * 设置任务执行情况回调接口
					 */
					importBuilder.setExportResultHandler(new ExportResultHandler<String>() {
						@Override
						public void success(TaskCommand<String> taskCommand, String result) {
							TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
							logger.info(taskMetrics.toString());
						}

						@Override
						public void error(TaskCommand<String> taskCommand, String result) {
							TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
							logger.info(taskMetrics.toString());
						}

						@Override
						public void exception(TaskCommand<String> taskCommand, Throwable exception) {
							TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
							logger.info(taskMetrics.toString());
						}


					});
					/**
					 * 执行es数据导入数据库表操作
					 */
					DataStream dataStream = importBuilder.builder();
					dataStream.execute();//执行导入操作
					hBaseExportBuilder = importBuilder;
					this.hbase2esDataStream = dataStream;
					return "HBase2ES job started.";
				}
				else{
					return "HBase2ES job has started.";
				}
			}
		}
		else{
			return "HBase2ES job has started.";
		}
	}
}
