//package com.example.esbboss.service;
//
//import com.xxl.job.core.biz.model.ReturnT;
//import com.xxl.job.core.handler.IJobHandler;
//import com.xxl.job.core.handler.annotation.JobHandler;
//import com.xxl.job.core.util.ShardingUtil;
//import org.frameworkset.elasticsearch.ElasticSearchHelper;
//import org.frameworkset.tran.config.ImportBuilder;
//import org.frameworkset.tran.plugin.db.input.DBInputConfig;
//import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
//import org.frameworkset.tran.schedule.ExternalScheduler;
//import org.frameworkset.tran.schedule.ImportIncreamentConfig;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.stereotype.Component;
//
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReentrantLock;
//
///**
// * 一个xxl-job调度的elasticsearch到database数据同步的作业案例
// * @author bboss
// */
//@Component
//@JobHandler(value = "SyncDataXXLJob")
//public class SyncDataXXLJob extends IJobHandler {
////    @Resource
////    private ICustomerService customerService;
////    @Resource
////    private IContactService contactService;
////    @Resource
////    private IContractService contractService;
//
//	private static Logger logger = LoggerFactory.getLogger(SyncDataXXLJob.class);
//
//
//	protected ExternalScheduler externalScheduler;
//
//	private Lock lock = new ReentrantLock();
//	public void init(){
//		externalScheduler = new ExternalScheduler();
//		externalScheduler.dataStream((Object params)->{
//			ShardingUtil.ShardingVO shardingVO = ShardingUtil.getShardingVo();
//			int shardIndex = 0;
//			if(shardingVO != null) {
//				shardIndex = shardingVO.getIndex();
//				logger.info("index:>>>>>>>>>>>>>>>>>>>" + shardingVO.getIndex());
//				logger.info("total:>>>>>>>>>>>>>>>>>>>" + shardingVO.getTotal());
//			}
//			logger.info("params:>>>>>>>>>>>>>>>>>>>" + params);
//			ImportBuilder importBuilder = new ImportBuilder();
//			//增量定时任务不要删表，但是可以通过删表来做初始化操作
////		if(dropIndice) {
//			try {
//				//清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
//				String repsonse = ElasticSearchHelper.getRestClientUtil().dropIndice("quartz");
//				System.out.println(repsonse);
//			} catch (Exception e) {
//			}
////		}
//
//
//			//指定导入数据的sql语句，必填项，可以设置自己的提取逻辑，
//			// 设置增量变量log_id，增量变量名称#[log_id]可以多次出现在sql语句的不同位置中，例如：
//			// select * from td_sm_log where log_id > #[log_id] and parent_id = #[log_id]
//			// 需要设置setLastValueColumn信息log_id，
//			// 通过setLastValueType方法告诉工具增量字段的类型，默认是数字类型
//			DBInputConfig dbInputConfig = new DBInputConfig();
//			dbInputConfig.setSql("select * from td_sm_log where log_id > #[log_id]");
//			importBuilder.setInputConfig(dbInputConfig)
//					.addIgnoreFieldMapping("remark1");
////		importBuilder.setSql("select * from td_sm_log ");
//			/**
//			 * es相关配置
//			 */
//			ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();
//			elasticsearchOutputConfig
//					.setIndex("quartz") ;//必填项
////					.setIndexType("quartz") //es 7以后的版本不需要设置indexType，es7以前的版本必需设置indexType
////				.setRefreshOption("refresh")//可选项，null表示不实时刷新，importBuilder.setRefreshOption("refresh");表示实时刷新
//			elasticsearchOutputConfig.setEsIdField("log_id");//设置文档主键，不设置，则自动产生文档id
//
//			elasticsearchOutputConfig.setDebugResponse(false);//设置是否将每次处理的reponse打印到日志文件中，默认false
//			elasticsearchOutputConfig.setDiscardBulkResponse(true);//设置是否需要批量处理的响应报文，不需要设置为false，true为需要，默认false
//			/**
//			 importBuilder.setEsIdGenerator(new EsIdGenerator() {
//			 //如果指定EsIdGenerator，则根据下面的方法生成文档id，
//			 // 否则根据setEsIdField方法设置的字段值作为文档id，
//			 // 如果默认没有配置EsIdField和如果指定EsIdGenerator，则由es自动生成文档id
//
//			 @Override
//			 public Object genId(Context context) throws Exception {
//			 return SimpleStringUtil.getUUID();//返回null，则由es自动生成文档id
//			 }
//			 });
//			 */
//			importBuilder.setOutputConfig(elasticsearchOutputConfig)
//					.setUseJavaName(false) //可选项,将数据库字段名称转换为java驼峰规范的名称，true转换，false不转换，默认false，例如:doc_id -> docId
//					.setUseLowcase(false)  //可选项，true 列名称转小写，false列名称不转换小写，默认false，只要在UseJavaName为false的情况下，配置才起作用
//					.setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
//					.setBatchSize(10);  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理
//
//			//定时任务配置，
//			//采用内部定时任务
////		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//////					 .setScheduleDate(date) //指定任务开始执行时间：日期
////				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
////				.setPeriod(10000L); //每隔period毫秒执行，如果不设置，只执行一次
//			//采用外部定时任务
//			importBuilder.setExternalTimer(true);
//			//定时任务配置结束
////
////		//设置任务执行拦截器，可以添加多个
////		importBuilder.addCallInterceptor(new CallInterceptor() {
////			@Override
////			public void preCall(TaskContext taskContext) {
////				System.out.println("preCall");
////			}
////
////			@Override
////			public void afterCall(TaskContext taskContext) {
////				System.out.println("afterCall");
////			}
////
////			@Override
////			public void throwException(TaskContext taskContext, Throwable e) {
////				System.out.println("throwException");
////			}
////		}).addCallInterceptor(new CallInterceptor() {
////			@Override
////			public void preCall(TaskContext taskContext) {
////				System.out.println("preCall 1");
////			}
////
////			@Override
////			public void afterCall(TaskContext taskContext) {
////				System.out.println("afterCall 1");
////			}
////
////			@Override
////			public void throwException(TaskContext taskContext, Throwable e) {
////				System.out.println("throwException 1");
////			}
////		});
////		//设置任务执行拦截器结束，可以添加多个
//			//增量配置开始
//			importBuilder.setLastValueColumn("log_id");//指定数字增量查询字段
//			importBuilder.setFromFirst(true);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
//			//setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
//			importBuilder.setLastValueStorePath("logtable_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//			importBuilder.setLastValueStoreTableName("logs"+shardIndex);//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab，增量状态表会自动创建。如果xxl-job是shard分片模式运行，
//			// 需要独立的表来记录每个分片增量同步状态，
//			// 并且采用xxl-job等分布式任务调度引擎时，同步状态表必须存放于db.config=test指定的数据源，不能采用本地sqlite数据库
//			importBuilder.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
//			// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
//			//增量配置结束
//
//			//映射和转换配置开始
////		/**
////		 * db-es mapping 表字段名称到es 文档字段的映射：比如document_id -> docId
////		 * 可以配置mapping，也可以不配置，默认基于java 驼峰规则进行db field-es field的映射和转换
////		 */
////		importBuilder.addFieldMapping("document_id","docId")
////				.addFieldMapping("docwtime","docwTime")
////				.addIgnoreFieldMapping("channel_id");//添加忽略字段
////
////
////		/**
////		 * 为每条记录添加额外的字段和值
////		 * 可以为基本数据类型，也可以是复杂的对象
////		 */
////		importBuilder.addFieldValue("testF1","f1value");
////		importBuilder.addFieldValue("testInt",0);
////		importBuilder.addFieldValue("testDate",new Date());
////		importBuilder.addFieldValue("testFormateDate","yyyy-MM-dd HH",new Date());
////		TestObject testObject = new TestObject();
////		testObject.setId("testid");
////		testObject.setName("jackson");
////		importBuilder.addFieldValue("testObject",testObject);
////
////		/**
////		 * 重新设置es数据结构
////		 */
////		importBuilder.setDataRefactor(new DataRefactor() {
////			public void refactor(Context context) throws Exception  {
////				CustomObject customObject = new CustomObject();
////				customObject.setAuthor((String)context.getValue("author"));
////				customObject.setTitle((String)context.getValue("title"));
////				customObject.setSubtitle((String)context.getValue("subtitle"));
////				customObject.setIds(new int[]{1,2,3});
////				context.addFieldValue("docInfo",customObject);//如果还需要构建更多的内部对象，可以继续构建
////
////				//上述三个属性已经放置到docInfo中，如果无需再放置到索引文档中，可以忽略掉这些属性
////				context.addIgnoreFieldMapping("author");
////				context.addIgnoreFieldMapping("title");
////				context.addIgnoreFieldMapping("subtitle");
////			}
////		});
//			//映射和转换配置结束
//
//			/**
//			 * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
//			 */
//			importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
//			importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
//			importBuilder.setThreadCount(5);//设置批量导入线程池工作线程数量
//			importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
//			importBuilder.setAsyn(false);//true 异步方式执行，不等待所有导入作业任务结束，方法快速返回；false（默认值） 同步方式执行，等待所有导入作业任务结束，所有作业结束后方法才返回
//
//			/**
//			 * 执行数据库表数据导入es操作
//			 */
//			return importBuilder;
//		});
//
//    }
//	public ReturnT<String> execute(String param){
//		try {
//			lock.lock();
//			externalScheduler.execute(  param);
//			return SUCCESS;
//		}
//		finally {
//			lock.unlock();
//		}
//	}
//
//	public void destroy(){
//		if(externalScheduler != null){
//			externalScheduler.destroy();
//		}
//	}
//
//}