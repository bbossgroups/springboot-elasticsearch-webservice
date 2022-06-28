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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.boot.BBossESStarter;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.plugin.es.input.ElasticsearchInputConfig;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * <p>Description: 从es中查询数据导入es案例,基于时间戳增量同步，，可以采用slicescroll和scroll两种方式从源Elasticsearch拉取数据</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 14:39
 * @author biaoping.yin
 * @version 1.0
 */
@Service
public class NewES2ESScrollTimestampMultiJobDemo {
	private static Logger logger = LoggerFactory.getLogger(NewES2ESScrollTimestampMultiJobDemo.class);
	@Autowired
	@Qualifier("bbossESStarter")
	private BBossESStarter bbossESStarter;
	@Autowired
	@Qualifier("bbossESStarterLogs")
	private BBossESStarter bbossESStarterLogs;
	public void mulitExecute(){


		scheduleScrollRefactorImportData("job1");
		scheduleScrollRefactorImportData("job2");
		logger.info("complete.");
	}



	public void scheduleScrollRefactorImportData(String jobName){
		ImportBuilder importBuilder = new ImportBuilder();
		importBuilder.setBatchSize(1000) //设置批量从源Elasticsearch中拉取的记录数
				.setFetchSize(5000); //设置批量写入目标Elasticsearch记录数
		ElasticsearchOutputConfig esOutputConfig = new ElasticsearchOutputConfig();
		esOutputConfig.setTargetElasticsearch("logs");
		esOutputConfig.setIndex("newes2esdemo-"+jobName);
		esOutputConfig.setDebugResponse(false);//设置是否将每次处理的reponse打印到日志文件中，默认false
		esOutputConfig.setDiscardBulkResponse(true);//设置是否需要批量处理的响应报文，不需要设置为false，true为需要，默认false

		/**
		 * 如果指定索引文档元数据字段为为文档_id,那么需要指定前缀meta:，如果是其他数据字段就不需要
		 * **文档_id*
		 *private String id;
		 *    **文档对应索引类型信息*
		 *private String type;
		 *    **文档对应索引字段信息*
		 *private Map<String, List<Object>> fields;
		 * **文档对应版本信息*
		 *private long version;
		 *  **文档对应的索引名称*
		 *private String index;
		 *  **文档对应的高亮检索信息*
		 *private Map<String, List<Object>> highlight;
		 *     **文档对应的排序信息*
		 *private Object[] sort;
		 *     **文档对应的评分信息*
		 *private Double score;
		 *     **文档对应的父id*
		 *private Object parent;
		 *     **文档对应的路由信息*
		 *private String routing;
		 *     **文档对应的是否命中信息*
		 *private boolean found;
		 *     **文档对应的nested检索信息*
		 *private Map<String, Object> nested;
		 *     **文档对应的innerhits信息*
		 *private Map<String, Map<String, InnerSearchHits>> innerHits;
		 *     **文档对应的索引分片号*
		 *private String shard;
		 *     **文档对应的elasticsearch集群节点名称*
		 *private String node;
		 *    **文档对应的打分规则信息*
		 *private Explanation explanation;
		 *
		 *private long seqNo;//"_index": "trace-2017.09.01",
		 *private long primaryTerm;//"_index": "trace-2017.09.01",
		 */
		esOutputConfig.setEsIdField("meta:_id");
		importBuilder.setOutputConfig(esOutputConfig);
		//指定导入数据的sql语句，必填项，可以设置自己的提取逻辑，
		// 设置增量变量log_id，增量变量名称#[log_id]可以多次出现在sql语句的不同位置中，例如：
		// select * from td_sm_log where log_id > #[log_id] and parent_id = #[log_id]
		// log_id和数据库对应的字段一致,就不需要设置setLastValueColumn信息，
		// 但是需要设置setLastValueType告诉工具增量字段的类型
		/**
		 * es相关配置
		 */
		ElasticsearchInputConfig elasticsearchInputConfig = new ElasticsearchInputConfig();
//		elasticsearchInputConfig.setIndex("es2esdemo"); //全局设置要目标elasticsearch索引名称
					 //.setIndexType("es2esdemo"); //全局设值目标elasticsearch索引类型名称，如果是Elasticsearch 7以后的版本不需要配置
		elasticsearchInputConfig
//				.setTargetElasticsearch("targetElasticsearch")//设置目标Elasticsearch集群数据源名称，和源elasticsearch集群一样都在application.properties文件中配置
				.setSourceElasticsearch("default");
		elasticsearchInputConfig.setDslFile("dsl.xml") //指定从源dbdemo表检索数据的dsl语句配置文件名称，可以通过addParam方法传递dsl中的变量参数值
				.setDslName("scrollQuery") //指定从源dbdemo表检索数据的dsl语句名称，可以通过addParam方法传递dsl中的变量参数值
				.setScrollLiveTime("10m") // 指定scroll查询context有效期，这里是10分钟
//				.setSliceQuery(true) // 指定scroll查询为slice查询
//				.setDslName("scrollSliceQuery") //指定从源dbdemo表检索数据的slice scroll dsl语句名称，可以通过addParam方法传递dsl中的变量参数值
//				.setSliceSize(5) // 指定slice数量，与索引debdemo的shard数量一致即可
				.setQueryUrl("dbdemo/_search"); // 指定从dbdemo索引表检索数据

		importBuilder.setInputConfig(elasticsearchInputConfig)
//				//添加dsl中需要用到的参数及参数值
				.addParam("var1","v1")
				.addParam("var2","v2")
				.addParam("var3","v3");

		//定时任务配置，
		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
				.setPeriod(10000L); //每隔period毫秒执行，如果不设置，只执行一次
		//定时任务配置结束

		//设置任务执行拦截器，可以添加多个
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
			public void throwException(TaskContext taskContext, Exception e) {
				System.out.println("throwException");
			}
		}).addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {
				System.out.println("preCall 1");
			}

			@Override
			public void afterCall(TaskContext taskContext) {
				System.out.println("afterCall 1");
			}

			@Override
			public void throwException(TaskContext taskContext, Exception e) {
				System.out.println("throwException 1");
			}
		});
//		//设置任务执行拦截器结束，可以添加多个
		//增量配置开始
//		importBuilder.setLastValueColumn("logId");//指定数字增量查询字段变量名称
		importBuilder.setLastValueColumn("logOpertime");//手动指定日期增量查询字段变量名称
		importBuilder.setLastValueType(ImportIncreamentConfig.TIMESTAMP_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
		//		指定日期增量字段日期格式，当增量字段为日期类型且日期格式不是默认的
//		yyyy-MM-dd'T'HH:mm:ss.SSS'Z'时，需要设置字段相对应的日期格式，例如：yyyy-MM-dd HH:mm:ss
//				,如果是默认utc格式，则不需要手动设置指定
//		importBuilder.setLastValueDateformat("yyyy-MM-dd HH:mm:ss");
		importBuilder.setFromFirst(true);//任务重启时，重新开始采集数据，true 重新开始，false不重新开始，适合于每次全量导入数据的情况，如果是全量导入，可以先删除原来的索引数据
		importBuilder.setStatusDbname(jobName);
		importBuilder.setLastValueStorePath(jobName+"_newes2esdemo_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
		importBuilder.setIncreamentEndOffset(5000);


		// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
		//设置增量查询的起始值lastvalue
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
		importBuilder.addFieldValue("author","作者");

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


				context.addFieldValue("author","duoduo");
				context.addFieldValue("title","解放");
				context.addFieldValue("subtitle","小康");

//				context.addIgnoreFieldMapping("title");
				//上述三个属性已经放置到docInfo中，如果无需再放置到索引文档中，可以忽略掉这些属性
//				context.addIgnoreFieldMapping("author");

//				//修改字段名称title为新名称newTitle，并且修改字段的值
//				context.newName2ndData("title","newTitle",(String)context.getValue("title")+" append new Value");
				context.addIgnoreFieldMapping("subtitle");
				/**
				 * 获取ip对应的运营商和区域信息
				 */
				Map ipInfo = (Map)context.getValue("ipInfo");
				if(ipInfo != null)
					context.addFieldValue("ipinfo", SimpleStringUtil.object2json(ipInfo));
				else{
					context.addFieldValue("ipinfo", "");
				}
				DateFormat dateFormat = SerialUtil.getDateFormateMeta().toDateFormat();
				Date optime = context.getDateValue("logOpertime",dateFormat);
				context.addFieldValue("logOpertime",optime);
				context.addFieldValue("collecttime",new Date());

				/**
				 //关联查询数据,单值查询
				 Map headdata = SQLExecutor.queryObjectWithDBName(Map.class,context.getEsjdbc().getDbConfig().getDbName(),
				 "select * from head where billid = ? and othercondition= ?",
				 context.getIntegerValue("billid"),"otherconditionvalue");//多个条件用逗号分隔追加
				 //将headdata中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
				 context.addFieldValue("headdata",headdata);
				 //关联查询数据,多值查询
				 List<Map> facedatas = SQLExecutor.queryListWithDBName(Map.class,context.getEsjdbc().getDbConfig().getDbName(),
				 "select * from facedata where billid = ?",
				 context.getIntegerValue("billid"));
				 //将facedatas中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
				 context.addFieldValue("facedatas",facedatas);
				 */
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
		importBuilder.setAsyn(false);//true 异步方式执行，不等待所有导入作业任务结束，方法快速返回；false（默认值） 同步方式执行，等待所有导入作业任务结束，所有作业结束后方法才返回
		importBuilder.setPrintTaskLog(true);

		/**
		 * 执行es数据导入数据库表操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//执行导入操作
	}
}
