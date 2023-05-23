package com.example.esbboss.service;

import org.frameworkset.elasticsearch.ElasticSearchException;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.boot.BBossESStarter;
import org.frameworkset.elasticsearch.boot.ElasticSearchBoot;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.client.ClientUtil;
import org.frameworkset.elasticsearch.entity.MapRestResponse;
import org.frameworkset.elasticsearch.template.ESInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: ydzy-report
 * @Author: ygl
 * @Date: 2020/5/6 14:33
 * @Desc:
 */

@SpringBootTest
@RunWith(SpringRunner.class)
public class SpanQueryTest {

	private Logger logger = LoggerFactory.getLogger(SpanQueryTest.class);//日志

	@Autowired
	private BBossESStarter bbossESStarter;//bboss启动器

	private ClientInterface clientInterface;//bboss dsl工具

	private String spanQueryIndexName = "article";//spanQuery索引名称

	private String spanQueryDSLPath = "esmapper/span_query.xml";//spanQuery dsl文件

	private String paragraphWordsIndexName1 = "sample1";//同段/同句索引名称1
	private String paragraphWordsIndexName2 = "sample2";//同段/同句索引名称2

	private String paragraphWordsDSLPath = "esmapper/sentence_paragrah.xml";//同段/同句 dsl文件

	/**
	 * 创建student索引
	 */
	@Test
	public void dropAndCreateArticleIndice() {
		try {
			clientInterface = bbossESStarter.getConfigRestClient(spanQueryDSLPath);//bboss读取xml
			/*检查索引是否存在，存在就删除重建*/
			if (clientInterface.existIndice(spanQueryIndexName)) {
				clientInterface.dropIndice(spanQueryIndexName);
			}
			clientInterface.createIndiceMapping(spanQueryIndexName, "createArticleIndice");
			logger.info("create indice" + spanQueryIndexName + "is done");
		} catch (ElasticSearchException e) {
			logger.error("create indice" + spanQueryIndexName + "is faild" , e);
			throw e;
		}
	}
	@Test
	public void testSingleESDatasourceBoot(){
		Map properties = new HashMap();
		/**
		 * 这里只设置必须的配置项，其他的属性参考配置文件：resources/application.properties
		 *
		 */
		//认证账号和口令配置，如果启用了安全认证才需要，支持xpack和searchguard
		properties.put("elasticsearch.serverNames","es233");
		properties.put("es233.elasticUser","elastic");
		properties.put("es233.elasticPassword","changeme");
		//es服务器地址和端口，多个用逗号分隔
		properties.put("es233.elasticsearch.rest.hostNames","localhost:9200");
		//是否在控制台打印dsl语句，log4j组件日志级别为INFO或者DEBUG
		properties.put("es233.elasticsearch.showTemplate","true");
		//集群节点自动发现
		properties.put("es233.elasticsearch.discoverHost","true");
//		properties.put("http.timeoutSocket",60000);
//		properties.put("http.timeoutConnection",40000);
//		properties.put("http.connectionRequestTimeout",70000);

		ElasticSearchBoot.boot(properties);
		ClientInterface clientUtil = ElasticSearchHelper.getRestClientUtil("es233");
		//获取ES版本信息
		String result = clientUtil.executeHttp("/?pretty", ClientInterface.HTTP_GET);
		System.out.println(result);
	}
	/**
	 * 添加article索引数据
	 */
	@Test
	public void insertIndiceData() {
		try {
			clientInterface = bbossESStarter.getConfigRestClient(spanQueryDSLPath);//bboss读取xml
			ClientInterface restClient = ElasticSearchHelper.getRestClientUtil();//插入数据用RestClient
			ESInfo esInfo = clientInterface.getESInfo("bulkInsertArticleData");//获取插入数据
			StringBuilder recipedata = new StringBuilder();
			recipedata.append(esInfo.getTemplate().trim());
			recipedata.append("\n");
			restClient.executeHttp(spanQueryIndexName + "/_bulk?refresh", recipedata.toString(), ClientUtil.HTTP_POST);
		} catch (ElasticSearchException e) {
			logger.error(spanQueryIndexName + "插入数据失败，请检查错误日志",e);
			throw e;
		}
		long recipeCount = clientInterface.countAll(spanQueryIndexName);
		logger.info(spanQueryIndexName + "当前条数" + recipeCount);
	}

	/**
	 * 测试SpanTermQuery
	 */
	@Test
	public void testSpanTermQuery() {
		try {
			clientInterface = bbossESStarter.getConfigRestClient(spanQueryDSLPath);
			//封装请求参数
			Map<String, String> queryParams = new HashMap<String, String>(5);
			queryParams.put("spanTermValue", "red");
			//String queryResult = clientInterface.executeRequest(spanQueryIndexName + "/_search?search_type=dfs_query_then_fetch", "testSpanTermQuery",queryParams);
			//String resultJson = JSON.toJSONString(JSON.parseObject(queryResult), SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteDateUseDateFormat);
			//logger.info("testSpanTermQuery查询结果： ");
			//logger.info(resultJson);
			//Bboss执行查询，返回结果
			MapRestResponse testSpanTermQuery = clientInterface.search(spanQueryIndexName + "/_search?search_type=dfs_query_then_fetch", "testSpanTermQuery", queryParams);
			//ES返回结果遍历
			testSpanTermQuery.getSearchHits().getHits().forEach(searchHit -> {
				logger.info("\n文档ID： " + searchHit.getId() + "\n" + "文档_source" + searchHit.getSource().toString());
			});
		} catch (ElasticSearchException e) {
			logger.error("testSpanTermQuery 执行失败" , e);
			throw e;
		}
	}

	/**
	 * 测试SpanNearQuery
	 */
	@Test
	public void testSpanNearQuery() {
		try {
			clientInterface = bbossESStarter.getConfigRestClient(spanQueryDSLPath);
			//封装请求参数
			Map<String, String> queryParams = new HashMap<>(5);
			queryParams.put("spanTermValue1", "quick");
			queryParams.put("spanTermValue2", "brown");
			queryParams.put("slop", "0");
			MapRestResponse testSpanTermQuery = clientInterface.search(spanQueryIndexName + "/_search?search_type=dfs_query_then_fetch", "testSpanNearQuery", queryParams);
			//ES返回结果遍历
			testSpanTermQuery.getSearchHits().getHits().forEach(searchHit -> {
				logger.info("\n文档ID： " + searchHit.getId() + "\n" + "文档_source: " + searchHit.getSource().toString());
			});
		} catch (ElasticSearchException e) {
			logger.error("testSpanTermQuery 执行失败" , e);
			throw e;
		}
	}

	/**
	 * 测试SpanNotQuery
	 */
	@Test
	public void testSpanNotQuery() {
		try {
			clientInterface = bbossESStarter.getConfigRestClient(spanQueryDSLPath);
			//封装请求参数
			Map<String, String> queryParams = new HashMap<>(5);
			queryParams.put("spanTermValue1", "quick");
			queryParams.put("spanTermValue2", "fox");
			queryParams.put("slop", "1");
			queryParams.put("spanNotValue", "red");
			MapRestResponse testSpanTermQuery = clientInterface.search(spanQueryIndexName + "/_search?search_type=dfs_query_then_fetch", "testSpanNotQuery", queryParams);
			//ES返回结果遍历
			testSpanTermQuery.getSearchHits().getHits().forEach(searchHit -> {
				logger.info("\n文档ID： " + searchHit.getId() + "\n" + "文档_source: " + searchHit.getSource().toString());
			});
		} catch (ElasticSearchException e) {
			logger.error("testSpanTermQuery 执行失败" , e);
			throw e;
		}
	}

	/**
	 * 创建simple1索引
	 */
	@Test
	public void dropAndCreateSample1Indice() {
		try {
			clientInterface = bbossESStarter.getConfigRestClient(paragraphWordsDSLPath);//bboss读取xml
			/*检查索引是否存在，存在就删除重建*/
			if (clientInterface.existIndice(paragraphWordsIndexName1)) {
				clientInterface.dropIndice(paragraphWordsIndexName1);
			}

			Map<String, String> simpleParams = new HashMap<>();
			/*mapping创建分词器，带有特殊字符，使用Bboss传参方式处理*/
//			String simpleMapping =
//					"<h1> => \\u0020paragraph\\u0020\"\"\",\n" +
//					"\"\"\"</h1> => \\u0020sentence\\u0020paragraph\\u0020\"\"\",\n" +
//					"\"\"\"<h2> => \\u0020paragraph\\u0020\"\"\",\n" +
//					"\"\"\"</h2> => \\u0020sentence\\u0020paragraph\\u0020\"\"\",\n" +
//					"\"\"\"<p> => \\u0020paragraph\\u0020\"\"\",\n" +
//					"\"\"\"</p> => \\u0020sentence\\u0020paragraph\\u0020\"\"\",\n" +
//					"\"\"\"! => \\u0020sentence\\u0020\"\"\",\n" +
//					"\"\"\"? => \\u0020sentence\\u0020\"\"\",\n" +
//					"\"\"\"。=> \\u0020sentence\\u0020\"\"\",\n" +
//					"\"\"\"？=> \\u0020sentence\\u0020\"\"\",\n" +
//					"\"\"\"！=> \\u0020sentence";
//
//			simpleParams.put("sampleMappings", simpleMapping);
			/*传参，创建索引*/
			clientInterface.createIndiceMapping(paragraphWordsIndexName1, "createSample1Indice");
			logger.info("create indice: " + paragraphWordsIndexName1 + " is done");
			String mapping = clientInterface.getIndexMapping(paragraphWordsIndexName1);
			logger.info(mapping);

			String result = clientInterface.executeHttp(paragraphWordsIndexName1 + "/_analyze", "_analyzesample1", ClientUtil.HTTP_POST);
			logger.info(result);
		} catch (ElasticSearchException e) {
			logger.error("create indice faild: " + paragraphWordsIndexName1 , e);
		}
	}

	/**
	 * 添加simp1数据
	 */
	@Test
	public void insertSimple1IndiceData() {
		try {
			clientInterface = bbossESStarter.getConfigRestClient(paragraphWordsDSLPath);//bboss读取xml
			ClientInterface restClient = ElasticSearchHelper.getRestClientUtil();//插入数据用RestClient
			ESInfo esInfo = clientInterface.getESInfo("bulkSample1Data");//获取插入数据
			StringBuilder recipedata = new StringBuilder();
			recipedata.append(esInfo.getTemplate().trim());
			recipedata.append("\n");
			restClient.executeHttp(paragraphWordsIndexName1 + "/_bulk?refresh", recipedata.toString(), ClientUtil.HTTP_POST);
		} catch (ElasticSearchException e) {
			logger.error(spanQueryIndexName + "插入数据失败，请检查错误日志",e);
			throw e;
		}
		long recipeCount = clientInterface.countAll(spanQueryIndexName);
		logger.info(spanQueryIndexName + "当前条数" + recipeCount);
	}

	/**
	 * 测试HTML同段搜索
	 */
	@Test
	public void testHtmlParagraphQuery() {
		try {
			clientInterface = bbossESStarter.getConfigRestClient(paragraphWordsDSLPath);
			//封装请求参数
			Map<String, String> queryParams = new HashMap<>(5);
			queryParams.put("spanTermValue1", "java");
			queryParams.put("spanTermValue2", "javascript");
			queryParams.put("slop", "3");
			queryParams.put("queryType", "paragraph");
			MapRestResponse testSpanTermQuery = clientInterface.search(paragraphWordsIndexName1 + "/_search?search_type=dfs_query_then_fetch", "testParagraphQuery", queryParams);
			//ES返回结果遍历
			testSpanTermQuery.getSearchHits().getHits().forEach(searchHit -> {
				logger.info("\n文档ID： " + searchHit.getId() + "\n" + "文档_source: " + searchHit.getSource().toString());
			});
		} catch (ElasticSearchException e) {
			logger.error("testSpanTermQuery 执行失败" , e);
			throw e;
		}
	}

	/**
	 * 创建simple1索引
	 */
	@Test
	public void dropAndCreateSample2Indice() {
		try {
			clientInterface = bbossESStarter.getConfigRestClient(paragraphWordsDSLPath);//bboss读取xml
			/*检查索引是否存在，存在就删除重建*/
			if (clientInterface.existIndice(paragraphWordsIndexName2)) {
				clientInterface.dropIndice(paragraphWordsIndexName2);
			}

//			Map<String, String> simpleParams = new HashMap<>();
//			/*mapping创建分词器，带有特殊字符，使用Bboss传参方式处理*/
//			String simpleMapping = " \"\"\"\\n => \\u0020sentence\\u0020paragraph\\u0020 \"\"\",\n" +
//					"\"\"\"! => \\u0020sentence\\u0020 \"\"\",\n" +
//					"\"\"\"? => \\u0020sentence\\u0020 \"\"\",\n" +
//					"\"\"\"。=> \\u0020sentence\\u0020 \"\"\",\n" +
//					"\"\"\"？=> \\u0020sentence\\u0020 \"\"\",\n" +
//					"\"\"\"！=> \\u0020sentence\\u0020\"\"\"";
//
//			simpleParams.put("sampleMappings", simpleMapping);
			/*传参，创建索引*/
			clientInterface.createIndiceMapping(paragraphWordsIndexName2, "createSample2Indice");
			logger.info("create indice: " + paragraphWordsIndexName2 + " is done");
			String mapping = clientInterface.getIndexMapping(paragraphWordsIndexName2);
			logger.info(mapping);

			String result = clientInterface.executeHttp(paragraphWordsIndexName2 + "/_analyze", "_analyzesample2", ClientUtil.HTTP_POST);
			logger.info(result);
		} catch (ElasticSearchException e) {
			logger.error("create indice faild: " + paragraphWordsIndexName2 , e);
			throw e;
		}
	}

	/**
	 * 测试同段搜索
	 */
	@Test
	public void insertSimple2IndiceData() {
		try {
			clientInterface = bbossESStarter.getConfigRestClient(paragraphWordsDSLPath);//bboss读取xml
			ClientInterface restClient = ElasticSearchHelper.getRestClientUtil();//插入数据用RestClient
			ESInfo esInfo = clientInterface.getESInfo("bulkSample2Data");//获取插入数据
			StringBuilder recipedata = new StringBuilder();
			recipedata.append(esInfo.getTemplate().trim());
			recipedata.append("\n");
			restClient.executeHttp(paragraphWordsIndexName1 + "/_bulk?refresh", recipedata.toString(), ClientUtil.HTTP_POST);
		} catch (ElasticSearchException e) {
			logger.error(spanQueryIndexName + "插入数据失败，请检查错误日志",e);
			throw e;
		}
		long recipeCount = clientInterface.countAll(spanQueryIndexName);
		logger.info(spanQueryIndexName + "当前条数" + recipeCount);
	}

	/**
	 * 测试HTML同段搜索
	 */
	@Test
	public void testTextParagraphQuery() {
		try {
			clientInterface = bbossESStarter.getConfigRestClient(paragraphWordsDSLPath);
			//封装请求参数
			Map<String, String> queryParams = new HashMap<>(5);
			queryParams.put("spanTermValue1", "java");
			queryParams.put("spanTermValue2", "javascript");
			queryParams.put("slop", "3");
			queryParams.put("queryType", "paragraph");
			MapRestResponse testSpanTermQuery = clientInterface.search(paragraphWordsIndexName2 + "/_search?search_type=dfs_query_then_fetch", "testParagraphQuery", queryParams);
			//ES返回结果遍历
			testSpanTermQuery.getSearchHits().getHits().forEach(searchHit -> {
				logger.info("\n文档ID： " + searchHit.getId() + "\n" + "文档_source: " + searchHit.getSource().toString());
			});
		} catch (ElasticSearchException e) {
			logger.error("testSpanTermQuery 执行失败" , e);
			throw e;
		}
	}
}
