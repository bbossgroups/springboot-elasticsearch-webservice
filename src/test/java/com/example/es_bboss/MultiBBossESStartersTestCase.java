/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.es_bboss;


import org.frameworkset.elasticsearch.boot.BBossESStarter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * 多集群演示功能测试用例，spring boot配置项以spring.elasticsearch.bboss.集群名称开头，例如：
 * spring.elasticsearch.bboss.default 默认es集群
 * spring.elasticsearch.bboss.logs  logs es集群
 * 两个集群通过 com.example.esbboss.MultiESSTartConfigurer加载application.yml中的配置并初始化其中的es集群组件实例
 * @author yinbp [122054810@qq.com]
 */
@RunWith(SpringRunner.class)
@SpringBootTest
//@ActiveProfiles("multi-datasource")
public class MultiBBossESStartersTestCase {
	@Autowired
	private BBossESStarter bbossESStarter;
//	@Autowired BBossESStarter bbossESStarterLogs;
    @Test
    public void testMultiBBossESStarters() throws Exception {

		//验证环境,获取es状态
//		String response = bbossESStarter.getRestClient().executeHttp("_cluster/state?pretty",ClientInterface.HTTP_GET);
//		System.out.println(response);


		//判断索引类型是否存在，false表示不存在，正常返回true表示存在
		boolean exist = bbossESStarter.getRestClient().existIndiceType("twitter","tweet");
		System.out.println("default twitter/tweet:"+exist);
		//获取logs对应的Elasticsearch集群客户端，并进行existIndiceType操作
		exist = bbossESStarter.getRestClient("logs").existIndiceType("twitter","tweet");
		System.out.println("logs twitter/tweet:"+exist);
		//获取logs对应的Elasticsearch集群客户端，判读索引是否存在，false表示不存在，正常返回true表示存在
		exist = bbossESStarter.getRestClient("logs").existIndice("twitter");
		System.out.println("logs  twitter:"+exist);
		//获取logs对应的Elasticsearch集群客户端，判断索引是否定义
		exist = bbossESStarter.getRestClient("logs").existIndice("agentinfo");
		System.out.println("logs agentinfo:"+exist);
    }

}
