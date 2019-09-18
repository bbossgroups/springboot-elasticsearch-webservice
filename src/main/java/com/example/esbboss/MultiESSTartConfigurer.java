package com.example.esbboss;
/*
 *  Copyright 2008 biaoping.yin
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

import org.frameworkset.elasticsearch.boot.BBossESStarter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * 多集群演示功能测试用例，spring boot配置项以spring.elasticsearch.bboss.集群名称开头，例如：
 * spring.elasticsearch.bboss.default 默认es集群
 * spring.elasticsearch.bboss.logs  logs es集群
 * 两个集群通过 com.example.esbboss.MultiESSTartConfigurer加载application.yml中的配置并初始化其中的es集群组件实例
 * @author yinbp [122054810@qq.com]
 */
@Configuration
public class MultiESSTartConfigurer {
	@Primary
	@Bean(initMethod = "start")
	@ConfigurationProperties("spring.elasticsearch.bboss.default")
	public BBossESStarter bbossESStarter(){
		return new BBossESStarter();

	}

	@Bean(initMethod = "start")
	@ConfigurationProperties("spring.elasticsearch.bboss.logs")
	public BBossESStarter bbossESStarterLogs(){
		return new BBossESStarter();
	}
}
