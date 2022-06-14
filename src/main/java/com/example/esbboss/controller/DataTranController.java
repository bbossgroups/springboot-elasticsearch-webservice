package com.example.esbboss.controller;
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

import com.example.esbboss.service.DataTran;
import com.example.esbboss.service.NewES2ESScrollTimestampMultiJobDemo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2020/1/5 12:14
 * @author biaoping.yin
 * @version 1.0
 */
@RestController
public class DataTranController {
	@Autowired
	private DataTran dataTran;
	@Autowired
	private NewES2ESScrollTimestampMultiJobDemo newES2ESScrollTimestampMultiJobDemo;


	/**
	 * 启动db-es同步作业
	 * @return
	 */
	@RequestMapping("/scheduleMultiES2ESJob")
	public @ResponseBody
	String scheduleMultiES2ESJob(){
		newES2ESScrollTimestampMultiJobDemo.mulitExecute();
		return "scheduleMultiES2ESJob end";
	}

	/**
	 * 启动db-es同步作业
	 * @return
	 */
	@RequestMapping("/scheduleDB2ESJob")
	public @ResponseBody
	String scheduleDB2ESJob(){
		return dataTran.scheduleDB2ESJob();
	}

	/**
	 * 停止db-es同步作业
	 * @return
	 */
	@RequestMapping("/stopDB2ESJob")
	public @ResponseBody String stopDB2ESJob(){
		return dataTran.stopDB2ESJob();
	}

	/**
	 * 启动hbase-es同步作业
	 * @return
	 */
	@RequestMapping("/scheduleHBase2ESJob")
	public @ResponseBody
	String scheduleHBase2ESJob(){
		return dataTran.scheduleHBase2ESJob();
	}

	/**
	 * 停止作业
	 * @return
	 */
	@RequestMapping("/stopHBase2ESJob")
	public @ResponseBody String stopHBase2ESJob(){
		return dataTran.stopHBase2ESJob();
	}
}
