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

import com.example.esbboss.service.AutoschedulePauseDataTran;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
public class ScheduleControlDataTranController {
	@Autowired
	@Qualifier("autoschedulePauseDataTran")
	private AutoschedulePauseDataTran dataTran;

	/**
	 * 启动db-es同步作业
	 * @return
	 */
	@RequestMapping("/schedulecontrol/scheduleDB2ESJob")
	public @ResponseBody
	String scheduleDB2ESJob(boolean autoPause){
		return dataTran.scheduleDB2ESJob(autoPause);
	}

	/**
	 * 停止db-es同步作业
	 * @return
	 */
	@RequestMapping("/schedulecontrol/stopDB2ESJob")
	public @ResponseBody String stopDB2ESJob(){
		return dataTran.stopDB2ESJob();
	}

	/**
	 * 暂停调度db-es同步作业
	 * @return
	 */
	@RequestMapping("/schedulecontrol/pauseScheduleDB2ESJob")
	public @ResponseBody String pauseScheduleDB2ESJob(){
		return dataTran.pauseScheduleDB2ESJob();
	}

	/**
	 * 继续调度db-es同步作业
	 * @return
	 */
	@RequestMapping("/schedulecontrol/resumeScheduleDB2ESJob")
	public @ResponseBody String resumeScheduleDB2ESJob(){
		return dataTran.resumeScheduleDB2ESJob();
	}

	/**
	 * 启动hbase-es同步作业
	 * @return
	 */
	@RequestMapping("/schedulecontrol/scheduleHBase2ESJob")
	public @ResponseBody
	String scheduleHBase2ESJob(){
		return dataTran.scheduleHBase2ESJob();
	}

	/**
	 * 停止作业
	 * @return
	 */
	@RequestMapping("/schedulecontrol/stopHBase2ESJob")
	public @ResponseBody String stopHBase2ESJob(){
		return dataTran.stopHBase2ESJob();
	}

	/**
	 * 暂停调度hbase-es同步作业
	 * @return
	 */
	@RequestMapping("/schedulecontrol/pauseScheduleHBase2ESJob")
	public @ResponseBody String pauseScheduleHBase2ESJob(){
		return dataTran.pauseScheduleHBase2ESJob();
	}

	/**
	 * 继续调度hbase-es同步作业
	 * @return
	 */
	@RequestMapping("/schedulecontrol/resumeScheduleHBase2ESJob")
	public @ResponseBody String resumeScheduleHBase2ESJob(){
		return dataTran.resumeScheduleHBase2ESJob();
	}


	/**
	 * 启动file-es同步作业
	 * @return
	 */
	@RequestMapping("/schedulecontrol/startfile2es")
	public @ResponseBody
	String startfile2es(boolean autoPause){
		return dataTran.startfile2es( autoPause);
	}

	/**
	 * 停止作业
	 * @return
	 */
	@RequestMapping("/schedulecontrol/stopfile2es")
	public @ResponseBody String stopfile2es(){
		return dataTran.stopfile2es();
	}

	/**
	 * 暂停调度hbase-es同步作业
	 * @return
	 */
	@RequestMapping("/schedulecontrol/pauseFile2es")
	public @ResponseBody String pauseFile2es(){
		return dataTran.pauseFile2es();
	}

	/**
	 * 继续调度hbase-es同步作业
	 * @return
	 */
	@RequestMapping("/schedulecontrol/resumeFile2es")
	public @ResponseBody String resumeFile2es(){
		return dataTran.resumeFile2es();
	}
}
