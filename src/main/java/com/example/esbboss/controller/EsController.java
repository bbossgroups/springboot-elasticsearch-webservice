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
import com.example.esbboss.service.DocumentCRUD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yinbp[yin-bp@163.com]
 */
@RestController
public class EsController {
    private Logger logger = LoggerFactory.getLogger(EsController.class);

    @Autowired
    private DocumentCRUD documentCRUD;
    @RequestMapping("/health")
    public @ResponseBody String health() {
        return "ok";
    }
    @RequestMapping("/testBBossIndexCrud")
    public @ResponseBody
    DemoSearchResult testBBossIndexCrud()  {
        documentCRUD.dropAndCreateAndGetIndice();
        documentCRUD.addAndUpdateDocument();
        DemoSearchResult demoSearchResult = documentCRUD.search();
        documentCRUD.searchAllPararrel();
//        documentCRUD.deleteDocuments();
        return demoSearchResult;
    }

    @RequestMapping("/testBBossSearch")
    public @ResponseBody
    DemoSearchResult testBBossSearch()  {
//        documentCRUD.dropAndCreateAndGetIndice();
//        documentCRUD.addAndUpdateDocument();
        try {
            DemoSearchResult demoSearchResult = documentCRUD.search();
//        documentCRUD.searchAllPararrel();
//        documentCRUD.deleteDocuments();
            return demoSearchResult;
        }
        catch (Exception e){
            logger.error("",e);
            throw  e;
        }
    }
    @RequestMapping("/scheduleDB2ESJob")
    public @ResponseBody String scheduleDB2ESJob(){
        return documentCRUD.scheduleDB2ESJob();
    }

}
