package com.example.esbboss.controller;


import com.example.esbboss.entity.DemoSearchResult;
import com.example.esbboss.service.DocumentCRUD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class EsController {
    @Autowired
    private DocumentCRUD documentCRUD;
    @RequestMapping("/testBBossCrud")
    public @ResponseBody
    DemoSearchResult testBBossCrud()  {
        documentCRUD.dropAndCreateAndGetIndice();
        documentCRUD.addAndUpdateDocument();
        DemoSearchResult demoSearchResult = documentCRUD.search();
        documentCRUD.searchAllPararrel();
        documentCRUD.deleteDocuments();
        return demoSearchResult;
    }

}
