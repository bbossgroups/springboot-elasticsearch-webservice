package com.example.esbboss.service;
/**
 * Copyright 2023 bboss
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

import org.frameworkset.spi.assemble.PropertiesInterceptor;
import org.frameworkset.spi.assemble.PropertyContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/9/11
 * @author biaoping.yin
 * @version 1.0
 */
public class TestPropertiesInterceptor implements PropertiesInterceptor {
    @Override
    public Object convert(PropertyContext propertyContext) {
        if(propertyContext.getProperty() == null){
            return propertyContext.getValue();
        }
        //对加密口令进行解密处理，根据实际加密算法采用特定的解密算法即可
        else if(propertyContext.getProperty().equals("elasticPassword")
                || propertyContext.getProperty().equals("http.authPassword")) {
//            BasicTextEncryptor passwordEncrypt = new BasicTextEncryptor();
//            // 设置salt值，可随意定义
//            passwordEncrypt.setPassword("dvvm");
//            return passwordEncrypt.decrypt(String.valueOf(propertyContext.getValue()));
            return propertyContext.getValue();
        }
        else if(propertyContext.getProperty().equals("http.authAccount")
                || propertyContext.getProperty().equals("elasticUser")) {
            return propertyContext.getValue();
        }

        return propertyContext.getValue();
    }
}
