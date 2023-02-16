package com.example.esbboss.entity;
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.entity.TimeMetric;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/2/15
 * @author biaoping.yin
 * @version 1.0
 */
public class LoginUserMetric extends TimeMetric {
    private String logUser;
    @Override
    public void init(MapData firstData) {
        CommonRecord data = (CommonRecord) firstData.getData();
        logUser = (String) data.getData("logOperuser");
    }

    @Override
    public void incr(MapData data) {
        count ++;
    }

    public String getLogUser() {
        return logUser;
    }
}
