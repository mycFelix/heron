//  Copyright 2016 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package org.apache.storm.metric.api.rpc;

import org.apache.storm.metric.api.IMetric;

public interface IShellMetric extends IMetric {
    /***
     * @function
     *     This interface is used by ShellBolt and ShellSpout through RPC call to update Metric 
     * @param
     *     value used to update metric, its's meaning change according implementation
     *     Object can be any json support types: String, Long, Double, Boolean, Null, List, Map
     * */
    public void updateMetricFromRPC(Object value);
}
