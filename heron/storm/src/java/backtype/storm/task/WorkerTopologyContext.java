// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backtype.storm.task;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class WorkerTopologyContext extends GeneralTopologyContext {
  private com.twitter.heron.api.topology.TopologyContext delegate_;
    
  public WorkerTopologyContext(
          StormTopology topology,
          Map stormConf,
          Map<Integer, String> taskToComponent,
          Map<String, List<Integer>> componentToSortedTasks,
          Map<String, Map<String, Fields>> componentToStreamToFields,
          String stormId,
          String codeDir,
          String pidDir,
          Integer workerPort,
          List<Integer> workerTasks,
          Map<String, Object> defaultResources,
          Map<String, Object> userResources
          ) {
      super(topology, stormConf, taskToComponent, componentToSortedTasks, componentToStreamToFields, stormId);
      throw new RuntimeException("WorkerTopologyContext should never be init this way");
    }

  public WorkerTopologyContext(com.twitter.heron.api.topology.TopologyContext _delegate) {
    super(_delegate);
    this.delegate_ = _delegate;
  }

  /**
   * Gets all the task ids that are running in this worker process
   * (including the task for this task).
   * In Heron parlance, since every instance is running as a seperate process
   * this will just return the current instance's taskId
   */
  public List<Integer> getThisWorkerTasks() {
    List<Integer> retval = new LinkedList<Integer>();
    retval.add(delegate_.getThisTaskId());
    return retval;
  }
    
  /**
   * Return the port that the worker is running on.
   * This was typically done to differentiate workers running
   * on the same machine.
   * In Heron parlance, we just return the taskId since that
   * should be unique
   */
  public Integer getThisWorkerPort() {
    return delegate_.getThisTaskId();
  }

  /**
   * Gets the location of the external resources for this worker on the
   * local filesystem. These external resources typically include bolts implemented
   * in other languages, such as Ruby or Python.
   */
  public String getCodeDir() {
    throw new RuntimeException("Not supported");
  }

  /**
   * If this task spawns any subprocesses, those subprocesses must immediately
   * write their PID to this directory on the local filesystem to ensure that
   * Storm properly destroys that process when the worker is shutdown.
   */
  public String getPIDDir() {
    throw new RuntimeException("Not supported");
  }
    
  public Object getResource(String name) {
    throw new RuntimeException("Not supported");
  }
    
  public ExecutorService getSharedExecutor() {
    throw new RuntimeException("Not supported");
  }
}
