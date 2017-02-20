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

package com.twitter.heron.scheduler.yarn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.scheduler.SchedulerMain;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Cluster;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Environ;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.HeronCorePackageName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.HttpPort;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Role;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyJar;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyPackageName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.VerboseLogMode;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlan.ContainerPlan;
import com.twitter.heron.spi.packing.Resource;

/**
 * {@link HeronMasterDriver} is a topology's ApplicationMaster running on YARN clusters. It performs
 * container management for Heron's YARN Scheduler using REEF framework. This includes making
 * container request for heron instances, providing package needed to start Heron components,
 * monitoring the containers and killing containers when requested.
 * <p></p>
 * REEF libraries communicate with YARN RM on behalf of this AM. The communication between REEF and
 * {@link HeronMasterDriver} is event driven. {@link HeronMasterDriver} provides handlers for
 * some of the events generated by the framework, including
 * <ol>
 * <li>configuring newly allocated containers, {@link ContainerAllocationHandler}</li>
 * <li>launching Heron components in the containers {@link HeronWorkerLauncher}</li>
 * <li>handing errors/failures, {@link FailedContainerHandler} and
 * {@link HeronWorkerTaskFailureHandler}</li>
 * </ol>
 * <p></p>
 * On initialization {@link HeronMasterDriver} unpacks Heron package and starts the Heron scheduler.
 * It then receives a {@link PackingPlan} from the scheduler. It then submit new container requests
 * to the REEF framework and provides the desired container size. Some of the handlers listed above
 * are then invoked and heron components will be activated.
 * <p></p>
 * The containers allocated by YARN may be bigger than the requested size. This is a consequence of
 * a YARN scheduler optimization, which allocates resources in fixed memory and cpu increments. For
 * e.g. if the memory increment is 100 MB, YARN may allocated a container of 200 MB when 150 MB is
 * requested. As a result, the actual resource allocation may be little more than what was
 * requested. Moreover YARN does not provide any mechanism to match the container request to
 * allocated container. So the AM needs to find a suitable heron {@link ContainerPlan} to be
 * launched in the the newly allocated YARN container.
 */
@Unit
public class HeronMasterDriver {
  static final int TMASTER_CONTAINER_ID = 0;
  private static final Logger LOG = Logger.getLogger(HeronMasterDriver.class.getName());
  private final String topologyPackageName;
  private final String heronCorePackageName;
  private final EvaluatorRequestor requestor;
  private final REEFFileNames reefFileNames;
  private final String localHeronConfDir;
  private final String cluster;
  private final String role;
  private final String topologyName;
  private final String env;
  private final String topologyJar;
  private final int httpPort;
  private final boolean verboseMode;

  // This map contains all the workers this application will be managing. Some of these workers
  // may be allocated and running.
  private HashMap<Integer, ContainerPlan> containerPlans = new HashMap<>();
  // This map contains all the allocated workers managed by this scheduler. The workers can be
  // looked up by heron's executor id or REEF's container id.
  private MultiKeyWorkerMap multiKeyWorkerMap;

  private TMaster tMaster;

  // TODO: https://github.com/twitter/heron/issues/949: implement Driver HA

  private String componentRamMap;

  private AtomicBoolean isTopologyKilled = new AtomicBoolean(false);

  @Inject
  public HeronMasterDriver(EvaluatorRequestor requestor,
                           final REEFFileNames fileNames,
                           @Parameter(Cluster.class) String cluster,
                           @Parameter(Role.class) String role,
                           @Parameter(TopologyName.class) String topologyName,
                           @Parameter(Environ.class) String env,
                           @Parameter(TopologyJar.class) String topologyJar,
                           @Parameter(TopologyPackageName.class) String topologyPackageName,
                           @Parameter(HeronCorePackageName.class) String heronCorePackageName,
                           @Parameter(HttpPort.class) int httpPort,
                           @Parameter(VerboseLogMode.class) boolean verboseMode)
      throws IOException {

    // REEF related initialization
    this.requestor = requestor;
    this.reefFileNames = fileNames;

    // Heron related initialization
    this.localHeronConfDir = ".";
    this.cluster = cluster;
    this.role = role;
    this.topologyName = topologyName;
    this.topologyPackageName = topologyPackageName;
    this.heronCorePackageName = heronCorePackageName;
    this.env = env;
    this.topologyJar = topologyJar;
    this.httpPort = httpPort;
    this.verboseMode = verboseMode;
    this.multiKeyWorkerMap = new MultiKeyWorkerMap();

    // This instance of Driver will be used for managing topology containers
    HeronMasterDriverProvider.setInstance(this);
  }

  private static int getCpuForExecutor(Resource resource) {
    return (int) Math.ceil(resource.getCpu());
  }

  /**
   * Container allocation is asynchronous. Requests all containers in the input packing plan
   * serially to ensure allocated resources match the required resources.
   */
  void scheduleHeronWorkers(PackingPlan topologyPacking) throws ContainerAllocationException {
    this.componentRamMap = topologyPacking.getComponentRamDistribution();
    scheduleHeronWorkers(topologyPacking.getContainers());
  }

  /*
   * Must be invoked after workers are scheduled. TMaster needs component ram map.
   */
  void launchTMaster() {
    tMaster = buildTMaster(Executors.newSingleThreadExecutor());
    tMaster.launch();
  }

  /**
   * Container allocation is asynchronous. Requests all containers in the input set serially
   * to ensure allocated resources match the required resources.
   */
  void scheduleHeronWorkers(Set<ContainerPlan> containers) throws ContainerAllocationException {
    for (ContainerPlan containerPlan : containers) {
      int id = containerPlan.getId();
      if (containerPlans.containsKey(containerPlan.getId())) {
        throw new ContainerAllocationException("Received duplicate allocation request for " + id);
      }
      Resource reqResource = containerPlan.getRequiredResource();
      containerPlans.put(id, containerPlan);
      requestContainerForWorker(id, new HeronWorker(id, reqResource));
    }
  }

  /**
   * YARN allocates resources in fixed increments of memory and cpu. As a result, the actual
   * resource allocation may be little more than what was requested. This method finds the biggest
   * heron containerPlan that will fit the allocated YARN container. In some cases the YARN CPU
   * scheduling may be disabled, resulting in default core allocation to each container. This
   * method can ignore core fitting in such a case.
   */
  @VisibleForTesting
  Optional<HeronWorker> findLargestFittingWorker(AllocatedEvaluator evaluator,
                                                 Collection<HeronWorker> pendingWorkers,
                                                 boolean ignoreCpu) {
    ByteAmount allocatedRam
        = ByteAmount.fromMegabytes(evaluator.getEvaluatorDescriptor().getMemory());
    int allocatedCores = evaluator.getEvaluatorDescriptor().getNumberOfCores();

    HeronWorker biggestFittingWorker = null;
    for (HeronWorker worker : pendingWorkers) {
      if (worker.mem.greaterThan(allocatedRam)) {
        continue;
      }

      if (!ignoreCpu) {
        if (worker.cores > allocatedCores) {
          continue;
        }
      }

      if (biggestFittingWorker != null) {
        if (worker.mem.lessThan(biggestFittingWorker.mem)
            || worker.cores < biggestFittingWorker.cores) {
          continue;
        }
      }
      biggestFittingWorker = worker;
    }

    return Optional.fromNullable(biggestFittingWorker);
  }

  @VisibleForTesting
  Set<HeronWorker> getWorkersAwaitingAllocation() {
    Set<HeronWorker> workersAwaitingAllocation = new HashSet<>();
    for (Integer id : containerPlans.keySet()) {
      if (multiKeyWorkerMap.lookupByWorkerId(id).isPresent()) {
        // this container plan is already allocated to a container
        continue;
      }
      workersAwaitingAllocation
          .add(new HeronWorker(id, containerPlans.get(id).getRequiredResource()));
    }
    LOG.info("Number of workers awaiting allocation: " + workersAwaitingAllocation.size());
    return workersAwaitingAllocation;
  }

  public void killTopology() {
    LOG.log(Level.INFO, "Kill topology: {0}", topologyName);
    isTopologyKilled.set(true);

    tMaster.killTMaster();

    for (HeronWorker worker : multiKeyWorkerMap.getHeronWorkers()) {
      AllocatedEvaluator evaluator = multiKeyWorkerMap.detachEvaluatorAndRemove(worker);
      LOG.log(Level.INFO, "Killing container {0} for worker {1}",
          new Object[]{evaluator.getId(), worker.workerId});
      evaluator.close();
    }
  }

  /**
   * Terminates any yarn containers associated with the given containers.
   */
  public void killWorkers(Set<ContainerPlan> containers) {
    for (ContainerPlan container : containers) {
      LOG.log(Level.INFO, "Find and kill container for worker {0}", container.getId());
      Optional<HeronWorker> worker = multiKeyWorkerMap.lookupByWorkerId(container.getId());
      if (worker.isPresent()) {
        LOG.log(Level.INFO, "Killing container {0} for worker {1}",
            new Object[]{worker.get().evaluator.getId(), worker.get().workerId});
        AllocatedEvaluator evaluator = multiKeyWorkerMap.detachEvaluatorAndRemove(worker.get());
        evaluator.close();
      } else {
        LOG.log(Level.WARNING, "Did not find worker for {0}", container.getId());
      }
      containerPlans.remove(container.getId());
    }
  }

  public void restartTopology() throws ContainerAllocationException {
    for (HeronWorker worker : multiKeyWorkerMap.getHeronWorkers()) {
      restartWorker(worker.workerId);
    }
  }

  public void restartWorker(int id) throws ContainerAllocationException {
    LOG.log(Level.INFO, "Find & restart container for id={0}", id);

    Optional<HeronWorker> worker = multiKeyWorkerMap.lookupByWorkerId(id);
    if (!worker.isPresent()) {
      LOG.log(Level.WARNING, "Requesting a new container for: {0}", id);
      ContainerPlan containerPlan = containerPlans.get(id);
      if (containerPlan == null) {
        throw new IllegalArgumentException(
            String.format("There is no container for %s in packing plan.", id));
      }
      worker = Optional.of(new HeronWorker(id, containerPlan.getRequiredResource()));
    } else {
      AllocatedEvaluator evaluator = multiKeyWorkerMap.detachEvaluatorAndRemove(worker.get());
      LOG.log(Level.INFO, "Shutting down container {0}", evaluator.getId());
      evaluator.close();
    }

    requestContainerForWorker(worker.get().workerId, worker.get());
  }

  @VisibleForTesting
  void requestContainerForWorker(int id, final HeronWorker worker) {
    int cpu = worker.cores;
    ByteAmount mem = worker.mem;
    EvaluatorRequest evaluatorRequest = createEvaluatorRequest(cpu, mem);
    LOG.info(String.format("Requesting container for worker: %d, mem: %s, cpu: %d", id, mem, cpu));
    requestor.submit(evaluatorRequest);
  }

  @VisibleForTesting
  EvaluatorRequest createEvaluatorRequest(int cpu, ByteAmount mem) {
    return EvaluatorRequest
        .newBuilder()
        .setNumber(1)
        .setMemory(((Long) mem.asMegabytes()).intValue())
        .setNumberOfCores(cpu)
        .build();
  }

  @VisibleForTesting
  Configuration createContextConfig(int executorId) {
    return ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, executorId + "")
        .build();
  }

  String getComponentRamMap() {
    return componentRamMap;
  }

  void submitHeronExecutorTask(int workerId) {
    Optional<HeronWorker> worker = multiKeyWorkerMap.lookupByWorkerId(workerId);
    if (!worker.isPresent()) {
      return;
    }

    LOG.log(Level.INFO, "Submitting evaluator task for id: {0}", workerId);

    // topologyName and other configurations are required by Heron Executor and Task to load
    // configuration files. Using REEF configuration model is better than depending on external
    // persistence.
    final Configuration taskConf = HeronTaskConfiguration.CONF
        .set(TaskConfiguration.TASK, HeronExecutorTask.class)
        .set(TaskConfiguration.ON_CLOSE, HeronExecutorTask.HeronExecutorTaskTerminator.class)
        .set(TaskConfiguration.IDENTIFIER, workerId + "")
        .set(HeronTaskConfiguration.TOPOLOGY_NAME, topologyName)
        .set(HeronTaskConfiguration.TOPOLOGY_JAR, topologyJar)
        .set(HeronTaskConfiguration.TOPOLOGY_PACKAGE_NAME, topologyPackageName)
        .set(HeronTaskConfiguration.HERON_CORE_PACKAGE_NAME, heronCorePackageName)
        .set(HeronTaskConfiguration.ROLE, role)
        .set(HeronTaskConfiguration.ENV, env)
        .set(HeronTaskConfiguration.CLUSTER, cluster)
        .set(HeronTaskConfiguration.COMPONENT_RAM_MAP, getComponentRamMap())
        .set(HeronTaskConfiguration.CONTAINER_ID, workerId)
        .set(HeronTaskConfiguration.VERBOSE, verboseMode)
        .build();
    worker.get().context.submitTask(taskConf);
  }

  @VisibleForTesting
  Optional<Integer> lookupByEvaluatorId(String id) {
    Optional<HeronWorker> result = multiKeyWorkerMap.lookupByEvaluatorId(id);
    if (!result.isPresent()) {
      return Optional.absent();
    }

    return Optional.of(result.get().workerId);
  }

  @VisibleForTesting
  Optional<ContainerPlan> lookupByContainerPlan(int id) {
    return Optional.fromNullable(containerPlans.get(id));
  }

  @VisibleForTesting
  TMaster buildTMaster(ExecutorService executor) {
    TMaster tMasterManager = new TMaster();
    tMasterManager.executor = executor;
    return tMasterManager;
  }

  /**
   * {@link HeronWorker} is a data class which connects reef ids, heron ids and related objects.
   * All the pointers in an instance are related to one container. A container is a reef object,
   * owns one heron worker id and its handlers
   */
  @VisibleForTesting
  static final class HeronWorker {
    private int workerId;
    private int cores;
    private ByteAmount mem;

    private AllocatedEvaluator evaluator;
    private ActiveContext context;

    HeronWorker(int id, int cores, ByteAmount mem) {
      this.workerId = id;
      this.cores = cores;
      this.mem = mem;
    }

    HeronWorker(int id, Resource resource) {
      this.workerId = id;
      this.cores = getCpuForExecutor(resource);
      this.mem = resource.getRam();
    }

    public int getWorkerId() {
      return workerId;
    }
  }

  /**
   * {@link MultiKeyWorkerMap} is a helper class to provide multi key lookup of a
   * {@link HeronWorker} instance. It also ensures thread safety and update order.
   */
  private static final class MultiKeyWorkerMap {
    private Map<Integer, HeronWorker> workerMap = new HashMap<>();
    private Map<String, HeronWorker> evaluatorWorkerMap = new HashMap<>();

    void assignEvaluatorToWorker(HeronWorker worker, AllocatedEvaluator evaluator) {
      worker.evaluator = evaluator;
      synchronized (workerMap) {
        workerMap.put(worker.workerId, worker);
        evaluatorWorkerMap.put(evaluator.getId(), worker);
      }
    }

    Optional<HeronWorker> lookupByEvaluatorId(String evaluatorId) {
      synchronized (workerMap) {
        return Optional.fromNullable(evaluatorWorkerMap.get(evaluatorId));
      }
    }

    Optional<HeronWorker> lookupByWorkerId(int workerId) {
      HeronWorker worker;
      synchronized (workerMap) {
        worker = workerMap.get(workerId);
      }
      if (worker == null) {
        LOG.log(Level.INFO, "Container for executor id: {0} not found.", workerId);
      }
      return Optional.fromNullable(worker);
    }

    AllocatedEvaluator detachEvaluatorAndRemove(HeronWorker worker) {
      synchronized (workerMap) {
        workerMap.remove(worker.workerId);
        evaluatorWorkerMap.remove(worker.evaluator.getId());
      }
      AllocatedEvaluator evaluator = worker.evaluator;
      worker.evaluator = null;
      return evaluator;
    }

    ArrayList<HeronWorker> getHeronWorkers() {
      synchronized (workerMap) {
        return new ArrayList<>(workerMap.values());
      }
    }
  }

  /**
   * {@link ContainerAllocationException} represents an error while trying to allocate a
   * container
   */
  public static final class ContainerAllocationException extends Exception {
    static final long serialVersionUID = 1L;

    public ContainerAllocationException(String message) {
      this(message, null);
    }

    public ContainerAllocationException(String message, Exception e) {
      super(message, e);
    }
  }

  /**
   * This class manages the TMaster executor process, including launching the TMaster, monitoring it
   * and killing it when needed.
   */
  @VisibleForTesting
  class TMaster implements Runnable {
    private ExecutorService executor;
    private Future<?> tMasterFuture;
    private CountDownLatch tMasterErrorCounter = new CountDownLatch(3);

    void launch() {
      LOG.log(Level.INFO, "Launching executor for TM: {0}", topologyName);

      tMasterFuture = executor.submit(this);

      // the following task will restart the tMaster if it fails
      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            tMasterFuture.get();
            LOG.log(Level.INFO, "TMaster executor terminated, {0}", topologyName);
          } catch (InterruptedException | ExecutionException e) {
            LOG.log(Level.WARNING, "Error while waiting for TMaster executor", e);
          }

          if (isTopologyKilled.get()) {
            LOG.log(Level.INFO, "The topology is killed, {0}", topologyName);
            return;
          }

          tMasterErrorCounter.countDown();
          long counter = tMasterErrorCounter.getCount();
          if (counter > 0) {
            LOG.log(Level.WARNING, "Restarting TMaster, attempts left: {0}", counter);
            launch();
          }
        }
      });
    }

    void killTMaster() {
      LOG.log(Level.INFO, "Killing TMaster process: {0}", topologyName);
      if (!tMasterFuture.isDone()) {
        tMasterFuture.cancel(true);
      }
      executor.shutdownNow();
    }

    HeronExecutorTask getTMasterExecutorTask() {
      return new HeronExecutorTask(reefFileNames, TMASTER_CONTAINER_ID,
          cluster, role, topologyName, env, topologyPackageName, heronCorePackageName, topologyJar,
          getComponentRamMap(), verboseMode);
    }

    @Override
    public void run() {
      HeronExecutorTask tMasterTask = getTMasterExecutorTask();
      tMasterTask.startExecutor();
    }
  }

  /**
   * {@link HeronSchedulerLauncher} is the first class initialized on the server by REEF. This is
   * responsible for unpacking binaries and launching Heron Scheduler.
   */
  class HeronSchedulerLauncher implements EventHandler<StartTime> {
    @Override
    public void onNext(StartTime value) {
      String globalFolder = reefFileNames.getGlobalFolder().getPath();

      HeronReefUtils.extractPackageInSandbox(globalFolder, topologyPackageName, localHeronConfDir);
      HeronReefUtils.extractPackageInSandbox(globalFolder, heronCorePackageName, localHeronConfDir);

      launchScheduler();
    }

    private void launchScheduler() {
      try {
        LOG.log(Level.INFO, "Launching Heron scheduler");
        SchedulerMain schedulerMain = SchedulerMain.createInstance(cluster,
            role,
            env,
            topologyJar,
            topologyName,
            httpPort,
            false);
        schedulerMain.runScheduler();
      } catch (IOException e) {
        throw new RuntimeException("Failed to launch Heron Scheduler", e);
      }
    }
  }

  /**
   * Initializes worker on the allocated container
   */
  class ContainerAllocationHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(AllocatedEvaluator evaluator) {
      EvaluatorDescriptor descriptor = evaluator.getEvaluatorDescriptor();
      LOG.log(Level.INFO, String.format("New container received, id: %s, mem: %d, cores: %d",
          evaluator.getId(), descriptor.getMemory(), descriptor.getNumberOfCores()));

      Optional<HeronWorker> result;
      HeronWorker worker;
      synchronized (containerPlans) {
        Set<HeronWorker> workersAwaitingAllocation = getWorkersAwaitingAllocation();

        if (workersAwaitingAllocation.isEmpty()) {
          LOG.log(Level.INFO, "Could not find any workers waiting for allocation, closing {0}",
              evaluator.getId());
          evaluator.close();
          return;
        }

        result = findLargestFittingWorker(evaluator, workersAwaitingAllocation, true);
        if (!result.isPresent()) {
          LOG.warning("Could not find a fitting worker in awaiting workers");
          // TODO may need counting of missed allocation
          evaluator.close();
          return;
        }

        worker = result.get();
        LOG.info(String.format("Worker:%d, cores:%d, mem:%s fits in the allocated container",
            worker.workerId, worker.cores, worker.mem));
        workersAwaitingAllocation.remove(worker);
        multiKeyWorkerMap.assignEvaluatorToWorker(worker, evaluator);
      }

      LOG.log(Level.INFO, "Activating container {0} for heron worker, id: {1}",
          new Object[]{evaluator.getId(), worker.workerId});
      Configuration context = createContextConfig(worker.workerId);
      evaluator.submitContext(context);
    }
  }

  /**
   * Initializes worker on the allocated container
   */
  class FailedContainerHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(FailedEvaluator evaluator) {
      LOG.log(Level.WARNING, "Container:{0} failed", evaluator.getId());
      Optional<HeronWorker> worker = multiKeyWorkerMap.lookupByEvaluatorId(evaluator.getId());
      if (!worker.isPresent()) {
        LOG.log(Level.WARNING,
            "Unknown executor id for failed container: {0}, skip renew action",
            evaluator.getId());
        return;
      }
      LOG.log(Level.INFO, "Trying to relaunch worker {0} running on failed container {1}",
          new Object[]{worker.get().workerId, evaluator.getId()});
      multiKeyWorkerMap.detachEvaluatorAndRemove(worker.get());

      requestContainerForWorker(worker.get().workerId, worker.get());
    }
  }

  /**
   * Once the container starts, this class starts Heron's executor process. Heron executor is
   * started as a task. This task can be killed and the container can be reused.
   */
  public final class HeronWorkerLauncher implements EventHandler<ActiveContext> {
    @Override
    public void onNext(ActiveContext context) {
      if (isTopologyKilled.get()) {
        LOG.log(Level.WARNING, "Topology has been killed, close new context: {0}", context.getId());
        context.close();
        return;
      }

      int workerId = Integer.valueOf(context.getId());
      Optional<HeronWorker> worker = multiKeyWorkerMap.lookupByWorkerId(workerId);
      if (!worker.isPresent()) {
        context.close();
        return;
      }

      worker.get().context = context;
      submitHeronExecutorTask(workerId);
    }
  }

  /**
   * This class manages active tasks. The task handlers provided by REEF will be memorized to be
   * used later for operations like topology restart. Restarting a task does not require new
   * container request.
   */
  public final class HeronWorkerStartHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(RunningTask runningTask) {
      LOG.log(Level.INFO, "Task, id:{0}, has started.", runningTask.getId());
    }
  }

  public final class HeronWorkerTaskFailureHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(FailedTask failedTask) {
      LOG.log(Level.WARNING, "Task {0} failed. Relaunching the task", failedTask.getId());
      if (isTopologyKilled.get()) {
        LOG.info("The topology is killed. Ignore task fail event");
        return;
      }

      submitHeronExecutorTask(Integer.valueOf(failedTask.getId()));
    }
  }

  public final class HeronWorkerTaskCompletedErrorHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(CompletedTask task) {
      LOG.log(Level.INFO, "Task {0} completed.", task.getId());
      if (isTopologyKilled.get()) {
        LOG.info("The topology is killed. Ignore task complete event");
        return;
      }
      LOG.log(Level.WARNING, "Task should not complete, relaunching {0}", task.getId());
      submitHeronExecutorTask(Integer.valueOf(task.getId()));
    }
  }
}
