package kitade.coins;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

public class ApplicationMasterAsync extends AMRMClientAsync.AbstractCallbackHandler {

    private final Configuration conf;
    private final NMClient nmClient;
    private final CLCUtils util;
    private final String jarPath;
    private final int countContainer;
    private int countContainerFinish = 0;
    
    public ApplicationMasterAsync(String jarPath, int countContainer) {
        this.conf = new YarnConfiguration();
        this.nmClient = NMClient.createNMClient();
        this.nmClient.init(conf);
        this.nmClient.start();
        this.util = new CLCUtils(conf);
        this.jarPath = jarPath;
        this.countContainer = countContainer;
    }
    
    @Override
    public float getProgress() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void onContainersAllocated(List<Container> arg0) {
        for(Container container : arg0) {
            try {
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(ApplicationMaster.createWorkerCommands());
                ctx.setLocalResources(Collections.singletonMap("yarn-app.jar", util.createJarResource(jarPath)));
                ctx.setEnvironment(util.createDefaultEnvironment());
                System.out.println(String.format("Launching container: %s", container.getId()));
                nmClient.startContainer(container, ctx);
            }
            catch(IOException | YarnException e) {
                System.err.println(String.format("Error launching container: %s", container.getId()));
                System.err.println(e);
            }
        }
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> arg0) {
        for (ContainerStatus status : arg0) {
            System.out.println(String.format("Completed container: %s (%s)", status.getContainerId(), status));
            synchronized (this) {
                ++countContainerFinish;
            }
        }
    }

    @Override
    public void onContainersUpdated(List<UpdatedContainer> arg0) {
        // TODO Auto-generated method stub
    }

    @Override
    public void onError(Throwable arg0) {
        // TODO Auto-generated method stub
    }

    @Override
    public void onNodesUpdated(List<NodeReport> arg0) {
        // TODO Auto-generated method stub
    }

    @Override
    public void onShutdownRequest() {
        // TODO Auto-generated method stub
    }
    
    public boolean isAllContainersCompleted() {
        return countContainerFinish >= countContainer;
    }
    
    public void run() throws YarnException, IOException, InterruptedException {
        AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(conf);
        rmClient.start();
        
        // ResourceManager に登録
        System.out.println("Start registration AM");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("End registration AM");
        
        // ワーカコンテナの優先度を設定
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);
        
        // ワーカコンテナのリソースを設定
        Resource resource = Records.newRecord(Resource.class);
        resource.setMemorySize(128);
        resource.setVirtualCores(1);
        
        // ResourceManager にコンテナをリクエスト
        for(int i = 0; i < countContainer; ++i) {
            //                                                  (capacity, nodes, racks, priority)
            ContainerRequest containerAsk = new ContainerRequest(resource, null,  null,  priority);
            System.out.println(String.format("Making worker container request: (%d/%d)", (i + 1), countContainer));
            rmClient.addContainerRequest(containerAsk);
        }
        
        System.out.println("Waiting for containers to finish");
        while(!isAllContainersCompleted()) {
            Thread.sleep(100);
        }
        
        // ResourceManager から登録解除
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    }

    public static void main(String[] args) throws Exception {
        final String argJarPath = args[0];
        final String argCountContainer = args[1];
        int countContainer = Integer.parseInt(argCountContainer);
        ApplicationMasterAsync amAsync = new ApplicationMasterAsync(argJarPath, countContainer);
        amAsync.run();
    }
}
