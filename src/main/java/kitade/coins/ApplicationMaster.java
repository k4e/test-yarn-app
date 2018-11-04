package kitade.coins;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

public class ApplicationMaster {

    public static List<String> createWorkerCommands() {
        return Arrays.asList(
                String.format(
                        "$JAVA_HOME/bin/java -Xmx256M kitade.coins.Worker %s %d 1> %s/stdout 2> %s/stderr",
                        ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                        ApplicationConstants.LOG_DIR_EXPANSION_VAR
                        )
                );
    }
    
    public static void main(String[] args) throws Exception {
        final String argJarPath = args[0];
        final String argCountContainer = args[1];
        int countContainer = Integer.parseInt(argCountContainer);
        
        Configuration conf = new YarnConfiguration();
        
        AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();
        
        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();
        
        CLCUtils util = new CLCUtils(conf);
        
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
        
        // コンテナ受取・起動、終わるまで待つ
        int responseId = 0;
        int completedContainers = 0;
        while(completedContainers < countContainer) {
            AllocateResponse response = rmClient.allocate(responseId++);
            for(Container container : response.getAllocatedContainers()) {
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(createWorkerCommands());
                ctx.setLocalResources(Collections.singletonMap("yarn-app.jar", util.createJarResource(argJarPath)));
                ctx.setEnvironment(util.createDefaultEnvironment());
                System.out.println(String.format("Launching container: %s", container.getId()));
                nmClient.startContainer(container, ctx);
            }
            for(ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++completedContainers;
                System.out.println(String.format("Launching container: %s (%s)", status.getContainerId(), status));
            }
            Thread.sleep(100);
        }
        
        // ResourceManager から登録解除
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    }
}
