package k4e.coins.tsukuba;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

public class Client {
    
    public static List<String> createAMCommands(String jarPath, int countContainer, String workerClass, String... options) {
        StringBuilder optionsBuilder = new StringBuilder();
        for(String o : options) {
            optionsBuilder.append(o);
            optionsBuilder.append(" ");
        }
        return Arrays.asList(
                String.format(
                        "$JAVA_HOME/bin/java -Xmx256M kitade.coins.ApplicationMasterAsync %s %d %s %s 1> %s/stdout 2> %s/stderr",
                        jarPath,
                        countContainer,
                        workerClass,
                        optionsBuilder.toString(),
                        ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                        ApplicationConstants.LOG_DIR_EXPANSION_VAR)
                );
    }
    
    public static void main(String[] args) throws Exception {
        final String argJarPath = args[0];
        final String argCountContainer = args[1];
        final String argWorkerClass = args[2];
        final String[] argOptions = (args.length < 4 ? new String[0] : Arrays.copyOfRange(args, 3, args.length));
        int countContainer = Integer.parseInt(argCountContainer);
        
        // YarnClient を生成
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        
        CLCUtils util = new CLCUtils(conf);
        
        // アプリケーションを作成
        YarnClientApplication app = yarnClient.createApplication();
        
        // ApplicationMaster を起動するための Container Launch Context をセットアップ
        ContainerLaunchContext amCtx = Records.newRecord(ContainerLaunchContext.class);
        amCtx.setCommands(createAMCommands(argJarPath, countContainer, argWorkerClass, argOptions));
        
        // ApplicationMaster のローカルリソースに Jar を設定
        amCtx.setLocalResources(Collections.singletonMap("yarn-app.jar", util.createJarResource(argJarPath)));
        
        // ApplicationMster の CLASSPATH を設定
        amCtx.setEnvironment(util.createDefaultEnvironment());
        
        // ApplicationMaster のリソースを設定
        Resource amResource = Records.newRecord(Resource.class);
        amResource.setMemorySize(256);
        amResource.setVirtualCores(1);
        
        // ApplicationSubmissionContext をセットアップ
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName("test-yarn-app.kitade.coins");
        appContext.setAMContainerSpec(amCtx);
        appContext.setResource(amResource);
        appContext.setQueue("default");
        
        // アプリケーションをサブミット
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application: " + appId);
        yarnClient.submitApplication(appContext);
        
        // アプリケーションが終わるまで待つ
        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while(appState != YarnApplicationState.FINISHED &&
                appState != YarnApplicationState.KILLED &&
                appState != YarnApplicationState.FAILED) {
            Thread.sleep(100);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }
        
        System.out.println(String.format(
                "Application %s finished with %s at %d",
                appId.toString(), appState.toString(), appReport.getFinishTime()));
    }
}
