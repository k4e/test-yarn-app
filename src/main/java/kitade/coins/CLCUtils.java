package kitade.coins;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class CLCUtils {

    private Configuration conf;
    
    public CLCUtils(Configuration conf) {
        this.conf = conf;
    }
    
    public LocalResource createJarResource(String jarPath) throws IOException {
        Path path = new Path(jarPath);
        LocalResource jar = Records.newRecord(LocalResource.class);
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(path);
        jar.setResource(ConverterUtils.getYarnUrlFromPath(path));
        jar.setSize(jarStat.getLen());
        jar.setTimestamp(jarStat.getModificationTime());
        jar.setType(LocalResourceType.FILE);
        jar.setVisibility(LocalResourceVisibility.PUBLIC);
        return jar;
    }
    
    public Map<String, String> createDefaultEnvironment() {
        Map<String, String> env = new HashMap<>();
        for(String cp : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(env, Environment.CLASSPATH.name(), cp.trim());
        }
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), Environment.PWD.$() + File.separator + "*");
        return env;
    }
}
