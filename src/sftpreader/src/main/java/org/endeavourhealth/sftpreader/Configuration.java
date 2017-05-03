package org.endeavourhealth.sftpreader;

import com.kstruct.gethostname4j.Hostname;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.config.ConfigManagerException;
import org.endeavourhealth.common.postgres.PgDataSource;
import org.endeavourhealth.common.postgres.PgStoredProcException;
import org.endeavourhealth.common.postgres.logdigest.LogDigestAppender;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbGlobalConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpReaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class Configuration {

    // class members //
    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);
    private static final String PROGRAM_CONFIG_MANAGER_NAME = "sftpreader";
    private static final String INSTANCE_NAMES_JAVA_PROPERTY = "INSTANCE_NAMES";
    private static final String INSTANCE_NAMES_SEPERATOR = ";";

    private static Configuration instance = null;

    public static Configuration getInstance() throws Exception {
        if (instance == null)
            instance = new Configuration();

        return instance;
    }

    // instance members //
    private String machineName;
    private List<String> instanceNames;
    private String postgresUrl;
    private String postgresUsername;
    private String postgresPassword;
    private DbGlobalConfiguration dbGlobalConfiguration;
    private List<DbConfiguration> dbConfiguration;

    private Configuration() throws Exception {
        initialiseMachineName();
        retrieveInstanceName();
        initialiseConfigManager();
        addHL7LogAppender();
        loadDbConfiguration();
    }

    private void initialiseMachineName() throws SftpReaderException {
        try {
            machineName = Hostname.getHostname();
        } catch (Exception e) {
            throw new SftpReaderException("Error getting machine name");
        }
    }

    private void retrieveInstanceName() throws SftpReaderException {
        try {
            String instanceNames = System.getProperty(INSTANCE_NAMES_JAVA_PROPERTY);

            if (StringUtils.isEmpty(instanceNames))
                throw new SftpReaderException("Could not find " + INSTANCE_NAMES_JAVA_PROPERTY + " Java -D property");

            this.instanceNames = Arrays.stream(StringUtils.split(instanceNames, INSTANCE_NAMES_SEPERATOR))
                    .map(t -> t.trim())
                    .collect(Collectors.toList());

        } catch (Exception e) {
            throw new SftpReaderException("Could not read " + INSTANCE_NAMES_JAVA_PROPERTY + " Java -D property");
        }
    }

    private void initialiseConfigManager() throws ConfigManagerException {
        ConfigManager.Initialize(PROGRAM_CONFIG_MANAGER_NAME);

        postgresUrl = ConfigManager.getConfiguration("postgres-url");
        postgresUsername = ConfigManager.getConfiguration("postgres-username");
        postgresPassword = ConfigManager.getConfiguration("postgres-password");
    }

    private void addHL7LogAppender() throws SftpReaderException {
        try {
            LogDigestAppender.addLogAppender(new DataLayer(getDatabaseConnection()));
        } catch (Exception e) {
            throw new SftpReaderException("Error adding SFTP Reader log appender", e);
        }
    }

    private void loadDbConfiguration() throws PgStoredProcException, SQLException, SftpReaderException {
        DataLayer dataLayer = new DataLayer(getDatabaseConnection());

        this.dbGlobalConfiguration = dataLayer.getGlobalConfiguration(getMachineName());

        this.dbConfiguration = new ArrayList<>();

        for (String instanceName : instanceNames)
            this.dbConfiguration.add(dataLayer.getConfiguration(instanceName));
    }

    public DataSource getDatabaseConnection() throws SQLException {
        return PgDataSource.get(postgresUrl, postgresUsername, postgresPassword);
    }

    public String getMachineName() { return machineName; }

    public DbGlobalConfiguration getGlobalConfiguration() {
        return this.dbGlobalConfiguration;
    }

    public DbConfiguration getConfiguration(String instanceName) throws SftpReaderException {
        List<DbConfiguration> dbConfigurations = this.dbConfiguration
                .stream()
                .filter(t -> t.getInstanceId().equals(instanceName))
                .collect(Collectors.toList());

        if (dbConfigurations.size() == 0)
            throw new SftpReaderException("Could not find configuration with instance name " + instanceName);

        if (dbConfigurations.size() > 1)
            throw new SftpReaderException("Multiple configurations found with instance name " + instanceName);

        return dbConfigurations.get(0);
    }

    public List<DbConfiguration> getConfigurations() {
        return this.dbConfiguration;
    }

    public String[] getInstanceNames() {
        return this.instanceNames.toArray(new String[this.instanceNames.size()]);
    }
}
