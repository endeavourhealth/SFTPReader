package org.endeavourhealth.sftpreader;

import com.kstruct.gethostname4j.Hostname;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.config.ConfigManagerException;
import org.endeavourhealth.common.postgres.PgDataSource;
import org.endeavourhealth.common.postgres.PgStoredProcException;
import org.endeavourhealth.common.postgres.logdigest.LogDigestAppender;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstance;
import org.endeavourhealth.sftpreader.model.exceptions.SftpReaderException;
import org.endeavourhealth.sftpreader.utilities.StringHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class Configuration {

    // class members //
    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);
    private static final String PROGRAM_CONFIG_MANAGER_NAME = "sftpreader";
    private static final String INSTANCE_NAME_JAVA_PROPERTY = "INSTANCE_NAME";

    private volatile static Configuration instance = null;

    public static Configuration getInstance() throws Exception {
        if (instance == null) {
            synchronized (Configuration.class) {
                if (instance == null)
                    instance = new Configuration();
            }
        }

        return instance;
    }

    // instance members //
    private String postgresUrl;
    private String postgresUsername;
    private String postgresPassword;
    private DataSource dataSource;

    private String machineName;
    private String instanceName;
    private DbInstance dbInstanceConfiguration;
    private List<DbConfiguration> dbConfiguration;

    private Configuration() throws Exception {
        initialiseConfigManager();
        initialiseDBConnectionPool();
        initialiseMachineName();
        retrieveInstanceName();
        //addHL7LogAppender();
        loadDbConfiguration();
    }

    public String getMachineName() { return machineName; }
    public String getInstanceName() { return instanceName; }
    public List<DbConfiguration> getConfigurations() { return this.dbConfiguration; }
    public DbInstance getInstanceConfiguration() { return this.dbInstanceConfiguration; }

    private void initialiseMachineName() throws SftpReaderException {
        try {
            machineName = Hostname.getHostname();
        } catch (Exception e) {
            throw new SftpReaderException("Error getting machine name");
        }
    }

    private void retrieveInstanceName() throws SftpReaderException {
        try {
            this.instanceName = System.getProperty(INSTANCE_NAME_JAVA_PROPERTY);

            if (StringUtils.isEmpty(this.instanceName))
                throw new SftpReaderException("Could not find " + INSTANCE_NAME_JAVA_PROPERTY + " Java -D property");

        } catch (SftpReaderException e) {
            throw e;
        } catch (Exception e) {
            throw new SftpReaderException("Could not read " + INSTANCE_NAME_JAVA_PROPERTY + " Java -D property");
        }
    }

    private void initialiseConfigManager() throws ConfigManagerException {
        ConfigManager.Initialize(PROGRAM_CONFIG_MANAGER_NAME);

        postgresUrl = ConfigManager.getConfiguration("postgres-url");
        postgresUsername = ConfigManager.getConfiguration("postgres-username");
        postgresPassword = ConfigManager.getConfiguration("postgres-password");
    }

    /*private void addHL7LogAppender() throws SftpReaderException {
        try {
            LogDigestAppender.addLogAppender(new DataLayer(getDatabaseConnection()));
        } catch (Exception e) {
            throw new SftpReaderException("Error adding SFTP Reader log appender", e);
        }
    }*/

    private void loadDbConfiguration() throws PgStoredProcException, SQLException, SftpReaderException {
        DataLayer dataLayer = new DataLayer(getDatabaseConnection());

        this.dbInstanceConfiguration = dataLayer.getInstanceConfiguration(this.instanceName, getMachineName());

        this.dbConfiguration = new ArrayList<>();

        for (String configurationId : this.dbInstanceConfiguration.getConfigurationIds())
            this.dbConfiguration.add(dataLayer.getConfiguration(configurationId));
    }

    public DataSource getDatabaseConnection() throws SQLException {
        return this.dataSource;
    }

    public DataSource getNonPooledDatabaseConnection() throws SQLException {
        return PgDataSource.get(postgresUrl, postgresUsername, postgresPassword);
    }

    public DbConfiguration getConfiguration(String configurationId) throws SftpReaderException {
        List<DbConfiguration> dbConfigurations = this.dbConfiguration
                .stream()
                .filter(t -> t.getConfigurationId().equals(configurationId))
                .collect(Collectors.toList());

        if (dbConfigurations.size() == 0)
            throw new SftpReaderException("Could not find configuration with id " + configurationId);

        if (dbConfigurations.size() > 1)
            throw new SftpReaderException("Multiple configurations found with id " + configurationId);

        return dbConfigurations.get(0);
    }

    public String getConfigurationIdsForDisplay() {
        String commaSeperatedString = StringUtils.join(this.getInstanceConfiguration().getConfigurationIds(), ", ");
        return StringHelper.replaceLast(commaSeperatedString, ",", " and");
    }

    private synchronized void initialiseDBConnectionPool() throws SftpReaderException {
        try {
            if (this.dataSource == null) {

                HikariDataSource hikariDataSource = new HikariDataSource();
                hikariDataSource.setJdbcUrl(postgresUrl);
                hikariDataSource.setUsername(postgresUsername);
                hikariDataSource.setPassword(postgresPassword);
                hikariDataSource.setMaximumPoolSize(2);
                hikariDataSource.setMinimumIdle(1);
                hikariDataSource.setIdleTimeout(60000);
                hikariDataSource.setPoolName("SFTPReaderDBConnectionPool");

                this.dataSource = hikariDataSource;
            }
        } catch (Exception e) {
            throw new SftpReaderException("Error creating Hikari connection pool", e);
        }
    }
}
