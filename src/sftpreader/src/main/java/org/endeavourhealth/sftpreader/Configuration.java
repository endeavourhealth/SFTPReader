package org.endeavourhealth.sftpreader;

import com.fasterxml.jackson.databind.JsonNode;
import com.kstruct.gethostname4j.Hostname;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.model.MySqlDataLayer;
import org.endeavourhealth.sftpreader.model.PostgresDataLayer;
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
    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);


    private static final String INSTANCE_NAME_JAVA_PROPERTY = "INSTANCE_NAME";

    private static Configuration instance = null;

    public static Configuration getInstance() throws Exception {
        if (instance == null) {
            synchronized (Configuration.class) {
                if (instance == null) {
                    instance = new Configuration();
                }
            }
        }

        return instance;
    }

    private DataLayerI cachedDataLayer = null;
    private String machineName;
    private String instanceName;
    private DbInstance dbInstanceConfiguration;
    private List<DbConfiguration> dbConfiguration;

    private Configuration() throws Exception {
        initialiseDataLayer();
        initialiseMachineName();
        retrieveInstanceName();
        //addHL7LogAppender();
        loadDbConfiguration();
    }

    public String getMachineName() {
        return machineName;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public List<DbConfiguration> getConfigurations() {
        return this.dbConfiguration;
    }

    public DbInstance getInstanceConfiguration() {
        return this.dbInstanceConfiguration;
    }

    private void initialiseMachineName() throws SftpReaderException {
        try {
            machineName = Hostname.getHostname();
        } catch (Exception e) {
            throw new SftpReaderException("Error getting machine name", e);
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

    /*private void addHL7LogAppender() throws SftpReaderException {
        try {
            LogDigestAppender.addLogAppender(new PostgresDataLayer(getDatabaseConnection()));
        } catch (Exception e) {
            throw new SftpReaderException("Error adding SFTP Reader log appender", e);
        }
    }*/

    private void loadDbConfiguration() throws Exception {

        DataLayerI dataLayer = getDataLayer();
        this.dbInstanceConfiguration = dataLayer.getInstanceConfiguration(this.instanceName, this.machineName);

        this.dbConfiguration = new ArrayList<>();

        for (String configurationId : this.dbInstanceConfiguration.getConfigurationIds()) {
            DbConfiguration configuration = dataLayer.getConfiguration(configurationId);
            this.dbConfiguration.add(configuration);
        }
    }

    public DataLayerI getDataLayer() throws Exception {
        return cachedDataLayer;
    }

    /*public DataSource getNonPooledDatabaseConnection() throws SQLException {
        return PgDataSource.get(dbUrl, dbUsername, dbPassword);
    }*/

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

    private synchronized void initialiseDataLayer() throws Exception {

        String dbUrl = null;
        String dbUsername = null;
        String dbPassword = null;
        String dbDriverClassName = null;

        //new settings are stored in a single JSON structure, but old settings are stored in three separate records
        JsonNode json = ConfigManager.getConfigurationAsJson("database");
        if (json == null) {
            dbUrl = ConfigManager.getConfiguration("postgres-url");
            dbUsername = ConfigManager.getConfiguration("postgres-username");
            dbPassword = ConfigManager.getConfiguration("postgres-password");
            dbDriverClassName = "org.postgresql.Driver";

        } else {
            dbUrl = json.get("url").asText();
            dbUsername = json.get("username").asText();
            dbPassword = json.get("password").asText();
            dbDriverClassName = json.get("class").asText();
        }

        //bit of a lazy way to check, but it works
        if (dbDriverClassName.contains("postgresql")) {
            this.cachedDataLayer = new PostgresDataLayer(dbUrl, dbUsername, dbPassword, dbDriverClassName);

        } else {
            this.cachedDataLayer = new MySqlDataLayer(dbUrl, dbUsername, dbPassword, dbDriverClassName);
        }



    }
}
