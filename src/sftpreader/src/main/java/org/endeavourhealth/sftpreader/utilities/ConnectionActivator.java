package org.endeavourhealth.sftpreader.utilities;

import org.endeavourhealth.sftpreader.implementations.*;
import org.endeavourhealth.sftpreader.implementations.barts.*;
import org.endeavourhealth.sftpreader.implementations.emis.*;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.utilities.file.FileConnection;
import org.endeavourhealth.sftpreader.utilities.sftp.SftpConnection;
import org.slf4j.LoggerFactory;

public class ConnectionActivator {
    // do this properly - instatiate dynamically based on configuration against interface type

    public static Connection createConnection(String interfaceTypeName, ConnectionDetails connectionDetails) {
        if (interfaceTypeName.toUpperCase().startsWith("EMIS-EXTRACT-SERVICE")) {
            return new SftpConnection(connectionDetails);
        } else {
            return new FileConnection(connectionDetails);
        }
    }
}
