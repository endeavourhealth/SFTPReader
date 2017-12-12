package org.endeavourhealth.sftpreader.sources;

public class ConnectionActivator {
    // do this properly - instatiate dynamically based on configuration against interface type

    public static Connection createConnection(String interfaceTypeName, ConnectionDetails connectionDetails) {

        String interfaceType = interfaceTypeName.toUpperCase();
        if (interfaceType.startsWith("EMIS-EXTRACT-SERVICE")) {
            return new SftpConnection(connectionDetails);
        } else if (interfaceType.startsWith("VISION-EXTRACT-SERVICE")) {
            return new SftpConnection(connectionDetails);
        } else {
            return new FileConnection(connectionDetails);
        }
    }
}
