package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.sftpreader.implementations.barts.*;
import org.endeavourhealth.sftpreader.implementations.emis.*;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;

public class ImplementationActivator {
    // do this properly - instatiate dynamically based on configuration against interface type

    public static SftpFilenameParser createFilenameParser(String filename, DbConfiguration dbConfiguration, String interfaceTypeName) {
        if (interfaceTypeName.toUpperCase().startsWith("EMIS")) {
            return new EmisSftpFilenameParser(filename, dbConfiguration);
        } else {
            return new BartsSftpFilenameParser(filename, dbConfiguration);
        }
    }

    public static SftpBatchValidator createSftpBatchValidator(String interfaceTypeName) {
        if (interfaceTypeName.toUpperCase().startsWith("EMIS")) {
            return new EmisSftpBatchValidator();
        } else {
            return new BartsSftpBatchValidator();
        }
    }

    public static SftpBatchSequencer createSftpBatchSequencer(String interfaceTypeName) {
        if (interfaceTypeName.toUpperCase().startsWith("EMIS")) {
            return new EmisSftpBatchSequencer();
        } else {
            return new BartsSftpBatchSequencer();
        }
    }

    public static SftpNotificationCreator createSftpNotificationCreator(String interfaceTypeName) {
        if (interfaceTypeName.equalsIgnoreCase("EMIS-EXTRACT-SERVICE-5-1")) {
            return new EmisSftpNotificationCreator();
        } else {
            return new BartsSftpNotificationCreator();
        }
    }

    public static SftpBatchSplitter createSftpBatchSplitter(String interfaceTypeName) {
        if (interfaceTypeName.equalsIgnoreCase("EMIS-EXTRACT-SERVICE-5-1")) {
            return new EmisSftpBatchSplitter();
        } else {
            return new BartsSftpBatchSplitter();
        }
    }

    public static SftpOrganisationHelper createSftpOrganisationHelper(String interfaceTypeName) {
        if (interfaceTypeName.equalsIgnoreCase("EMIS-EXTRACT-SERVICE-5-1")) {
            return new EmisSftpOrganisationHelper();
        } else {
            return new BartsSftpOrganisationHelper();
        }
    }

    public static SftpSlackNotifier createSftpSlackNotifier(String interfaceTypeName) {
        if (interfaceTypeName.equalsIgnoreCase("EMIS-EXTRACT-SERVICE-5-1")) {
            return new EmisSftpSlackNotifier();
        } else {
            return new BartsSftpSlackNotifier();
        }
    }
}
