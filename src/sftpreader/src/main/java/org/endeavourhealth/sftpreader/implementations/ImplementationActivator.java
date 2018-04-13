package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.sftpreader.implementations.barts.*;
import org.endeavourhealth.sftpreader.implementations.emis.*;
import org.endeavourhealth.sftpreader.implementations.homerton.*;
import org.endeavourhealth.sftpreader.implementations.vision.*;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.time.LocalDateTime;
import java.util.UUID;

public class ImplementationActivator {
    private static final Logger LOG = LoggerFactory.getLogger(ImplementationActivator.class);

    private static String getClassPackageAndPrefix(DbConfiguration dbConfiguration) throws Exception {
        String contentType = dbConfiguration.getSoftwareContentType();
        if (contentType.equalsIgnoreCase("EMISCSV")) {
            return "org.endeavourhealth.sftpreader.implementations.emis.Emis";

        } else if (contentType.equalsIgnoreCase("VISIONCSV")) {
            return "org.endeavourhealth.sftpreader.implementations.vision.Vision";

        } else if (contentType.equalsIgnoreCase("BARTSCSV")) {
            return "org.endeavourhealth.sftpreader.implementations.barts.Barts";

        } else if (contentType.equalsIgnoreCase("HOMERTONCSV")) {
            return "org.endeavourhealth.sftpreader.implementations.homerton.Homerton";

        } else if (contentType.equalsIgnoreCase("TPPCSV")) {
            return "org.endeavourhealth.sftpreader.implementations.tpp.Tpp";

        } else {
            throw new Exception("Unknown content type [" + contentType + "]");
        }
    }

    public static SftpFilenameParser createFilenameParser(RemoteFile remoteFile, DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "SftpFilenameParser";

        Class cls = Class.forName(clsName);
        Constructor<SftpFilenameParser> constructor = cls.getConstructor(RemoteFile.class, DbConfiguration.class);
        return constructor.newInstance(remoteFile, dbConfiguration);
    }

    public static SftpBatchValidator createSftpBatchValidator(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "SftpBatchValidator";

        Class cls = Class.forName(clsName);
        Constructor<SftpBatchValidator> constructor = cls.getConstructor();
        return constructor.newInstance();
    }

    public static SftpBatchSplitter createSftpBatchSplitter(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "SftpBatchSplitter";

        Class cls = Class.forName(clsName);
        Constructor<SftpBatchSplitter> constructor = cls.getConstructor();
        return constructor.newInstance();
    }

    public static SftpBatchSequencer createSftpBatchSequencer(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "SftpBatchSequencer";

        Class cls = Class.forName(clsName);
        Constructor<SftpBatchSequencer> constructor = cls.getConstructor();
        return constructor.newInstance();
    }

    public static SftpNotificationCreator createSftpNotificationCreator(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "SftpNotificationCreator";

        Class cls = Class.forName(clsName);
        Constructor<SftpNotificationCreator> constructor = cls.getConstructor();
        return constructor.newInstance();
    }

    public static SftpOrganisationHelper createSftpOrganisationHelper(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "SftpOrganisationHelper";

        Class cls = Class.forName(clsName);
        Constructor<SftpOrganisationHelper> constructor = cls.getConstructor();
        return constructor.newInstance();
    }

    public static SftpSlackNotifier createSftpSlackNotifier(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "SftpSlackNotifier";

        Class cls = Class.forName(clsName);
        Constructor<SftpSlackNotifier> constructor = cls.getConstructor();
        return constructor.newInstance();
    }

    public static SftpBatchUnzipperDecrypter createSftpUnzipperDecrypter(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "SftpBatchUnzipperDecrypter";

        Class cls = Class.forName(clsName);
        Constructor<SftpBatchUnzipperDecrypter> constructor = cls.getConstructor();
        return constructor.newInstance();
    }

}
