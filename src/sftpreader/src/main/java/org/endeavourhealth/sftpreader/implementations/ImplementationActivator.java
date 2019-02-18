package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

public class ImplementationActivator {
    private static final Logger LOG = LoggerFactory.getLogger(ImplementationActivator.class);

    private static String getClassPackageAndPrefix(DbConfiguration dbConfiguration) throws Exception {
        String contentType = dbConfiguration.getSoftwareContentType();
        if (contentType.equalsIgnoreCase("EMISCSV")) {
            //depending on the version, we're either doing a regular Emis extract or their custom ones
            String version = dbConfiguration.getSoftwareVersion();
            if (version.equalsIgnoreCase("CUSTOM")) {
                return "org.endeavourhealth.sftpreader.implementations.emisCustom.EmisCustom";
            } else {
                return "org.endeavourhealth.sftpreader.implementations.emis.Emis";
            }

        } else if (contentType.equalsIgnoreCase("VISIONCSV")) {
            return "org.endeavourhealth.sftpreader.implementations.vision.Vision";

        } else if (contentType.equalsIgnoreCase("BARTSCSV")) {
            return "org.endeavourhealth.sftpreader.implementations.barts.Barts";

        } else if (contentType.equalsIgnoreCase("HOMERTONCSV")) {
            return "org.endeavourhealth.sftpreader.implementations.homerton.Homerton";

        } else if (contentType.equalsIgnoreCase("TPPCSV")) {
            return "org.endeavourhealth.sftpreader.implementations.tpp.Tpp";

        } else if (contentType.equalsIgnoreCase("ADASTRACSV")) {
            return "org.endeavourhealth.sftpreader.implementations.adastra.Adastra";

        } else {
            throw new Exception("Unknown content type [" + contentType + "]");
        }
    }

    public static SftpFilenameParser createFilenameParser(boolean isRawFile, RemoteFile remoteFile, DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "FilenameParser";

        Class cls = Class.forName(clsName);
        Constructor<SftpFilenameParser> constructor = cls.getConstructor(Boolean.TYPE, RemoteFile.class, DbConfiguration.class);
        return constructor.newInstance(new Boolean(isRawFile), remoteFile, dbConfiguration);
    }

    public static SftpBatchValidator createSftpBatchValidator(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "BatchValidator";

        Class cls = Class.forName(clsName);
        Constructor<SftpBatchValidator> constructor = cls.getConstructor();
        return constructor.newInstance();
    }

    public static SftpBatchSplitter createSftpBatchSplitter(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "BatchSplitter";

        Class cls = Class.forName(clsName);
        Constructor<SftpBatchSplitter> constructor = cls.getConstructor();
        return constructor.newInstance();
    }

    public static SftpBatchSequencer createSftpBatchSequencer(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "BatchSequencer";

        Class cls = Class.forName(clsName);
        Constructor<SftpBatchSequencer> constructor = cls.getConstructor();
        return constructor.newInstance();
    }

    public static SftpNotificationCreator createSftpNotificationCreator(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "NotificationCreator";

        Class cls = Class.forName(clsName);
        Constructor<SftpNotificationCreator> constructor = cls.getConstructor();
        return constructor.newInstance();
    }

    public static SftpOrganisationHelper createSftpOrganisationHelper(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "OrganisationHelper";

        Class cls = Class.forName(clsName);
        Constructor<SftpOrganisationHelper> constructor = cls.getConstructor();
        return constructor.newInstance();
    }

    public static SftpSlackNotifier createSftpSlackNotifier(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "SlackNotifier";

        Class cls = Class.forName(clsName);
        Constructor<SftpSlackNotifier> constructor = cls.getConstructor();
        return constructor.newInstance();
    }

    public static SftpBatchUnzipperDecrypter createSftpUnzipperDecrypter(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "BatchUnzipperDecrypter";

        Class cls = Class.forName(clsName);
        Constructor<SftpBatchUnzipperDecrypter> constructor = cls.getConstructor();
        return constructor.newInstance();
    }

    public static SftpPostSplitBatchValidator createSftpPostSplitBatchValidator(DbConfiguration dbConfiguration) throws Exception {
        String clsName = getClassPackageAndPrefix(dbConfiguration) + "PostSplitBatchValidator";

        Class cls = Class.forName(clsName);
        Constructor<SftpPostSplitBatchValidator> constructor = cls.getConstructor();
        return constructor.newInstance();
    }
}
