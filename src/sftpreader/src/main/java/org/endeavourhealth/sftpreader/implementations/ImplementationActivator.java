package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.sftpreader.implementations.barts.*;
import org.endeavourhealth.sftpreader.implementations.emis.*;
import org.endeavourhealth.sftpreader.implementations.homerton.*;
import org.endeavourhealth.sftpreader.implementations.vision.*;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;

import java.time.LocalDateTime;

public class ImplementationActivator {
    // do this properly - instatiate dynamically based on configuration against interface type

    public static SftpFilenameParser createFilenameParser(String filename, LocalDateTime lastModified, DbConfiguration dbConfiguration, String interfaceTypeName) {
        if (interfaceTypeName.toUpperCase().startsWith("EMIS")) {
            return new EmisSftpFilenameParser(filename, lastModified, dbConfiguration);
        } else {
            if (interfaceTypeName.toUpperCase().startsWith("VISION")) {
                return new VisionSftpFilenameParser(filename, lastModified, dbConfiguration);
            } else {
                if (interfaceTypeName.toUpperCase().startsWith("BARTS")) {
                    return new BartsSftpFilenameParser(filename, lastModified, dbConfiguration);
                } else {
                    return new HomertonSftpFilenameParser(filename, lastModified, dbConfiguration);
                }
            }
        }
    }

    public static SftpBatchValidator createSftpBatchValidator(String interfaceTypeName) {
        if (interfaceTypeName.toUpperCase().startsWith("EMIS")) {
            return new EmisSftpBatchValidator();
        } else {
            if (interfaceTypeName.toUpperCase().startsWith("VISION")) {
                return new VisionSftpBatchValidator();
            } else {
                if (interfaceTypeName.toUpperCase().startsWith("BARTS")) {
                    return new BartsSftpBatchValidator();
                } else {
                    return new HomertonSftpBatchValidator();
                }
            }
        }
    }

    public static SftpBatchSplitter createSftpBatchSplitter(String interfaceTypeName) {
        if (interfaceTypeName.toUpperCase().startsWith("EMIS")) {
            return new EmisSftpBatchSplitter();
        } else {
            if (interfaceTypeName.toUpperCase().startsWith("VISION")) {
                return new VisionSftpBatchSplitter();
            } else {
                if (interfaceTypeName.toUpperCase().startsWith("BARTS")) {
                    return new BartsSftpBatchSplitter();
                } else {
                    return new HomertonSftpBatchSplitter();
                }
            }
        }
    }

    public static SftpBatchSequencer createSftpBatchSequencer(String interfaceTypeName) {
        if (interfaceTypeName.toUpperCase().startsWith("EMIS")) {
            return new EmisSftpBatchSequencer();
        } else {
            if (interfaceTypeName.toUpperCase().startsWith("VISION")) {
                return new VisionSftpBatchSequencer();
            } else {
                if (interfaceTypeName.toUpperCase().startsWith("BARTS")) {
                    return new BartsSftpBatchSequencer();
                } else {
                    return new HomertonSftpBatchSequencer();
                }
            }
        }
    }

    public static SftpNotificationCreator createSftpNotificationCreator(String interfaceTypeName) {
        if (interfaceTypeName.toUpperCase().startsWith("EMIS")) {
            return new EmisSftpNotificationCreator();
        } else {
            if (interfaceTypeName.toUpperCase().startsWith("VISION")) {
                return new VisionSftpNotificationCreator();
            } else {
                if (interfaceTypeName.toUpperCase().startsWith("BARTS")) {
                    return new BartsSftpNotificationCreator();
                } else {
                    return new HomertonSftpNotificationCreator();
                }
            }
        }
    }



    public static SftpOrganisationHelper createSftpOrganisationHelper(String interfaceTypeName) {
        if (interfaceTypeName.toUpperCase().startsWith("EMIS")) {
            return new EmisSftpOrganisationHelper();
        } else {
            if (interfaceTypeName.toUpperCase().startsWith("VISION")) {
                return new VisionSftpOrganisationHelper();
            } else {
                if (interfaceTypeName.toUpperCase().startsWith("BARTS")) {
                    return new BartsSftpOrganisationHelper();
                } else {
                    return new HomertonSftpOrganisationHelper();
                }
            }
        }
    }

    public static SftpSlackNotifier createSftpSlackNotifier(String interfaceTypeName) {
        if (interfaceTypeName.toUpperCase().startsWith("EMIS")) {
            return new EmisSftpSlackNotifier();
        } else {
            if (interfaceTypeName.toUpperCase().startsWith("VISION")) {
                return new VisionSftpSlackNotifier();
            } else {
                if (interfaceTypeName.toUpperCase().startsWith("BARTS")) {
                    return new BartsSftpSlackNotifier();
                } else {
                    return new HomertonSftpSlackNotifier();
                }
            }
        }
    }

    public static SftpBatchUnzipperDecrypter createSftpUnzipperDecrypter(String interfaceTypeName) {
        if (interfaceTypeName.toUpperCase().startsWith("EMIS")) {
            return new EmisSftpBatchUnzipperDecrypter();
        } else {
            if (interfaceTypeName.toUpperCase().startsWith("VISION")) {
                return new VisionSftpBatchUnzipperDecrypter();
            } else {
                if (interfaceTypeName.toUpperCase().startsWith("BARTS")) {
                    return new BartsSftpBatchUnzipperDecrypter();
                } else {
                    return new HomertonSftpBatchUnzipperDecrypter();
                }
            }
        }
    }


}
