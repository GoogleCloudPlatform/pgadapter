CREATE TABLE databasechangeloglock (
    ID INTEGER NOT NULL,
    LOCKED BOOLEAN NOT NULL,
    LOCKGRANTED TIMESTAMPTZ,
    LOCKEDBY VARCHAR(255),
    PRIMARY KEY (ID)
);

CREATE TABLE databasechangelog (
    ID VARCHAR(255) NOT NULL PRIMARY KEY,
    AUTHOR VARCHAR(255) NOT NULL,
    FILENAME VARCHAR(255) NOT NULL,
    DATEEXECUTED TIMESTAMPTZ NOT NULL,
    ORDEREXECUTED INTEGER NOT NULL,
    EXECTYPE VARCHAR(10) NOT NULL,
    MD5SUM VARCHAR(35),
    DESCRIPTION VARCHAR(255),
    COMMENTS VARCHAR(255),
    TAG VARCHAR(255),
    LIQUIBASE VARCHAR(20),
    CONTEXTS VARCHAR(255),
    LABELS VARCHAR(255),
    DEPLOYMENT_ID VARCHAR(10)
);

