package org.ch_diag;

import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.maven.artifact.versioning.ComparableVersion;

import java.util.Properties;

public class SysConf {
    String current_dir;
    String url;
    Namespace options;
    ComparableVersion cluster_version = null;
    Properties conn_prop;

    String CH_DIAG_VERSION = "0.1";

    public SysConf(Namespace options) {
        this.current_dir = System.getProperty("user.dir");
        this.conn_prop = new Properties();
        this.conn_prop.setProperty("client_name", "ch-diag");
        this.options = options;

        if (options.get("ca_certs") != "") {
            this.url = String.format(
                    "jdbc:clickhouse://%s:%s/%s?ssl=true&sslmode=strict&sslrootcert=%s",
                    options.get("host"),
                    options.get("port"),
                    options.get("database"),
                    options.get("ca_certs")
            );
        }
        if (
                (options.get("ca_certs") == "") &&
                (options.get("certfile") != "") &&
                (options.get("keyfile") != "")
        ) {
            this.url = String.format(
                    "jdbc:clickhouse://%s:%s/%s?ssl=true&sslmode=strict&sslcert=%s&sslkey=%s",
                    options.get("host"),
                    options.get("port"),
                    options.get("database"),
                    options.get("certfile"),
                    options.get("keyfile")
            );
        }
        if (
                (options.get("ca_certs") == "") &&
                (options.get("certfile") == "") &&
                (options.get("keyfile") == "")
        ) {
            this.url = String.format(
                    "jdbc:clickhouse://%s:%s/%s?ssl=false&sslmode=none",
                    options.get("host"),
                    options.get("port"),
                    options.get("database")
            );
        }
    }
}
