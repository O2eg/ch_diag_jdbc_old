package org.ch_diag;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.Date;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.common.collect.Lists;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.maven.artifact.versioning.ComparableVersion;
import org.apache.commons.io.IOUtils;
import ru.yandex.clickhouse.ClickHouseDataSource;

public class Main {
    private static Namespace parse(String[] args) throws ArgumentParserException {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("ch-diag")
                .defaultHelp(true)
                .description("clickhouse diagnostic tool options");

        // Required arguments
        parser.addArgument("--version")
                .help("Show the version number and exit");

        parser.addArgument("--debug")
                .help("Enable debug mode")
                .action(Arguments.storeTrue())
                .setDefault(false);

        parser.addArgument("--add-params-to-report")
                .help("Show ch-diag parameters in report")
                .action(Arguments.storeTrue())
                .setDefault(false);

        parser.addArgument("--host")
                .setDefault("127.0.0.1");

        parser.addArgument("--protocol")
                .choices("http", "https", "tcp", "tcps", "grpc", "grpcs").setDefault("http");

        parser.addArgument("--port")
                .setDefault("9010");

        parser.addArgument("--database")
                .setDefault("default");

        parser.addArgument("--user")
                .setDefault("default");

        parser.addArgument("--password")
                .setDefault("default");

        parser.addArgument("--keyfile")
                .setDefault("");

        parser.addArgument("--certfile")
                .setDefault("");

        parser.addArgument("--ca-certs")
                .setDefault("");

        parser.addArgument("--cluster-name")
                .setDefault("AUTO");

        parser.addArgument("--use-ts-in-output-file-name")
                .setDefault("")
                .action(Arguments.storeTrue())
                .setDefault(false);

        return parser.parseArgs(args);
    }

    private static String get_scalar(Namespace options, String url, Properties prop, String sql) throws SQLException {
        String result = null;
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, prop);
        try {
            Connection conn = dataSource.getConnection(options.get("user"), options.get("password"));
            conn.setReadOnly(true);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            result = rs.getString(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    private static List<String> get_setof(Namespace options, String url, Properties prop, String sql) throws SQLException {
        List<String> result = new ArrayList<String>();
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, prop);
        try {
            Connection conn = dataSource.getConnection(options.get("user"), options.get("password"));
            conn.setReadOnly(true);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    private static String get_specific_sql(ComparableVersion ver, Object items) {
        List<List<String>> items_normalized = new ArrayList<>();
        ArrayList items_list = (ArrayList) items;
        for (Object v: items_list) {
            ArrayList item = (ArrayList) v;
            if (item.get(1).toString().equals("+")) {
                List<String> innerList = new ArrayList<>();
                innerList.add(item.get(0).toString());
                innerList.add("100");
                innerList.add(item.get(2).toString());
                items_normalized.add(innerList);
            } else {
                List<String> innerList = new ArrayList<>();
                innerList.add(item.get(0).toString());
                innerList.add(item.get(1).toString());
                innerList.add(item.get(2).toString());
                items_normalized.add(innerList);
            }
        }

       for (List<String> v: items_normalized) {
           ComparableVersion v_l =  new ComparableVersion(v.get(0));
           ComparableVersion v_r = new ComparableVersion(v.get(1));
           if ((ver.compareTo(v_l) == 1) && (ver.compareTo(v_r) == -1))
               return v.get(2);
       }
        return "";
    }

    class ReportItem {
        public String state;
        public String type;
        public String description;
        public String header;
        public String footer;
        public Object report_sql;
        public Result result;
    }

    class SectionItem {
        public String header;
        public String description;
        public String state;
        public Map<String, ReportItem> reports;
    }

    class ReportStruct {
        public String header;
        public String description;
        public Map<String, SectionItem> sections;
    }

    private static String load_file(String fileName) throws IOException {
        InputStream inStream = new FileInputStream(fileName);
        String content = IOUtils.toString(inStream, StandardCharsets.UTF_8.name());
        return content;
    }

    class Result {
        List<String> columns;
        List<List<String>> rows;
        public Result(List<String> columns, List<List<String>> rows){
            this.columns = columns;
            this.rows = rows;
        }
    }
    private void build_report_for_cluster(SysConf conf, String cluster, ReportStruct rs, Integer threads_num) {
        HashMap<String, String> tasks = new HashMap<>();
        rs.header += String.format(" [%s]", cluster);

        rs.description = String.format(
                "<b>Collecting datetime:</b> %s, <b>ch_diag version:</b> %s",
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()),
                conf.CH_DIAG_VERSION
        );

        for (Map.Entry<String, SectionItem> section : rs.sections.entrySet()) {
            // System.out.println("Key = " + section.getKey() + ", Value = " + section.getValue());
            Map<String, ReportItem> reports = section.getValue().reports;
            for (Map.Entry<String, ReportItem> report : reports.entrySet()) {
                ReportItem report_item = report.getValue();
                String sql_file = null;
                if (report_item.report_sql instanceof Collection) {
                    sql_file = get_specific_sql(conf.cluster_version, report_item.report_sql);
                }
                else {
                    sql_file = report_item.report_sql.toString();
                }

                String sql_content = null;
                try {
                    sql_content = load_file(Paths.get(conf.current_dir, "sql", section.getKey(), sql_file).toString());
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
                sql_content = sql_content.replace("_CLUSTER_NAME",cluster);
                tasks.put(report.getKey(), sql_content);
            }
        }

        HashMap <String, Result> task_result = new HashMap<>();
        for (Map.Entry<String, String> task : tasks.entrySet()) {
            try {
                System.out.println("Processing " + task.getKey() + "...");
                ClickHouseDataSource dataSource = new ClickHouseDataSource(conf.url, conf.conn_prop);
                Connection conn = dataSource.getConnection(conf.options.get("user"), conf.options.get("password"));
                conn.setReadOnly(true);
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery(task.getValue());
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<String> columns = new ArrayList<>();
                for (int c = 1; c <= resultSetMetaData.getColumnCount(); c++) {
                    columns.add(resultSetMetaData.getColumnName(c));
                }
                List<List<String>> rows = Lists.newArrayList();
                while (resultSet.next()) {
                    List<String> vals = new ArrayList<>();
                    for (int c = 1; c <= resultSetMetaData.getColumnCount(); c++) {
                        vals.add(resultSet.getString(c));
                    }
                    rows.add(Lists.newArrayList(vals));
                }
                Result res = new Result(columns, rows);
                task_result.put(task.getKey(), res);
            } catch (SQLException e) {
                List<String> columns = new ArrayList<>();
                columns.add("Exception");
                List<List<String>> rows = Lists.newArrayList();

                List<String> vals = new ArrayList<>();
                vals.add(e.toString());
                rows.add(Lists.newArrayList(vals));

                Result res = new Result(columns, rows);
                task_result.put(task.getKey(), res);
                e.printStackTrace();
            }
        }

        for (Map.Entry<String, SectionItem> section : rs.sections.entrySet()) {
            Map<String, ReportItem> reports = section.getValue().reports;
            for (Map.Entry<String, ReportItem> report : reports.entrySet()) {
                ReportItem report_item = report.getValue();
                report_item.result = task_result.get(report.getKey());
                report.setValue(report_item);
            }
        }

        try {
            save_report_struct(conf, rs, cluster);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ReportStruct load_report_struct(SysConf conf) throws FileNotFoundException {
        Gson gson = new Gson();
        return gson.fromJson(new FileReader(
            Paths.get(conf.current_dir, "sql", "report_struct.json").toString()),
            ReportStruct.class
        );
    }

    public static void save_report_struct(SysConf conf, ReportStruct rs, String cluster) throws IOException {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(rs);
        String html_result = load_file(Paths.get(conf.current_dir, "template", "report.html").toString());
        html_result = html_result.replace("_REPORT_DATA", json);

        FileOutputStream fop = null;
        File file;
        try {
            String file_name = null;
            if (conf.options.getBoolean("use_ts_in_output_file_name"))
                file_name = "report_" + cluster + "_" + Instant.now().getEpochSecond() + ".html";
            else
                file_name = "report_" + cluster + ".html";

            File output_dir = new File(Paths.get(conf.current_dir, "output").toString());
            if (!output_dir.exists()){
                output_dir.mkdir();
            }

            file = new File(Paths.get(conf.current_dir, "output", file_name).toString());
            fop = new FileOutputStream(file);
            if (!file.exists()) {
                file.createNewFile();
            }
            byte[] contentInBytes = html_result.getBytes();
            fop.write(contentInBytes);
            fop.flush();
            fop.close();
            System.out.println("Report saved to " + Paths.get(conf.current_dir, "output", file_name));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void build_reports(SysConf conf) {
        List<String> clusters = new ArrayList<String>();
        String cluster_res = null;
        ReportStruct rs = null;

        try {
            rs = load_report_struct(conf);
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }

        try {
            conf.cluster_version = new ComparableVersion(
                    get_scalar(conf.options, conf.url, conf.conn_prop, "select version()")
            );

            if (conf.options.getString("cluster_name") == "AUTO") {
                String sql = new StringBuilder()
                        .append("select cluster, count(1) as cnt\n")
                        .append("from system.clusters\n")
                        .append("group by cluster\n")
                        .append("order by cnt desc\n")
                        .append("limit 1")
                        .toString();
                cluster_res = get_scalar(
                    conf.options,
                    conf.url,
                    conf.conn_prop,
                    sql
                );
                clusters.add(cluster_res);
            } else
            if (conf.options.getString("cluster_name") == "ALL") {
                clusters = get_setof(
                        conf.options,
                        conf.url,
                        conf.conn_prop,
                        "select cluster from system.clusters group by cluster"
                );
            } else {
                clusters.add(conf.options.getString("cluster_name"));
            }
            for (String cluster: clusters) {
                build_report_for_cluster(conf, cluster, rs, 1);
            }
        } catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Namespace options = null;
        try {
            options = parse(args);
        } catch (ArgumentParserException e) {
            e.printStackTrace();
            System.exit(1);
        }

        if (options.get("debug")) {
            System.out.println(options);
            System.out.println("Working Directory = " + System.getProperty("user.dir"));
        }
        var classInstance = new Main();
        classInstance.build_reports(new SysConf(options));
    }
}