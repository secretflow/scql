package org.secretflow.scql.hive;

import static com.google.protobuf.Any.pack;
import static org.apache.arrow.adapter.jdbc.JdbcToArrow.sqlToArrowVectorIterator;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowUtils.jdbcToArrowSchema;

import com.google.protobuf.ByteString;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.types.pojo.Schema;

public final class ScqlHiveFlightSqlServer extends NoOpFlightSqlProducer {
  private static final Calendar UTC_CALENDAR = JdbcToArrowUtils.getUtcCalendar();

  private final BufferAllocator allocator;
  private final QueryBackend backend;
  private final Location endpointLocation;
  private final String party;

  private ScqlHiveFlightSqlServer(
      BufferAllocator allocator, QueryBackend backend, Location endpointLocation, String party) {
    this.allocator = allocator;
    this.backend = backend;
    this.endpointLocation = endpointLocation;
    this.party = party;
  }

  public static void main(String[] args) throws Exception {
    final ServerConfig config = ServerConfig.parse(args);

    try (RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        QueryBackend backend = createBackend(config);
        BufferAllocator serverAllocator =
            rootAllocator.newChildAllocator("scql-hive-flight-server", 0, Long.MAX_VALUE);
        ScqlHiveFlightSqlServer producer =
            new ScqlHiveFlightSqlServer(
                serverAllocator,
                backend,
                Location.forGrpcInsecure(config.advertiseHost, config.port),
                config.party);
        FlightServer server =
            FlightServer.builder(
                    rootAllocator, Location.forGrpcInsecure(config.host, config.port), producer)
                .build()) {
      System.out.println("============================================================");
      System.out.println("SCQL Hive Arrow Flight SQL Server (Java)");
      System.out.println("============================================================");
      System.out.println("party      : " + config.party);
      System.out.println("backend    : " + config.backend);
      System.out.println("listen     : grpc://" + config.host + ":" + config.port);
      System.out.println("advertise  : grpc://" + config.advertiseHost + ":" + config.port);
      System.out.println("jdbc       : " + backend.describe());
      System.out.println("------------------------------------------------------------");
      server.start();
      System.out.println("server started, press Ctrl+C to stop");
      server.awaitTermination();
    }
  }

  private static QueryBackend createBackend(ServerConfig config) throws Exception {
    if ("hive".equals(config.backend)) {
      return new HiveBackend(config);
    }
    return new DuckDbBackend(config);
  }

  @Override
  public FlightInfo getFlightInfoStatement(
      CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
    final String query = backend.preprocess(command.getQuery());
    log("GetFlightInfo", query);

    try (QueryResources resources = backend.execute(query)) {
      final Schema schema = jdbcToArrowSchema(resources.resultSet.getMetaData(), UTC_CALENDAR);
      final TicketStatementQuery ticket =
          TicketStatementQuery.newBuilder()
              .setStatementHandle(ByteString.copyFromUtf8(query))
              .build();
      return new FlightInfo(
          schema,
          descriptor,
          Collections.singletonList(new FlightEndpoint(new Ticket(pack(ticket).toByteArray()), endpointLocation)),
          -1,
          -1);
    } catch (SQLException e) {
      throw CallStatus.INTERNAL
          .withDescription("failed to get FlightInfo for query")
          .withCause(e)
          .toRuntimeException();
    }
  }

  @Override
  public SchemaResult getSchemaStatement(
      CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
    final String query = backend.preprocess(command.getQuery());

    try (QueryResources resources = backend.execute(query)) {
      return new SchemaResult(jdbcToArrowSchema(resources.resultSet.getMetaData(), UTC_CALENDAR));
    } catch (SQLException e) {
      throw CallStatus.INTERNAL
          .withDescription("failed to get query schema")
          .withCause(e)
          .toRuntimeException();
    }
  }

  @Override
  public void getStreamStatement(
      TicketStatementQuery ticket, CallContext context, FlightProducer.ServerStreamListener listener) {
    final String query = backend.preprocess(ticket.getStatementHandle().toStringUtf8());
    log("DoGet", query);

    try (QueryResources resources = backend.execute(query)) {
      final ResultSet resultSet = resources.resultSet;
      final Schema schema = jdbcToArrowSchema(resultSet.getMetaData(), UTC_CALENDAR);
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        final VectorLoader loader = new VectorLoader(root);
        listener.start(root);

        final ArrowVectorIterator iterator = sqlToArrowVectorIterator(resultSet, allocator);
        boolean wroteBatch = false;
        try {
          while (iterator.hasNext()) {
            final VectorSchemaRoot batch = iterator.next();
            if (batch.getRowCount() == 0) {
              continue;
            }
            final VectorUnloader unloader = new VectorUnloader(batch);
            loader.load(unloader.getRecordBatch());
            listener.putNext();
            root.clear();
            wroteBatch = true;
          }
        } finally {
          iterator.close();
        }

        if (!wroteBatch) {
          listener.putNext();
        }
        listener.completed();
      }
    } catch (Exception e) {
      listener.error(
          CallStatus.INTERNAL
              .withDescription("failed to stream query result")
              .withCause(e)
              .toRuntimeException());
    }
  }

  @Override
  public void listFlights(
      CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
    listener.onError(CallStatus.UNIMPLEMENTED.withDescription("listFlights is not implemented").toRuntimeException());
  }

  @Override
  public void close() throws Exception {
    allocator.close();
  }

  private void log(String phase, String query) {
    final String compact = query.replaceAll("\\s+", " ").trim();
    System.out.println("[" + party + "] " + phase + " - " + compact);
  }

  private static final class ServerConfig {
    private String party = "alice";
    private String host = "0.0.0.0";
    private String advertiseHost = "localhost";
    private int port = 8815;
    private String backend = "duckdb";
    private String hiveHost;
    private int hivePort = 10000;
    private String hiveUser;
    private String hivePassword;
    private String hiveDatabase = "default";
    private boolean hiveDatabaseExplicit;
    private String hiveAuth = "NONE";
    private String duckdbPath;

    static ServerConfig parse(String[] args) {
      final ServerConfig config = new ServerConfig();
      for (int i = 0; i < args.length; i++) {
        final String arg = args[i];
        switch (arg) {
          case "--party":
            config.party = requireValue(args, ++i, arg);
            break;
          case "--host":
            config.host = requireValue(args, ++i, arg);
            break;
          case "--advertise-host":
            config.advertiseHost = requireValue(args, ++i, arg);
            break;
          case "--port":
            config.port = Integer.parseInt(requireValue(args, ++i, arg));
            break;
          case "--backend":
            config.backend = requireValue(args, ++i, arg).toLowerCase(Locale.ROOT);
            break;
          case "--hive-host":
            config.hiveHost = requireValue(args, ++i, arg);
            break;
          case "--hive-port":
            config.hivePort = Integer.parseInt(requireValue(args, ++i, arg));
            break;
          case "--hive-user":
            config.hiveUser = requireValue(args, ++i, arg);
            break;
          case "--hive-password":
            config.hivePassword = requireValue(args, ++i, arg);
            break;
          case "--hive-database":
            config.hiveDatabase = requireValue(args, ++i, arg);
            config.hiveDatabaseExplicit = true;
            break;
          case "--hive-auth":
            config.hiveAuth = requireValue(args, ++i, arg).toUpperCase(Locale.ROOT);
            break;
          case "--duckdb-path":
            config.duckdbPath = requireValue(args, ++i, arg);
            break;
          case "--help":
          case "-h":
            printUsageAndExit(0);
            break;
          default:
            throw new IllegalArgumentException("unknown argument: " + arg);
        }
      }

      if (!"duckdb".equals(config.backend) && !"hive".equals(config.backend)) {
        throw new IllegalArgumentException("--backend must be duckdb or hive");
      }
      if ("hive".equals(config.backend) && (config.hiveHost == null || config.hiveHost.isEmpty())) {
        throw new IllegalArgumentException("--hive-host is required when --backend hive");
      }
      if ("hive".equals(config.backend) && !config.hiveDatabaseExplicit) {
        config.hiveDatabase = config.party;
      }
      if (config.duckdbPath == null || config.duckdbPath.isEmpty()) {
        config.duckdbPath = "/tmp/scql-flight-" + config.party + ".duckdb";
      }
      return config;
    }

    private static String requireValue(String[] args, int index, String argName) {
      if (index >= args.length) {
        throw new IllegalArgumentException("missing value for " + argName);
      }
      return args[index];
    }

    private static void printUsageAndExit(int code) {
      System.out.println("Usage: java -jar scql-hive-flight-sql-server.jar [options]");
      System.out.println("  --party alice|bob");
      System.out.println("  --host 0.0.0.0");
      System.out.println("  --advertise-host localhost");
      System.out.println("  --port 8815");
      System.out.println("  --backend duckdb|hive");
      System.out.println("  --hive-host localhost");
      System.out.println("  --hive-port 10000");
      System.out.println("  --hive-user hive");
      System.out.println("  --hive-password ******");
      System.out.println("  --hive-database <default: same as --party when backend=hive>");
      System.out.println("  --hive-auth NONE|LDAP|KERBEROS");
      System.out.println("  --duckdb-path /tmp/scql-flight-alice.duckdb");
      System.exit(code);
    }
  }

  private interface QueryBackend extends AutoCloseable {
    String preprocess(String query);

    QueryResources execute(String query) throws SQLException;

    String describe();

    @Override
    default void close() throws Exception {}
  }

  private static final class QueryResources implements AutoCloseable {
    private final Connection connection;
    private final Statement statement;
    private final ResultSet resultSet;

    private QueryResources(Connection connection, Statement statement, ResultSet resultSet) {
      this.connection = connection;
      this.statement = statement;
      this.resultSet = resultSet;
    }

    @Override
    public void close() throws SQLException {
      try {
        resultSet.close();
      } finally {
        try {
          statement.close();
        } finally {
          connection.close();
        }
      }
    }
  }

  private static final class HiveBackend implements QueryBackend {
    private final String jdbcUrl;
    private final Properties properties = new Properties();
    private final HiveDialectConverter converter;

    private HiveBackend(ServerConfig config) throws Exception {
      Class.forName("org.apache.hive.jdbc.HiveDriver");
      this.jdbcUrl = buildJdbcUrl(config);
      if (config.hiveUser != null && !config.hiveUser.isEmpty()) {
        properties.setProperty("user", config.hiveUser);
      }
      if (config.hivePassword != null && !config.hivePassword.isEmpty()) {
        properties.setProperty("password", config.hivePassword);
      }
      this.converter = new HiveDialectConverter(config.party, config.hiveDatabase);
    }

    @Override
    public String preprocess(String query) {
      return converter.convert(query);
    }

    @Override
    public QueryResources execute(String query) throws SQLException {
      final Connection connection = DriverManager.getConnection(jdbcUrl, properties);
      final Statement statement = connection.createStatement();
      statement.setFetchSize(1024);
      final ResultSet resultSet = statement.executeQuery(query);
      return new QueryResources(connection, statement, resultSet);
    }

    @Override
    public String describe() {
      return jdbcUrl;
    }

    private static String buildJdbcUrl(ServerConfig config) {
      final StringBuilder sb =
          new StringBuilder()
              .append("jdbc:hive2://")
              .append(config.hiveHost)
              .append(":")
              .append(config.hivePort)
              .append("/")
              .append(config.hiveDatabase);
      if (!"NONE".equals(config.hiveAuth)) {
        sb.append(";auth=").append(config.hiveAuth);
      }
      return sb.toString();
    }
  }

  private static final class DuckDbBackend implements QueryBackend {
    private final String jdbcUrl;
    private final String party;
    private final HiveDialectConverter converter;

    private DuckDbBackend(ServerConfig config) throws Exception {
      Class.forName("org.duckdb.DuckDBDriver");
      this.jdbcUrl = "jdbc:duckdb:" + config.duckdbPath;
      this.party = config.party;
      this.converter = new HiveDialectConverter(config.party, "default");
      initialize();
    }

    @Override
    public String preprocess(String query) {
      return converter.convert(query);
    }

    @Override
    public QueryResources execute(String query) throws SQLException {
      final Connection connection = DriverManager.getConnection(jdbcUrl);
      final Statement statement = connection.createStatement();
      statement.execute("CREATE SCHEMA IF NOT EXISTS \"default\"");
      statement.execute("SET search_path TO \"default\"");
      final ResultSet resultSet = statement.executeQuery(query);
      return new QueryResources(connection, statement, resultSet);
    }

    @Override
    public String describe() {
      return jdbcUrl;
    }

    private void initialize() throws SQLException {
      final File file = new File(jdbcUrl.substring("jdbc:duckdb:".length()));
      final File parent = file.getParentFile();
      if (parent != null) {
        parent.mkdirs();
      }
      try (Connection connection = DriverManager.getConnection(jdbcUrl);
          Statement statement = connection.createStatement()) {
        statement.execute("CREATE SCHEMA IF NOT EXISTS \"default\"");
        statement.execute("SET search_path TO \"default\"");
        if ("alice".equalsIgnoreCase(party)) {
          statement.execute(
              "CREATE TABLE IF NOT EXISTS user_credit (ID VARCHAR, credit_rank INTEGER, income INTEGER, age INTEGER)");
          if (tableCount(statement, "user_credit") == 0) {
            statement.execute(
                "INSERT INTO user_credit VALUES "
                    + "('id0001', 6, 100000, 20),"
                    + "('id0002', 5, 90000, 19),"
                    + "('id0003', 6, 89700, 32),"
                    + "('id0005', 6, 607000, 30),"
                    + "('id0006', 5, 30070, 25),"
                    + "('id0007', 6, 12070, 28),"
                    + "('id0008', 6, 200800, 50),"
                    + "('id0009', 6, 607000, 30),"
                    + "('id0010', 5, 30070, 25),"
                    + "('id0011', 5, 12070, 28),"
                    + "('id0012', 6, 200800, 50),"
                    + "('id0013', 5, 30070, 25),"
                    + "('id0014', 5, 12070, 28),"
                    + "('id0015', 6, 200800, 18),"
                    + "('id0016', 5, 30070, 26),"
                    + "('id0017', 5, 12070, 27),"
                    + "('id0018', 6, 200800, 16),"
                    + "('id0019', 6, 30070, 25),"
                    + "('id0020', 5, 12070, 28)");
          }
        } else {
          statement.execute(
              "CREATE TABLE IF NOT EXISTS user_stats (ID VARCHAR, order_amount INTEGER, is_active INTEGER)");
          if (tableCount(statement, "user_stats") == 0) {
            statement.execute(
                "INSERT INTO user_stats VALUES "
                    + "('id0001', 5000, 1),"
                    + "('id0002', 3000, 1),"
                    + "('id0003', 8000, 0),"
                    + "('id0005', 12000, 1),"
                    + "('id0006', 1500, 1),"
                    + "('id0007', 2500, 0),"
                    + "('id0008', 9500, 1),"
                    + "('id0009', 7000, 1),"
                    + "('id0010', 500, 0),"
                    + "('id0011', 3500, 1),"
                    + "('id0012', 15000, 1),"
                    + "('id0013', 2000, 0),"
                    + "('id0014', 4500, 1),"
                    + "('id0015', 6500, 1),"
                    + "('id0016', 1000, 0),"
                    + "('id0017', 8500, 1),"
                    + "('id0018', 11000, 1),"
                    + "('id0019', 3200, 1),"
                    + "('id0020', 7500, 0)");
          }
        }
      }
    }

    private static int tableCount(Statement statement, String tableName) throws SQLException {
      try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private static final class HiveDialectConverter {
    private static final Pattern MYSQL_SIGNED_CAST_PATTERN =
        Pattern.compile(
            "\\bCAST\\s*\\((.+?)\\s+AS\\s+(?:SIGNED|UNSIGNED)(?:\\s+INTEGER)?\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern MYSQL_STRING_CAST_PATTERN =
        Pattern.compile(
            "\\bCAST\\s*\\((.+?)\\s+AS\\s+(?:VARCHAR|CHAR)(?:\\s*\\(\\s*\\d+\\s*\\))?\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern CURDATE_PATTERN =
        Pattern.compile("\\bCURDATE\\s*\\(\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CURRENT_DATE_FN_PATTERN =
        Pattern.compile("\\bCURRENT_DATE\\s*\\(\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern NOW_PATTERN =
        Pattern.compile("\\bNOW\\s*\\(\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CURRENT_TIMESTAMP_FN_PATTERN =
        Pattern.compile("\\bCURRENT_TIMESTAMP\\s*\\(\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern LAST_DAY_PATTERN =
        Pattern.compile("\\bLAST_DAY\\s*\\(([^()]+?)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern STR_TO_DATE_PATTERN =
        Pattern.compile("\\bSTR_TO_DATE\\s*\\(([^,]+?),\\s*'([^']*)'\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern DATE_FORMAT_PATTERN =
        Pattern.compile("\\bDATE_FORMAT\\s*\\(([^,]+?),\\s*'([^']*)'\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern FROM_UNIXTIME_UNIX_TIMESTAMP_PATTERN =
        Pattern.compile("\\bFROM_UNIXTIME\\s*\\(\\s*UNIX_TIMESTAMP\\s*\\(([^,]+?),\\s*'([^']*)'\\s*\\)\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern DATE_ADD_PATTERN =
        Pattern.compile(
            "\\b(?:ADDDATE|DATE_ADD)\\s*\\(\\s*([^,]+?)\\s*,\\s*INTERVAL\\s+([^\\s,)]+)\\s+DAY\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern DATE_ADD_SIMPLE_PATTERN =
        Pattern.compile(
            "\\b(?:ADDDATE|DATE_ADD)\\s*\\(\\s*([^,]+?)\\s*,\\s*([^\\s,)]+)\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern DATE_SUB_PATTERN =
        Pattern.compile(
            "\\b(?:SUBDATE|DATE_SUB)\\s*\\(\\s*([^,]+?)\\s*,\\s*INTERVAL\\s+([^\\s,)]+)\\s+DAY\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern DATE_SUB_SIMPLE_PATTERN =
        Pattern.compile(
            "\\b(?:SUBDATE|DATE_SUB)\\s*\\(\\s*([^,]+?)\\s*,\\s*([^\\s,)]+)\\s*\\)",
            Pattern.CASE_INSENSITIVE);

    private final Pattern prefixPattern;

    private HiveDialectConverter(String party, String database) {
      final List<String> prefixes =
          new ArrayList<>(Arrays.asList("alice", "bob", "default", "hive_demo"));
      if (party != null && !party.isEmpty()) {
        prefixes.add(party.toLowerCase(Locale.ROOT));
      }
      if (database != null && !database.isEmpty()) {
        prefixes.add(database.toLowerCase(Locale.ROOT));
      }
      this.prefixPattern =
          Pattern.compile("\\b(?:" + String.join("|", prefixes) + ")\\.", Pattern.CASE_INSENSITIVE);
    }

    private String convert(String query) {
      String rewritten = query == null ? "" : query.trim();
      while (rewritten.endsWith(";")) {
        rewritten = rewritten.substring(0, rewritten.length() - 1).trim();
      }
      rewritten = prefixPattern.matcher(rewritten).replaceAll("");
      rewritten = rewritten.replaceAll("(?i)\\bIFNULL\\s*\\(", "COALESCE(");
      rewritten = NOW_PATTERN.matcher(rewritten).replaceAll("CURRENT_TIMESTAMP");
      rewritten = CURRENT_TIMESTAMP_FN_PATTERN.matcher(rewritten).replaceAll("CURRENT_TIMESTAMP");
      rewritten = CURDATE_PATTERN.matcher(rewritten).replaceAll("CURRENT_DATE");
      rewritten = CURRENT_DATE_FN_PATTERN.matcher(rewritten).replaceAll("CURRENT_DATE");
      rewritten = rewriteDateAddSub(rewritten, DATE_ADD_PATTERN, "DATE_ADD");
      rewritten = rewriteDateAddSub(rewritten, DATE_ADD_SIMPLE_PATTERN, "DATE_ADD");
      rewritten = rewriteDateAddSub(rewritten, DATE_SUB_PATTERN, "DATE_SUB");
      rewritten = rewriteDateAddSub(rewritten, DATE_SUB_SIMPLE_PATTERN, "DATE_SUB");
      rewritten = rewriteLastDay(rewritten);
      rewritten = rewriteStrToDate(rewritten);
      rewritten = rewriteFromUnixtimeUnixTimestamp(rewritten);
      rewritten = rewriteDateFormat(rewritten);
      rewritten = MYSQL_SIGNED_CAST_PATTERN.matcher(rewritten).replaceAll("CAST($1 AS BIGINT)");
      rewritten = MYSQL_STRING_CAST_PATTERN.matcher(rewritten).replaceAll("CAST($1 AS STRING)");
      return rewritten;
    }


    private String rewriteLastDay(String query) {
      return replaceAll(
          query,
          LAST_DAY_PATTERN,
          matcher -> "CAST(LAST_DAY(" + matcher.group(1).trim() + ") AS TIMESTAMP)");
    }

    private String rewriteStrToDate(String query) {
      return replaceAll(
          query,
          STR_TO_DATE_PATTERN,
          matcher ->
              "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP("
                  + matcher.group(1).trim()
                  + ", '"
                  + translateMySqlDateFormat(matcher.group(2))
                  + "')) AS TIMESTAMP)");
    }

    private String rewriteFromUnixtimeUnixTimestamp(String query) {
      return replaceAll(
          query,
          FROM_UNIXTIME_UNIX_TIMESTAMP_PATTERN,
          matcher ->
              "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP("
                  + matcher.group(1).trim()
                  + ", '"
                  + translateMySqlDateFormat(matcher.group(2))
                  + "')) AS TIMESTAMP)");
    }

    private String rewriteDateFormat(String query) {
      return replaceAll(
          query,
          DATE_FORMAT_PATTERN,
          matcher ->
              "DATE_FORMAT("
                  + matcher.group(1).trim()
                  + ", '"
                  + translateMySqlDateFormat(matcher.group(2))
                  + "')");
    }

    private String rewriteDateAddSub(String query, Pattern pattern, String funcName) {
      return replaceAll(
          query,
          pattern,
          matcher ->
              "CAST("
                  + funcName
                  + "("
                  + matcher.group(1).trim()
                  + ", "
                  + matcher.group(2).trim()
                  + ") AS TIMESTAMP)");
    }

    private static String replaceAll(
        String input,
        Pattern pattern,
        java.util.function.Function<java.util.regex.Matcher, String> rewriter) {
      java.util.regex.Matcher matcher = pattern.matcher(input);
      StringBuffer sb = new StringBuffer();
      while (matcher.find()) {
        matcher.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(rewriter.apply(matcher)));
      }
      matcher.appendTail(sb);
      return sb.toString();
    }

    private static String translateMySqlDateFormat(String fmt) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < fmt.length(); i++) {
        char ch = fmt.charAt(i);
        if (ch != '%' || i + 1 >= fmt.length()) {
          sb.append(ch);
          continue;
        }
        char token = fmt.charAt(++i);
        switch (token) {
          case 'Y':
            sb.append("yyyy");
            break;
          case 'y':
            sb.append("yy");
            break;
          case 'm':
            sb.append("MM");
            break;
          case 'c':
            sb.append('M');
            break;
          case 'd':
            sb.append("dd");
            break;
          case 'e':
            sb.append('d');
            break;
          case 'H':
            sb.append("HH");
            break;
          case 'k':
            sb.append('H');
            break;
          case 'h':
          case 'I':
            sb.append("hh");
            break;
          case 'l':
            sb.append('h');
            break;
          case 'i':
            sb.append("mm");
            break;
          case 's':
          case 'S':
            sb.append("ss");
            break;
          case 'p':
            sb.append('a');
            break;
          case 'M':
            sb.append("MMMM");
            break;
          case 'b':
            sb.append("MMM");
            break;
          case 'W':
            sb.append("EEEE");
            break;
          case 'a':
            sb.append("EEE");
            break;
          default:
            sb.append('%').append(token);
            break;
        }
      }
      return sb.toString();
    }
  }

}
