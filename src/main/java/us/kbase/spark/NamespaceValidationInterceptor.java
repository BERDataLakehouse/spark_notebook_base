/*
 * Namespace Validation Server Interceptor for Spark Connect
 *
 * This gRPC server interceptor validates CREATE DATABASE/SCHEMA/NAMESPACE SQL
 * commands against allowed namespace prefixes. Users can only create databases
 * whose names start with their allowed prefixes (user prefix or writable
 * tenant prefix).
 *
 * Configuration (environment variables):
 * - BERDL_ALLOWED_NAMESPACE_PREFIXES: Comma-separated list of allowed prefixes
 *   (e.g., "u_tgu2__,kbase_,research_")
 *
 * Enable in spark-defaults.conf:
 *   spark.connect.grpc.interceptor.classes=...,us.kbase.spark.NamespaceValidationInterceptor
 */
package us.kbase.spark;

import org.sparkproject.connect.grpc.Metadata;
import org.sparkproject.connect.grpc.ServerCall;
import org.sparkproject.connect.grpc.ServerCallHandler;
import org.sparkproject.connect.grpc.ServerInterceptor;
import org.sparkproject.connect.grpc.Status;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * gRPC server interceptor that validates namespace creation against allowed prefixes.
 *
 * <p>Intercepts SQL commands sent via Spark Connect and blocks CREATE DATABASE/SCHEMA/NAMESPACE
 * statements where the database name does not match one of the configured allowed
 * namespace prefixes. This prevents users from creating databases outside their
 * governance-assigned namespaces.
 *
 * <p>Allowed prefixes are loaded from the {@code BERDL_ALLOWED_NAMESPACE_PREFIXES}
 * environment variable at interceptor initialization. The format is a comma-separated
 * list (e.g., {@code "u_alice__,kbase_,research_"}).
 *
 * <p>The {@code default} database is always allowed.
 */
public class NamespaceValidationInterceptor implements ServerInterceptor {

    private static final Logger LOGGER = Logger.getLogger(NamespaceValidationInterceptor.class.getName());

    /**
     * Regex to match CREATE DATABASE/SCHEMA/NAMESPACE statements and extract the database name.
     *
     * <p>Handles:
     * <ul>
     *   <li>{@code CREATE DATABASE mydb}</li>
     *   <li>{@code CREATE SCHEMA IF NOT EXISTS mydb}</li>
     *   <li>{@code CREATE NAMESPACE mydb} (Spark 4.0 synonym)</li>
     *   <li>{@code CREATE DATABASE `my_db`} (backtick-quoted)</li>
     *   <li>{@code CREATE DATABASE "my_db"} (double-quote-quoted)</li>
     *   <li>Case-insensitive matching</li>
     * </ul>
     *
     * <p>Group 3 captures the database name (with optional backticks/quotes).
     */
    private static final Pattern CREATE_DB_PATTERN = Pattern.compile(
            "(?i)\\bCREATE\\s+(DATABASE|SCHEMA|NAMESPACE)\\s+(IF\\s+NOT\\s+EXISTS\\s+)?(`[^`]+`|\"[^\"]+\"|\\S+)");

    private final List<String> allowedPrefixes;

    /**
     * Creates a new namespace validation interceptor.
     *
     * <p>Reads allowed prefixes from the {@code BERDL_ALLOWED_NAMESPACE_PREFIXES}
     * environment variable. If the variable is not set or empty, the interceptor
     * logs a warning and will reject all CREATE DATABASE statements (except for
     * the {@code default} database).
     */
    public NamespaceValidationInterceptor() {
        String envPrefixes = System.getenv("BERDL_ALLOWED_NAMESPACE_PREFIXES");
        if (envPrefixes == null || envPrefixes.trim().isEmpty()) {
            LOGGER.warning("BERDL_ALLOWED_NAMESPACE_PREFIXES not set. " +
                    "All CREATE DATABASE statements will be rejected except for 'default'.");
            this.allowedPrefixes = Collections.emptyList();
        } else {
            List<String> prefixes = new ArrayList<>();
            for (String prefix : envPrefixes.split(",")) {
                String trimmed = prefix.trim();
                if (!trimmed.isEmpty()) {
                    prefixes.add(trimmed);
                }
            }
            this.allowedPrefixes = Collections.unmodifiableList(prefixes);
        }

        LOGGER.info("Namespace Validation Interceptor initialized with allowed prefixes: " +
                this.allowedPrefixes);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        // Only intercept ExecutePlan calls — other RPCs (Config, AnalyzePlan, etc.) pass through
        String fullMethodName = call.getMethodDescriptor().getFullMethodName();
        if (!fullMethodName.contains("ExecutePlan")) {
            return next.startCall(call, headers);
        }

        ServerCall.Listener<ReqT> listener = next.startCall(call, headers);

        return new ServerCall.Listener<ReqT>() {
            private boolean rejected = false;

            @Override
            public void onMessage(ReqT message) {
                if (rejected) {
                    return;
                }

                String sql = extractSqlFromMessage(message);
                if (sql != null) {
                    String error = validateCreateDatabase(sql);
                    if (error != null) {
                        LOGGER.warning("Namespace validation failed: " + error);
                        rejected = true;
                        call.close(
                                Status.PERMISSION_DENIED.withDescription(error),
                                new Metadata());
                        return;
                    }
                }

                listener.onMessage(message);
            }

            @Override
            public void onHalfClose() {
                if (!rejected) {
                    listener.onHalfClose();
                }
            }

            @Override
            public void onCancel() {
                if (!rejected) {
                    listener.onCancel();
                }
            }

            @Override
            public void onComplete() {
                if (!rejected) {
                    listener.onComplete();
                }
            }

            @Override
            public void onReady() {
                if (!rejected) {
                    listener.onReady();
                }
            }
        };
    }

    /**
     * Extract SQL text from a Spark Connect ExecutePlanRequest message.
     *
     * <p>Uses reflection-free string inspection of the protobuf message's
     * {@code toString()} output to extract SQL text. This avoids direct
     * dependency on Spark Connect proto classes (which use shaded protobuf)
     * while remaining reliable for SQL command detection.
     *
     * <p>In Spark 4.0, SQL is always in a {@code query} field within a {@code sql} message:
     * <pre>
     *   plan {
     *     command {
     *       sql_command {
     *         input {
     *           sql {
     *             query: "CREATE DATABASE mydb"
     *           }
     *         }
     *       }
     *     }
     *   }
     * </pre>
     *
     * @param message the gRPC request message
     * @return the SQL string, or null if the message is not a SQL command
     */
    private <ReqT> String extractSqlFromMessage(ReqT message) {
        try {
            String protoStr = message.toString();

            // In Spark 4.0, SQL is always in a "query" field within a "sql" message:
            //   sql_command { input { sql { query: "..." } } }  (DDL/commands)
            //   root { sql { query: "..." } }                   (queries)
            // In both cases, extract the "query" field value.
            if (!protoStr.contains("query:")) {
                return null;
            }
            return extractQuotedField(protoStr, "query");

        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Could not extract SQL from message", e);
            return null;
        }
    }

    /**
     * Extract a quoted string field value from protobuf toString() output.
     *
     * <p>Looks for patterns like {@code fieldName: "value"} in the protobuf
     * text format and extracts the quoted value.
     *
     * @param protoStr the protobuf toString() output
     * @param fieldName the field name to extract
     * @return the field value, or null if not found
     */
    private String extractQuotedField(String protoStr, String fieldName) {
        String searchPattern = fieldName + ": \"";
        int start = protoStr.indexOf(searchPattern);
        if (start == -1) {
            return null;
        }
        start += searchPattern.length();

        // Find the closing quote, handling escaped quotes
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < protoStr.length(); i++) {
            char c = protoStr.charAt(i);
            if (c == '\\' && i + 1 < protoStr.length()) {
                sb.append(protoStr.charAt(i + 1));
                i++;
            } else if (c == '"') {
                break;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Validate a SQL statement for namespace creation rules.
     *
     * <p>If the SQL contains a CREATE DATABASE/SCHEMA/NAMESPACE statement, the
     * database name is checked against the allowed namespace prefixes. The
     * {@code default} database is always allowed.
     *
     * @param sql the SQL statement to validate
     * @return an error message if validation fails, or null if the statement is allowed
     */
    String validateCreateDatabase(String sql) {
        Matcher matcher = CREATE_DB_PATTERN.matcher(sql);
        if (!matcher.find()) {
            // Not a CREATE DATABASE/SCHEMA/NAMESPACE statement — allow
            return null;
        }

        String dbName = matcher.group(3);

        // Strip quoting (backticks or double quotes) if present
        if ((dbName.startsWith("`") && dbName.endsWith("`")) ||
                (dbName.startsWith("\"") && dbName.endsWith("\""))) {
            dbName = dbName.substring(1, dbName.length() - 1);
        }

        // Strip trailing semicolons
        if (dbName.endsWith(";")) {
            dbName = dbName.substring(0, dbName.length() - 1);
        }

        // Hive stores database names in lowercase
        dbName = dbName.toLowerCase(Locale.ROOT);

        // Always allow the 'default' database
        if ("default".equals(dbName)) {
            return null;
        }

        // Check against allowed prefixes
        for (String prefix : allowedPrefixes) {
            if (dbName.startsWith(prefix.toLowerCase(Locale.ROOT))) {
                return null;
            }
        }

        return "Database name '" + dbName + "' is not allowed. " +
                "Database names must start with one of the following prefixes: " +
                allowedPrefixes + ". " +
                "Use create_namespace_if_not_exists() to create databases with the correct prefix.";
    }
}
