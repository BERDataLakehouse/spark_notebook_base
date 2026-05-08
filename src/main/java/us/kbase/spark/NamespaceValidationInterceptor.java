/*
 * Namespace Validation Server Interceptor for Spark Connect
 *
 * This gRPC server interceptor validates CREATE DATABASE/SCHEMA/NAMESPACE SQL
 * commands against allowed namespace prefixes or configured Polaris catalog
 * aliases. Users can only create Delta/Hive databases whose names start with
 * their allowed prefixes, or Iceberg namespaces inside catalog aliases assigned
 * to the current user.
 *
 * Configuration (environment variables):
 * - BERDL_ALLOWED_NAMESPACE_PREFIXES: Comma-separated list of allowed prefixes
 *   (e.g., "u_tgu2__,kbase_,research_")
 * - BERDL_ALLOWED_NAMESPACE_CATALOGS: Comma-separated list of allowed catalog
 *   aliases for catalog-qualified Iceberg namespaces (e.g., "my,tgu2,kbase")
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
 * statements where the database name does not match one of the configured
 * allowed namespace prefixes or catalog aliases. This prevents users from
 * creating Delta/Hive databases outside their governance-assigned namespaces
 * while still allowing Polaris Iceberg namespaces inside user-visible catalogs.
 *
 * <p>Allowed prefixes are loaded from the {@code BERDL_ALLOWED_NAMESPACE_PREFIXES}
 * environment variable at interceptor initialization. Allowed catalog aliases are
 * loaded from {@code BERDL_ALLOWED_NAMESPACE_CATALOGS}. Both formats are
 * comma-separated lists.
 *
 * <p>The {@code default} database is always allowed.
 */
public class NamespaceValidationInterceptor implements ServerInterceptor {

    private static final Logger LOGGER = Logger.getLogger(NamespaceValidationInterceptor.class.getName());
    private static final String IDENTIFIER_PART = "(`[^`]+`|\"[^\"]+\"|[^\\s.;]+)";

    /**
     * Regex to match CREATE DATABASE/SCHEMA/NAMESPACE statements and extract the database or namespace identifier.
     *
     * <p>Handles:
     * <ul>
     *   <li>{@code CREATE DATABASE mydb}</li>
     *   <li>{@code CREATE SCHEMA IF NOT EXISTS mydb}</li>
     *   <li>{@code CREATE NAMESPACE my.demo} (Spark 4.0 / Iceberg)</li>
     *   <li>{@code CREATE NAMESPACE `my`.`demo`} (part-quoted multipart identifier)</li>
     *   <li>{@code CREATE DATABASE `my_db`} (backtick-quoted)</li>
     *   <li>{@code CREATE DATABASE "my_db"} (double-quote-quoted)</li>
     *   <li>Case-insensitive matching</li>
     * </ul>
     *
     * <p>Group 3 captures the database or namespace identifier (with optional backticks/quotes).
     */
    private static final Pattern CREATE_DB_PATTERN = Pattern.compile(
            "(?i)\\bCREATE\\s+(DATABASE|SCHEMA|NAMESPACE)\\s+(IF\\s+NOT\\s+EXISTS\\s+)?("
                    + IDENTIFIER_PART + "(\\s*\\.\\s*" + IDENTIFIER_PART + ")*\\s*;?)");

    private final List<String> allowedPrefixes;
    private final List<String> allowedCatalogs;

    /**
     * Creates a new namespace validation interceptor.
     *
     * <p>Reads allowed prefixes from the {@code BERDL_ALLOWED_NAMESPACE_PREFIXES}
     * environment variable. If the variable is not set or empty, the interceptor
     * logs a warning and will reject all unqualified CREATE DATABASE statements
     * except for the {@code default} database.
     */
    public NamespaceValidationInterceptor() {
        this(
                System.getenv("BERDL_ALLOWED_NAMESPACE_PREFIXES"),
                System.getenv("BERDL_ALLOWED_NAMESPACE_CATALOGS"));
    }

    NamespaceValidationInterceptor(String envPrefixes, String envCatalogs) {
        this.allowedPrefixes = parseCsvEnv(envPrefixes);
        this.allowedCatalogs = parseCsvEnv(envCatalogs);

        if (this.allowedPrefixes.isEmpty()) {
            LOGGER.warning("BERDL_ALLOWED_NAMESPACE_PREFIXES not set. "
                    + "All unqualified CREATE DATABASE statements will be rejected except for 'default'.");
        }

        LOGGER.info("Namespace Validation Interceptor initialized with allowed prefixes: "
                + this.allowedPrefixes + " and allowed catalogs: " + this.allowedCatalogs);
    }

    private static List<String> parseCsvEnv(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }

        List<String> items = new ArrayList<>();
        for (String item : value.split(",")) {
            String trimmed = item.trim();
            if (!trimmed.isEmpty()) {
                items.add(trimmed.toLowerCase(Locale.ROOT));
            }
        }
        return Collections.unmodifiableList(items);
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
     * database name is checked against allowed namespace prefixes and catalog
     * aliases. The {@code default} database is always allowed.
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

        String dbName = normalizeIdentifier(matcher.group(3));

        // Always allow the 'default' database
        if ("default".equals(dbName)) {
            return null;
        }

        if (hasAllowedPrefix(dbName)) {
            return null;
        }

        String[] catalogParts = dbName.split("\\.", 2);
        if (catalogParts.length == 2) {
            String catalog = catalogParts[0];
            String namespace = catalogParts[1];

            // Explicit spark_catalog references are still Delta/Hive namespaces
            // and must obey prefix-based governance.
            if ("spark_catalog".equals(catalog)) {
                if ("default".equals(namespace) || hasAllowedPrefix(namespace)) {
                    return null;
                }
                return deniedMessage(dbName);
            }

            if (allowedCatalogs.contains(catalog)) {
                return null;
            }
        }

        return deniedMessage(dbName);
    }

    private boolean hasAllowedPrefix(String dbName) {
        for (String prefix : allowedPrefixes) {
            if (dbName.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private String deniedMessage(String dbName) {
        return "Database name '" + dbName + "' is not allowed. " +
                "Unqualified Delta/Hive database names must start with one of the following prefixes: " +
                allowedPrefixes + ". Catalog-qualified Iceberg namespaces must use one of the following catalogs: " +
                allowedCatalogs + ". " +
                "Use create_namespace_if_not_exists() to create databases with the correct prefix.";
    }

    private String normalizeIdentifier(String identifier) {
        String value = identifier.trim();

        while (value.endsWith(";")) {
            value = value.substring(0, value.length() - 1).trim();
        }

        List<String> parts = splitIdentifierParts(value);
        List<String> normalizedParts = new ArrayList<>();
        for (String part : parts) {
            String normalized = part.trim();
            if ((normalized.startsWith("`") && normalized.endsWith("`")) ||
                    (normalized.startsWith("\"") && normalized.endsWith("\""))) {
                normalized = normalized.substring(1, normalized.length() - 1);
            }
            normalizedParts.add(normalized.toLowerCase(Locale.ROOT));
        }

        return String.join(".", normalizedParts);
    }

    private List<String> splitIdentifierParts(String identifier) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        Character quote = null;

        for (int i = 0; i < identifier.length(); i++) {
            char c = identifier.charAt(i);
            if (quote != null) {
                current.append(c);
                if (c == quote) {
                    quote = null;
                }
            } else if (c == '`' || c == '"') {
                quote = c;
                current.append(c);
            } else if (c == '.') {
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        parts.add(current.toString());
        return parts;
    }
}
