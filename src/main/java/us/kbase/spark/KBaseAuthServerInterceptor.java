/*
 * KBase Authentication Server Interceptor for Spark Connect
 *
 * This gRPC server interceptor validates KBase authentication tokens for
 * Spark Connect requests. It provides:
 *
 * - Localhost bypass: Local connections (127.0.0.1, ::1) are allowed without tokens
 * - Token validation: Remote connections require valid KBase tokens
 * - User verification: Token username must match the pod owner (USER env var)
 *
 * Configuration (environment variables):
 * - KBASE_AUTH_URL: KBase Auth2 service URL (required)
 * - USER: Pod owner username (required for remote access validation)
 *
 * Enable in spark-defaults.conf:
 *   spark.connect.grpc.interceptor.classes=us.kbase.spark.KBaseAuthServerInterceptor
 */
package us.kbase.spark;

// Use Spark's shaded gRPC classes (relocated under org.sparkproject.connect)
import org.sparkproject.connect.grpc.Grpc;
import org.sparkproject.connect.grpc.Metadata;
import org.sparkproject.connect.grpc.ServerCall;
import org.sparkproject.connect.grpc.ServerCallHandler;
import org.sparkproject.connect.grpc.ServerInterceptor;
import org.sparkproject.connect.grpc.Status;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gRPC server interceptor that validates KBase authentication tokens.
 *
 * <p>This interceptor is designed for Spark Connect servers running in Kubernetes
 * pods where each user has their own dedicated Spark cluster. It allows:
 * <ul>
 *   <li>Local connections without authentication (for notebooks in the same pod)</li>
 *   <li>Remote connections with KBase token authentication</li>
 * </ul>
 */
public class KBaseAuthServerInterceptor implements ServerInterceptor {

    private static final Logger LOGGER = Logger.getLogger(KBaseAuthServerInterceptor.class.getName());

    // Metadata keys for authentication headers
    private static final Metadata.Key<String> AUTH_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> KBASE_TOKEN_KEY =
            Metadata.Key.of("x-kbase-token", Metadata.ASCII_STRING_MARSHALLER);

    private final String authUrl;
    private final String podOwner;
    private final HttpClient httpClient;

    /**
     * Creates a new KBase auth interceptor.
     * Configuration is read from environment variables.
     *
     * @throws IllegalStateException if KBASE_AUTH_URL environment variable is not set
     */
    public KBaseAuthServerInterceptor() {
        String envAuthUrl = System.getenv("KBASE_AUTH_URL");
        if (envAuthUrl == null || envAuthUrl.isEmpty()) {
            throw new IllegalStateException(
                "KBASE_AUTH_URL environment variable is required but not set. " +
                "Please configure the KBase Auth2 service URL.");
        }
        this.authUrl = envAuthUrl;
        this.podOwner = System.getenv("USER");
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        LOGGER.info("KBase Auth Interceptor initialized:");
        LOGGER.info("  Auth URL: " + this.authUrl);
        LOGGER.info("  Pod Owner: " + this.podOwner);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        // =====================================================
        // LOCALHOST BYPASS: Skip validation for local connections
        // =====================================================
        // This allows notebooks running in the same pod to connect
        // without authentication, while requiring tokens for remote access.

        SocketAddress remoteAddr = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        LOGGER.fine("Incoming connection - remoteAddr: " + remoteAddr);

        if (remoteAddr instanceof InetSocketAddress) {
            InetSocketAddress inetAddr = (InetSocketAddress) remoteAddr;
            String hostAddress = inetAddr.getAddress().getHostAddress();
            boolean isLoopback = inetAddr.getAddress().isLoopbackAddress();
            LOGGER.fine("Connection from IP: " + hostAddress + " (isLoopback: " + isLoopback + ")");

            // Allow localhost connections without authentication
            // Use isLoopbackAddress() to handle all forms of localhost (127.0.0.1, ::1, 0:0:0:0:0:0:0:1, etc.)
            if (isLoopback) {
                LOGGER.fine("Allowing local connection from " + hostAddress + " without auth");
                return next.startCall(call, headers);
            }

            LOGGER.fine("Remote connection from " + hostAddress + " - requiring authentication");
        } else {
            LOGGER.warning("Remote address is not InetSocketAddress: " +
                (remoteAddr != null ? remoteAddr.getClass().getName() : "null"));
        }

        // =====================================================
        // REMOTE ACCESS: Validate KBase token
        // =====================================================

        // Extract token from metadata
        String token = extractToken(headers);

        if (token == null) {
            LOGGER.warning("Missing authentication token for remote connection");
            call.close(
                    Status.UNAUTHENTICATED.withDescription("Missing authentication token. " +
                            "Remote connections require a valid KBase token in the 'authorization' header."),
                    headers);
            return new ServerCall.Listener<>() {};
        }

        // Validate token with KBase Auth2
        try {
            String username = validateToken(token);

            // Check if user matches pod owner
            if (podOwner != null && !username.equals(podOwner)) {
                LOGGER.warning("User " + username + " attempted to access cluster owned by " + podOwner);
                call.close(
                        Status.PERMISSION_DENIED.withDescription(
                                "User '" + username + "' is not authorized to access this cluster. " +
                                        "This cluster belongs to '" + podOwner + "'."),
                        headers);
                return new ServerCall.Listener<>() {};
            }

            LOGGER.fine("Authenticated request from user: " + username);

            // Token valid, proceed with request
            return next.startCall(call, headers);

        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Token validation failed", e);
            call.close(
                    Status.UNAUTHENTICATED.withDescription(
                            "Authentication failed. Please check your token."),
                    headers);
            return new ServerCall.Listener<>() {};
        }
    }

    /**
     * Extract token from request metadata.
     * Checks both 'authorization' (Bearer token) and 'x-kbase-token' headers.
     */
    private String extractToken(Metadata headers) {
        // Try Authorization header first (Bearer token format)
        String authHeader = headers.get(AUTH_KEY);
        if (authHeader != null && authHeader.toLowerCase().startsWith("bearer ")) {
            return authHeader.substring(7);
        }

        // Fall back to x-kbase-token header
        String kbaseToken = headers.get(KBASE_TOKEN_KEY);
        if (kbaseToken != null && !kbaseToken.isEmpty()) {
            return kbaseToken;
        }

        return null;
    }

    /**
     * Validate token with KBase Auth2 service and return username.
     */
    private String validateToken(String token) throws IOException, InterruptedException {
        String tokenEndpoint = authUrl + (authUrl.endsWith("/") ? "" : "/") + "api/V2/token";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenEndpoint))
                .header("Authorization", token)
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("KBase auth service returned status " + response.statusCode());
        }

        // Parse JSON response to extract username
        // Using simple parsing to avoid additional dependencies
        String body = response.body();
        if (body == null || body.trim().isEmpty()) {
            throw new RuntimeException("KBase auth service returned an empty response body");
        }
        return extractJsonField(body, "user");
    }

    /**
     * Simple JSON field extraction (avoids external JSON library dependency).
     */
    private String extractJsonField(String json, String fieldName) {
        String searchPattern = "\"" + fieldName + "\":\"";
        int start = json.indexOf(searchPattern);
        if (start == -1) {
            throw new RuntimeException("Field '" + fieldName + "' not found in response");
        }
        start += searchPattern.length();
        int end = json.indexOf("\"", start);
        if (end == -1) {
            throw new RuntimeException("Malformed JSON response");
        }
        return json.substring(start, end);
    }
}
