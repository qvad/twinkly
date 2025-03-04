package org.example;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private Connection pgConnection;
    private Connection ybConnection;
    private boolean inTransaction = false; // Track if a transaction is active

    private final Map<String, String> preparedStatements = new HashMap<>();

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
        establishConnections();
    }

    private void establishConnections() {
        try {
            // Explicitly load PostgreSQL driver
            Class.forName("org.postgresql.Driver");

            Properties props = new Properties();
            props.setProperty("user", "postgres");
            props.setProperty("password", "postgres");

            // Keep connections open instead of creating new ones per query
            this.pgConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", props);
            this.ybConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5433/yugabyte", props);

            System.out.println("✅ PostgreSQL and YugabyteDB connections established.");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("PostgreSQL JDBC Driver not found!", e);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to connect to databases", e);
        }
    }

    private void reconnectIfNeeded() {
        try {
            if (pgConnection == null || pgConnection.isClosed()) {
                System.err.println("🔄 Reconnecting to PostgreSQL...");
                Properties props = new Properties();
                props.setProperty("user", "postgres");
                props.setProperty("password", "postgres");
                this.pgConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5433/postgres", props);
            }
            if (ybConnection == null || ybConnection.isClosed()) {
                System.err.println("🔄 Reconnecting to YugabyteDB...");
                Properties props = new Properties();
                props.setProperty("user", "postgres");
                props.setProperty("password", "postgres");
                this.ybConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5433/yugabyte", props);
            }
        } catch (SQLException e) {
            System.err.println("❌ Failed to reconnect: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try (
                InputStream input = clientSocket.getInputStream();
                OutputStream output = clientSocket.getOutputStream()
        ) {
            if (!handleStartupPacket(input, output)) {
                return; // Failed startup
            }

            while (!clientSocket.isClosed()) {
                int messageType = input.read();
                if (messageType == -1) {
                    System.out.println("Client disconnected.");
                    break;
                }

                // Ignore null bytes (\0) which are not valid message types
                if (messageType == 0) {
                    handleStartupMessage(input, output);
                    return;
                }

                System.out.println("Received message type: " + (char) messageType);

                switch (messageType) {
                    case 'Q': // Simple Query
                        handleQuery(input, output);
                        break;
                    case 'P': // Parse (Prepared Statement)
                        handleParse(input, output);
                        break;
                    case 'B': // Bind (Prepared Statement)
                        handleBind(input, output);
                        break;
                    case 'E': // Execute (Prepared Statement)
                        handleExecute(input, output);
                        break;
                    case 'D': // Describe
                        handleDescribe(input, output);
                        break;
                    case 'S': // Sync
                        handleSync(input, output);
                        break;
                    case 'X': // Termination
                        System.out.println("Client requested termination.");
                        return;
                    default:
                        System.err.println("Unknown message type: " + messageType + " (" + (char) messageType + ")");
                        logUnknownMessageContent(input);
                        skipUnknownMessage(input);
                        return;
                }
            }
        } catch (IOException | SQLException e) {
            System.err.println("I/O error: " + e.getMessage());
        } finally {
            closeConnections();
        }
    }

    private void handleStartupMessage(InputStream input, OutputStream output) throws IOException {
        // Read the 4-byte length field
        byte[] lengthBytes = new byte[4];
        if (input.read(lengthBytes) != 4) {
            System.err.println("Failed to read StartupMessage length.");
            return;
        }

        int length = ByteBuffer.wrap(lengthBytes).getInt();
        if (length <= 8) { // Minimum length is 8 (4 bytes for length + 4 bytes for protocol version)
            System.err.println("Invalid StartupMessage length: " + length);
            return;
        }

        // Read the 4-byte protocol version
        byte[] protocolBytes = new byte[4];
        if (input.read(protocolBytes) != 4) {
            System.err.println("Failed to read protocol version.");
            return;
        }

        int protocolVersion = ByteBuffer.wrap(protocolBytes).getInt();
        System.out.println("✅ StartupMessage received. Protocol version: " + protocolVersion);

        // Check if this is a valid PostgreSQL startup message (protocol version 3.0)
        if (protocolVersion != 196608) {
            System.err.println("Unsupported protocol version: " + protocolVersion);
            return;
        }

        // Parse the key-value pairs
        HashMap<String, String> startupParams = new HashMap<>();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        while ((b = input.read()) != -1) { // Read until a null terminator
            if (b == 0) {
                String param = buffer.toString(StandardCharsets.UTF_8);
                buffer.reset();

                if (param.isEmpty()) break; // End of key-value pairs
                String value = buffer.toString(StandardCharsets.UTF_8);

                // Store the key-value pair
                startupParams.put(param, value);
            } else {
                buffer.write(b);
            }
        }

        String user = startupParams.get("user");
        String database = startupParams.get("database");
        System.out.println("✅ Parsed StartupMessage: user = " + user + ", database = " + database);

        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "postgres");

        // Close existing PostgreSQL and YugabyteDB connections
        closeConnections();

        try {
            // Reinitialize connections to the new database
            pgConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5433/" + database, props);
            ybConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5433/" + database, props);
        } catch (SQLException e) {
            System.out.println("Failed to connect to database: " + e.getMessage());
            sendErrorToClient(output, "Database switch failed: " + e.getMessage());
        }

        sendCommandComplete(output, "Database switched to " + database);
        sendReadyForQuery(output, 'I');

        System.out.println("✅ Connections re-established for user: " + user + ", database: " + database);
    }

    private void logUnknownMessageContent(InputStream input) throws IOException {
        // Read the message length
        byte[] lengthBytes = new byte[4];
        if (input.read(lengthBytes) != 4) {
            System.err.println("Failed to read length of unknown message.");
            return;
        }

        // Parse length as unsigned int
        int length = parseUnsignedInt(lengthBytes) - 4; // Subtract 4 for the length field itself

        // Validate length (reasonable range: 1 to 8 MB)
        final int MAX_MESSAGE_LENGTH = 8 * 1024 * 1024; // 8 MB
        if (length <= 0 || length > MAX_MESSAGE_LENGTH) {
            System.err.println("Invalid or malformed message length: " + length + ". Skipping.");
            input.skip(4); // Skip the invalid length field to recover
            return;
        }

        // Check if sufficient bytes are available
        int availableBytes = input.available();
        if (length > availableBytes) {
            System.err.println("Malformed message: Declared length (" + length +
                    ") exceeds available bytes (" + availableBytes + "). Skipping.");
            input.skip(availableBytes); // Skip remaining available bytes
            return;
        }

        // Read the message content with a timeout
        try {
            byte[] content = input.readNBytes(length);
            System.err.println("Unknown message content (hex): " + bytesToHex(content));
        } catch (SocketTimeoutException e) {
            System.err.println("Socket timeout while reading unknown message content. Skipping.");
        }
    }

    private int parseUnsignedInt(byte[] bytes) {
        return (bytes[0] & 0xFF) << 24 |
                (bytes[1] & 0xFF) << 16 |
                (bytes[2] & 0xFF) << 8 |
                (bytes[3] & 0xFF);
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            hexString.append(String.format("%02x ", b));
        }
        return hexString.toString();
    }

    private void closeConnections() {
        try {
            if (pgConnection != null && !pgConnection.isClosed()) {
                pgConnection.close();
                System.out.println("PostgreSQL connection closed.");
            }
        } catch (SQLException e) {
            System.err.println("Error closing PostgreSQL connection: " + e.getMessage());
        }

        try {
            if (ybConnection != null && !ybConnection.isClosed()) {
                ybConnection.close();
                System.out.println("YugabyteDB connection closed.");
            }
        } catch (SQLException e) {
            System.err.println("Error closing YugabyteDB connection: " + e.getMessage());
        }
    }

    public boolean handleStartupPacket(InputStream input, OutputStream output) throws IOException {
        byte[] lengthBytes = new byte[4];
        if (input.read(lengthBytes) != 4) {
            System.err.println("Failed to read startup packet length.");
            return false;
        }

        int length = ByteBuffer.wrap(lengthBytes).getInt();
        if (length < 8) {
            System.err.println("Invalid startup packet length: " + length);
            return false;
        }

        byte[] startupPacket = new byte[length - 4];
        if (input.read(startupPacket) != length - 4) {
            System.err.println("Failed to read full startup packet.");
            return false;
        }

        ByteBuffer buffer = ByteBuffer.wrap(startupPacket);
        int protocolVersion = buffer.getInt();

        // Handle SSL request
        if (protocolVersion == 80877103) { // SSL request packet
            System.out.println("SSL request received, rejecting...");
            output.write(new byte[]{'N'}); // Reject SSL request
            output.flush();
            return handleStartupPacket(input, output); // Reattempt startup
        }

        System.out.println("Received startup packet (protocol version " + protocolVersion + ")");

        // Parse key-value pairs (user, database, etc.)
        String user = null, database = null;
        while (buffer.hasRemaining()) {
            String key = readNullTerminatedString(buffer);
            if (key.isEmpty()) break;
            String value = readNullTerminatedString(buffer);

            if (key.equals("user")) {
                user = value;
            } else if (key.equals("database")) {
                database = value;
            }
        }

        System.out.println("User: " + user + ", Database: " + database);

        // Skip password authentication
        sendAuthenticationOk(output);
        return true;
    }

    private void handleParse(InputStream input, OutputStream output) throws IOException {
        // Read message length
        byte[] lengthBytes = new byte[4];
        input.read(lengthBytes);
        int length = ByteBuffer.wrap(lengthBytes).getInt() - 4;

        // Read the prepared statement name (null-terminated string)
        String statementName = readNullTerminatedString(input);
        if (statementName.isEmpty()) {
            statementName = "unnamed";  // Ensure consistent naming
        }

        // Read the actual SQL query (null-terminated string)
        String query = readNullTerminatedString(input);

        // Read parameter count (not used for now)
        byte[] paramCountBytes = new byte[2];
        input.read(paramCountBytes);
        int paramCount = ((paramCountBytes[0] & 0xFF) << 8) | (paramCountBytes[1] & 0xFF);

        // Skip parameter types if any
        if (paramCount > 0) {
            byte[] paramTypes = new byte[paramCount * 4];
            input.read(paramTypes);
        }

        System.out.println("📢 Parsed prepared statement: " + statementName + " -> " + query);

        // Store the prepared statement
        preparedStatements.put(statementName, query);

        // Send ParseComplete response
        output.write(new byte[]{'1', 0, 0, 0, 4});
        output.flush();
    }

    private void handleBind(InputStream input, OutputStream output) throws IOException {
        // Read message length
        byte[] lengthBytes = new byte[4];
        input.read(lengthBytes);
        int length = ByteBuffer.wrap(lengthBytes).getInt() - 4;

        // Just skip the rest of the bind message for now
        if (length > 0) {
            input.readNBytes(length);
        }

        // Send BindComplete response
        output.write(new byte[]{'2', 0, 0, 0, 4});
        output.flush();
    }

    private void handleExecute(InputStream input, OutputStream output) throws IOException {
        reconnectIfNeeded();

        // Read message length
        byte[] lengthBytes = new byte[4];
        input.read(lengthBytes);
        int length = ByteBuffer.wrap(lengthBytes).getInt() - 4;

        // Read the portal name (null-terminated string)
        String portalName = readNullTerminatedString(input);

        // Read max rows to return
        byte[] maxRowsBytes = new byte[4];
        input.read(maxRowsBytes);
        int maxRows = ByteBuffer.wrap(maxRowsBytes).getInt();

        System.out.println("📢 Executing portal: " + portalName + ", MaxRows: " + maxRows);

        // For simplicity, use the 'unnamed' prepared statement by default
        String statementName = "unnamed";
        String query = preparedStatements.get(statementName);

        if (query == null) {
            System.err.println("❌ Prepared statement not found: " + statementName);
            sendErrorToClient(output, "Prepared statement not found: " + statementName);
            return;
        }

        boolean isTransactionCommand = query.matches("(?i)^(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE).*");

        if (query.matches("(?i)^BEGIN.*")) {
            inTransaction = true;
        } else if (query.matches("(?i)^(COMMIT|ROLLBACK).*")) {
            inTransaction = false;
        }

        boolean pgSuccess = true;
        boolean ybSuccess = true;
        SQLException pgException = null;
        SQLException ybException = null;

        try {
            reconnectIfNeeded();

            // Execute on PostgreSQL
            try (Statement pgStmt = pgConnection.createStatement()) {
                if (isTransactionCommand) {
                    pgStmt.execute(query);

                    if (query.matches("(?i)^BEGIN.*")) {
                        pgConnection.setAutoCommit(false);
                    } else if (query.matches("(?i)^COMMIT.*")) {
                        pgConnection.commit();
                        pgConnection.setAutoCommit(true);
                    } else if (query.matches("(?i)^ROLLBACK.*")) {
                        pgConnection.rollback();
                        pgConnection.setAutoCommit(true);
                    }
                } else {
                    pgStmt.execute(query);
                }
            } catch (SQLException e) {
                pgSuccess = false;
                pgException = e;
                System.err.println("❌ PostgreSQL Error: " + e.getMessage());
            }

            // Execute on YugabyteDB
            try (Statement ybStmt = ybConnection.createStatement()) {
                if (isTransactionCommand) {
                    ybStmt.execute(query);

                    if (query.matches("(?i)^BEGIN.*")) {
                        ybConnection.setAutoCommit(false);
                    } else if (query.matches("(?i)^COMMIT.*")) {
                        ybConnection.commit();
                        ybConnection.setAutoCommit(true);
                    } else if (query.matches("(?i)^ROLLBACK.*")) {
                        ybConnection.rollback();
                        ybConnection.setAutoCommit(true);
                    }
                } else {
                    ybStmt.execute(query);
                }
            } catch (SQLException e) {
                ybSuccess = false;
                ybException = e;
                System.err.println("❌ YugabyteDB Error: " + e.getMessage());
            }

            // Handle inconsistent execution results
            if (pgSuccess != ybSuccess) {
                System.err.println("⚠️ Inconsistent execution between PostgreSQL and YugabyteDB!");
                if (!pgSuccess) {
                    sendErrorToClient(output, "ERROR in PostgreSQL: " + pgException.getMessage());
                } else {
                    sendErrorToClient(output, "ERROR in YugabyteDB: " + ybException.getMessage());
                }
                return;
            }

            // If both fail, send an error
            if (!pgSuccess && !ybSuccess) {
                sendErrorToClient(output, "ERROR: " + pgException.getMessage());
                return;
            }

            // If both succeed, process results and send CommandComplete
            System.out.println("🔄 Transaction state: " + (inTransaction ? "ACTIVE" : "INACTIVE"));

            // Don't send CommandComplete here; it will be sent after Sync message

            // Extract command from query for the CommandComplete message
            String command = query.split("\\s+")[0].toUpperCase();
            sendCommandComplete(output, command);

        } catch (Exception e) {
            System.err.println("❌ Database error: " + e.getMessage());
            sendErrorToClient(output, e.getMessage());
        }
    }

    private void skipUnknownMessage(InputStream input) throws IOException {
        byte[] lengthBytes = new byte[4];

        // Read message length safely
        if (input.read(lengthBytes) != 4) {
            System.err.println("Failed to read message length.");
            return;
        }

        int length = ByteBuffer.wrap(lengthBytes).getInt() - 4;

        if (length < 0 || length > 10000) {  // Prevent absurdly large values
            System.err.println("Skipping invalid message length: " + length);
            return;
        }

        // Read and discard the full message to keep stream aligned
        input.readNBytes(length);
    }

    private void handleDescribe(InputStream input, OutputStream output) throws IOException {
        // Read message length
        byte[] lengthBytes = new byte[4];
        if (input.read(lengthBytes) != 4) {
            throw new IOException("Failed to read Describe message length.");
        }
        int length = ByteBuffer.wrap(lengthBytes).getInt() - 4;

        // Read type (S = Statement, P = Portal)
        int typeByte = input.read();
        if (typeByte == -1) {
            throw new IOException("Failed to read Describe type.");
        }
        char describeType = (char) typeByte;

        if (describeType != 'S' && describeType != 'P') {
            throw new IOException("Invalid Describe type: " + describeType);
        }

        // Read name (null-terminated string)
        String name = readNullTerminatedString(input);

        System.out.println("Describe: " + describeType + " " + name);

        // For now, send a NoData response (n)
        // Later, this can be extended to send RowDescription (T) if applicable
        sendNoDataResponse(output);
    }

    private void sendNoDataResponse(OutputStream output) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(5); // Message type (1 byte) + length (4 bytes)
        buffer.put((byte) 'n'); // NoData message type
        buffer.putInt(4);       // Message length (4 bytes, including length field)
        output.write(buffer.array());
        output.flush();
    }

    private void handleSync(InputStream input, OutputStream output) throws IOException {
        // Read the 4-byte length field (should always be 4 for Sync)
        byte[] lengthBytes = new byte[4];
        if (input.read(lengthBytes) != 4) {
            System.err.println("Failed to read Sync message length.");
            return;
        }

        int length = ByteBuffer.wrap(lengthBytes).getInt();
        if (length != 4) {
            System.err.println("Invalid Sync message length: " + length);
            return;
        }

        System.out.println("✅ Sync message received. Resetting transaction state.");

        // Send a ReadyForQuery (Z) message with transaction state 'I' (Idle)
        sendReadyForQuery(output, 'I');
    }

    private void sendAuthenticationOk(OutputStream output) throws IOException {
        // Send AuthenticationOk
        output.write(new byte[]{
                'R', 0, 0, 0, 8, 0, 0, 0, 0 // AuthenticationOk
        });

        // Send ParameterStatus (set client encoding)
        sendParameterStatus(output, "client_encoding", "UTF8");
        sendParameterStatus(output, "server_version", "14.2"); // Must be set to avoid errors
        sendParameterStatus(output, "server_version_num", "140002");
        sendParameterStatus(output, "DateStyle", "ISO, MDY");
        sendParameterStatus(output, "integer_datetimes", "on");
        sendParameterStatus(output, "TimeZone", "UTC");

        // Send ReadyForQuery (Idle mode)
        output.write(new byte[]{
                'Z', 0, 0, 0, 5, 'I'
        });

        output.flush();
    }

    private String readNullTerminatedString(ByteBuffer buffer) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            if (b == 0) break;
            byteArrayOutputStream.write(b);
        }
        return byteArrayOutputStream.toString(StandardCharsets.UTF_8);
    }

    private void sendParameterStatus(OutputStream output, String key, String value) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write(key.getBytes(StandardCharsets.UTF_8));
        byteArrayOutputStream.write(0);
        byteArrayOutputStream.write(value.getBytes(StandardCharsets.UTF_8));
        byteArrayOutputStream.write(0);

        byte[] parameterBytes = byteArrayOutputStream.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(5 + parameterBytes.length);
        buffer.put((byte) 'S');
        buffer.putInt(4 + parameterBytes.length);
        buffer.put(parameterBytes);

        output.write(buffer.array());
    }

    private void handleQuery(InputStream input, OutputStream output) throws IOException, SQLException {
        byte[] lengthBytes = new byte[4];
        input.read(lengthBytes);
        int length = ByteBuffer.wrap(lengthBytes).getInt();

        byte[] queryBytes = new byte[length - 4];
        input.read(queryBytes);
        String query = new String(queryBytes, StandardCharsets.UTF_8).trim();
        // Remove null terminator if present
        if (query.endsWith("\0")) {
            query = query.substring(0, query.length() - 1);
        }

        System.out.println("Received query: " + query);

        // Extract the command name
        String command = query.split("\\s+")[0].toUpperCase();

        boolean isTransactionCommand = command.equals("BEGIN") || command.equals("COMMIT") ||
                command.equals("ROLLBACK") || command.equals("SAVEPOINT") ||
                command.equals("RELEASE");

        System.out.println("Transaction state before query: " + (inTransaction ? "ACTIVE" : "INACTIVE"));

        // Update transaction state
        if (command.equals("BEGIN")) {
            inTransaction = true;
        } else if (command.equals("COMMIT") || command.equals("ROLLBACK")) {
            inTransaction = false;
        }

        // Handle empty or special queries
        if (query.trim().isEmpty()) {
            sendCommandComplete(output, "EMPTY");
            sendReadyForQuery(output, inTransaction ? 'T' : 'I');
            return;
        }

        // Reconnect if needed
        reconnectIfNeeded();

        // Ensure queries use the same connection inside a transaction
        if (inTransaction) {
            pgConnection.setAutoCommit(false);
            ybConnection.setAutoCommit(false);
            System.out.println("autoCommit remains FALSE inside transaction.");
        }

        // Prevent SAVEPOINT outside transactions
        if (command.equals("SAVEPOINT") && !inTransaction) {
            sendErrorToClient(output, "SAVEPOINT can only be used in transaction blocks");
            return;
        }

        // Special handling for transaction commands
        if (isTransactionCommand) {
            try {
                handleTransaction(output, pgConnection, ybConnection, query, command);
                System.out.println("Transaction state after query: " + (inTransaction ? "ACTIVE" : "INACTIVE"));
                return;
            } catch (SQLException e) {
                sendErrorToClient(output, e.getMessage());
                return;
            }
        }

        // Handle regular queries
        try {
            // Execute query on both databases
            boolean pgSuccess = true;
            boolean ybSuccess = true;
            SQLException pgException = null;
            SQLException ybException = null;
            boolean isQuery = false;
            boolean isQueryYB = false;

            // Execute on PostgreSQL
            try (Statement pgStmt = pgConnection.createStatement()) {
                String pgQuery = query.replace("WITH COLOCATION = true", "");
                if (pgQuery.contains("COLOCATION = true")) {
                    pgQuery = pgQuery.replace("COLOCATION = true", "");
                }
                isQuery = pgStmt.execute(pgQuery);


                // If it's a SELECT query
                if (isQuery) {
                    ResultSet pgResult = pgStmt.getResultSet();

                    // Execute on YugabyteDB
                    try (Statement ybStmt = ybConnection.createStatement()) {
                        isQueryYB = ybStmt.execute(query);
                        ResultSet ybResult = ybStmt.getResultSet();

                        // Compare results if not a system query
                        if (resultsDifferent(pgResult, ybResult)) {
                            System.err.println("Inconsistent! Results differ for query: " + query);
                        }

                        // Send PostgreSQL results to client
                        sendResultsToClient(output, pgResult);
                    } catch (SQLException e) {
                        ybSuccess = false;
                        ybException = e;
                        System.err.println("YugabyteDB Error: " + e.getMessage());

                        // Still send PostgreSQL results to client
                        sendResultsToClient(output, pgResult);
                    }
                } else {
                    // It's an UPDATE/INSERT/DELETE query
                    int pgUpdateCount = pgStmt.getUpdateCount();

                    // Execute on YugabyteDB
                    try (Statement ybStmt = ybConnection.createStatement()) {
                        ybStmt.execute(query);
                        int ybUpdateCount = ybStmt.getUpdateCount();

                        System.out.println("PostgreSQL updated rows: " + pgUpdateCount);
                        System.out.println("YugabyteDB updated rows: " + ybUpdateCount);

                        sendUpdateToClient(output, command, pgUpdateCount, inTransaction ? 'T' : 'I');
                    } catch (SQLException e) {
                        ybSuccess = false;
                        ybException = e;
                        System.err.println("YugabyteDB Error: " + e.getMessage());

                        sendUpdateToClient(output, command, pgUpdateCount, inTransaction ? 'T' : 'I');
                    }
                }
            } catch (SQLException e) {
                pgSuccess = false;
                pgException = e;
                System.err.println("PostgreSQL Error: " + e.getMessage());

                // Try YugabyteDB
                try (Statement ybStmt = ybConnection.createStatement()) {
                    isQueryYB = ybStmt.execute(query);

                    if (isQueryYB) {
                        // It's a SELECT query
                        ResultSet ybResult = ybStmt.getResultSet();
                        sendResultsToClient(output, ybResult);
                    } else {
                        // It's an UPDATE/INSERT/DELETE query
                        int ybUpdateCount = ybStmt.getUpdateCount();
                        sendUpdateToClient(output, command, ybUpdateCount, inTransaction ? 'T' : 'I');
                    }
                } catch (SQLException yb_e) {
                    ybSuccess = false;
                    ybException = yb_e;
                    System.err.println("YugabyteDB Error: " + yb_e.getMessage());

                    // Both databases failed
                    if (!pgSuccess && !ybSuccess) {
                        if (!ybException.getMessage().equals(pgException.getMessage())) {
                            sendErrorToClient(output, "ERROR in PostgreSQL: " + pgException.getMessage() +
                                    "\nERROR in YugabyteDB: " + ybException.getMessage());
                        } else {
                            sendErrorToClient(output, pgException.getMessage());
                        }
                    }
                }
            }

            // Check for inconsistent results
            if (pgSuccess != ybSuccess) {
                System.err.println("Inconsistent query behavior between PostgreSQL and YugabyteDB!");
                if (!pgSuccess) {
                    System.err.println("PostgreSQL failed but YugabyteDB succeeded.");
                } else {
                    System.err.println("YugabyteDB failed but PostgreSQL succeeded.");
                }
                // Note: We've already sent results from whichever database succeeded
            }

        } catch (Exception e) {
            System.err.println("Database error: " + e.getMessage());
            sendErrorToClient(output, e.getMessage());
        }
    }

    private void handleTransaction(OutputStream output, Connection pgConn, Connection ybConn, String query, String command) throws IOException, SQLException {
        System.out.println("Handling transaction command: " + command);

        // BEGIN transaction
        if (command.equalsIgnoreCase("BEGIN")) {
            pgConn.setAutoCommit(false);
            ybConn.setAutoCommit(false);
            inTransaction = true;
            System.out.println("Transaction started, autoCommit is now FALSE.");
        }

        try (Statement pgStmt = pgConn.createStatement();
             Statement ybStmt = ybConn.createStatement()) {

            // If it's a SAVEPOINT, ensure it's inside a transaction
            if (command.startsWith("SAVEPOINT") && !inTransaction) {
                sendErrorToClient(output, "SAVEPOINT can only be used inside transactions");
                return;
            }

            // Execute transaction command on both databases
            pgStmt.execute(query);
            ybStmt.execute(query);

            // COMMIT or ROLLBACK
            if (command.equalsIgnoreCase("COMMIT")) {
                pgConn.commit();
                ybConn.commit();
                pgConn.setAutoCommit(true);
                ybConn.setAutoCommit(true);
                inTransaction = false;
                System.out.println("Transaction committed.");
            } else if (command.equalsIgnoreCase("ROLLBACK")) {
                pgConn.rollback();
                ybConn.rollback();
                pgConn.setAutoCommit(true);
                ybConn.setAutoCommit(true);
                inTransaction = false;
                System.out.println("Transaction rolled back.");
            }

            sendCommandComplete(output, command);
            sendReadyForQuery(output, inTransaction ? 'T' : 'I');

        } catch (SQLException e) {
            System.err.println("Transaction error: " + e.getMessage());
            rollbackTransactions(pgConn, ybConn);
            throw e;
        }
    }

    private void rollbackTransactions(Connection pgConn, Connection ybConn) {
        try {
            if (pgConn != null && !pgConn.getAutoCommit()) {
                pgConn.rollback();
                pgConn.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            System.err.println("PostgreSQL rollback failed: " + ex.getMessage());
        }

        try {
            if (ybConn != null && !ybConn.getAutoCommit()) {
                ybConn.rollback();
                ybConn.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            System.err.println("YugabyteDB rollback failed: " + ex.getMessage());
        }

        inTransaction = false;
        System.out.println("Rollback attempted.");
    }

    private boolean resultsDifferent(ResultSet rs1, ResultSet rs2) throws SQLException {
        ResultSetMetaData metaData1 = rs1.getMetaData();
        ResultSetMetaData metaData2 = rs2.getMetaData();

        // Compare column count
        if (metaData1.getColumnCount() != metaData2.getColumnCount()) {
            return true;
        }

        // Create copies of the result sets to avoid consuming them
        rs1.beforeFirst();
        rs2.beforeFirst();

        // Compare row by row
        while (rs1.next() && rs2.next()) {
            for (int i = 1; i <= metaData1.getColumnCount(); i++) {
                Object val1 = rs1.getObject(i);
                Object val2 = rs2.getObject(i);

                // Handle null values
                if (val1 == null && val2 == null) {
                    continue;
                }

                if (val1 == null || val2 == null || !val1.equals(val2)) {
                    return true;
                }
            }
        }

        // Check if one result set has more rows than the other
        if (rs1.next() || rs2.next()) {
            return true;
        }

        // Reset to beginning for subsequent use
        rs1.beforeFirst();
        rs2.beforeFirst();

        return false;
    }

    private void sendResultsToClient(OutputStream output, ResultSet rs) throws SQLException, IOException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        System.out.println("Sending SELECT results: " + columnCount + " columns");

        // Build RowDescription (T) message
        ByteArrayOutputStream rowDescStream = new ByteArrayOutputStream();
        rowDescStream.write('T'); // RowDescription message type

        ByteArrayOutputStream columnData = new ByteArrayOutputStream();
        columnData.write(shortToBytes(columnCount)); // Number of columns

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            columnData.write(columnName.getBytes(StandardCharsets.UTF_8)); // Column name
            columnData.write(0); // Null-terminated

            columnData.write(intToBytes(0)); // Table OID (set to 0)
            columnData.write(shortToBytes(0)); // Column attribute number

            int pgType = convertJdbcTypeToPgType(metaData.getColumnType(i));
            int columnSize = metaData.getColumnDisplaySize(i);

            columnData.write(intToBytes(pgType)); // PostgreSQL Type OID
            columnData.write(shortToBytes(columnSize > 0 ? columnSize : 0)); // Column size
            columnData.write(intToBytes(-1)); // Type modifier (-1 means none)
            columnData.write(shortToBytes(0)); // Format code (0 = text)
        }

        // Calculate total message length
        byte[] columnMetadataBytes = columnData.toByteArray();
        int messageLength = 4 + columnMetadataBytes.length;

        rowDescStream.write(intToBytes(messageLength)); // Message length
        rowDescStream.write(columnMetadataBytes); // Column metadata content

        output.write(rowDescStream.toByteArray());
        output.flush();

        int rowCount = 0;

        // Send DataRow (D) messages
        while (rs.next()) {
            rowCount++;

            ByteArrayOutputStream dataRowStream = new ByteArrayOutputStream();
            dataRowStream.write('D'); // DataRow message type

            ByteArrayOutputStream rowData = new ByteArrayOutputStream();
            rowData.write(shortToBytes(columnCount)); // Number of columns

            for (int i = 1; i <= columnCount; i++) {
                Object value = rs.getObject(i);

                if (value == null) {
                    rowData.write(intToBytes(-1)); // NULL value
                } else {
                    byte[] valueBytes = value.toString().getBytes(StandardCharsets.UTF_8);
                    rowData.write(intToBytes(valueBytes.length));
                    rowData.write(valueBytes);
                }
            }

            byte[] rowBytes = rowData.toByteArray();
            int rowLength = 4 + rowBytes.length; // Length includes itself

            dataRowStream.write(intToBytes(rowLength)); // Message length
            dataRowStream.write(rowBytes); // Actual row data

            output.write(dataRowStream.toByteArray());
        }

        System.out.println("Sent " + rowCount + " rows to client");

        // Send CommandComplete (C)
        sendCommandComplete(output, "SELECT " + rowCount);

        // Send ReadyForQuery (Z) with correct transaction state
        sendReadyForQuery(output, inTransaction ? 'T' : 'I');
    }

    // Convert JDBC types to PostgreSQL OIDs
    private int convertJdbcTypeToPgType(int jdbcType) {
        switch (jdbcType) {
            case Types.BOOLEAN:
                return 16; // bool
            case Types.SMALLINT:
                return 21; // int2
            case Types.INTEGER:
                return 23; // int4
            case Types.BIGINT:
                return 20; // int8
            case Types.REAL:
                return 700; // float4
            case Types.FLOAT:
            case Types.DOUBLE:
                return 701; // float8
            case Types.NUMERIC:
            case Types.DECIMAL:
                return 1700; // numeric
            case Types.CHAR:
                return 18; // char
            case Types.VARCHAR:
                return 1043; // varchar
            case Types.DATE:
                return 1082; // date
            case Types.TIME:
                return 1083; // time
            case Types.TIMESTAMP:
                return 1114; // timestamp
            case Types.BINARY:
            case Types.VARBINARY:
                return 17; // bytea
            case Types.OTHER:
                return 25; // text (default)
            default:
                return 25; // text
        }
    }

    private void sendUpdateToClient(OutputStream output, String command, int updateCount, char transactionStatus) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        buffer.write('C'); // CommandComplete message type

        // Handle transaction messages properly
        String commandTag;
        if (command.equalsIgnoreCase("INSERT")) {
            commandTag = "INSERT 0 " + updateCount;
        } else if (command.equalsIgnoreCase("UPDATE")) {
            commandTag = "UPDATE " + updateCount;
        } else if (command.equalsIgnoreCase("DELETE")) {
            commandTag = "DELETE " + updateCount;
        } else if (command.equalsIgnoreCase("EXECUTE")) {
            commandTag = "EXECUTE";
        } else if (command.equalsIgnoreCase("BEGIN")) {
            commandTag = "BEGIN";
        } else if (command.equalsIgnoreCase("COMMIT")) {
            commandTag = "COMMIT";
        } else if (command.equalsIgnoreCase("ROLLBACK")) {
            commandTag = "ROLLBACK";
        } else {
            commandTag = command.toUpperCase() + " " + updateCount;
        }

        byte[] commandBytes = commandTag.getBytes(StandardCharsets.UTF_8);

        // Calculate correct message length
        int messageLength = 4 + commandBytes.length + 1; // 4-byte length + command + null terminator

        buffer.write(intToBytes(messageLength)); // Correct message length
        buffer.write(commandBytes);
        buffer.write(0); // Null terminator

        System.out.println("Sending CommandComplete: " + commandTag);

        output.write(buffer.toByteArray());
        output.flush();

        // Send ReadyForQuery with correct transaction state
        sendReadyForQuery(output, transactionStatus);
    }

    private void sendErrorToClient(OutputStream output, String errorMessage) throws IOException {
        ByteArrayOutputStream errorStream = new ByteArrayOutputStream();

        errorStream.write('E'); // ErrorResponse message type

        ByteArrayOutputStream errorContent = new ByteArrayOutputStream();

        // Severity field (S = ERROR)
        errorContent.write('S');
        errorContent.write("ERROR".getBytes(StandardCharsets.UTF_8));
        errorContent.write(0); // Null terminator

        // Message field (M = Error message)
        errorContent.write('M');
        errorContent.write(errorMessage.getBytes(StandardCharsets.UTF_8));
        errorContent.write(0); // Null terminator

        // SQLSTATE (C = SQLSTATE error code, using a generic example '42601' - syntax error)
        errorContent.write('C');
        errorContent.write("42601".getBytes(StandardCharsets.UTF_8));
        errorContent.write(0); // Null terminator

        // End of error message (final 0 byte)
        errorContent.write(0);

        // Calculate message length (4 bytes for length + content length)
        byte[] errorBytes = errorContent.toByteArray();
        int messageLength = 4 + errorBytes.length;

        // Write message length
        errorStream.write(intToBytes(messageLength));

        // Write actual error message content
        errorStream.write(errorBytes);

        // Send error response to client
        output.write(errorStream.toByteArray());
        output.flush();

        // Send ReadyForQuery (Z) to properly close the error response
        sendReadyForQuery(output, inTransaction ? 'T' : 'I');
    }

    private void sendCommandComplete(OutputStream output, String commandTag) throws IOException {
        byte[] commandBytes = commandTag.getBytes(StandardCharsets.UTF_8);
        int messageLength = 4 + commandBytes.length + 1; // 4 bytes for length + command + null terminator

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write('C'); // CommandComplete message type
        buffer.write(intToBytes(messageLength)); // Message length
        buffer.write(commandBytes);
        buffer.write(0); // Null terminator

        System.out.println("Sending CommandComplete: " + commandTag);

        output.write(buffer.toByteArray());
        output.flush();
    }

    private void sendReadyForQuery(OutputStream output, char transactionStatus) throws IOException {
        System.out.println("Sending ReadyForQuery: " + transactionStatus);
        output.write(new byte[]{
                'Z', 0, 0, 0, 5, (byte) transactionStatus
        });
        output.flush();
    }

    private String readNullTerminatedString(InputStream input) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        while (true) {
            int b = input.read();
            if (b == 0 || b == -1) break; // Stop at null terminator
            byteArrayOutputStream.write(b);
        }
        return byteArrayOutputStream.toString(StandardCharsets.UTF_8);
    }

    private byte[] intToBytes(int value) {
        return new byte[]{
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value
        };
    }

    private byte[] shortToBytes(int value) {
        return new byte[]{
                (byte) (value >>> 8),
                (byte) value
        };
    }
}