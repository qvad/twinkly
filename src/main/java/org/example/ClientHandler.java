package org.example;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
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


    private Map<String, String> preparedStatements = new HashMap<>();

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
        try {
            // Explicitly load PostgreSQL driver
            Class.forName("org.postgresql.Driver");

            Properties props = new Properties();
            props.setProperty("user", "postgres");
            props.setProperty("password", "postgres");
            props.setProperty("ssl", "false");

            pgConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5433/postgres", props);
            ybConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5433/yugabyte", props);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("PostgreSQL JDBC Driver not found!", e);
        } catch (SQLException e) {
            try {
                if (pgConnection != null) {
                    pgConnection.close();
                }
            } catch (SQLException ex) {
                e.printStackTrace();
            }
            try {
                if (ybConnection != null) {
                    ybConnection.close();
                }
            } catch (SQLException ex) {
                e.printStackTrace();
            }
            e.printStackTrace();
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

            while (true) {
                int messageType = input.read();
                if (messageType == -1) {
                    System.out.println("Client disconnected.");
                    return;
                }

                // Ignore null bytes (\0) which are not valid message types
                if (messageType == 0) {
                    continue;
                }

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
                        System.out.println("Client disconnected.");
                        return;
                    default:
                        System.err.println("Unknown message type: " + (char) messageType);
                        skipUnknownMessage(input); // Ignore unknown message
                }
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
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
        byte[] lengthBytes = new byte[4];
        input.read(lengthBytes);
        int length = ByteBuffer.wrap(lengthBytes).getInt() - 4;

        // Read statement name (null-terminated string)
        ByteArrayOutputStream nameStream = new ByteArrayOutputStream();
        while (true) {
            int b = input.read();
            if (b == 0) break;
            nameStream.write(b);
        }
        String statementName = new String(nameStream.toByteArray(), StandardCharsets.UTF_8);

        // Read query string (null-terminated)
        ByteArrayOutputStream queryStream = new ByteArrayOutputStream();
        while (true) {
            int b = input.read();
            if (b == 0) break;
            queryStream.write(b);
        }
        String query = new String(queryStream.toByteArray(), StandardCharsets.UTF_8);

        // Assign a default name if the statement name is empty
        if (statementName.isEmpty()) {
            statementName = "unnamed_" + System.nanoTime();
        }

        System.out.println("Parsed prepared statement: " + statementName + " -> " + query);

        // Store query in prepared statement cache
        preparedStatements.put(statementName, query);

        // Send ParseComplete response
        output.write(new byte[]{'1', 0, 0, 0, 4});
        output.flush();
    }

    private void handleBind(InputStream input, OutputStream output) throws IOException {
        skipUnknownMessage(input); // Ignore parameters for now

        // Send BindComplete response
        output.write(new byte[]{'2', 0, 0, 0, 4});
        output.flush();
    }

    private void handleExecute(InputStream input, OutputStream output) throws IOException {
        // Read message length
        byte[] lengthBytes = new byte[4];
        input.read(lengthBytes);
        int length = ByteBuffer.wrap(lengthBytes).getInt() - 4;

        // Read the prepared statement name (null-terminated string)
        ByteArrayOutputStream nameStream = new ByteArrayOutputStream();
        while (true) {
            int b = input.read();
            if (b == 0) break;
            nameStream.write(b);
        }
        String statementName = new String(nameStream.toByteArray(), StandardCharsets.UTF_8);

        // Read portal name (usually empty, terminated by 0)
        while (input.read() != 0) {}

        // Read max rows to return (ignore for now)
        byte[] maxRowsBytes = new byte[4];
        input.read(maxRowsBytes);

        System.out.println("Executing prepared statement: " + statementName);

        // Retrieve the actual SQL query from the prepared statement cache
        String query = preparedStatements.get(statementName);
        if (query == null) {
            System.err.println("Prepared statement not found: " + statementName);
            sendErrorToClient(output, "Prepared statement not found");
            return;
        }

        // Execute the query
        try (Statement pgStmt = pgConnection.createStatement();
             Statement ybStmt = ybConnection.createStatement()) {

            boolean isQuery = pgStmt.execute(query);
            boolean isQueryYB = ybStmt.execute(query);

            if (isQuery && isQueryYB) {
                ResultSet pgResult = pgStmt.getResultSet();
                ResultSet ybResult = ybStmt.getResultSet();
                if (!compareResults(pgResult, ybResult)) {
                    System.err.println("Results differ for query: " + query);
                }
                sendResultsToClient(output, pgResult);
            } else {
                int pgUpdate = pgStmt.getUpdateCount();
                int ybUpdate = ybStmt.getUpdateCount();
                if (pgUpdate != ybUpdate) {
                    System.err.println("Update results differ for query: " + query);
                }
                sendUpdateToClient(output, String.valueOf(pgUpdate));
            }
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            sendErrorToClient(output, e.getMessage());
        }

        // Send CommandComplete response
        output.write(new byte[]{'C', 0, 0, 0, 8, 'E', 'X', 'E', 'C', 'U', 'T', 'E', 0});

        // Send ReadyForQuery (Idle mode)
        output.write(new byte[]{'Z', 0, 0, 0, 5, 'I'});
        output.flush();
    }

    private void skipUnknownMessage(InputStream input) throws IOException {
        byte[] lengthBytes = new byte[4];
        if (input.read(lengthBytes) != 4) {
            System.err.println("Failed to read message length.");
            return;
        }
        int length = ByteBuffer.wrap(lengthBytes).getInt() - 4;
        if (length > 0) {
            input.readNBytes(length); // Skip the remaining bytes
        }
    }

    private void handleDescribe(InputStream input, OutputStream output) throws IOException {
        skipUnknownMessage(input); // Skip Describe message for now

        // Send NoData response (to indicate we’re not handling it fully yet)
        output.write(new byte[]{'n', 0, 0, 0, 4});
        output.flush();
    }

    private void handleSync(InputStream input, OutputStream output) throws IOException {
        skipUnknownMessage(input); // Skip Sync message

        // Send ReadyForQuery (Idle mode)
        output.write(new byte[]{'Z', 0, 0, 0, 5, 'I'});
        output.flush();
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
        return new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8);
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
        output.flush();
    }

    private void handleQuery(InputStream input, OutputStream output) throws IOException, SQLException {
        byte[] lengthBytes = new byte[4];
        input.read(lengthBytes);
        int length = ByteBuffer.wrap(lengthBytes).getInt();

        byte[] queryBytes = new byte[length - 4];
        input.read(queryBytes);
        String query = new String(queryBytes, StandardCharsets.UTF_8).trim();

        System.out.println("Received query: " + query);

        // Execute query on both databases
        try (Connection pgConn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
             Connection ybConn = DriverManager.getConnection("jdbc:postgresql://localhost:5433/yugabyte", "postgres", "postgres");
             Statement pgStmt = pgConn.createStatement();
             Statement ybStmt = ybConn.createStatement()) {

            boolean isQuery = pgStmt.execute(query);
            boolean isQueryYB = ybStmt.execute(query);

            if (isQuery && isQueryYB) {
                ResultSet pgResult = pgStmt.getResultSet();
                ResultSet ybResult = ybStmt.getResultSet();

                if (!compareResults(pgResult, ybResult)) {
                    System.err.println("Results differ for query: " + query);
                }

                sendResultsToClient(output, pgResult);
            } else {
                int pgUpdate = pgStmt.getUpdateCount();
                int ybUpdate = ybStmt.getUpdateCount();

                if (pgUpdate != ybUpdate) {
                    System.err.println("Update results differ for query: " + query);
                }

                sendUpdateToClient(output, String.valueOf(pgUpdate));
            }
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            sendErrorToClient(output, e.getMessage());
        }
    }

    private boolean compareResults(ResultSet rs1, ResultSet rs2) throws SQLException {
        while (rs1.next() && rs2.next()) {
            for (int i = 1; i <= rs1.getMetaData().getColumnCount(); i++) {
                Object val1 = rs1.getObject(i);
                Object val2 = rs2.getObject(i);
                if (!val1.equals(val2)) {
                    return false;
                }
            }
        }
        return true;
    }

    private void sendResultsToClient(OutputStream output, ResultSet rs) throws SQLException, IOException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        // Send RowDescription (column metadata)
        ByteArrayOutputStream rowDescStream = new ByteArrayOutputStream();
        rowDescStream.write('T'); // RowDescription message type
        rowDescStream.write(intToBytes(6 + (columnCount * 18))); // Message length
        rowDescStream.write(shortToBytes(columnCount)); // Number of columns

        for (int i = 1; i <= columnCount; i++) {
            rowDescStream.write(metaData.getColumnName(i).getBytes(StandardCharsets.UTF_8)); // Column name
            rowDescStream.write(0); // Null-terminated
            rowDescStream.write(intToBytes(0)); // Table OID (unused)
            rowDescStream.write(shortToBytes(0)); // Column attribute number
            rowDescStream.write(intToBytes(23)); // Data type OID (int4 for now)
            rowDescStream.write(shortToBytes(4)); // Data type size
            rowDescStream.write(intToBytes(-1)); // Type modifier
            rowDescStream.write(shortToBytes(0)); // Format code
        }
        output.write(rowDescStream.toByteArray());

        // Send DataRow messages (actual result set)
        while (rs.next()) {
            ByteArrayOutputStream dataRowStream = new ByteArrayOutputStream();
            dataRowStream.write('D'); // DataRow message type
            dataRowStream.write(intToBytes(6 + (columnCount * 8))); // Message length
            dataRowStream.write(shortToBytes(columnCount)); // Number of columns

            for (int i = 1; i <= columnCount; i++) {
                String value = rs.getString(i);
                if (value == null) {
                    dataRowStream.write(intToBytes(-1)); // NULL value
                } else {
                    byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
                    dataRowStream.write(intToBytes(valueBytes.length));
                    dataRowStream.write(valueBytes);
                }
            }
            output.write(dataRowStream.toByteArray());
        }

        // Send CommandComplete
        output.write(new byte[]{'C', 0, 0, 0, 8, 'S', 'E', 'L', 'E', 'C', 'T', 0});

        // Send ReadyForQuery (Idle)
        output.write(new byte[]{'Z', 0, 0, 0, 5, 'I'});
        output.flush();
    }


    private void sendUpdateToClient(OutputStream output, String commandTag) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        buffer.write('C'); // CommandComplete message type

        byte[] commandBytes = commandTag.getBytes(StandardCharsets.UTF_8);
        buffer.write(intToBytes(commandBytes.length + 4 + 1)); // Correct message length
        buffer.write(commandBytes);
        buffer.write(0); // Null-terminated

        output.write(buffer.toByteArray());
        output.flush();
    }

    private void sendErrorToClient(OutputStream output, String errorMessage) throws IOException {
        ByteArrayOutputStream errorStream = new ByteArrayOutputStream();

        errorStream.write('E'); // ErrorResponse message type

        // Calculate message length
        byte[] errorBytes = errorMessage.getBytes(StandardCharsets.UTF_8);
        int messageLength = 6 + errorBytes.length + 6; // 6 bytes for headers and null terminator

        errorStream.write(intToBytes(messageLength));

        // Error fields: Severity (S), Message (M), and null-terminator
        errorStream.write('S'); // Severity field
        errorStream.write("ERROR".getBytes(StandardCharsets.UTF_8));
        errorStream.write(0);

        errorStream.write('M'); // Message field
        errorStream.write(errorBytes);
        errorStream.write(0);

        errorStream.write(0); // End of error response

        output.write(errorStream.toByteArray());
        output.flush();
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
