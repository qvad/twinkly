-- YugabyteDB initialization script for integration tests
-- Note: This runs after YugabyteDB starts, may need manual execution

-- Create test user and database
-- Ignore errors if they exist (idempotency is hard without IF NOT EXISTS for users in older PG compat, but YB might support it)
-- We use DO block for idempotency if possible, or just ignore errors.
-- Actually, we dropped everything in previous steps, but user/db persist.

DROP DATABASE IF EXISTS testdb;
DROP USER IF EXISTS testuser;
CREATE USER testuser WITH PASSWORD 'testpass';
CREATE DATABASE testdb WITH OWNER testuser;

\c testdb

DROP VIEW IF EXISTS user_order_summary;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS users;

-- Create test tables (same structure as PostgreSQL for compatibility testing)
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(50),
    in_stock BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL DEFAULT 1,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test data (identical to PostgreSQL)
INSERT INTO users (username, email) VALUES 
    ('alice', 'alice@example.com'),
    ('bob', 'bob@example.com'),
    ('charlie', 'charlie@example.com');

INSERT INTO products (name, price, category) VALUES
    ('Laptop', 999.99, 'Electronics'),
    ('Mouse', 29.99, 'Electronics'),
    ('Keyboard', 79.99, 'Electronics'),
    ('Notebook', 4.99, 'Office Supplies'),
    ('Pen', 1.99, 'Office Supplies');

INSERT INTO orders (user_id, product_id, quantity) VALUES
    (1, 1, 1),
    (1, 2, 2),
    (2, 3, 1),
    (3, 4, 5),
    (2, 1, 1);

-- Create indexes for testing (YugabyteDB compatible)
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_orders_user_id ON orders(user_id);

-- Create a view for testing (same as PostgreSQL)
CREATE VIEW user_order_summary AS
SELECT 
    u.username,
    u.email,
    COUNT(o.id) as order_count,
    COALESCE(SUM(p.price * o.quantity), 0) as total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
LEFT JOIN products p ON o.product_id = p.id
GROUP BY u.id, u.username, u.email
ORDER BY total_spent DESC;