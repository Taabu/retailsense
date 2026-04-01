-- Create metadata tables
CREATE TABLE IF NOT EXISTS stores (
    store_id VARCHAR(50) PRIMARY KEY,
    store_name VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS zones (
    zone_id VARCHAR(50) PRIMARY KEY,
    store_id VARCHAR(50) REFERENCES stores(store_id),
    zone_name VARCHAR(100),
    product_category VARCHAR(100),
    area_sqm DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed sample data
INSERT INTO stores (store_id, store_name, city, country) VALUES
    ('ZRH_01', 'Zurich Central', 'Zurich', 'Switzerland'),
    ('BER_01', 'Berlin Mitte', 'Berlin', 'Germany');

INSERT INTO zones (zone_id, store_id, zone_name, product_category) VALUES
    ('ZONE_A', 'ZRH_01', 'Aisle 1 - Dairy', 'Dairy'),
    ('ZONE_B', 'ZRH_01', 'Aisle 2 - Beverages', 'Beverages'),
    ('ZONE_C', 'BER_01', 'Aisle 1 - Electronics', 'Electronics');