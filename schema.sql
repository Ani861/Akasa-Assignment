
DROP DATABASE IF EXISTS etl;
CREATE DATABASE etl CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE etl;

CREATE TABLE customers (
  customer_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  customer_name VARCHAR(200),
  mobile_number VARCHAR(32) UNIQUE,
  region VARCHAR(100),
  PRIMARY KEY (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE orders (
  order_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  mobile_number VARCHAR(32),
  customer_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  order_date_time DATETIME,
  order_total DECIMAL(12,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (order_id),
  CONSTRAINT fk_customer FOREIGN KEY (customer_id)
    REFERENCES customers(customer_id)
    ON DELETE SET NULL
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE order_items (
  id INT AUTO_INCREMENT PRIMARY KEY,
  order_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  sku_id VARCHAR(64),
  sku_count INT,
  line_amount DECIMAL(12,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (order_id) REFERENCES orders(order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE ingestion_log (
  id INT AUTO_INCREMENT PRIMARY KEY,
  source_file VARCHAR(255),
  file_checksum VARCHAR(128),
  rows_int INT,
  status VARCHAR(50),
  run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE dead_letter (
  id INT AUTO_INCREMENT PRIMARY KEY,
  source VARCHAR(50),
  raw_data TEXT,
  error_message TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE INDEX idx_orders_date ON orders(order_date_time);
CREATE INDEX idx_customers_region ON customers(region);
CREATE INDEX idx_order_items_order ON order_items(order_id);
