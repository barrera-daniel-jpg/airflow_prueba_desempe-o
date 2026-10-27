CREATE SCHEMA IF NOT EXISTS dwh;

-- Modelo estrella: primero las dimensiones, referenciadas luego por las tablas de hechos.
CREATE TABLE IF NOT EXISTS dwh.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    customer_type VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_code VARCHAR(50) UNIQUE NOT NULL,
    canonical_description TEXT,
    category VARCHAR(100),
    country_origin VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INT NOT NULL
);

-- 2. Tablas de Hechos (Dependen de las dimensiones)
CREATE TABLE IF NOT EXISTS dwh.fact_sales (
    sale_id SERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    line_index INT NOT NULL,
    product_key INT REFERENCES dwh.dim_products(product_key),
    customer_key INT REFERENCES dwh.dim_customers(customer_key),
    date_key INT REFERENCES dwh.dim_date(date_key),
    country VARCHAR(100) NOT NULL,
    quantity INT NOT NULL,
    unit_price NUMERIC(12, 4) NOT NULL,
    gross_revenue NUMERIC(12, 4) NOT NULL,
    transaction_timestamp_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    source_file VARCHAR(50) NOT NULL,
    CONSTRAINT unique_sale_line UNIQUE (invoice_no, product_key, transaction_timestamp_utc, line_index)
);

CREATE TABLE IF NOT EXISTS dwh.fact_returns (
    return_id SERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    line_index INT NOT NULL,
    product_key INT REFERENCES dwh.dim_products(product_key),
    customer_key INT REFERENCES dwh.dim_customers(customer_key),
    date_key INT REFERENCES dwh.dim_date(date_key),
    country VARCHAR(100) NOT NULL,
    quantity_returned INT NOT NULL,
    unit_price NUMERIC(12, 4) NOT NULL,
    refund_amount NUMERIC(12, 4) NOT NULL,
    transaction_timestamp_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    source_file VARCHAR(50) NOT NULL,
    CONSTRAINT unique_return_line UNIQUE (invoice_no, product_key, transaction_timestamp_utc, line_index)
);

-- 3. Tabla de Auditoría
CREATE TABLE IF NOT EXISTS dwh.data_quality_rejects (
    reject_id SERIAL PRIMARY KEY,
    source_file VARCHAR(50) NOT NULL,
    raw_payload JSONB NOT NULL,
    rejection_reason VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);