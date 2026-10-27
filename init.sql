CREATE DATABASE IF NOT EXISTS iot_data;
USE iot_data;

CREATE TABLE IF NOT EXISTS gemini_serie_limpa (
    id INT AUTO_INCREMENT PRIMARY KEY,
    node_id VARCHAR(50) NOT NULL,
    ts_gateway DATETIME NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value FLOAT NOT NULL,
    INDEX (node_id, ts_gateway),
    INDEX (metric_name, ts_gateway)
);

CREATE TABLE IF NOT EXISTS gemini_agregados (
    id INT AUTO_INCREMENT PRIMARY KEY,
    node_id VARCHAR(50) NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    time_bucket DATETIME NOT NULL,
    avg_val FLOAT,
    min_val FLOAT,
    max_val FLOAT,
    UNIQUE KEY (node_id, metric_name, time_bucket)
);

CREATE TABLE IF NOT EXISTS gemini_quarentena (
    id INT AUTO_INCREMENT PRIMARY KEY,
    packet_id VARCHAR(100),
    motivo_rejeicao VARCHAR(255),
    payload_bruto TEXT,
    data_rejeicao DATETIME DEFAULT CURRENT_TIMESTAMP
);