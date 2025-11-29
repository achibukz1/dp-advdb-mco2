CREATE TABLE IF NOT EXISTS recovery_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    target_node INT NOT NULL,
    source_node INT NOT NULL,
    sql_statement TEXT NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    status ENUM('PENDING', 'COMPLETED', 'FAILED') DEFAULT 'PENDING',
    retry_count INT DEFAULT 0,
    error_message TEXT NULL,
    transaction_hash VARCHAR(64) NOT NULL,
    
    INDEX idx_target_node (target_node),
    INDEX idx_status (status),
    INDEX idx_timestamp (timestamp),
    INDEX idx_transaction_hash (transaction_hash)
) ENGINE=InnoDB;