-- FT transaction ↔ uploads many-to-many links

CREATE TABLE uploads_ft_transaction_links (
    transaction_id   NUMBER NOT NULL,
    upload_id        NUMBER NOT NULL,
    created_at       TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_uploads_ft_tx_links PRIMARY KEY (transaction_id, upload_id),
    CONSTRAINT fk_uploads_ft_tx_links_tx FOREIGN KEY (transaction_id)
        REFERENCES ft_transactions(transaction_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_uploads_ft_tx_links_upload FOREIGN KEY (upload_id)
        REFERENCES uploads_files(upload_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_uploads_ft_tx_links_upload ON uploads_ft_transaction_links(upload_id);

COMMIT;
