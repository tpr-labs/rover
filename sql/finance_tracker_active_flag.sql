-- Migration: add active flag to FT transactions

DECLARE
  v_count NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO v_count
  FROM user_tab_columns
  WHERE table_name = 'FT_TRANSACTIONS'
    AND column_name = 'IS_ACTIVE';

  IF v_count = 0 THEN
    EXECUTE IMMEDIATE q'[
      ALTER TABLE ft_transactions
      ADD (is_active CHAR(1) DEFAULT 'Y' NOT NULL)
    ]';
  END IF;
END;
/

UPDATE ft_transactions
SET is_active = 'Y'
WHERE is_active IS NULL;

DECLARE
  v_count NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO v_count
  FROM user_constraints
  WHERE table_name = 'FT_TRANSACTIONS'
    AND constraint_name = 'CHK_FT_TRANSACTIONS_ACTIVE';

  IF v_count = 0 THEN
    EXECUTE IMMEDIATE q'[
      ALTER TABLE ft_transactions
      ADD CONSTRAINT chk_ft_transactions_active CHECK (is_active IN ('Y', 'N'))
    ]';
  END IF;
END;
/

DECLARE
  v_count NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO v_count
  FROM user_indexes
  WHERE index_name = 'IDX_FT_TRANSACTIONS_ACTIVE';

  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_ft_transactions_active ON ft_transactions(is_active)';
  END IF;
END;
/

COMMIT;
