-- Shortcuts project seed data in kv_store

-- Dashboard card seed for /shortcuts project
MERGE INTO kv_store t
USING (
  SELECT 'shortcuts' AS item_key, 'Shortcuts' AS item_value, 'dashboard' AS category FROM dual
) s
ON (t.item_key = s.item_key)
WHEN MATCHED THEN
  UPDATE SET t.item_value = s.item_value, t.category = s.category, t.is_active = 'Y'
WHEN NOT MATCHED THEN
  INSERT (item_key, item_value, category, is_active)
  VALUES (s.item_key, s.item_value, s.category, 'Y');

COMMIT;
