-- API Keys project bootstrap (kv_store-backed)

-- Dashboard card seed for /api project
MERGE INTO kv_store t
USING (
  SELECT 'api' AS item_key,
         'API Keys' AS item_value,
         '{"icon":"fa-solid fa-key","order":6}' AS additional_info,
         'dashboard' AS category
  FROM dual
) s
ON (t.item_key = s.item_key)
WHEN MATCHED THEN
  UPDATE SET t.item_value = s.item_value,
             t.additional_info = s.additional_info,
             t.category = s.category,
             t.is_active = 'Y'
WHEN NOT MATCHED THEN
  INSERT (item_key, item_value, additional_info, category, is_active)
  VALUES (s.item_key, s.item_value, s.additional_info, s.category, 'Y');

-- Toggle to globally enable/disable future API-key auth checks
MERGE INTO kv_store t
USING (
  SELECT 'ALLOW_API_KEY_AUTH' AS item_key,
         'N' AS item_value,
         'Set Y to enable API-key authorization checks for future API endpoints' AS additional_info,
         'toggle' AS category
  FROM dual
) s
ON (t.item_key = s.item_key)
WHEN MATCHED THEN
  UPDATE SET t.item_value = s.item_value,
             t.additional_info = s.additional_info,
             t.category = s.category,
             t.is_active = 'Y'
WHEN NOT MATCHED THEN
  INSERT (item_key, item_value, additional_info, category, is_active)
  VALUES (s.item_key, s.item_value, s.additional_info, s.category, 'Y');

-- Header name config for future API-key auth middleware
MERGE INTO kv_store t
USING (
  SELECT 'API_KEY_HEADER_NAME' AS item_key,
         'X-API-Key' AS item_value,
         'HTTP header used to pass API key for protected endpoints' AS additional_info,
         'config' AS category
  FROM dual
) s
ON (t.item_key = s.item_key)
WHEN MATCHED THEN
  UPDATE SET t.item_value = s.item_value,
             t.additional_info = s.additional_info,
             t.category = s.category,
             t.is_active = 'Y'
WHEN NOT MATCHED THEN
  INSERT (item_key, item_value, additional_info, category, is_active)
  VALUES (s.item_key, s.item_value, s.additional_info, s.category, 'Y');

COMMIT;
