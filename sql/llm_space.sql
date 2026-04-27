-- LLM Space dashboard card seed

MERGE INTO kv_store t
USING (
  SELECT 'dash_llm_space' AS item_key,
         'LLM Space' AS item_value,
         '{"icon":"fa-solid fa-chart-pie"}' AS additional_info,
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

COMMIT;
