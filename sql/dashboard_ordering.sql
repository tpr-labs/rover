-- Dashboard ordering seed/update
-- Uses explicit additional_info.order values for canonical card ordering.

MERGE INTO kv_store t
USING (
  SELECT 'bookmarks' AS item_key, 1 AS display_order FROM dual UNION ALL
  SELECT 'sb' AS item_key, 2 AS display_order FROM dual UNION ALL
  SELECT 'ft' AS item_key, 3 AS display_order FROM dual UNION ALL
  SELECT 'dash_llm_space' AS item_key, 4 AS display_order FROM dual UNION ALL
  SELECT 'shortcuts' AS item_key, 5 AS display_order FROM dual UNION ALL
  SELECT 'toggles' AS item_key, 6 AS display_order FROM dual UNION ALL
  SELECT 'sql' AS item_key, 7 AS display_order FROM dual UNION ALL
  SELECT 'kv' AS item_key, 8 AS display_order FROM dual UNION ALL
  SELECT 'uploads' AS item_key, 9 AS display_order FROM dual
) s
ON (t.item_key = s.item_key AND LOWER(TRIM(NVL(t.category, ''))) = 'dashboard')
WHEN MATCHED THEN
  UPDATE SET t.additional_info =
    CASE t.item_key
      WHEN 'bookmarks' THEN '{"order":1}'
      WHEN 'sb' THEN '{"order":2}'
      WHEN 'ft' THEN '{"order":3}'
      WHEN 'dash_llm_space' THEN '{"icon":"fa-solid fa-chart-pie","order":4}'
      WHEN 'shortcuts' THEN '{"order":5}'
      WHEN 'toggles' THEN '{"order":6}'
      WHEN 'sql' THEN '{"order":7}'
      WHEN 'kv' THEN '{"order":8}'
      WHEN 'uploads' THEN '{"order":9}'
      ELSE t.additional_info
    END;

COMMIT;
