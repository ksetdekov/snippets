with missing_day_types as (
    SELECT DISTINCT
        sh.month_begin,
        sh.nm_day_type_day,
        sh.sub_product_type_code,
        sh.report_type_day
    FROM (
        SELECT
            month_begin,
            nm_day_type_day,
            sub_product_type_code,
            report_type_day,
            count(*) as cnt
        FROM final_calib
        GROUP BY
            month_begin,
            nm_day_type_day,
            sub_product_type_code,
            report_type_day
    ) fc
    RIGHT JOIN sh_day_type sh
        ON sh.nm_day_type_day = fc.nm_day_type_day
       AND sh.report_type_day = fc.report_type_day
       AND sh.month_begin = fc.month_begin
       AND sh.sub_product_type_code = fc.sub_product_type_code
    WHERE fc.nm_day_type_day IS NULL
)
SELECT * FROM missing_day_types;