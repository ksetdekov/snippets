CREATE OR REPLACE FUNCTION cpu_rnr.f_fill_t_fc_migr_rat_ik_subproduct(
    v_start date,
    v_end date,
    n_mobs int4,
    v_id numeric
)
RETURNS void
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
    -- Step 1: Remove old entries for the specified fc_id
    DELETE FROM cpu_rnr.t_drivers_fc_migr_rating
     WHERE fc_id = v_id;
     
    -- Pause for a brief moment to allow the delete to complete
    PERFORM pg_sleep(2);

    -- Step 2: Insert new data into cpu_rnr.t_drivers_fc_migr_rating
    INSERT INTO cpu_rnr.t_drivers_fc_migr_rating (
         product_oo,
         sub_product_type_code,  -- New column for sub_product_type_code
         rating_ttc_1,
         rating_ttc_2,
         mob,
         delinquency_days_2,
         ost,
         part,
         fc_id,
         create_dt
    )
    (
      WITH rat_history AS (
           SELECT 
              m.gregor_dt,
              m.cred_pd_ttc_ord AS rating_ttc, 
              m.cred_sid,
              -- Compute the sub_product and its gov support flag via a lateral join
              CASE 
                WHEN sp.sub_product = 'ИЖС' THEN 0           -- ИЖС = 0
                WHEN sp.flag_gov_support = true THEN 1         -- ГОС_НЕ_ИЖС = 1
                WHEN sp.flag_gov_support = false THEN 2        -- Базовые_НЕ_ИЖС = 2
              END AS sub_product_type_code,  -- New column added
              LEAST(
                 cpu_rnr.f_months_between(m.gregor_dt::date,
                   rrb_pmd.f_last_day(ad.cred_issue_dt::date)
                 ),
                 n_mobs
              ) AS mob,
              LEAST(COALESCE(ad.debt_ovr_days_cnt, 0), 1261) AS delinquency_days,
              SUM(
                COALESCE(ad.DEBT_DUE_RUB_AMT, 0) + 
                COALESCE(ad.DEBT_OVR_RUB_AMT, 0)
              ) AS ost
           FROM ods_view.v_39_74_t_cred_risk_metrics m
           JOIN ods_view.v_04_12_t_cred_detail ad 
             ON m.cred_sid = ad.cred_sid 
            AND m.gregor_dt::date = ad.gregor_dt::date
           -- Join to the sup_products table using the application number:
           JOIN rrb_mrg.m_mrg_app sup_products
             ON ad.appl_num = sup_products.app_id::text
           -- Use a lateral join to compute the mapping and gov support flag
           JOIN LATERAL (
               SELECT 
                  rrb_pmd._f_rtools_map_ik_sub_product(sup_products.product) AS sub_product,
                  rrb_pmd._f_rtools_check_gov_support(
                      rrb_pmd._f_rtools_map_ik_sub_product(sup_products.product)
                  ) AS flag_gov_support
           ) sp ON TRUE
           WHERE m.ctl_src_code = 'СВ ПВР' 
             AND m.gregor_dt::date BETWEEN v_start AND v_end
           GROUP BY 
              m.gregor_dt,
              m.cred_pd_ttc_ord,
              m.cred_sid,
              sp.sub_product,
              sp.flag_gov_support,
              ad.cred_issue_dt,
              LEAST(
                 cpu_rnr.f_months_between(m.gregor_dt::date,
                   rrb_pmd.f_last_day(ad.cred_issue_dt::date)
                 ),
                 n_mobs
              ),
              LEAST(COALESCE(ad.debt_ovr_days_cnt, 0), 1261)
      ),
      t_migr_rat_mgr_pil AS (
           SELECT 
              r1.gregor_dt AS dat1,
              r2.gregor_dt AS dat2,
              r1.sub_product_type_code AS product_oo,  -- Use the new sub_product_type_code
              r1.rating_ttc AS rating_ttc1,
              r2.rating_ttc AS rating_ttc2,
              r2.mob,
              r2.delinquency_days,
              SUM(r2.ost) AS ost
           FROM rat_history r1 
           INNER JOIN rat_history r2
              ON r1.cred_sid = r2.cred_sid 
             AND r2.gregor_dt::date = rrb_pmd.f_last_day(
                    (r1.gregor_dt::date + INTERVAL '1 day')::date
                 )
           GROUP BY 
              r1.gregor_dt,
              r2.gregor_dt,
              r1.sub_product_type_code,
              r1.rating_ttc,
              r2.rating_ttc,
              r2.mob,
              r2.delinquency_days
      ),
      t_drivers_fc_migr_rat AS (
           SELECT 
              dat1::date,
              dat2::date,
              product_oo, 
              sub_product_type_code,  -- Include the new column in this stage as well
              rating_ttc1 AS rating_ttc_1, 
              rating_ttc2 AS rating_ttc_2,
              0 AS flag_new, 
              mob,
              delinquency_days AS delinquency_days_2, 
              ost, 
              SUM(ost) OVER (
                  PARTITION BY dat1, product_oo, rating_ttc1, delinquency_days, mob
              ) AS ost_all,
              t1.ost / (
                 SUM(ost) OVER (
                    PARTITION BY dat1, product_oo, rating_ttc1, delinquency_days, mob
                 ) + 0.0001
              ) AS part,
              v_id AS fc_id,
              NOW()::date AS create_dt
           FROM t_migr_rat_mgr_pil t1
           WHERE dat2::date <= v_end
             AND delinquency_days < 93
      )
      
      SELECT 
         t.product_oo,
         t.sub_product_type_code,  -- Include the new column in the final selection
         t.rating_ttc_1,
         t.rating_ttc_2,
         t.mob,
         t.delinquency_days_2,
         t.ost,
         t.part,
         t.fc_id,
         t.create_dt
      FROM (
           SELECT
              product_oo,
              sub_product_type_code,  -- Include the new column here
              rating_ttc_1,
              rating_ttc_2,
              CASE 
                 WHEN mob IS NULL THEN NULL 
                 ELSE LEAST(mob, n_mobs) 
              END AS mob,
              delinquency_days_2,
              SUM(ost) AS ost
           FROM t_drivers_fc_migr_rat
           WHERE
              (dat2 BETWEEN v_start AND v_end)
              -- If you previously filtered on a text code (e.g. 'ПК') you may need to update
              -- or remove that condition now that product_oo is numeric.
              AND dat2 NOT IN (DATE '2021-12-31', DATE '2022-04-30')
              AND fc_id = v_id
           GROUP BY
              product_oo,
              sub_product_type_code,  -- Group by the new column as well
              rating_ttc_1,
              rating_ttc_2,
              CASE 
                WHEN mob IS NULL THEN NULL 
                ELSE LEAST(mob, n_mobs) 
              END,
              delinquency_days_2
      ) t
      WHERE ost > 0

      UNION

      (
          SELECT 
             1 AS product_oo, 
             0 AS sub_product_type_code,  -- Default value for the new column
             26 AS rating_ttc_1, 
             26 AS rating_ttc_2, 
             n_mobs AS mob, 
             1261 AS delinquency_days_2, 
             1000 AS ost, 
             1 AS part, 
             v_id AS fc_id, 
             NOW() AS create_dt
      )
    );
END;
$$
EXECUTE ON ANY;
