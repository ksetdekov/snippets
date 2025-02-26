with subproduct_info as ( 
select 
rrb_pmd._f_rtools_map_ik_sub_product(product) as sub_product,
rrb_pmd._f_rtools_check_gov_support(sub_product) as flag_gov_support
FROM rrb_mrg.m_mrg_app
)
select 
case when subproduct_info.sub_product = 'ИЖС' then 0             -- ИЖС = 0
                when subproduct_info.flag_gov_support = true then 1                               -- ГОС_НЕ_ИЖС = 1
                when subproduct_info.flag_gov_support =  false then 2                      -- Базовые_НЕ_ИЖС = 2
             end as sub_product_type_code,
sub_product,
flag_gov_support
from subproduct_info;           

select distinct(rrb_pmd._f_rtools_map_ik_sub_product(sup_products.product)) 
FROM rrb_mrg.m_mrg_app as sup_products;
--НКПЗН                                  |
--Первичка                               |
--Вторичка                               |
--Прочее_ДЦ(выдача вне кредитной фабрики)|
--ИЖС                                    |
--Прочее_Рефинансирование                |
--Прочее_Военная                         |

CREATE OR REPLACE FUNCTION cpu_rnr.f_fill_t_fc_migr_rat_ik_subproduct(v_start date, v_end date, n_mobs int4, v_id numeric)
       RETURNS void
       LANGUAGE plpgsql
       VOLATILE
AS $$
       
begin
    delete from cpu_rnr.t_drivers_fc_migr_rating where fc_id = v_id;
    perform pg_sleep(2)
    ;
       insert into cpu_rnr.t_drivers_fc_migr_rating (product_oo, rating_ttc_1, rating_ttc_2, mob, delinquency_days_2, ost, part, fc_id, create_dt)
             (
                    with rat_history as (
                    -- сюда завести rrb_mrg.m_mrg_app as sup_products на ней присоединить тип ad.appl_num = sup_products.app_id::text
                    -- пройти по процедуре сбора фактов - 
                    --         , rrb_pmd._f_rtools_map_ik_sub_product(sup_products.product) as sub_product
            --         , rrb_pmd._f_rtools_check_gov_support(sup_products.sub_product) as flag_gov_support
                           select 
                                  m.gregor_dt,
                                  m.cred_pd_ttc_ord as rating_ttc, 
                                  m.cred_sid,
                                  ad.product_type_code,
                                  least(cpu_rnr.f_months_between(m.gregor_dt::date, rrb_pmd.f_last_day(ad.cred_issue_dt::date)), n_mobs) as mob,
                                  LEAST(coalesce(ad.debt_ovr_days_cnt, 0), 1261) as delinquency_days,
                                  sum(coalesce(ad.DEBT_DUE_RUB_AMT, 0) + coalesce(ad.DEBT_OVR_RUB_AMT, 0)) as ost
                           from ods_view.v_39_74_t_cred_risk_metrics m
                                  join ods_view.v_04_12_t_cred_detail ad 
                                        on m.cred_sid = ad.cred_sid 
                                               and m.gregor_dt::date=ad.gregor_dt::date
                           where m.ctl_src_code='СВ ПВР' 
                                  and m.gregor_dt::date >= v_start
                                  and m.gregor_dt::date <= v_end
                           group by m.gregor_dt,
                                  m.cred_pd_ttc_ord,
                                  m.cred_sid,
                                  ad.product_type_code,
                                  ad.cred_issue_dt,
                                  least(cpu_rnr.f_months_between(m.gregor_dt::date, rrb_pmd.f_last_day(ad.cred_issue_dt::date)), 37),
                                  LEAST(coalesce(ad.debt_ovr_days_cnt, 0), 1261)
                    ),
                    t_migr_rat_mgr_pil as (
                           select 
                                  r1.gregor_dt as dat1,
                                  r2.gregor_dt as dat2,
                                  r1.product_type_code as product_oo,
                                  r1.rating_ttc as rating_ttc1,
                                  r2.rating_ttc as rating_ttc2,
                                  r2.mob,
                                  r2.delinquency_days,
                                  sum(r2.ost) as ost
                           from rat_history r1 
                                  inner join rat_history r2
                                        on r1.cred_sid=r2.cred_sid 
                                               and r2.gregor_dt::date=rrb_pmd.f_last_day((r1.gregor_dt::date + interval '1 days')::date)
                           group by r1.gregor_dt,
                                  r2.gregor_dt,
                                  r1.product_type_code,
                                  r1.rating_ttc,
                                  r2.rating_ttc,
                                  r2.mob,
                                  r2.delinquency_days
                    ),
                    t_drivers_fc_migr_rat as (
                           Select 
                                  dat1::date,
                                  dat2::date,
                                  product_oo, 
                                  rating_ttc1 rating_ttc_1, 
                                  rating_ttc2 rating_ttc_2,
                                  0 flag_new, 
                                  mob,
                                  delinquency_days delinquency_days_2, 
                                  ost, 
                                  sum(ost) over (partition by Dat1, product_oo, rating_ttc1, delinquency_days, mob) as ost_all,
                                  t1.ost/(sum(ost) over (partition by Dat1,product_oo, rating_ttc1, delinquency_days, mob)+0.0001) as part,
                                  v_id fc_id,
                                  NOW()::date create_dt
                           from t_migr_rat_mgr_pil t1
                           where dat2::date<=v_end
                                  and  delinquency_days<93
                    )
                    select 
                           t.*,
                        ost/sum(ost) over (partition by PRODUCT_OO,RATING_TTC_1,DELINQUENCY_DAYS_2,mob) as part,
                        v_id as v_fc_id,
                        now() as  create_dt
                    from (
                        select
                             1 as PRODUCT_OO,
                             RATING_TTC_1 as RATING_TTC_1,
                             RATING_TTC_2 as RATING_TTC_2,
                             case when MOB is null then null else least(MOB,n_mobs) end as MOB,
                             DELINQUENCY_DAYS_2,
                             sum(ost)  as ost
                        from t_drivers_fc_migr_rat
                        where
                            (dat2 between v_start and v_end)
                                  and (dat2 NOT between date'2023-01-01' and date'2023-01-09')
                            and dat2!=date'2021-12-31'
                            and dat2!=date'2022-04-30'
                             and PRODUCT_OO = 'ПК'
                             and fc_id=v_id
                         group by
                             PRODUCT_OO,
                             RATING_TTC_1,
                             RATING_TTC_2,
                             case when MOB is null then null else least(MOB,n_mobs) end,
                             DELINQUENCY_DAYS_2
                    ) t
                    WHERE ost > 0
             union (
                    select 
                           1 as product_oo, 
                           26 as rating_ttc_1, 
                           26 as rating_ttc_2, 
                           n_mobs as mob, 
                           1261 as delinquency_days_2, 
                           1000 as ost, 
                           1 as part, 
                           v_id as fc_id, 
                           now() as create_dt
                    )
             );
end;

$$
EXECUTE ON ANY;

