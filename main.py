import psycopg2 as psycopg2
import os

from psycopg2 import OperationalError


def connection():
    connection_ = psycopg2.connect(
           host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
           port='6432',
           dbname='market_db',
           user=os.environ['DB_USER'],
           password=os.environ['DB_PASSWORD'],
           target_session_attrs='read-write',
           sslmode='verify-full'
           )
    return connection_


def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")


delete_1 = "DELETE FROM sku_cat_brand_list;"


call_2 = "CALL insert_sku_cat_brand_list();"


call_3 = "CALL insert_data_analytics_main();"


#
# select_1 = """
#         WITH calc_metrics AS (
#             SELECT
#                 sku_id,
#                 sku_name,
#                 "date",
#                 COALESCE(adv_sum_all/(NULLIF(revenue,0)), 0) * 100 drr
#             FROM
#                 data_analytics_bydays_main dabm
#             -- WHERE api_id LIKE :api_id AND brand_id LIKE :brand_id AND category_id LIKE :category_id
#             )
#         SELECT * FROM calc_metrics
#         WHERE :trgt_drr >= drr
#         ORDER BY drr DESC
#
# """
# select_2 = """
#         WITH calc_metrics AS (
#             SELECT
#                 sku_id,
#                 sku_name,
#                 "date",
#                 COALESCE(adv_sum_all/(NULLIF(revenue,0)), 0) * 100 drr
#             FROM
#                 data_analytics_bydays_main dabm
#             -- WHERE api_id LIKE :api_id AND brand_id LIKE :brand_id AND category_id LIKE :category_id
#             )
#         SELECT * FROM calc_metrics
#         WHERE :trgt_drr < drr
#         ORDER BY drr ASC
# """
# select_3 = """
#             WITH calc_metrics AS (
#             SELECT
#                 sku_id,
#                 "date",
#                 adv_sum_all,
#                 revenue,
#                 ordered_units,
#                 COALESCE(adv_sum_all/(NULLIF(revenue,0)), 0) * 100 drr
#             FROM
#                 data_analytics_bydays_main dabm
#             -- WHERE api_id LIKE :api_id AND brand_id LIKE :brand_id AND category_id LIKE :category_id
#             )
#         SELECT
#             clc.sku_id,
#             clc."date",
#             clc.revenue,
#             clc.ordered_units,
#             clc.drr,
#             clc.adv_sum_all,
#             pl.product_id,
#             count(max_action_price) cmap,
#             array_agg(max_action_price::int) map_arr,
#             array_agg((price - max_action_price)::int) pmap_arr
#         FROM calc_metrics clc
#         LEFT JOIN product_list pl ON clc.sku_id = pl.fbo_sku
#         LEFT JOIN mark_actions ma ON pl.product_id = ma.id_product AND clc.date = ma.date_end
#         WHERE :trgt_drr < drr AND (price - max_action_price) < adv_sum_all AND (price - max_action_price)
#         < :trgt_drr * revenue/ordered_units AND (price - max_action_price) != 0
#         GROUP BY
#             clc.sku_id,
#             clc."date",
#             clc.drr,
#             clc.adv_sum_all, clc.revenue, clc.ordered_units,
#             pl.product_id
#         ORDER BY drr ASC
# """
# select_4 = """
#         WITH calc_am AS (
#             SELECT
#                 "date",
#                 avg(COALESCE(hits_tocart/(NULLIF(hits_view,0)), 0) * 100) ctr_avg,
#                 percentile_cont(0.5) WITHIN GROUP (ORDER BY hits_view) hits_view_median
#             FROM data_analytics_bydays_main dabm
#             GROUP BY "date"
#         ),
#         calc_metrics AS (
#             SELECT
#                 sku_id,
#                 dabm."date",
#                 ca.ctr_avg,
#                 ca.hits_view_median,
#                 sum(revenue) revenue,
#                 sum(adv_sum_all) adv_sum_all,
#                 sum(hits_view) hits_view,
#                 sum(hits_tocart) hits_tocart,
#                 COALESCE(sum(hits_tocart)/(NULLIF(sum(hits_view),0)), 0) * 100  ctr,
#                 COALESCE(sum(adv_sum_all)/(NULLIF(sum(revenue),0)), 0) * 100 drr,
#                 COALESCE(sum(adv_sum_all)/(NULLIF(sum(hits_view),0)), 0) * 1000 cpm
#             FROM
#                 data_analytics_bydays_main dabm
#             LEFT JOIN calc_am ca ON dabm."date" = ca."date"
#             GROUP BY
#                 sku_id, dabm."date",
#                 ca.ctr_avg,	ca.hits_view_median
#                 )
#         SELECT
#             *,
#             CASE WHEN drr <= :trgt_drr AND drr != 0  THEN '<= drr_trgt'
#                 WHEN drr = 0 THEN 'null drr'
#                 WHEN drr > :trgt_drr AND drr != 0 THEN '> drr_trgt'
#             END  drr_flg,
#             cpm * (hits_view_median - hits_view) / 1000 hits_view_budget,
#             hits_view_median * revenue/hits_view  revenue_potential
#         FROM calc_metrics cm
#         WHERE ctr > ctr_avg AND hits_view < hits_view_median
# """
select_5 = """
        WITH calc_am AS ( 
        SELECT 
            "date",
            avg(COALESCE(hits_tocart/(NULLIF(hits_view,0)), 0) * 100) ctr_avg,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY hits_view) hits_view_median
        FROM data_analytics_bydays_main dabm 
        GROUP BY "date"	
    ),
    calc_metrics AS (  
        SELECT 
            dabm."date",
            brand_name, 
            ca.ctr_avg,
            ca.hits_view_median,
            sum(hits_view) hits_view,
            COALESCE(sum(hits_tocart)/(NULLIF(sum(hits_view),0)), 0) * 100  ctr
        FROM
            data_analytics_bydays_main dabm 
        LEFT JOIN calc_am ca ON dabm."date" = ca."date"
        GROUP BY 	
            dabm."date",
            brand_name,
            ca.ctr_avg,	ca.hits_view_median )
    SELECT 
        brand_name,
        "date",
        ctr,
        ctr_avg,
        hits_view,
        hits_view_median
    FROM calc_metrics cm
    WHERE ctr > ctr_avg AND hits_view < hits_view_median
        GROUP BY 
        brand_name,
        "date",
        ctr,
        ctr_avg,
        hits_view,
        hits_view_median;
"""
select_6 = """
        WITH calc_am AS ( 
        SELECT 
            "date",
            avg(COALESCE(hits_tocart/(NULLIF(hits_view,0)), 0) * 100) ctr_avg,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY hits_view) hits_view_median
        FROM data_analytics_bydays_main dabm 
        GROUP BY "date"	
    ),
    calc_metrics AS (  
        SELECT 
            dabm."date",
            category_id,
            category_name, 
            ca.ctr_avg,
            ca.hits_view_median,
            sum(hits_view) hits_view,
            COALESCE(sum(hits_tocart)/(NULLIF(sum(hits_view),0)), 0) * 100  ctr
        FROM
            data_analytics_bydays_main dabm 
        LEFT JOIN calc_am ca ON dabm."date" = ca."date"
        GROUP BY 	
            dabm."date",
            category_id,
            category_name,
            ca.ctr_avg,	ca.hits_view_median
            )
    SELECT 
        category_id,
        category_name,
        "date"
        ctr,
        ctr_avg,
        hits_view,
        hits_view_median
    FROM calc_metrics cm
    WHERE ctr > ctr_avg AND hits_view < hits_view_median
        GROUP BY 
        category_id, category_name, "date",
        ctr,
        ctr_avg,
        hits_view,
        hits_view_median;
"""
select_7 = """
    WITH calc_am AS ( 
        SELECT 
            "date",
            sum(revenue) revenue_all
        FROM data_analytics_bydays_main dabm 
        GROUP BY "date"	
    ) 
        SELECT 
            dabm."date",
            sku_id,
            sku_name, 
            ca.revenue_all,
            revenue 
        FROM
            data_analytics_bydays_main dabm 
        LEFT JOIN calc_am ca ON dabm."date" = ca."date"
    WHERE revenue >= ca.revenue_all*20/100 AND revenue <> 0;
        
"""
select_8 = """
        SELECT 
            id_product,
            pl.offer_id,
            pl."name"
        FROM mark_actions ma
        LEFT JOIN product_list pl ON ma.id_product = pl.product_id   
        WHERE (price - max_action_price) = 0
        GROUP BY 
            id_product,
            pl.offer_id,
            pl."name";

"""
select_9 = """
        SELECT 
            sku_id,
            sku_name,
            sum(ts.fbo_present) fbo_present,
            COALESCE(sum(ordered_units)/(NULLIF(sum(ts.fbo_present), 0)), 0) spd_fbo,
            sum(ts.fbs_present) fbs_present,
            COALESCE(sum(ordered_units)/(NULLIF(sum(ts.fbs_present), 0)), 0) spd_fbs
        FROM data_analytics_bydays_main dabm 
        LEFT JOIN product_list pl ON dabm.sku_id = pl.fbo_sku
        LEFT JOIN total_stock ts ON pl.product_id  = ts.product_id 
        WHERE ts."date" <= now()::date AND ts."date" >= (now() - interval '30 days')::date  
        GROUP BY 
            sku_id,
            sku_name;
"""
select_10 = """
        WITH calc AS ( 	
        SELECT 
            ts.product_id,
            sku_id,
            sku_name,
            COALESCE(sum(ordered_units)/(NULLIF(sum(ts.fbo_present), 0)), 0) spd_fbo,
            COALESCE(sum(ordered_units)/(NULLIF(sum(ts.fbs_present), 0)), 0) spd_fbs
        FROM data_analytics_bydays_main dabm 
        LEFT JOIN product_list pl ON dabm.sku_id = pl.fbo_sku
        LEFT JOIN total_stock ts ON pl.product_id  = ts.product_id 
        WHERE ts."date" <= now()::date AND ts."date" >= (now() - interval '30 days')::date  
        GROUP BY 
            ts.product_id,
            sku_id,
            sku_name
            )
    SELECT 
        clc.product_id,
        sku_id,
        sku_name,
        COALESCE(ts.fbo_present/(NULLIF(spd_fbo, 0)), 0) idc_fbo,
        COALESCE(ts.fbo_present/(NULLIF(spd_fbs, 0)), 0) idc_fbs
    FROM calc clc
    LEFT JOIN total_stock ts ON clc.product_id  = ts.product_id 
    WHERE ts."date" = now()::date;	
"""


if __name__ == '__main__':
    conn = connection()
    execute_query(conn, delete_1)
    execute_query(conn, call_2)
    execute_query(conn, call_3)
    # list_1 = execute_read_query(conn, select_1)
    # list_2 = execute_read_query(conn, select_2)
    # list_3 = execute_read_query(conn, select_3)
    # list_4 = execute_read_query(conn, select_4)
    list_5 = execute_read_query(conn, select_5)
    print(len(list_5))
    list_6 = execute_read_query(conn, select_6)
    print(len(list_6))
    list_7 = execute_read_query(conn, select_7)
    print(len(list_7))
    list_8 = execute_read_query(conn, select_8)
    print(len(list_8))
    list_9 = execute_read_query(conn, select_9)
    print(len(list_9))
    list_10 = execute_read_query(conn, select_10)
    print(len(list_10))
