import psycopg2 as psycopg2
import os
import requests
from psycopg2 import OperationalError

from config import auth_dict


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


# def execute_query(connection, query):
#     connection.autocommit = True
#     cursor = connection.cursor()
#     try:
#         cursor.execute(query)
#         print("Query executed successfully")
#     except OperationalError as e:
#         print(f"The error '{e}' occurred")


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")


delete_1 = "DELETE FROM sku_cat_brand_list RETURNING id_sku;"


call_2 = """
    INSERT INTO sku_cat_brand_list (id_sku, id_category, brand_name)
    SELECT sku_id, category_id, brand_name FROM (
    SELECT  
            foo.fbo_sku sku_id, 
            -- foo.product_id,
            foo.category_id category_id,
            foo.value brand_name,	
            ROW_NUMBER() OVER(PARTITION BY foo.fbo_sku) rnk
        FROM
            (SELECT  pa.attribute_id, pa.value, pl.fbo_sku, pl.product_id, pl.category_id FROM product_list pl
                JOIN product_attr pa ON pa.product_id = pl.product_id) foo
        WHERE (foo.attribute_id = '31' OR foo.attribute_id = '85') AND  foo.value <> '��� ������'  AND  foo.value <> '' AND foo.fbo_sku <> '0') foo
    WHERE rnk = 1
    RETURNING sku_id;
"""


call_3 = """
    INSERT INTO data_analytics_bydays_main (
        api_id, sku_id, sku_name, "date", category_id, category_name, brand_id, brand_name, hits_view_search, hits_view_pdp, hits_view,
        hits_tocart_search, hits_tocart_pdp, hits_tocart, session_view_search, session_view_pdp, session_view, conv_tocart_search,
        conv_tocart_pdp, conv_tocart, revenue,"returns", cancellations, ordered_units, delivered_units, adv_view_pdp, adv_view_search_category,
        adv_view_all, adv_sum_all, position_category, postings, postings_premium)
    WITH dab1rnk AS (
        SELECT 
            *, ROW_NUMBER() OVER(PARTITION BY sku_id, "date"  ORDER BY id) rnk1
        FROM data_analytics_bydays1
    ),
         dab2rnk AS (
        SELECT 
            *, ROW_NUMBER() OVER(PARTITION BY sku_id, "date"  ORDER BY id) rnk2
        FROM data_analytics_bydays2
    )
    SELECT
        -- Main block --
        COALESCE(dab1.api_id, dab2.api_id) 					api_id,
        COALESCE(dab1.sku_id, dab2.sku_id) 					sku_id,
        COALESCE(dab1.sku_name, dab2.sku_name) 				sku_name,
        COALESCE(dab1."date" , dab2."date") 				"date",
        COALESCE(dab1.category_id , dab2.category_id) 		category_id,
        COALESCE(dab1.category_name , dab2.category_name) 	category_name,
        COALESCE(dab1.brand_id , dab2.brand_id) 			brand_id,
        COALESCE(scbl.brand_name, dab1.brand_name , dab2.brand_name) brand_name,
        -- Metrics block table1 --
        COALESCE(dab1.hits_view_search, 0) 					hits_view_search,
        COALESCE(dab1.hits_view_pdp, 0) 					hits_view_pdp,
        COALESCE(dab1.hits_view, 0) 						hits_view,
        COALESCE(dab1.hits_tocart_search, 0) 				hits_tocart_search,
        COALESCE(dab1.hits_tocart_pdp, 0) 					hits_tocart_pdp,
        COALESCE(dab1.hits_tocart, 0) 						hits_tocart,
        COALESCE(dab1.session_view_search, 0) 				session_view_search,
        COALESCE(dab1.session_view_pdp, 0) 					session_view_pdp,
        COALESCE(dab1.session_view, 0) 						session_view,
        COALESCE(dab1.conv_tocart_search, 0) 				conv_tocart_search,
        COALESCE(dab1.conv_tocart_pdp, 0) 					conv_tocart_pdp,
        COALESCE(dab1.conv_tocart, 0) 						conv_tocart,
        COALESCE(dab1.revenue, 0) 							revenue,
        -- Metrics block table2 --
        COALESCE(dab2."returns", 0) 						"returns",
        COALESCE(dab2.cancellations, 0) 					cancellations,
        COALESCE(dab2.ordered_units, 0) 					ordered_units,
        COALESCE(dab2.delivered_units, 0) 					delivered_units,
        COALESCE(dab2.adv_view_pdp, 0) 						adv_view_pdp,
        COALESCE(dab2.adv_view_search_category, 0) 			adv_view_search_category,
        COALESCE(dab2.adv_view_all, 0) 						adv_view_all,
        COALESCE(dab2.adv_sum_all, 0) 						adv_view_sum_all,
        COALESCE(dab2.position_category, 0) 				position_category,
        COALESCE(dab2.postings, 0) 							postings,
        COALESCE(dab2.postings_premium, 0) 					postings_premium
    FROM dab1rnk dab1 
    FULL JOIN dab2rnk dab2 ON dab1.sku_id = dab2.sku_id AND dab1."date" = dab2."date"
    LEFT JOIN (SELECT id_sku, brand_name FROM sku_cat_brand_list) scbl ON dab1.sku_id =  scbl.id_sku OR dab2.sku_id =  scbl.id_sku
    WHERE (dab1.rnk1 = 1 OR dab2.rnk2=1) AND (dab1."date" = (now() - interval '1 day')::date OR dab2."date" = (now() - interval '1 day')::date)
    RETURNING sku_id;
"""


def clear_append_into_sheets(res):
    """ Перезаписывает данные в таблицу google sheets  """
    res.update(auth_dict)
    url_for_sheets = 'http://127.0.0.1:5001/sheets/clear_append'
    response = requests.post(url_for_sheets, json=res)
    return response


def get_data_for_sheets(data_from_first_request, data_from_second_request):
    data = {}
    data['data'] = [{"sku_id_запрос1": res1}, {"sku_id_запрос2": res2}]
    return data


if __name__ == '__main__':
    conn = connection()
    first = execute_read_query(conn, delete_1)
    second = execute_read_query(conn, call_2)
    third = execute_read_query(conn, call_3)
    res1 = [pair[0] for pair in second]
    res2 = [pair[0] for pair in third]
    data_for_sheets = get_data_for_sheets(res1, res2)
    clear_append_into_sheets(data_for_sheets)


