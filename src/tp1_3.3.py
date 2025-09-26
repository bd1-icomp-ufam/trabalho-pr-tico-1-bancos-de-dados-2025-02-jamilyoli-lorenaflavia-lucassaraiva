import psycopg2
import pandas as pd 
import time
import warnings


warnings.filterwarnings("ignore")

conn = psycopg2.connect(
    dbname="ecommerce",
    user="postgres",
    password="postgres",
    host="db",
    port=5432
)


id_do_produto = "0738700797"
nome_do_produto = "Candlemas: Feast of Flames"

query11 = f"""
SELECT review_id, helpfull, rating
FROM Review AS R
WHERE product = '{id_do_produto}'
ORDER BY rating DESC, helpfull DESC
LIMIT 5;
"""
time.sleep(0.5)

query12 = f"""
SELECT review_id, helpfull, rating
FROM Review AS R
WHERE product = '{id_do_produto}'
ORDER BY rating ASC, helpfull DESC
LIMIT 5;
"""
time.sleep(0.5)

query2 = f"""
SELECT product_asin, similar_asin
FROM similar_products AS S 
JOIN product AS simp ON S.similar_asin = simp.asin
JOIN product AS p ON S.product_asin = p.asin
WHERE simp.sales_rank < p.sales_rank AND product_asin = '{id_do_produto}';
"""
time.sleep(0.5)

query3 = f"""
SELECT "data", AVG (r.rating) AS rating_media
FROM review AS r 
JOIN product AS p ON r.product = p.asin
WHERE p.asin = '{id_do_produto}'
GROUP BY r.data
ORDER BY r.data;
"""
time.sleep(0.5)
query4 = f"""
SELECT ranked.group, ranked.asin, ranked.title, ranked.sales_rank, ranked.src
FROM (SELECT p.asin, p.title, p.sales_rank, p.group,
	  ROW_NUMBER() OVER (PARTITION BY p.group ORDER BY p.sales_rank ASC) AS src
      FROM product AS p 
	  WHERE p.sales_rank>0
) AS ranked
WHERE src <=10
ORDER BY ranked.group ASC, ranked.sales_rank ASC;
"""
time.sleep(0.5)
query5 = f"""
SELECT p.asin, p.title, AVG(r.helpfull) AS media_rating
FROM product AS p 
JOIN review AS r ON p.asin = r.product
GROUP BY p.asin, p.title
ORDER BY media_rating DESC
LIMIT 10;
"""
time.sleep(0.5)
query6 = f"""
SELECT cn.Category_id, cn.Title, AVG(r.Helpfull) AS avgh
FROM categories_names AS cn 
JOIN categories_products AS cp ON cn.category_id = cp.leaf_category
JOIN product AS p ON cp.asin = p.asin 
JOIN review AS r ON p.asin = r.product
GROUP BY cn.category_id, cn.title
ORDER BY avgh DESC
LIMIT 5;
"""
query7 = f"""
SELECT ranked.group, ranked.customer_id, ranked.count
FROM ( SELECT p.group, r.customer_id, COUNT(*) AS count,
       ROW_NUMBER() OVER (PARTITION BY p.group ORDER BY COUNT(*) DESC) AS rn
       FROM review AS r
       JOIN product AS p ON r.product = p.asin
       GROUP BY p.group, r.customer_id
) AS ranked
WHERE rn <= 10
ORDER BY ranked.group, ranked.count DESC;
"""

q11 = pd.read_sql_query(query11, conn)
print("1. 5 comentários mais úteis e com maior avaliação do produto exemplo:", id_do_produto,"(Candlemas: Feast of Flames)")
print(q11)

q12 = pd.read_sql_query(query12, conn)
print("\n\n1. 5 comentários mais úteis e com menor avaliação do produto exemplo:", id_do_produto,"(Candlemas: Feast of Flames)")
print(q12)

q2 = pd.read_sql_query(query2, conn)
print("\n\n2. Produtos similares com maiores vendas (melhor salesrank) que:", id_do_produto,"(Candlemas: Feast of Flames)")
print(q2)

q3 = pd.read_sql_query(query3, conn)
print("\n\n3. Evolução diária das médias de avaliação do produto:", id_do_produto,"(Candlemas: Feast of Flames)")
print(q3)

q4 = pd.read_sql_query(query4, conn)
print("\n\n4. 10 produtos líderes de venda em cada grupo de produtos")
print(q4)

q5 = pd.read_sql_query(query5, conn)
print("\n\n5. 10 produtos com a maior média de avaliações úteis positivas por produto")
print(q5)

q6 = pd.read_sql_query(query6, conn)
print("\n\n6. Lista das 5 categorias com a maior média de avaliações úteis positivas")
print(q6)

q7 = pd.read_sql_query(query7, conn)
print("\n\n7. Lista dos 10 clientes que mais fizeram comentários por grupo de produto")
print(q7)

conn.close()
