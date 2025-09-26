import gzip
import pandas as pd
import psycopg2
import time


file_path_in = "/data/snap_amazon.txt.gz"
file_path_out = "/app/out"


def parse_amazon_meta(file_path):
    products = [] #product = {"Id": , "ASIN": ,"title": ,"group": ,"salesrank": ,"similar_number": ,"category_number": ,"review_number": , "downloads": ,"avg_rating": }
    similars = [] #similar_list = {"product_ASIN": , "similar_ASIN": }
    reviews = [] #review = {"review_id": , "date":, "customer_id": , "rating":, "votes": , "helpfull": }
    category_products = [] #category_relation{"leaf_category": ,"ASIN": }
    category_relations = [] #category_parents{"parent_id", "child_id", "depth"}
    category_names = []#{"category_name, category_id"}
    name_seen = set()
    relation_seen = set()
    review_id = 0
    print("Iniciando parsing...")
    with gzip.open(file_path, "rt", encoding="latin-1") as f:
        next(f)
        next(f)
        count_linha = 1
        product = {}
        cond_teste = 1
        for line in f:
            line = line.strip()
            count_linha = count_linha + 1
            if (line and not(line.startswith("categories:"))):
                if line.startswith("Id:"):
                    if(product):
                        products.append(product)
                    product = {}
                    id = line[5:].strip()
                    if id.isdigit():
                        #print("id encontrado:" , id)
                        product["Id"] = id
                        if(int(id) % 10000 == 0):
                            print("progresso:", id)
                        
                    else:
                        print("!!!erro!!! id na linha:", count_linha)
                        exit()
                elif(line.startswith("ASIN:")):
                    asin = line[6:].strip()
                    if asin:
                        #print("asin encontrado:" , asin)
                        product["ASIN"] = asin
                    else:
                        print("!!!erro!!! asin na linha:", count_linha)
                        exit()
                elif(line == "discontinued product"):
                    product["title"] = None
                    product["group"] = None
                    product["salesrank"] = None
                    product["review_number"] = None
                elif(line.startswith("title:")):
                    title = line[7:].strip()
                    #print("título encontrado:" , title)
                    product["title"] = title
                elif(line.startswith("group")):
                    group = line[7:].strip()
                    #print("grupo encontrado:", group)
                    product["group"] = group
                elif(line.startswith("salesrank:")):
                    sl = line[11:].strip()
                    if sl:
                        #print("salesrank encontrado:" , sl)
                        product["salesrank"] = sl
                    else:
                        print("!!!erro!!! sl na linha:", count_linha)
                        exit()
                elif(line.startswith("similar:")):
                    x = line.find(" ", 9)
                    if x == -1:
                        snum = line[9:]
                    else:
                        snum = line[9:x]
                    if snum.isdigit():
                        #print("número de similares:", snum)
                        #product["similar_number"] = snum
                        if(snum !=  "0"):
                            asin_sim = line[x:].strip().split("  ")
                            snum = int(snum)-1
                            while(snum >= 0):
                                similar = {}
                                #print("similar encontrado:", asin_sim[snum])
                                similar["product_ASIN"] = asin
                                similar["similar_ASIN"] = asin_sim[snum]
                                similars.append(similar)
                                snum = snum-1
                    else:
                        print("!!!erro!!! sNum na linha:", count_linha)
                        exit()
                #elif(line.startswith("categories:")):
                    #ncat = line[12:].strip()
                    #if ncat.isdigit():
                    #    #print("categorias encontradas:" , ncat)
                    #    product["category_number"] = ncat
                    #else:
                    #    print("!!!erro!!! sl na linha:", count_linha)
                    #    exit()
                elif(line.startswith("|")):
                    categories = line.split("|") #lista com as categorias da linha
                    x = 0
                    for c in categories:
                        categories[x] = c.split("[")
                        x = x+1
                    while([""] in categories):
                        categories.remove([""])
                    x = 0
                    for c in categories:
                        categories[x][-1] = categories[x][-1].replace("]", "")#retira ] do id
                        for c2 in categories[x]:
                            if "]" in c2:
                                categories[x][0] = categories[x][0] + "[" + c2 #para nomes com [ 
                        x = x+1

                    i = len(categories)-1
                    x = 0
                    #uso dos sets para confirmar se ja viu uma chave
                    while(x <= i):
                        #print(categories[x])
                        c_id = categories[x][-1]
                        if not (c_id in name_seen):
                            category_name = {} #relaciona id da categoria com nome da categoria
                            category_name["title"] = categories[x][0]
                            category_name["category_id"] = c_id
                            name_seen.add(c_id) #ja viu o nome desse id
                            category_names.append(category_name)
                        k = len(categories)-1
                        while(x <= k):
                            if not ((c_id, categories[k][-1]) in relation_seen):
                                category_relation = {} #relaciona categoria filho com categorias pai e profundidade na árvore
                                category_relation["parent_id"] = c_id
                                category_relation["child_id"] = categories[k][-1]
                                category_relation["depth"] = k-x 
                                category_relations.append(category_relation)
                                relation_seen.add((c_id, categories[k][-1])) 
                            k = k-1  
                        if(x == i):
                            category_leaf = {} #relaciona produto e categoria (id)
                            category_leaf["ASIN"] = asin
                            category_leaf["leaf_category"] = c_id
                            category_products.append(category_leaf)
                        x = x+1
                elif(line.startswith("reviews:")):    
                    line = line[9:].strip()
                    line = line.split("  ")
                    for x in range(len(line)):
                        line[x] = line[x].split(" ")
                    product["review_number"] = line[0][1]
                    #product["downloaded_reviews"] = line[1][1]
                    #product["avg_rating"] = line[2][2]
                    nrev = int(line[1][1]) #downloads indica quantas reviews tem, ou seja, quantas das próximas linhas são reviews
                    x = 0
                    while(x < nrev):
                        line = f.readline().rstrip()
                        if line.startswith("   "):
                            line = line.strip()
                            line = line.split()
                            review = {"review_id": review_id, "product": asin , "date": line[0], "customer_id": line[2], "rating": line[4], "votes": line[6], "helpfull": line[6]}
                            review_id = review_id + 1
                            reviews.append(review)
                        else:
                            print("erro: linha destinada a review não possui formato de review:\n na linha:", count_linha)
                        x = x+1
    products.append(product)           
    print("Parsing terminado.")
    print("Convertendo para csv...")
    pr = pd.DataFrame(products)
    #print(pr)
    sm = pd.DataFrame(similars)
    #print(sm)
    rv = pd.DataFrame(reviews)
    #print(rv)
    cp = pd.DataFrame(category_products)
    #print(cp)
    cr = pd.DataFrame(category_relations)
    #print(cr)
    cn = pd.DataFrame(category_names)

    pr.to_csv(file_path_out + "/pr_amazon-meta.csv", index=False)
    sm.to_csv(file_path_out + "/sm_amazon-meta.csv", index=False)
    rv.to_csv(file_path_out + "/rv_amazon-meta.csv", index=False)
    cp.to_csv(file_path_out + "/cp_amazon-meta.csv", index=False)
    cr.to_csv(file_path_out + "/cr_amazon-meta.csv", index=False)
    cn.to_csv(file_path_out + "/cn_amazon-meta.csv", index=False)
    print("Arquivos csv gerados.")
    #print(cn)
    #print("teste de duplicata")
    #x = 0
    #for p in range (len(category_products)):
    #    for h in range(len(category_products)):
    #        if(p != h and category_products[p] == category_products[h]):
    #            print("duplicata encontrada:")
    #            print("1:", p)
    #            print("2:", h)

def wait_for_postgres(timeout=60, interval=1):
    start = time.time()
    while True:
        try:
            conn = psycopg2.connect(
                dbname="ecommerce",
                user="postgres",
                password="postgres",
                host="db",
                port = 5432
            )
            conn.close()
            print("Postgres pronto!")
            return
        except Exception as e:
            if time.time() - start > timeout:
                raise TimeoutError("Não foi possível conectar ao Postgres dentro do timeout.") from e
            print("Aguardando Postgres... tentando novamente em 1s")
            time.sleep(interval)




def get_conn():
    return psycopg2.connect(dbname="ecommerce", user="postgres", password="postgres", host="db", port = 5432) 


                    

def create_tables(conn):
    with conn.cursor() as cur:
        print("Criando tabelas...\n\n")
        start2 = time.time()
        #cria a tabela product
        start = time.time()
        cur.execute("""
                CREATE TABLE IF NOT EXISTS product(
	                    id 			SERIAL,
	                    asin			VARCHAR(50) PRIMARY KEY,
	                    title			TEXT,
	                    "group"			TEXT,
	                    sales_rank		INT,
	                    review_number		INT
                );
        """)
        print("-tabela products criada \nem:", round(time.time()-start, 4),"s\n")
        #cria a tabela category names
        start = time.time()
        cur.execute("""
                CREATE TABLE IF NOT EXISTS categories_names(
                    title			TEXT,
                    category_id		VARCHAR(50) PRIMARY KEY
                );
        """)
        print("-tabela category_names criada \nem:", round(time.time()-start, 4),"s\n")
        #cria a tabela category_products
        start = time.time()
        cur.execute("""
                CREATE TABLE IF NOT EXISTS categories_products(
                    asin	VARCHAR(50),
                    leaf_category VARCHAR (50),
                    
                    PRIMARY KEY(asin, leaf_category),

                    FOREIGN KEY (asin) REFERENCES product (asin),
                    FOREIGN KEY (leaf_category) REFERENCES categories_names(category_id)
                ); 
        """)
        print("-tabela category_products criada \nem:", round(time.time()-start, 4),"s\n")
        #cria a tabela category_relations
        start = time.time()
        cur.execute("""
                CREATE TABLE IF NOT EXISTS categories_relations(
                    parent_id	VARCHAR(50),
                    child_id VARCHAR (50),
                    "depth" INT,
                    PRIMARY KEY(parent_id, child_id),
                    
                    FOREIGN KEY (parent_id) REFERENCES categories_names (category_id),
                    FOREIGN KEY (child_id) REFERENCES categories_names(category_id)
                ); 
        """)
        print("-tabela category_relations criada \nem:", round(time.time()-start, 4),"s\n")
        #cria a tabela similar_products
        start = time.time()
        cur.execute("""
                CREATE TABLE IF NOT EXISTS similar_products(
                    product_asin	VARCHAR(50),
                    similar_asin  VARCHAR (50),
                    
                    PRIMARY KEY(product_asin, similar_asin),
                    
                    FOREIGN KEY (product_asin) REFERENCES product (asin),
                    FOREIGN KEY (similar_asin) REFERENCES product (asin)
                );
        """)  
        print("-tabela similar_products criada \nem:", round(time.time()-start, 4),"s\n")          
        #cria a tabela reviews
        start = time.time()
        cur.execute(""" 
                CREATE TABLE IF NOT EXISTS review(
                    review_id	VARCHAR(50) PRIMARY KEY,
                    product		TEXT,
                    "data" 		DATE,
                    customer_id	VARCHAR(50),
                    rating		INT,
                    votes		INT,
                    helpfull	INT,
                    
                    FOREIGN KEY (product) REFERENCES product (asin)
                ); 
        """)
        print("-tabela reviews criada \nem:", round(time.time()-start, 4),"s\n")
        print("Todas as tabelas criadas em:", round(time.time()-start2, 4), "s")
    conn.commit()


def copy_data_batch(conn):
    """Carrega os CSVs gerados para o banco, tabela-a-tabela, com commits imediatos
       e usando copy_expert para streaming (mais estável para arquivos grandes)."""
    from psycopg2 import sql

    csv_folder = "/app/out"   # ajuste se seu script grava em outro lugar
    tables = [
        ("products", "products.csv"),
        ("category_names", "category_names.csv"),
        ("category_products", "category_products.csv"),
        ("category_relations", "category_relations.csv"),
        ("similar_products", "similar_products.csv"),
        ("reviews", "reviews.csv"),
    ]

    for table, filename in tables:
        path = f"{csv_folder}/{filename}"
        print(f"-Carregando {table} de {path}")
        start = time.time()
        with conn.cursor() as cur:
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as f:
                    # usa copy_expert para streaming e performance
                    cur.copy_expert(sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(sql.Identifier(table)), f)
                conn.commit()
                elapsed = time.time() - start
                print(f"-Dados carregados para: {table}\nem: {elapsed:.4f} s\n")
            except Exception as e:
                # rollback e reporta erro (não fecha conexão aqui — caller decide)
                conn.rollback()
                print(f"Erro ao carregar {table}: {e}")
                raise


def copy_data(conn):
    copy_path = file_path_out
    with conn.cursor() as cur:
        print("Copiando dados para tabelas...")
        start2 = time.time()
        #copia os dados de product
        start = time.time()
        cur.execute(f""" 
            COPY product (id,asin,title,"group",sales_rank,review_number) 
            FROM '{copy_path + "/pr_amazon-meta.csv"}' 
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
        """)
        print("-Dados carregados para: products \nem:", round(time.time()-start, 4),"s\n")
        #copia os dados de category_names
        start = time.time()
        cur.execute(f""" 
            COPY categories_names (title, category_id)
            FROM '{copy_path + "/cn_amazon-meta.csv"}' 
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
        """)
        print("-Dados carregados para: category_names \nem:", round(time.time()-start, 4),"s\n")
        #copia os dados de category_products
        start = time.time()
        cur.execute(f""" 
            COPY categories_products (asin, leaf_category)
            FROM '{copy_path + "/cp_amazon-meta.csv"}' 
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
        """)
        print("-Dados carregados para: category_products \nem:", round(time.time()-start, 4),"s\n")
        #copia os dados de category_relations
        start = time.time()
        cur.execute(f""" 
            COPY categories_relations(parent_id,child_id,"depth")
            FROM '{copy_path + "/cr_amazon-meta.csv"}' 
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
        """)
        print("-Dados carregados para: category_relations\n em:", round(time.time()-start, 4),"s\n")
        #copia os dados de review
        start = time.time()
        cur.execute(f""" 
            COPY review (review_id,product,"data",customer_id,rating,votes,helpfull)
            FROM '{copy_path + "/rv_amazon-meta.csv"}' 
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
        """)
        print("-Dados carregados para: reviews \nem:", round(time.time()-start, 4),"s\n")

        #Para fazer a tabela similars, é necessário remover alguns ASINs de produtos que aparecem apenas em listas de similares, mas não em products
        #criando uma tabela temporária com os dados direto do csv sem restrições, depois colocando na tabela similars usando insert com uma condição para impedir ASINs sem referência em products
        start = time.time()
        cur.execute(f""" 
            CREATE TEMP TABLE tmp_similar_products
            (
                product_asin VARCHAR(50),
                similar_asin VARCHAR(50)
            );
        """)
        #copia os dados de similars pra tabela temporária
        cur.execute(f""" 
            COPY tmp_similar_products (product_asin, similar_asin)
            FROM '{copy_path + "/sm_amazon-meta.csv"}'
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
        """)
        #coloca de fato em similars apenas aqueles presentes no join de product e tmp_similar_products com condição de igualdade no ASIN de produto
        #isso efetivamente remove as tuplas com ASINs sem referência
        cur.execute(f""" 
            INSERT INTO similar_products (product_asin, similar_asin)
            SELECT t.product_asin, t.similar_asin
            FROM tmp_similar_products t
            JOIN product p ON p.asin = t.similar_asin;
        """)
        print("-Dados carregados para: similars \nem:", round(time.time()-start, 4),"s\n")
        print("tempo total de cópia: ", round(time.time()-start2, 4), "s")

def create_views(conn):
    with conn.cursor() as cur:
        start2 = time.time()
        start = time.time()
        print("Criando as views...")
        cur.execute("""
        CREATE VIEW similar_count AS
        SELECT s.product_asin, COUNT(*) AS total_similar
        FROM similar_products AS s
        GROUP BY s.product_asin;
        """)
        print("Similar_count criada em:", round(time.time()-start, 4),"s\n")
        start = time.time()
        cur.execute("""      
            CREATE VIEW category_count AS
            SELECT c.asin, COUNT(*) AS total_category
            FROM categories_products AS c
            GROUP BY c.asin;
        """)
        print("Category_count criada em:", round(time.time()-start, 4),"s\n")
        start = time.time()
        cur.execute("""
        CREATE VIEW rating_count AS
        SELECT r.product, COUNT(*) AS total_reviews, AVG(r.rating::numeric) AS avg_rating
        FROM review AS r
        GROUP BY r.product;
        """)
        print("rating_count criada em:", round(time.time()-start, 4),"s\n")
        print("Todas as views criadas em:", round(time.time()-start2, 4),"s\n")
def main():
    print("aaaa")
    #espera o postgre
    wait_for_postgres(timeout=120)
    
    parse_amazon_meta(file_path_in)
    
    conn = get_conn()
    try:
        create_tables(conn)

        copy_data_batch(conn)
        create_views(conn)
    finally:
        conn.close()
    print("yaaaaay")



















    #df_teste = pd.read_csv("/mnt/c/Users/lsara/Downloads/pr_amazon-meta.csv")
    #print(len(df_teste))
    #print(df_teste.tail)
    #print(df_teste["review_number"].sum())
    #df_teste2 = pd.read_csv("/mnt/c/Users/lsara/Downloads/cn_amazon-meta.csv")
    #print(df_teste2.tail)
    #df_william = df_teste2[df_teste2["category_id"] == "231231"]
    #print(df_william.head)


if __name__ == "__main__":
    main()
    



































"""
def parse_amazon_meta(file_path):
    products = []
    product = {}

    with gzip.open(file_path, "rt", encoding="latin-1") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            if line.startswith("Id:"):
                if product:
                    products.append(product)
                product = {
                    "similar": [],
                    "categories": [],
                    "reviews": []
                    }
                product["Id"] = int(line.split()[1])

            elif line.startswith("ASIN:"):
                product["ASIN"] = line.split()[1]

            elif line.startswith("title:"):
                product["title"] = line[len("title:"):].strip()

            elif line.startswith("group:"):
                product["group"] = line.split()[1]

            elif line.startswith("salesrank:"):
                product["salesrank"] = int(line.split()[1])

            elif line.startswith("similar:"):
                parts = line.split()
                product["similar"] = parts[2:]

            elif line.startswith("|"):
                product["categories"].append(line)

            elif line.startswith("reviews:"):
                match = re.search(r"total: (\d+).*avg rating: ([\d.]+)", line)
                if match:
                    product["reviews_summary"] = {
                        "total": int(match.group(1)),
                        "avg_rating": float(match.group(2))
                    }

            elif re.match(r"\d{4}-\d{1,2}-\d{1,2}", line):
                product["reviews"].append(line)

    if product:
        products.append(product)

    return pd.DataFrame(products)


# --- Carregar o dataset completo ---
df_full = parse_amazon_meta(file_path)

# --- Salvar em CSV (atenção: arquivo ficará MUITO grande) ---
df_full.to_csv("/mnt/c/Users/lsara/Downloads/amazon-meta.csv", index=False)
"""