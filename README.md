[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/jnRYu6vc)
# 1) Construir e subir os serviços
docker compose up -d --build

# 2) (Opcional) conferir saúde do PostgreSQL
docker compose ps

# 3) Criar esquema e carregar dados
docker compose run --rm app python src/tp1_3.2.py \
  --db-host db --db-port 5432 --db-name ecommerce --db-user postgres --db-pass postgres \
  --input /data/snap_amazon.txt

# 4) Executar o Dashboard (todas as consultas)
docker compose run --rm app python src/tp1_3.3.py \
  --db-host db --db-port 5432 --db-name ecommerce --db-user postgres --db-pass postgres \
  --output /app/out
