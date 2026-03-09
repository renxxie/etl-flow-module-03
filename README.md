# etl-flow-module-03

## Структура решения
- `docker-compose.yml` - файл для развертывания Airflow, PostgreSQL, MongoDB и Streamlit.
- `Dockerfile` - образ Airflow с необходимыми зависимостями.
- `init.sql` - скрипт инициализации PostgreSQL.
- `streamlit_app.py` - дашборд для визуализации данных.
- `dags/`
  - `1_generate_data_mongodb.py` - DAG для генерации синтетических данных.
  - `2_mongodb_to_postgres_etl.py` - DAG для ETL процесса(`ods`).
  - `3_build_data_marts.py` - DAG для построения аналитических витрин(`dm`).

## Запуск
1. Запустите сборку и старт сервисов:
   ```bash
   docker-compose up -d --build
   ```
2. Airflow будет доступен по адресу `http://localhost:8080`
   - **Login:** admin
   - **Password:** admin
3. Зайдите в Airflow UI и запустите последовательно:
   - `1_generate_data_mongodb`
   - `2_mongodb_to_postgres_etl`
   - `3_build_data_marts`
4. **Дашборд Streamlit** доступен по адресу: `http://localhost:8501`
   - Там можно просмотреть содержимое таблиц и графики по витринам.
5. Параметры подключения к PostgreSQL:
   - Host: `localhost`, Port: `5432`
   - БД: `dwh_db`, User: `dwh_user`, Password: `dwh_password`
   - Витрины: `dm.user_activity_mart`, `dm.support_efficiency_mart`
6. Остановить контейнеры(удалить vol `-v`):
   ```bash
   docker-compose down
   ```