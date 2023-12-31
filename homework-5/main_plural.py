import json
import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    conn = None

    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                create_suppliers_products_table(cur, suppliers)
                print("Таблица suppliers_products успешно создана")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""

    conn = None

    try:
        conn = psycopg2.connect(**params)
        conn.autocommit = True

        cur = conn.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
        cur.execute(f"CREATE DATABASE {db_name};")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, "r", encoding="UTF") as file:
        script = file.read()
    cur.execute(script)


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    cur.execute("""
        CREATE TABLE suppliers
        (
            supplier_id int GENERATED ALWAYS AS IDENTITY NOT NULL,
            company_name varchar(50),
            contact varchar(100),
            address varchar(200),
            phone varchar(20),
            fax varchar(20),
            homepage varchar(300),

            CONSTRAINT pk_suppliers_supplier_id PRIMARY KEY(supplier_id) 
        );
    """)


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file, "r", encoding="UTF-8") as j_file:
        suppliers_data = json.load(j_file)
        return suppliers_data


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""

    for data in suppliers:
        values = (
            data.get("company_name"),
            data.get("contact"),
            data.get("address"),
            data.get("phone"),
            data.get("fax"),
            data.get("homepage")
        )
        cur.execute("""
            INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage) 
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING supplier_id;    
        """, values)
        data["supplier_id"] = cur.fetchone()[0]


def create_suppliers_products_table(cur, suppliers) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""

    cur.execute("""       
        CREATE TABLE suppliers_products
        (
            supplier_id int,
            product_id int,
            
            CONSTRAINT fk_suppliers_products_supplier_id FOREIGN KEY(supplier_id) REFERENCES suppliers(supplier_id),
            CONSTRAINT fk_suppliers_products_product_id FOREIGN KEY(product_id) REFERENCES products(product_id)
        );
    """)

    for supplier in suppliers:
        supplier_id = supplier.get("supplier_id")
        products = supplier.get("products", [])
        if products:
            products = [product.replace("\'", "\'\'") if "\'" in product else product for product in products]
            insert = ", ".join(list(map(lambda x: "'" + x + "'", products)))
            cur.execute(f"""
                SELECT product_id
                FROM products
                WHERE product_name IN ({insert});
            """)

            product_id = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO suppliers_products
                VALUES (%s, %s);
            """, (supplier_id, product_id))


if __name__ == '__main__':
    main()
