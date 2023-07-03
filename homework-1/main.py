"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import pandas
import os
from secret_data import PASSWORD


def build_path(file):
    path_to_file = os.path.join(os.path.dirname(__file__), 'north_data', file)
    return path_to_file


if __name__ == '__main__':

    customers = pandas.read_csv(build_path('customers_data.csv'))
    employees = pandas.read_csv(build_path('employees_data.csv'))
    orders = pandas.read_csv(build_path('orders_data.csv'))

    with psycopg2.connect(
            host='localhost',
            database='north',
            user='postgres',
            password=PASSWORD
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE orders CASCADE")
            cur.execute("TRUNCATE TABLE customers CASCADE")
            cur.execute("TRUNCATE TABLE employees CASCADE")

            for index, row in customers.iterrows():
                insert = "INSERT INTO customers VALUES (%s, %s, %s)"
                values = (row['customer_id'], row['company_name'], row['contact_name'])
                cur.execute(insert, values)

            for index, row in employees.iterrows():
                insert = "INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)"
                values = (
                    row['employee_id'],
                    row['first_name'],
                    row['last_name'],
                    row['title'],
                    row['birth_date'],
                    row['notes']
                )
                cur.execute(insert, values)

            for index, row in orders.iterrows():
                insert = "INSERT INTO orders VALUES (%s, %s, %s, %s, %s)"
                values = (
                    row['order_id'],
                    row['customer_id'],
                    row['employee_id'],
                    row['order_date'],
                    row['ship_city']
                )
                cur.execute(insert, values)

    conn.close()
