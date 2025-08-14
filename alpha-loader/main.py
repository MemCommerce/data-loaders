import requests
import os
from psycopg2 import connect
from psycopg2.extensions import connection as PGConnection
from dotenv import load_dotenv


load_dotenv()

PRODUCTS_LIMIT = 100
IMAGE_PREFIX = "dataset_alpha/"

DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]

CATEGORIES_PATH = "temp/categories.csv"
COLORS_PATH = "temp/colors.csv"
PRODUCT_VARIANTS_PATH = "temp/product_variants.csv"
PRODUCTS_PATH = "temp/products.csv"
SIZES_PATH = "temp/sizes.csv"

CATEGORIES_URL = "https://raw.githubusercontent.com/MemCommerce/memcommerce-docs/refs/heads/main/datasets/alpha/categories.csv"
COLORS_URL = "https://raw.githubusercontent.com/MemCommerce/memcommerce-docs/refs/heads/main/datasets/alpha/colors.csv"
PRODUCT_VARIANTS_URL = "https://raw.githubusercontent.com/MemCommerce/memcommerce-docs/refs/heads/main/datasets/alpha/product_variants_with_sizes.csv"
PRODUCTS_URL = "https://raw.githubusercontent.com/MemCommerce/memcommerce-docs/refs/heads/main/datasets/alpha/products.csv"
SIZES_URL = "https://raw.githubusercontent.com/MemCommerce/memcommerce-docs/refs/heads/main/datasets/alpha/sizes.csv"

DATASETS_PATHS = (
    CATEGORIES_PATH,
    COLORS_PATH,
    PRODUCT_VARIANTS_PATH,
    PRODUCTS_PATH,
    SIZES_PATH,
)

URL_TO_PATH_MAPPING = (
    (
        CATEGORIES_URL,
        CATEGORIES_PATH,
    ),
    (
        COLORS_URL,
        COLORS_PATH,
    ),
    (
        PRODUCT_VARIANTS_URL,
        PRODUCT_VARIANTS_PATH,
    ),
    (
        PRODUCTS_URL,
        PRODUCTS_PATH,
    ),
    (
        SIZES_URL,
        SIZES_PATH,
    ),
)


def create_db():
    conn = connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    return conn


def check_is_dataset_extracted() -> bool:
    return all([os.path.isfile(path) for path in DATASETS_PATHS])


def extract_alpha_dataset():
    for url, path in URL_TO_PATH_MAPPING:
        resp = requests.get(url)
        with open(path, "wb") as file:
            file.write(resp.content)


def extract_datasets_if_missing():
    if not check_is_dataset_extracted():
        extract_alpha_dataset()


def load_sizes(conn: PGConnection):
    with conn.cursor() as cursor:
        with open(SIZES_PATH, "r", encoding="utf-8") as f:
            # Skip the header row
            next(f)
            cursor.copy_from(f, "sizes", sep=",", columns=("id", "label"))
    conn.commit()


def load_colors(conn: PGConnection):
    with conn.cursor() as cursor:
        with open(COLORS_PATH, "r", encoding="utf-8") as f:
            # Skip the header row
            next(f)
            cursor.copy_from(f, "colors", sep=",", columns=("id", "name", "hex"))
    conn.commit()


def load_categories(conn: PGConnection):
    with conn.cursor() as cursor:
        with open(CATEGORIES_PATH, "r", encoding="utf-8") as f:
            # Skip the header row
            next(f)
            cursor.copy_from(
                f, "categories", sep=",", columns=("id", "name", "description")
            )
    conn.commit()


def load_products(conn: PGConnection) -> None:
    from itertools import islice
    from io import StringIO

    with conn.cursor() as cursor:
        with open(PRODUCTS_PATH, "r", encoding="utf-8") as f:
            next(f)  # skip header
            buf = StringIO("".join(islice(f, PRODUCTS_LIMIT)))  # first N lines only
            cursor.copy_from(
                buf,
                "products",
                sep=",",
                columns=("id", "name", "brand", "description", "category_id"),
            )
    conn.commit()


def load_product_variants(conn: PGConnection):
    import pandas as pd
    import random
    import uuid
    from psycopg2.extras import execute_values

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM products
            """
        )
        allowed_ids: set[str] = {r[0] for r in cur.fetchall()}

    df = pd.read_csv(PRODUCT_VARIANTS_PATH)
    df = df[df["product_id"].isin(allowed_ids)].copy()

    df["image_name"] = (
        df["image_name"]
        .astype(str)
        .apply(lambda s: s if s.startswith(IMAGE_PREFIX) else f"{IMAGE_PREFIX}{s}")
    )

    df["price"] = [random.randint(10, 150) for _ in range(len(df))]

    df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    sql = """
        INSERT INTO product_variants (id, product_id, color_id, image_name, size_id, price)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    rows: list[tuple[str, str, str, str, str, float]] = list(
        df.where(pd.notna(df), None).itertuples(index=False, name=None)
    )

    for row in rows:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, row)
            except Exception as e:
                print(f"Error inserting row {row}: {e}")
        conn.commit()


def loading(conn: PGConnection):
    load_sizes(conn)
    load_colors(conn)
    load_categories(conn)
    load_products(conn)
    load_product_variants(conn)


def main():
    extract_datasets_if_missing()
    conn = create_db()
    loading(conn)


if __name__ == "__main__":
    main()
