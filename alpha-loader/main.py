import requests
import os

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


async def loading():
    pass


def main():
    import asyncio
    extract_datasets_if_missing()
    asyncio.run(loading())


if __name__ == "__main__":
    main()
