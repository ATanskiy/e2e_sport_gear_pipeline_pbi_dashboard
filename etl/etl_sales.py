import pandas as pd

# Processes 2 dfs online and offline
def transform_sales(online_df: pd.DataFrame, offline_df: pd.DataFrame, conn, schema) -> pd.DataFrame:

    s_online_df = online_df.copy()
    s_offline_df = offline_df.copy()

    # Standardize column names (renaming where necessary for the columns having similar business sense)
    s_offline_df.rename(columns={
        'product_name': 'product',
        'brand': 'brand_name',
        'category': 'product_category',
        'subcategory': 'product_subcategory',
        'date': 'tmstmp',
        'price': 'product_price',
        'amount_sold': 'total_amount',
        'cost_amount': 'total_costs'
    }, inplace=True)

    s_online_df.rename(columns={
        'payment_type': 'payment_method'
    }, inplace=True)

    # Add missing columns and fill with None where necessary
    missing_columns = set(s_offline_df.columns) - set(s_offline_df.columns)
    for col in missing_columns:
        s_offline_df[col] = None  # Fill missing columns in offline with None

    missing_columns = set(s_offline_df.columns) - set(s_offline_df.columns)
    for col in missing_columns:
        s_online_df[col] = None  # Fill missing columns in online with None

    # Add a sales channel column
    s_online_df['sales_channel'] = 'Online'
    s_offline_df['sales_channel'] = 'Offline'

    # Combine and sort
    combined_df = pd.concat([s_online_df, s_offline_df], ignore_index=True)
    combined_df.sort_values(by='tmstmp', inplace=True)
    combined_df.reset_index(drop=True, inplace=True)

    # retrieve all data from dimention tables:

    products = pd.read_sql(f"SELECT product_id, product, brand_name FROM {schema}.products", conn)
    customers = pd.read_sql(f"SELECT customer_id, customer_email FROM {schema}.customers", conn)
    stores = pd.read_sql(f"SELECT store_id, store_type, store_street, store_city,\
                            store_state FROM {schema}.stores", conn)
    employees = pd.read_sql(f"SELECT employee_id, employee_firstname, employee_lastname,\
                                    employee_email, employee_skill, employee_education\
                                    FROM {schema}.employees", conn)
    payment_methods = pd.read_sql(f"SELECT payment_method_id, payment_method\
                                    FROM {schema}.payment_methods", conn)
    shipping_methods = pd.read_sql(f"SELECT shipping_method_id, shipping_method\
                                    FROM {schema}.shipping_methods", conn)

    # Define join configurations: (right_df, left_on, right_on, join_type)
    joins = [
        (products, ['product', 'brand_name'], 'left'),
        (customers,['customer_email'],'left'),
        (stores, ['store_type', 'store_street', 'store_city', 'store_state'],'left'),
        (employees,
        ['employee_firstname', 'employee_lastname', 'employee_email', 'employee_skill', 'employee_education'],
        'left'),
        (payment_methods, ['payment_method'], 'left'),
        (shipping_methods, ['shipping_method'], 'left')
    ]

    # Apply merges
    for right_df, keys, join_type in joins:
        combined_df = combined_df.merge(
            right_df,
            on=keys,
            how=join_type
        )

    # replace store_id with 8 for Online stores
    combined_df.loc[combined_df['sales_channel'] == 'Online', 'store_id'] = 8

    # final touches
    combined_df['coupon_discount'] = combined_df.apply(
    lambda row: 0 if row['sales_channel'] == 'Offline' else float(row['coupon_discount']),
    axis=1)

    combined_df['shipping_method_id'] = combined_df.apply(
    lambda row: 5 if row['sales_channel'] == 'Offline' else row['shipping_method_id'],
    axis=1)

    # Final reorder
    final_columns = [
        'tmstmp', 'product_id', 'customer_id', 'store_id', 'employee_id',
        'payment_method_id', 'shipping_method_id', 'product_price', 'coupon_discount',
        'quantity_sold', 'total_amount', 'total_costs', 'sales_channel', 'store_website', 'supplier'
    ]

    return combined_df[final_columns].where(pd.notnull(combined_df), None)

# Upserts new customers to customers table in 2 schemas
def upsert_sales(df, conn, schema):
    if df.empty:
        print(f"⚠️ No sales records to insert for schema '{schema}'.")
        return

    columns = list(df.columns)
    placeholders = ', '.join(['%s'] * len(columns))
    col_str = ', '.join(columns)

    # Build UPDATE SET clause, excluding conflict keys
    update_str = ', '.join([
        f"{col} = EXCLUDED.{col}"
        for col in columns
        if col not in ('customer_id', 'tmstmp')  # do not update the conflict keys
    ])

    newly_inserted_count = 0

    with conn.cursor() as cur:
        for row in df.itertuples(index=False, name=None):
            sql = f"""
                INSERT INTO {schema}.sales ({col_str})
                VALUES ({placeholders})
                ON CONFLICT (customer_id, tmstmp) DO UPDATE
                SET {update_str}
                RETURNING xmax = 0;
            """
            cur.execute(sql, row)
            # In PostgreSQL, xmax = 0 means it was inserted, not updated
            is_inserted = cur.fetchone()[0]
            if is_inserted:
                newly_inserted_count += 1

        conn.commit()

    print(f"✅ Upserted {len(df)} records into {schema}.sales")
    print(f"🆕 Newly inserted: {newly_inserted_count}")