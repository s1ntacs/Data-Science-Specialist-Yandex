import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tempfile
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_source_tables(conn_id: str, run_date: str) -> dict:
    run_date = pd.Timestamp(run_date)

    # Получаем сырое соединение через Airflow Hook
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()

    try:
        # Передаем объект соединения conn, а не строку URI
        customers = pd.read_sql("SELECT * FROM customers", conn)
        sessions = pd.read_sql("SELECT * FROM sessions", conn)
        events = pd.read_sql("SELECT * FROM events", conn)
        orders = pd.read_sql("SELECT * FROM orders", conn)
    finally:
        conn.close()

    customers['signup_date'] = pd.to_datetime(customers['signup_date']).dt.tz_localize(None)
    sessions['start_time'] = pd.to_datetime(sessions['start_time']).dt.tz_localize(None)
    events['timestamp'] = pd.to_datetime(events['timestamp']).dt.tz_localize(None)
    orders['order_time'] = pd.to_datetime(orders['order_time']).dt.tz_localize(None)

    events = events.drop_duplicates(subset=['event_id'])
    sessions = sessions.drop_duplicates(subset=['session_id'])
    orders = orders.drop_duplicates(subset=['order_id'])

    events = events[events['product_id'].notna()]

    events_filtered = events[events['timestamp'] < run_date].copy()
    sessions_filtered = sessions[sessions['start_time'] < run_date].copy()
    orders_filtered = orders[orders['order_time'] < run_date].copy()

    return {
        'customers': customers,
        'sessions': sessions_filtered,
        'events': events_filtered,
        'orders': orders_filtered,
        'run_date': run_date
    }


def add_event_window_features(data: dict) -> pd.DataFrame:
    events = data['events']
    run_date = data['run_date']

    events_with_customer = events.merge(
        data['sessions'][['session_id', 'customer_id']],
        on='session_id',
        how='left'
    )

    window_7_start = run_date - timedelta(days=7)
    window_30_start = run_date - timedelta(days=30)

    result = pd.DataFrame()

    for window_start, suffix in [(window_7_start, '7'), (window_30_start, '30')]:
        page_view = events_with_customer[
            (events_with_customer['event_type'] == 'page_view') &
            (events_with_customer['timestamp'] >= window_start) &
            (events_with_customer['timestamp'] < run_date)
            ].groupby('customer_id').size().reset_index(name=f'page_view_{suffix}')

        add_to_cart = events_with_customer[
            (events_with_customer['event_type'] == 'add_to_cart') &
            (events_with_customer['timestamp'] >= window_start) &
            (events_with_customer['timestamp'] < run_date)
            ].groupby('customer_id').size().reset_index(name=f'add_to_cart_{suffix}')

        purchase = events_with_customer[
            (events_with_customer['event_type'] == 'purchase') &
            (events_with_customer['timestamp'] >= window_start) &
            (events_with_customer['timestamp'] < run_date)
            ].groupby('customer_id').size().reset_index(name=f'purchase_{suffix}')

        unique_products = events_with_customer[
            (events_with_customer['timestamp'] >= window_start) &
            (events_with_customer['timestamp'] < run_date)
            ].groupby('customer_id')['product_id'].nunique().reset_index(name=f'unique_products_{suffix}')

        if result.empty:
            result = data['customers'][['customer_id']].drop_duplicates()

        result = result.merge(page_view, on='customer_id', how='left')
        result = result.merge(add_to_cart, on='customer_id', how='left')
        result = result.merge(purchase, on='customer_id', how='left')
        result = result.merge(unique_products, on='customer_id', how='left')

    return result


def add_session_features(data: dict, features: pd.DataFrame) -> pd.DataFrame:
    sessions = data['sessions']
    events = data['events']
    run_date = data['run_date']

    window_7_start = run_date - timedelta(days=7)
    window_30_start = run_date - timedelta(days=30)

    session_lengths = sessions.merge(
        events[['session_id', 'timestamp']].groupby('session_id').agg(
            session_start=('timestamp', 'min'),
            session_end=('timestamp', 'max')
        ).reset_index(),
        on='session_id',
        how='left'
    )

    session_lengths['duration_seconds'] = (
            session_lengths['session_end'] - session_lengths['session_start']
    ).dt.total_seconds().fillna(0)

    avg_session_length_30 = session_lengths[
        (session_lengths['start_time'] >= window_30_start) &
        (session_lengths['start_time'] < run_date)
        ].groupby('customer_id')['duration_seconds'].mean().reset_index(name='avg_session_length_30')

    sessions_7 = sessions[
        (sessions['start_time'] >= window_7_start) &
        (sessions['start_time'] < run_date)
        ].groupby('customer_id').size().reset_index(name='sessions_7')

    sessions_30 = sessions[
        (sessions['start_time'] >= window_30_start) &
        (sessions['start_time'] < run_date)
        ].groupby('customer_id').size().reset_index(name='sessions_30')

    features = features.merge(avg_session_length_30, on='customer_id', how='left')
    features = features.merge(sessions_7, on='customer_id', how='left')
    features = features.merge(sessions_30, on='customer_id', how='left')

    return features


def add_order_features(data: dict, features: pd.DataFrame) -> pd.DataFrame:
    orders = data['orders']
    run_date = data['run_date']

    window_30_start = run_date - timedelta(days=30)

    last_purchase = orders.groupby('customer_id')['order_time'].max().reset_index(name='last_purchase_date')
    last_purchase['days_since_last_purchase'] = (
            run_date - last_purchase['last_purchase_date']
    ).dt.days
    last_purchase = last_purchase[['customer_id', 'days_since_last_purchase']]
    last_purchase['days_since_last_purchase'] = last_purchase['days_since_last_purchase'].fillna(-1)

    orders_30 = orders[
        (orders['order_time'] >= window_30_start) &
        (orders['order_time'] < run_date)
        ].groupby('customer_id').size().reset_index(name='orders_30')

    orders['total_usd'] = pd.to_numeric(orders['total_usd'], errors='coerce').fillna(0)

    sum_total_usd_30 = orders[
        (orders['order_time'] >= window_30_start) &
        (orders['order_time'] < run_date)
        ].groupby('customer_id')['total_usd'].sum().reset_index(name='sum_total_usd_30')

    avg_total_usd_30 = orders[
        (orders['order_time'] >= window_30_start) &
        (orders['order_time'] < run_date)
        ].groupby('customer_id')['total_usd'].mean().reset_index(name='avg_total_usd_30')

    features = features.merge(last_purchase, on='customer_id', how='left')
    features = features.merge(orders_30, on='customer_id', how='left')
    features = features.merge(sum_total_usd_30, on='customer_id', how='left')
    features = features.merge(avg_total_usd_30, on='customer_id', how='left')

    return features


def add_conversion_features(features: pd.DataFrame) -> pd.DataFrame:
    def safe_divide(num, denom):
        return np.where(denom != 0, num / denom, 0.0)

    features['conversion_page_to_cart_7'] = safe_divide(
        features['add_to_cart_7'].astype(float),
        features['page_view_7'].astype(float)
    )
    features['conversion_page_to_cart_30'] = safe_divide(
        features['add_to_cart_30'].astype(float),
        features['page_view_30'].astype(float)
    )
    features['conversion_cart_to_purchase_7'] = safe_divide(
        features['purchase_7'].astype(float),
        features['add_to_cart_7'].astype(float)
    )
    features['conversion_cart_to_purchase_30'] = safe_divide(
        features['purchase_30'].astype(float),
        features['add_to_cart_30'].astype(float)
    )
    return features


def build_batch_features(conn_id: str, run_date: str) -> pd.DataFrame:
    data = load_source_tables(conn_id, run_date)
    features = add_event_window_features(data)
    features = add_session_features(data, features)
    features = add_order_features(data, features)
    features = add_conversion_features(features)

    count_cols = ['page_view_7', 'page_view_30', 'add_to_cart_7', 'add_to_cart_30',
                  'purchase_7', 'purchase_30', 'unique_products_7', 'unique_products_30',
                  'sessions_7', 'sessions_30', 'orders_30']
    for col in count_cols:
        features[col] = features[col].fillna(0).astype(int)

    float_cols = ['avg_session_length_30', 'sum_total_usd_30', 'avg_total_usd_30']
    for col in float_cols:
        features[col] = features[col].fillna(0.0)

    features['days_since_last_purchase'] = features['days_since_last_purchase'].fillna(-1).astype(int)
    features['run_date'] = pd.Timestamp(run_date)

    return features


def save_features(features: pd.DataFrame, run_date: str, output_path: str = None) -> str:
    if output_path is None:
        output_path = tempfile.mkdtemp()

    os.makedirs(output_path, exist_ok=True)
    file_path = os.path.join(output_path, f"run_date={run_date}", "batch_features.csv")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    features.to_csv(file_path, index=False)
    return file_path