from flask import Flask, render_template
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
# Plotly for interactive charts
import plotly.graph_objects as go
# We no longer need to import 'json' explicitly here since we use fig.to_json()

app = Flask(__name__)

# Database configuration
DB_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.environ.get("POSTGRES_HOST", "db") 
DB_NAME = os.environ.get("POSTGRES_DB", "sales_db")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL)

def run_db_query(query):
    """Executes a SQL query and returns results as a pandas DataFrame."""
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection)
        return df
    except Exception as e:
        print(f"Database query failed: {e}") 
        return None

def calculate_kpis_and_data():
    query = "SELECT sales, order_date, order_number, product_line, country FROM sales_data"
    df = run_db_query(query)
    
    if df is None or df.empty:
        # Returning None for all six expected outputs
        return None, None, None, None, None, None 
    
    # --- KPI Calculation ---
    total_revenue = df['sales'].sum()
    estimated_profit = total_revenue * 0.30 
    total_orders = df['order_number'].nunique()
    aov = total_revenue / total_orders if total_orders else 0
    
    kpis = {
        'Total Revenue': f"${total_revenue:,.2f}",
        'Estimated Profit': f"${estimated_profit:,.2f}",
        'Total Orders': f"{total_orders:,}",
        'AOV': f"${aov:,.2f}"
    }

    # --- Segmentation Data ---
    product_sales = df.groupby('product_line')['sales'].sum().sort_values(ascending=False)
    country_sales = df.groupby('country')['sales'].sum().sort_values(ascending=False).head(10)
    
    # --- Forecasting Data ---
    df['order_date'] = pd.to_datetime(df['order_date'])
    
    # FIX: Using 'ME' (Month End) instead of deprecated 'M'
    monthly_sales = df.set_index('order_date').resample('ME')['sales'].sum().reset_index()
    
    X = np.arange(len(monthly_sales))
    y = monthly_sales['sales'].values
    z = np.polyfit(X, y, 1)
    p = np.poly1d(z)

    future_months = 6
    X_future = np.arange(len(monthly_sales), len(monthly_sales) + future_months)
    forecasted_sales = p(X_future)
    
    last_date = monthly_sales['order_date'].iloc[-1]
    
    # FIX: Using 'ME' (Month End) instead of deprecated 'M'
    future_dates = pd.date_range(start=last_date, periods=future_months + 1, freq='ME')[1:]


    # --- Chart Generation ---
    
    # 1. Forecast Chart
    fig_forecast = go.Figure()
    fig_forecast.add_trace(go.Scatter(
        x=monthly_sales['order_date'], y=monthly_sales['sales'], mode='lines+markers', name='Historical Sales', line=dict(color='blue')
    ))
    fig_forecast.add_trace(go.Scatter(
        x=future_dates, y=forecasted_sales, mode='lines+markers', name='Forecasted Sales', line=dict(color='red', dash='dash')
    ))
    fig_forecast.update_layout(title='Monthly Sales & 6-Month Forecast', xaxis_title='Date', yaxis_title='Total Sales ($)', template='plotly_white')
    
    # 2. Product Line Chart
    fig_product = go.Figure(data=[go.Bar(
        x=product_sales.index, y=product_sales.values, marker_color='skyblue'
    )])
    fig_product.update_layout(title='Revenue by Product Line', xaxis_title='Product Line', yaxis_title='Total Sales ($)', template='plotly_white')
    
    # 3. Country Chart
    fig_country = go.Figure(data=[go.Bar(
        x=country_sales.index, y=country_sales.values, marker_color='lightgreen'
    )])
    fig_country.update_layout(title='Top 10 Revenue by Country', xaxis_title='Country', yaxis_title='Total Sales ($)', template='plotly_white')

    # FIX: Convert Plotly figures to JSON using fig.to_json()
    forecast_json = fig_forecast.to_json()
    product_json = fig_product.to_json()
    country_json = fig_country.to_json()

    return kpis, product_sales, country_sales, forecast_json, product_json, country_json

@app.route('/')
def dashboard():
    kpis, product_sales, country_sales, forecast_json, product_json, country_json = calculate_kpis_and_data()
    
    if kpis is None:
        # Placeholder for a DB error template
        return "<h1 style='color:red;'>Database Connection Error or Data Empty. Check data loader logs.</h1>" 
        
    # Convert segmented dataframes to HTML tables for display next to charts
    product_sales_html = product_sales.reset_index().rename(columns={'index': 'Product Line', 'sales': 'Total Sales'}).to_html(classes='table table-striped', index=False)
    country_sales_html = country_sales.reset_index().rename(columns={'index': 'Country', 'sales': 'Total Sales'}).to_html(classes='table table-striped', index=False)
    
    return render_template(
        'dashboard.html',
        kpis=kpis,
        product_sales_html=product_sales_html,
        country_sales_html=country_sales_html,
        forecast_json=forecast_json,
        product_json=product_json,
        country_json=country_json
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)