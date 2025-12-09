from flask import Flask, render_template
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os

app = Flask(__name__)

# Database configuration (connecting to the 'db' service)
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
        # In a real app, you'd log this error
        print(f"Database query failed: {e}") 
        return None

def calculate_kpis_and_segments():
    # 1. Fetch all necessary data
    query = "SELECT sales, order_date, order_number, product_line, country FROM sales_data"
    df = run_db_query(query)
    
    if df is None or df.empty:
        return None, None, None, None
    
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

    # --- Segmentation and Forecasting Logic (Same as previous step) ---
    
    # Product Line
    product_sales = df.groupby('product_line')['sales'].sum().sort_values(ascending=False).reset_index()
    product_sales.columns = ['Product Line', 'Total Sales']
    product_sales['Total Sales'] = product_sales['Total Sales'].apply(lambda x: f"${x:,.2f}")
    
    # Country
    country_sales = df.groupby('country')['sales'].sum().sort_values(ascending=False).head(10).reset_index()
    country_sales.columns = ['Country', 'Total Sales']
    country_sales['Total Sales'] = country_sales['Total Sales'].apply(lambda x: f"${x:,.2f}")

    # Forecasting (Linear Trend Extrapolation)
    df['order_date'] = pd.to_datetime(df['order_date'])
    monthly_sales = df.set_index('order_date').resample('M')['sales'].sum().reset_index()
    
    X = np.arange(len(monthly_sales))
    y = monthly_sales['sales'].values
    z = np.polyfit(X, y, 1)
    p = np.poly1d(z)

    future_months = 6
    X_future = np.arange(len(monthly_sales), len(monthly_sales) + future_months)
    forecasted_sales = p(X_future)
    
    last_date = monthly_sales['order_date'].iloc[-1]
    future_dates = pd.date_range(start=last_date, periods=future_months + 1, freq='M')[1:]

    forecast_df = pd.DataFrame({
        'Date': future_dates.strftime('%Y-%m'),
        'Forecasted Sales': forecasted_sales
    })
    forecast_df['Forecasted Sales'] = forecast_df['Forecasted Sales'].apply(lambda x: f"${x:,.2f}")
    
    return kpis, product_sales, country_sales, forecast_df

@app.route('/')
def dashboard():
    kpis, product_sales, country_sales, forecast_df = calculate_kpis_and_segments()
    
    if kpis is None:
        return render_template('db_error.html') # Need a basic error page template
        
    # Convert dataframes to HTML tables for rendering
    product_sales_html = product_sales.to_html(classes='table table-striped', index=False)
    country_sales_html = country_sales.to_html(classes='table table-striped', index=False)
    forecast_html = forecast_df.to_html(classes='table table-striped', index=False)
    
    return render_template(
        'dashboard.html',
        kpis=kpis,
        product_sales_html=product_sales_html,
        country_sales_html=country_sales_html,
        forecast_html=forecast_html
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)