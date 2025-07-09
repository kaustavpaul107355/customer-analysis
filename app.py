import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# Databricks config
cfg = Config()

# Query the SQL warehouse with Service Principal credentials
def sql_query_with_service_principal(query: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        credentials_provider=lambda: cfg.authenticate  # Uses SP credentials from the environment variables
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

# Query the SQL warehouse with the user credentials
def sql_query_with_user_token(query: str, user_token: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        access_token=user_token  # Pass the user token into the SQL connect to query on behalf of user
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

st.set_page_config(layout="wide", page_title="Customer Purchase Behavior Analytics")

# Extract user access token from the request headers
user_token = st.context.headers.get('X-Forwarded-Access-Token')

# Main header
st.title("🛒 Customer Purchase Behavior Analytics")
st.markdown("---")

# Query the SQL data with the user credentials
try:
    data = sql_query_with_user_token("SELECT * FROM kaustavpaul_demo.demo_schema.customer_purchase_behavior LIMIT 5000", user_token=user_token)
    
    # Create tabs
    tab1, tab2 = st.tabs(["📋 Data Overview", "📊 Analytics"])
    
    with tab1:
        st.header("📋 Data Source Overview")
        
        # Data source information
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("🔍 Data Source Details")
            st.info(f"""
            **Table**: `kaustavpaul_demo.demo_schema.customer_purchase_behavior`
            
            **Records Loaded**: {len(data):,} records
            **Columns**: {len(data.columns)} fields
            **Data Type**: Customer Purchase Behavior Analytics
            """)
            
            # Column information
            st.subheader("📊 Column Information")
            col_info = []
            for col in data.columns:
                dtype = str(data[col].dtype)
                null_count = data[col].isnull().sum()
                unique_count = data[col].nunique()
                col_info.append({
                    "Column": col,
                    "Data Type": dtype,
                    "Null Values": null_count,
                    "Unique Values": unique_count
                })
            
            col_df = pd.DataFrame(col_info)
            st.dataframe(col_df, use_container_width=True)
        
        with col2:
            st.subheader("📈 Quick Stats")
            st.metric("Total Records", f"{len(data):,}")
            st.metric("Total Columns", len(data.columns))
            st.metric("Memory Usage", f"{data.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
            
            # Data quality indicators
            st.subheader("🔍 Data Quality")
            total_cells = len(data) * len(data.columns)
            null_cells = data.isnull().sum().sum()
            completeness = ((total_cells - null_cells) / total_cells) * 100
            st.metric("Data Completeness", f"{completeness:.1f}%")
        
        # Sample records
        st.subheader("📄 Sample Records")
        
        # Show first 10 records
        st.write("**First 10 Records:**")
        st.dataframe(data.head(10), use_container_width=True)
        
        # Show last 10 records
        st.write("**Last 10 Records:**")
        st.dataframe(data.tail(10), use_container_width=True)
    
    with tab2:
        st.header("📊 Analytics Dashboard")
        
        # Try to identify date and amount columns
        date_cols = [col for col in data.columns if 'date' in col.lower() or 'time' in col.lower()]
        amount_cols = [col for col in data.columns if 'amount' in col.lower() or 'price' in col.lower() or 'value' in col.lower()]
        category_cols = [col for col in data.columns if 'category' in col.lower() or 'product' in col.lower() or 'type' in col.lower()]
        customer_cols = [col for col in data.columns if 'customer' in col.lower() or 'user' in col.lower() or 'id' in col.lower()]
        
        # Category filter at the top
        st.subheader("🔍 Filter by Category")
        if category_cols:
            selected_category = st.selectbox("Select Category", 
                                           ["All Categories"] + list(data[category_cols[0]].unique()))
            if selected_category != "All Categories":
                filtered_data = data[data[category_cols[0]] == selected_category]
            else:
                filtered_data = data
        else:
            filtered_data = data
            st.info("No category columns available for filtering.")
        
        # Key metrics row
        st.subheader("🎯 Key Metrics")
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        
        with metric_col1:
            if amount_cols:
                st.metric("Average Purchase", f"${filtered_data[amount_cols[0]].mean():.2f}")
            else:
                st.metric("Total Records", f"{len(filtered_data):,}")
        
        with metric_col2:
            if amount_cols:
                st.metric("Total Revenue", f"${filtered_data[amount_cols[0]].sum():,.2f}")
            else:
                st.metric("Unique Categories", f"{len(category_cols) if category_cols else 0}")
        
        with metric_col3:
            if amount_cols:
                st.metric("Max Purchase", f"${filtered_data[amount_cols[0]].max():.2f}")
            else:
                st.metric("Date Range", f"{len(date_cols) if date_cols else 'N/A'}")
        
        with metric_col4:
            if amount_cols:
                st.metric("Min Purchase", f"${filtered_data[amount_cols[0]].min():.2f}")
            else:
                st.metric("Columns", len(filtered_data.columns))
        
        # Main visualizations
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.subheader("👥 Customer Analysis")
            
            if customer_cols and amount_cols:
                # Customer purchase analysis
                customer_purchases = filtered_data.groupby(customer_cols[0])[amount_cols[0]].agg(['sum', 'count', 'mean']).reset_index()
                customer_purchases.columns = [customer_cols[0], 'Total_Spent', 'Purchase_Count', 'Avg_Purchase']
                
                # Create scatter plot of customer behavior
                fig = px.scatter(customer_purchases, 
                               x='Purchase_Count', 
                               y='Total_Spent',
                               size='Avg_Purchase',
                               hover_data=[customer_cols[0]],
                               title="Customer Purchase Behavior Analysis",
                               labels={'Purchase_Count': 'Number of Purchases', 
                                      'Total_Spent': 'Total Amount Spent ($)',
                                      'Avg_Purchase': 'Average Purchase Amount'})
                st.plotly_chart(fig, use_container_width=True)
            elif customer_cols:
                # Customer frequency analysis
                customer_counts = filtered_data[customer_cols[0]].value_counts()
                fig = px.bar(x=customer_counts.head(20).index, 
                           y=customer_counts.head(20).values,
                           title="Top 20 Customers by Purchase Frequency",
                           labels={'x': 'Customer ID', 'y': 'Number of Purchases'})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Customer columns not detected for customer analysis.")
                st.dataframe(filtered_data.head(10))
        
        with col2:
            st.subheader("📊 Category Distribution")
            
            # Category distribution if available
            if category_cols:
                category_counts = filtered_data[category_cols[0]].value_counts()
                fig = px.pie(values=category_counts.values, names=category_counts.index, 
                            title="Purchase Distribution by Category")
                st.plotly_chart(fig, use_container_width=True, height=500)
            else:
                st.info("Category columns not detected for distribution analysis.")
        
        # Second row of visualizations
        st.subheader("🔍 Top Categories")
        if category_cols:
            top_categories = filtered_data[category_cols[0]].value_counts().head(10)
            fig = px.bar(x=top_categories.values, y=top_categories.index, orientation='h',
                        title="Top 10 Purchase Categories",
                        labels={'x': 'Count', 'y': 'Category'})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Category columns not detected for top categories analysis.")

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("Please check the table name and ensure you have access to kaustavpaul_demo.demo_schema.customer_purchase_behavior")
