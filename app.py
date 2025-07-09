import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Check environment variable with better error handling
warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID')
if not warehouse_id:
    st.error("❌ DATABRICKS_WAREHOUSE_ID environment variable is not set.")
    st.info("Please ensure the environment variable is configured in app.yaml or your environment.")
    st.stop()

# Databricks config
try:
    cfg = Config()
except Exception as e:
    st.error(f"❌ Failed to initialize Databricks configuration: {str(e)}")
    st.stop()

# Query the SQL warehouse with Service Principal credentials
def sql_query_with_service_principal(query: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    try:
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
            credentials_provider=lambda: cfg.authenticate  # Uses SP credentials from the environment variables
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall_arrow().to_pandas()
    except Exception as e:
        st.error(f"❌ Database connection error: {str(e)}")
        return pd.DataFrame()

# Query the SQL warehouse with the user credentials
def sql_query_with_user_token(query: str, user_token: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    if not user_token:
        st.error("❌ User access token is missing.")
        return pd.DataFrame()
    
    try:
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
            access_token=user_token  # Pass the user token into the SQL connect to query on behalf of user
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall_arrow().to_pandas()
    except Exception as e:
        st.error(f"❌ Database query error: {str(e)}")
        return pd.DataFrame()

st.set_page_config(layout="wide", page_title="Customer Purchase Behavior Analytics")

# Inject custom CSS for modern look
st.markdown('''
    <style>
    /* Card-like containers */
    .stDataFrame, .stPlotlyChart, .stMetric, .stTabs [data-baseweb="tab-list"] {
        background: #232946 !important;
        border-radius: 16px !important;
        box-shadow: 0 2px 16px 0 rgba(0,0,0,0.10);
        padding: 1.5rem 1rem 1rem 1rem;
        margin-bottom: 1.5rem;
    }
    /* Accent headers */
    h1, h2, h3, h4, h5, h6 {
        color: #00B8A9 !important;
        letter-spacing: 0.5px;
    }
    /* Tab highlight */
    .stTabs [data-baseweb="tab"] {
        color: #F4F4F9 !important;
        font-weight: 600;
        border-radius: 8px 8px 0 0;
        background: #393E46;
        margin-right: 2px;
    }
    .stTabs [aria-selected="true"] {
        background: #00B8A9 !important;
        color: #232946 !important;
    }
    /* Button accent */
    .stButton>button {
        background: linear-gradient(90deg, #00B8A9 0%, #3EDBF0 100%) !important;
        color: #232946 !important;
        border-radius: 8px !important;
        font-weight: 700;
        border: none;
    }
    /* Metric cards */
    .stMetric {
        background: #393E46 !important;
        border-radius: 12px !important;
        padding: 1rem !important;
        margin-bottom: 1rem;
    }
    /* Remove Streamlit watermark */
    footer {visibility: hidden;}
    </style>
''', unsafe_allow_html=True)

# Extract user access token from the request headers
user_token = st.context.headers.get('X-Forwarded-Access-Token')

# Main header
st.title("🛒 Customer Purchase Behavior Analytics")
st.markdown("---")

# Query the SQL data with the user credentials
try:
    data = sql_query_with_user_token("SELECT * FROM kaustavpaul_demo.demo_schema.customer_purchase_behavior LIMIT 5000", user_token=user_token)
    
    # Check if data is empty
    if data.empty:
        st.warning("⚠️ No data returned from the database. Please check your table and permissions.")
        st.stop()
    
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
                try:
                    dtype = str(data[col].dtype)
                    null_count = data[col].isnull().sum()
                    unique_count = data[col].nunique()
                    col_info.append({
                        "Column": col,
                        "Data Type": dtype,
                        "Null Values": null_count,
                        "Unique Values": unique_count
                    })
                except Exception as e:
                    col_info.append({
                        "Column": col,
                        "Data Type": "Error",
                        "Null Values": "Error",
                        "Unique Values": "Error"
                    })
            
            col_df = pd.DataFrame(col_info)
            st.dataframe(col_df, use_container_width=True)
        
        with col2:
            st.subheader("📈 Quick Stats")
            st.metric("Total Records", f"{len(data):,}")
            st.metric("Total Columns", len(data.columns))
            
            try:
                memory_usage = data.memory_usage(deep=True).sum() / 1024 / 1024
                st.metric("Memory Usage", f"{memory_usage:.2f} MB")
            except:
                st.metric("Memory Usage", "N/A")
            
            # Data quality indicators
            st.subheader("🔍 Data Quality")
            try:
                total_cells = len(data) * len(data.columns)
                null_cells = data.isnull().sum().sum()
                completeness = ((total_cells - null_cells) / total_cells) * 100 if total_cells > 0 else 0
                st.metric("Data Completeness", f"{completeness:.1f}%")
            except:
                st.metric("Data Completeness", "N/A")
        
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
            try:
                # Get unique categories and sort them
                unique_categories = sorted(data[category_cols[0]].dropna().unique())
                
                # Create multiselect for multiple category selection
                selected_categories = st.multiselect(
                    "Select Categories (leave empty for all categories)",
                    options=unique_categories,
                    default=[],
                    help="Choose one or more categories to filter the data. Leave empty to show all categories."
                )
                
                # Filter data based on selected categories
                if selected_categories:
                    filtered_data = data[data[category_cols[0]].isin(selected_categories)]
                    st.success(f"Showing data for {len(selected_categories)} selected category(ies): {', '.join(selected_categories)}")
                else:
                    filtered_data = data
                    st.info("Showing data for all categories")
            except Exception as e:
                filtered_data = data
                st.warning(f"Error in category filtering: {str(e)}")
        else:
            filtered_data = data
            st.info("No category columns available for filtering.")
        
        # Key metrics row
        st.subheader("🎯 Key Metrics")
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        
        with metric_col1:
            if amount_cols:
                try:
                    avg_purchase = filtered_data[amount_cols[0]].mean()
                    st.metric("Average Purchase", f"${avg_purchase:.2f}" if pd.notna(avg_purchase) else "$0.00")
                except:
                    st.metric("Average Purchase", "N/A")
            else:
                st.metric("Total Records", f"{len(filtered_data):,}")
        
        with metric_col2:
            if amount_cols:
                try:
                    total_revenue = filtered_data[amount_cols[0]].sum()
                    st.metric("Total Revenue", f"${total_revenue:,.2f}" if pd.notna(total_revenue) else "$0.00")
                except:
                    st.metric("Total Revenue", "N/A")
            else:
                st.metric("Unique Categories", f"{len(category_cols) if category_cols else 0}")
        
        with metric_col3:
            if amount_cols:
                try:
                    max_purchase = filtered_data[amount_cols[0]].max()
                    st.metric("Max Purchase", f"${max_purchase:.2f}" if pd.notna(max_purchase) else "$0.00")
                except:
                    st.metric("Max Purchase", "N/A")
            else:
                st.metric("Date Range", f"{len(date_cols) if date_cols else 'N/A'}")
        
        with metric_col4:
            if amount_cols:
                try:
                    min_purchase = filtered_data[amount_cols[0]].min()
                    st.metric("Min Purchase", f"${min_purchase:.2f}" if pd.notna(min_purchase) else "$0.00")
                except:
                    st.metric("Min Purchase", "N/A")
            else:
                st.metric("Columns", len(filtered_data.columns))
        
        # Main visualizations
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.subheader("👥 Customer Analysis")
            
            # Try to identify geographic columns
            state_cols = [col for col in data.columns if 'state' in col.lower() or 'province' in col.lower()]
            country_cols = [col for col in data.columns if 'country' in col.lower()]
            city_cols = [col for col in data.columns if 'city' in col.lower()]
            
            if customer_cols and amount_cols:
                # Customer purchase analysis with multiple metrics
                try:
                    customer_analysis = filtered_data.groupby(customer_cols[0]).agg({
                        amount_cols[0]: ['sum', 'mean', 'count'],
                    }).reset_index()
                    # Flatten column names
                    customer_analysis.columns = [customer_cols[0], 'Total_Spent', 'Avg_Purchase', 'Purchase_Count']
                    # Add customer segments with error handling
                    try:
                        customer_analysis['Customer_Segment'] = pd.cut(
                            customer_analysis['Total_Spent'], 
                            bins=3, 
                            labels=['Low Value', 'Medium Value', 'High Value']
                        )
                    except Exception as e:
                        st.warning(f"Could not create customer segments: {str(e)}")
                        customer_analysis['Customer_Segment'] = 'Unknown'
                except Exception as e:
                    st.error(f"Error in customer analysis: {str(e)}")
                    customer_analysis = pd.DataFrame()
                # Only show visualizations if we have data
                if not customer_analysis.empty:
                    col1a, col1b = st.columns(2)
                    with col1a:
                        try:
                            segment_counts = customer_analysis['Customer_Segment'].value_counts()
                            fig_segments = px.pie(
                                values=segment_counts.values, 
                                names=segment_counts.index,
                                title="Customer Value Segments",
                                color_discrete_sequence=px.colors.qualitative.Set3
                            )
                            st.plotly_chart(fig_segments, use_container_width=True)
                        except Exception as e:
                            st.error(f"Error creating segments chart: {str(e)}")
                    with col1b:
                        try:
                            top_customers = customer_analysis.nlargest(10, 'Total_Spent')
                            fig_top = px.bar(
                                top_customers, 
                                x=customer_cols[0], 
                                y='Total_Spent',
                                title="Top 10 Customers by Total Spent",
                                labels={'Total_Spent': 'Total Amount ($)'}
                            )
                            fig_top.update_xaxis(tickangle=45)
                            st.plotly_chart(fig_top, use_container_width=True)
                        except Exception as e:
                            st.error(f"Error creating top customers chart: {str(e)}")
                    try:
                        fig_scatter = px.scatter(
                            customer_analysis, 
                            x='Purchase_Count', 
                            y='Total_Spent',
                            size='Avg_Purchase',
                            color='Customer_Segment',
                            hover_data=[customer_cols[0]],
                            title="Customer Purchase Behavior Analysis",
                            labels={
                                'Purchase_Count': 'Number of Purchases', 
                                'Total_Spent': 'Total Amount Spent ($)',
                                'Avg_Purchase': 'Average Purchase Amount',
                                'Customer_Segment': 'Customer Segment'
                            }
                        )
                        st.plotly_chart(fig_scatter, use_container_width=True)
                    except Exception as e:
                        st.error(f"Error creating scatter plot: {str(e)}")
                    st.subheader("📊 Customer Insights")
                    insight_col1, insight_col2, insight_col3, insight_col4 = st.columns(4)
                    with insight_col1:
                        try:
                            st.metric("Total Customers", len(customer_analysis))
                        except:
                            st.metric("Total Customers", "N/A")
                    with insight_col2:
                        try:
                            avg_value = customer_analysis['Total_Spent'].mean()
                            st.metric("Avg Customer Value", f"${avg_value:.2f}" if pd.notna(avg_value) else "$0.00")
                        except:
                            st.metric("Avg Customer Value", "N/A")
                    with insight_col3:
                        try:
                            high_value_count = len(customer_analysis[customer_analysis['Customer_Segment'] == 'High Value'])
                            st.metric("High Value Customers", high_value_count)
                        except:
                            st.metric("High Value Customers", "N/A")
                    with insight_col4:
                        try:
                            avg_frequency = customer_analysis['Purchase_Count'].mean()
                            st.metric("Avg Purchase Frequency", f"{avg_frequency:.1f}" if pd.notna(avg_frequency) else "0.0")
                        except:
                            st.metric("Avg Purchase Frequency", "N/A")
                
            elif customer_cols:
                # Customer frequency analysis when no amount data
                customer_counts = filtered_data[customer_cols[0]].value_counts()
                
                col1a, col1b = st.columns(2)
                
                with col1a:
                    # Top customers by frequency
                    top_frequent = customer_counts.head(15)
                    fig_frequent = px.bar(
                        x=top_frequent.values, 
                        y=top_frequent.index,
                        orientation='h',
                        title="Top 15 Customers by Purchase Frequency",
                        labels={'x': 'Number of Purchases', 'y': 'Customer ID'}
                    )
                    st.plotly_chart(fig_frequent, use_container_width=True)
                
                with col1b:
                    # Customer frequency distribution
                    fig_dist = px.histogram(
                        x=customer_counts.values,
                        nbins=20,
                        title="Customer Purchase Frequency Distribution",
                        labels={'x': 'Number of Purchases', 'y': 'Number of Customers'}
                    )
                    st.plotly_chart(fig_dist, use_container_width=True)
                
                # Customer insights
                st.subheader("📊 Customer Insights")
                insight_col1, insight_col2, insight_col3, insight_col4 = st.columns(4)
                
                with insight_col1:
                    st.metric("Total Customers", len(customer_counts))
                
                with insight_col2:
                    st.metric("Avg Purchases per Customer", f"{customer_counts.mean():.1f}")
                
                with insight_col3:
                    st.metric("Most Frequent Customer", f"{customer_counts.max()} purchases")
                
                with insight_col4:
                    st.metric("Single Purchase Customers", len(customer_counts[customer_counts == 1]))
                
            else:
                # Geographic analysis when no customer columns
                st.subheader("🌍 Geographic Distribution")
                
                if state_cols or country_cols or city_cols:
                    col1a, col1b = st.columns(2)
                    
                    with col1a:
                        if state_cols:
                            # Customers per state
                            state_counts = filtered_data[state_cols[0]].value_counts().head(15)
                            fig_state = px.bar(
                                x=state_counts.values,
                                y=state_counts.index,
                                orientation='h',
                                title="Top 15 States by Customer Count",
                                labels={'x': 'Number of Customers', 'y': 'State'},
                                color=state_counts.values,
                                color_continuous_scale='viridis'
                            )
                            st.plotly_chart(fig_state, use_container_width=True)
                        elif city_cols:
                            # Customers per city
                            city_counts = filtered_data[city_cols[0]].value_counts().head(15)
                            fig_city = px.bar(
                                x=city_counts.values,
                                y=city_counts.index,
                                orientation='h',
                                title="Top 15 Cities by Customer Count",
                                labels={'x': 'Number of Customers', 'y': 'City'},
                                color=city_counts.values,
                                color_continuous_scale='plasma'
                            )
                            st.plotly_chart(fig_city, use_container_width=True)
                    
                    with col1b:
                        if country_cols:
                            # Customers per country
                            country_counts = filtered_data[country_cols[0]].value_counts()
                            fig_country = px.pie(
                                values=country_counts.values,
                                names=country_counts.index,
                                title="Customer Distribution by Country",
                                color_discrete_sequence=px.colors.qualitative.Set1
                            )
                            st.plotly_chart(fig_country, use_container_width=True)
                        elif state_cols:
                            # State distribution pie chart
                            state_pie = filtered_data[state_cols[0]].value_counts().head(10)
                            fig_state_pie = px.pie(
                                values=state_pie.values,
                                names=state_pie.index,
                                title="Top 10 States - Customer Distribution",
                                color_discrete_sequence=px.colors.qualitative.Set2
                            )
                            st.plotly_chart(fig_state_pie, use_container_width=True)
                    
                    # Geographic insights
                    st.subheader("📊 Geographic Insights")
                    geo_col1, geo_col2, geo_col3, geo_col4 = st.columns(4)
                    
                    with geo_col1:
                        if state_cols:
                            st.metric("Total States", filtered_data[state_cols[0]].nunique())
                        elif city_cols:
                            st.metric("Total Cities", filtered_data[city_cols[0]].nunique())
                        else:
                            st.metric("Total Records", len(filtered_data))
                    
                    with geo_col2:
                        if country_cols:
                            st.metric("Total Countries", filtered_data[country_cols[0]].nunique())
                        elif state_cols:
                            top_state = filtered_data[state_cols[0]].value_counts().index[0]
                            st.metric("Top State", top_state)
                        else:
                            st.metric("Unique Values", filtered_data.nunique().sum())
                    
                    with geo_col3:
                        if state_cols:
                            avg_customers_per_state = len(filtered_data) / filtered_data[state_cols[0]].nunique()
                            st.metric("Avg Customers/State", f"{avg_customers_per_state:.1f}")
                        elif city_cols:
                            avg_customers_per_city = len(filtered_data) / filtered_data[city_cols[0]].nunique()
                            st.metric("Avg Customers/City", f"{avg_customers_per_city:.1f}")
                        else:
                            st.metric("Data Completeness", f"{(1 - filtered_data.isnull().sum().sum() / (len(filtered_data) * len(filtered_data.columns))) * 100:.1f}%")
                    
                    with geo_col4:
                        if state_cols:
                            max_customers_state = filtered_data[state_cols[0]].value_counts().max()
                            st.metric("Max Customers/State", max_customers_state)
                        elif city_cols:
                            max_customers_city = filtered_data[city_cols[0]].value_counts().max()
                            st.metric("Max Customers/City", max_customers_city)
                        else:
                            st.metric("Columns", len(filtered_data.columns))
                
                else:
                    st.info("No geographic columns (state, country, city) detected for geographic analysis.")
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
