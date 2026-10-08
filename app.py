import streamlit as st
import pandas as pd
import plotly.express as px
import os
from pathlib import Path

# Page configuration
st.set_page_config(
    page_title="Ecommerce Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Define data paths
DATA_DIR = Path(__file__).parent / "data" / "gold"
GOLD_CSV = DATA_DIR / "companies_revenue_gold.csv"
SUMMARY_CSV = DATA_DIR / "companies_revenue_gold_summary.csv"


@st.cache_data
def load_gold_data():
    """Load the main gold layer data"""
    if not GOLD_CSV.exists():
        st.error(f"❌ Gold data file not found: {GOLD_CSV}")
        return None
    try:
        df = pd.read_csv(GOLD_CSV)
        df['scraped_date'] = pd.to_datetime(df['scraped_date'])
        return df
    except Exception as e:
        st.error(f"❌ Error loading gold data: {e}")
        return None


@st.cache_data
def load_summary_data():
    """Load the summary statistics"""
    if not SUMMARY_CSV.exists():
        st.error(f"❌ Summary file not found: {SUMMARY_CSV}")
        return None
    try:
        df = pd.read_csv(SUMMARY_CSV)
        return df
    except Exception as e:
        st.error(f"❌ Error loading summary data: {e}")
        return None


def get_summary_metric(summary_df, metric_name):
    """Extract a metric value from summary dataframe"""
    if summary_df is None:
        return None
    row = summary_df[summary_df['metric'] == metric_name]
    if row.empty:
        return None
    return row['value'].values[0]


def page_overview():
    """Overview page with KPIs and main visualizations"""
    st.title("📊 Overview")
    
    summary_df = load_summary_data()
    gold_df = load_gold_data()
    
    if summary_df is None or gold_df is None:
        return
    
    # KPI Cards
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_revenue = get_summary_metric(summary_df, 'total_revenue')
        st.metric("Total Revenue", f"${total_revenue:,.0f}")
    
    with col2:
        total_books = get_summary_metric(summary_df, 'total_rows')
        st.metric("Total Books", f"{int(total_books)}")
    
    with col3:
        avg_revenue = get_summary_metric(summary_df, 'avg_revenue')
        st.metric("Avg Revenue", f"${avg_revenue:,.0f}")
    
    with col4:
        top_performers = get_summary_metric(summary_df, 'top_performers_count')
        st.metric("Top Performers", f"{int(top_performers)}")
    
    with col5:
        total_profit = get_summary_metric(summary_df, 'total_profit')
        st.metric("Total Profit", f"${total_profit:,.0f}")
    
    st.divider()
    
    # Revenue Distribution Histogram
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Revenue Distribution")
        fig_hist = px.histogram(
            gold_df,
            x='revenue',
            nbins=50,
            title="Distribution of Revenue Across Books",
            labels={'revenue': 'Revenue ($)', 'count': 'Number of Books'},
            color_discrete_sequence=['#636EFA']
        )
        fig_hist.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_hist, use_container_width=True)
    
    with col2:
        st.subheader("Revenue by Category")
        revenue_by_category = gold_df.groupby('revenue_category').size()
        fig_pie = px.pie(
            values=revenue_by_category.values,
            names=revenue_by_category.index,
            title="Distribution of Books by Revenue Category",
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        fig_pie.update_layout(height=400)
        st.plotly_chart(fig_pie, use_container_width=True)


def page_top_books():
    """Top Books page with filterable table and chart"""
    st.title("🏆 Top Books")
    
    gold_df = load_gold_data()
    if gold_df is None:
        return
    
    # Slider for top N books
    col1, col2 = st.columns([3, 1])
    with col2:
        top_n = st.slider("Show Top N Books", min_value=5, max_value=50, value=10)
    
    # Get top N books by revenue
    top_books = gold_df.nlargest(top_n, 'revenue').copy()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader(f"Top {top_n} Books by Revenue")
        
        # Prepare display dataframe
        display_df = top_books[['title', 'revenue', 'sales', 'revenue_per_sale', 'is_top_performer']].copy()
        display_df['revenue'] = display_df['revenue'].apply(lambda x: f"${x:,.0f}")
        display_df['sales'] = display_df['sales'].astype(int)
        display_df['revenue_per_sale'] = display_df['revenue_per_sale'].apply(lambda x: f"${x:,.2f}")
        display_df['Top Performer'] = display_df['is_top_performer'].map({True: '⭐ Yes', False: 'No'})
        
        st.dataframe(
            display_df[['title', 'revenue', 'sales', 'revenue_per_sale', 'Top Performer']],
            use_container_width=True,
            hide_index=True
        )
    
    with col2:
        st.subheader(f"Top {top_n} Books Revenue Chart")
        
        # Create bar chart with color coding for top performers
        fig_bar = px.bar(
            top_books.sort_values('revenue', ascending=True),
            x='revenue',
            y='title',
            color='is_top_performer',
            title=f"Revenue of Top {top_n} Books",
            labels={'revenue': 'Revenue ($)', 'title': 'Book Title', 'is_top_performer': 'Top Performer'},
            color_discrete_map={True: '#EF553B', False: '#636EFA'},
            orientation='h'
        )
        fig_bar.update_layout(height=max(400, top_n * 15), showlegend=True)
        st.plotly_chart(fig_bar, use_container_width=True)


def page_category_analysis():
    """Category Analysis page with revenue and price breakdowns"""
    st.title("📈 Category Analysis")
    
    gold_df = load_gold_data()
    if gold_df is None:
        return
    
    # Revenue per category
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Total Revenue per Category")
        revenue_by_cat = gold_df.groupby('category')['revenue'].sum().sort_values(ascending=True)
        fig_rev = px.bar(
            x=revenue_by_cat.values,
            y=revenue_by_cat.index,
            orientation='h',
            title="Total Revenue by Category",
            labels={'x': 'Revenue ($)', 'y': 'Category'},
            color_discrete_sequence=['#636EFA']
        )
        fig_rev.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig_rev, use_container_width=True)
    
    with col2:
        st.subheader("Average Price per Category")
        price_by_cat = gold_df.groupby('category')['price'].mean().sort_values(ascending=True)
        fig_price = px.bar(
            x=price_by_cat.values,
            y=price_by_cat.index,
            orientation='h',
            title="Average Price by Category",
            labels={'x': 'Average Price ($)', 'y': 'Category'},
            color_discrete_sequence=['#00CC96']
        )
        fig_price.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig_price, use_container_width=True)
    
    st.divider()
    
    # Grouped analysis: Avg Revenue vs Avg Sales
    st.subheader("Revenue vs Sales Analysis by Category")
    
    category_stats = gold_df.groupby('category').agg({
        'revenue': 'mean',
        'sales': 'mean',
        'title': 'count'
    }).rename(columns={'title': 'count'}).reset_index()
    
    fig_grouped = px.bar(
        category_stats,
        x='category',
        y=['revenue', 'sales'],
        title="Average Revenue and Sales by Category",
        labels={'value': 'Value', 'variable': 'Metric'},
        barmode='group',
        color_discrete_map={'revenue': '#636EFA', 'sales': '#00CC96'}
    )
    fig_grouped.update_layout(height=400)
    st.plotly_chart(fig_grouped, use_container_width=True)


def page_rating_analysis():
    """Rating Analysis page with rating-based visualizations"""
    st.title("⭐ Rating Analysis")
    
    gold_df = load_gold_data()
    if gold_df is None:
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Average Revenue by Rating (Text)")
        rating_revenue = gold_df.groupby('rating')['revenue'].mean().sort_values(ascending=False)
        fig_rating = px.bar(
            x=rating_revenue.index,
            y=rating_revenue.values,
            title="Average Revenue by Rating",
            labels={'x': 'Rating', 'y': 'Average Revenue ($)'},
            color_discrete_sequence=['#636EFA']
        )
        fig_rating.update_layout(height=350, showlegend=False)
        st.plotly_chart(fig_rating, use_container_width=True)
    
    with col2:
        st.subheader("Average Revenue by Rating (Numeric)")
        rating_numeric_revenue = gold_df.groupby('rating_numeric')['revenue'].mean().sort_values()
        fig_numeric = px.line(
            x=rating_numeric_revenue.index,
            y=rating_numeric_revenue.values,
            title="Average Revenue by Numeric Rating",
            labels={'x': 'Numeric Rating', 'y': 'Average Revenue ($)'},
            markers=True
        )
        fig_numeric.update_traces(line_color='#00CC96', marker_size=8)
        fig_numeric.update_layout(height=350, showlegend=False)
        st.plotly_chart(fig_numeric, use_container_width=True)
    
    st.divider()
    
    st.subheader("Rating (Numeric) vs Revenue Scatter Plot")
    fig_scatter = px.scatter(
        gold_df,
        x='rating_numeric',
        y='revenue',
        color='is_top_performer',
        title="Relationship Between Numeric Rating and Revenue",
        labels={'rating_numeric': 'Numeric Rating', 'revenue': 'Revenue ($)', 'is_top_performer': 'Top Performer'},
        color_discrete_map={True: '#EF553B', False: '#636EFA'},
        hover_data=['title', 'category']
    )
    fig_scatter.update_layout(height=450)
    st.plotly_chart(fig_scatter, use_container_width=True)


def page_outliers():
    """Outliers & Z-Scores page"""
    st.title("🎯 Outliers & Z-Scores")
    
    gold_df = load_gold_data()
    if gold_df is None:
        return
    
    st.subheader("Revenue Outlier Analysis")
    
    # Threshold slider
    col1, col2 = st.columns([3, 1])
    with col2:
        threshold = st.slider(
            "Z-Score Threshold",
            min_value=0.5,
            max_value=3.0,
            value=1.5,
            step=0.1
        )
    
    # Identify outliers
    outliers = gold_df[abs(gold_df['revenue_zscore']) > threshold].copy()
    
    # Display outlier statistics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Outliers Detected", len(outliers))
    with col2:
        st.metric("Outlier Percentage", f"{len(outliers) / len(gold_df) * 100:.1f}%")
    with col3:
        st.metric("Total Books", len(gold_df))
    
    st.divider()
    
    # Scatter plot with outlier highlighting
    fig_zscore = px.scatter(
        gold_df,
        x='revenue_zscore',
        y='revenue',
        color=abs(gold_df['revenue_zscore']) > threshold,
        title=f"Revenue vs Z-Score (Threshold: ±{threshold})",
        labels={
            'revenue_zscore': 'Z-Score',
            'revenue': 'Revenue ($)',
            'color': 'Is Outlier'
        },
        color_discrete_map={True: '#EF553B', False: '#636EFA'},
        hover_data=['title', 'category', 'revenue_zscore']
    )
    
    # Add threshold lines
    fig_zscore.add_hline(y=None, line_dash="dash", opacity=0.5)
    fig_zscore.add_vline(x=threshold, line_dash="dash", line_color="red", opacity=0.5, annotation_text=f"+{threshold}")
    fig_zscore.add_vline(x=-threshold, line_dash="dash", line_color="red", opacity=0.5, annotation_text=f"-{threshold}")
    
    fig_zscore.update_layout(height=500)
    st.plotly_chart(fig_zscore, use_container_width=True)
    
    # Display outlier details
    if len(outliers) > 0:
        st.subheader("Detected Outliers")
        display_outliers = outliers[['title', 'revenue', 'revenue_zscore', 'category', 'is_top_performer']].copy()
        display_outliers['revenue'] = display_outliers['revenue'].apply(lambda x: f"${x:,.0f}")
        display_outliers['revenue_zscore'] = display_outliers['revenue_zscore'].apply(lambda x: f"{x:.2f}")
        display_outliers['Top Performer'] = display_outliers['is_top_performer'].map({True: '⭐', False: ''})
        
        st.dataframe(
            display_outliers[['title', 'revenue', 'revenue_zscore', 'category', 'Top Performer']],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info(f"✅ No outliers detected with current threshold of ±{threshold}")


# Sidebar Navigation
with st.sidebar:
    st.title("🎛️ Navigation")
    
    page = st.radio(
        "Select Page",
        ["Overview", "Top Books", "Category Analysis", "Rating Analysis", "Outliers & Z-Scores"],
        index=0
    )
    
    st.divider()
    
    # Refresh button
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.success("✅ Cache cleared! Data will reload on next interaction.")
    
    st.divider()
    
    # Info section
    st.subheader("ℹ️ Project Info")
    st.info(
        "**Ecommerce Analytics Dashboard**\n\n"
        "Data Pipeline: Bronze → Silver → Gold\n\n"
        "Data Location: `data/gold/`"
    )

# Route to selected page
if page == "Overview":
    page_overview()
elif page == "Top Books":
    page_top_books()
elif page == "Category Analysis":
    page_category_analysis()
elif page == "Rating Analysis":
    page_rating_analysis()
elif page == "Outliers & Z-Scores":
    page_outliers()
