import streamlit as st
import duckdb
import pandas as pd
import matplotlib.pyplot as plt

try:
    conn = duckdb.connect('/data/data.duckdb')

    tax_query = """
    SELECT
    tf.entity_id,
    dd.full_date,
    SUM(tf.state_taxes) AS total_state_taxes,
    SUM(tf.labour_taxes) AS total_labour_taxes,
    SUM(tf.revenue) AS total_revenue,
    SUM(tf.nr_employees) AS total_employees,
    ed.type,
    UPPER(ed.emtak) AS EMTAK
FROM tax_fact AS tf
JOIN entity_dim AS ed ON tf.entity_id = ed.entity_id
JOIN date_dim AS dd ON tf.date_id = dd.date_id
GROUP BY
    tf.entity_id,
    dd.full_date,
    dd.fiscal_year, 
    ed.type,
    ed.emtak;
"""
    tax_result_df = conn.execute(tax_query).df()
    tax_result_df['full_date'] = pd.to_datetime(tax_result_df['full_date'])

    conn.close()
    data_loaded = True

except Exception as e:
    st.error(f"Error loading data: {e}")
    data_loaded = False
    tax_result_df = pd.DataFrame()

def app():
    if data_loaded:
        st.title("Tax Data Visualizations")

        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            min_date = tax_result_df['full_date'].min().date()
            max_date = tax_result_df['full_date'].max().date()
            selected_dates = st.slider("Select Date Range", min_value=min_date, max_value=max_date, value=(min_date, max_date))
            selected_start_date = pd.Timestamp(selected_dates[0])
            selected_end_date = pd.Timestamp(selected_dates[1])
        
        with col2:
            type_options = ["Select All"] + list(tax_result_df['type'].unique())
            selected_type = st.selectbox("Select Type", options=type_options)
        
        with col3:
            emtak_options = ["Select All"] + list(tax_result_df['EMTAK'].unique())
            selected_emtak = st.selectbox("Select EMTAK", options=emtak_options)

        # Apply filters
        filtered_df = tax_result_df[(tax_result_df['full_date'] >= selected_start_date) & (tax_result_df['full_date'] <= selected_end_date)]
        
        if selected_type != "Select All":
            filtered_df = filtered_df[filtered_df['type'] == selected_type]
        
        if selected_emtak != "Select All":
            filtered_df = filtered_df[filtered_df['EMTAK'] == selected_emtak]

        # Metrics and visualizations
        col1, col2 = st.columns(2)
        
        with col1:
            num_unique_entities = filtered_df['entity_id'].nunique() if not filtered_df.empty else 0 
            st.metric("Unique Entities", num_unique_entities)
        
        with col2:
            avg_employees = filtered_df['total_employees'].mean() if not filtered_df.empty else 0 
            st.metric("Average Number of Employees", round(avg_employees, 2))

        if not filtered_df.empty:
            numeric_cols = ['total_state_taxes', 'total_labour_taxes', 'total_revenue', 'total_employees']
            grouped_data = filtered_df.groupby('full_date')[numeric_cols].mean().reset_index()
            smoothed_data = grouped_data
            smoothed_data[numeric_cols] = smoothed_data[numeric_cols].round(0)

            st.subheader("Average State Taxes and Labour Taxes Over Time")
            taxes_chart_data = smoothed_data[['full_date', 'total_state_taxes', 'total_labour_taxes']].rename(columns={'total_state_taxes': 'Avg State Taxes', 'total_labour_taxes': 'Avg Labour Taxes'})
            st.line_chart(taxes_chart_data.set_index('full_date'))

            st.subheader("Average Revenue Over Time")
            revenue_chart_data = smoothed_data[['full_date', 'total_revenue']].rename(columns={'total_revenue': 'Avg Revenue'})
            st.line_chart(revenue_chart_data.set_index('full_date'))

            st.subheader("EMTAK Distribution")
            emtak_counts = filtered_df['EMTAK'].value_counts()
            fig, ax1 = plt.subplots(figsize=(6, 6))

            def autopct_format(pct):
                return f'{pct:.1f}%' if pct > 5 else ''

            ax1.pie(emtak_counts, autopct=autopct_format, labels=emtak_counts.index)
            st.pyplot(fig)

            st.subheader("Type Distribution")
            type_counts = filtered_df['type'].value_counts()
            fig, ax2 = plt.subplots(figsize=(6, 6))

            def autopct_format(pct):
                return f'{pct:.1f}%' if pct > 5 else ''

            ax2.pie(type_counts, autopct=autopct_format, labels=type_counts.index)
            st.pyplot(fig)

        else:
            st.write("No data available for the selected filters.")
    else:
        st.write("Data loading failed. Check the error message above.")