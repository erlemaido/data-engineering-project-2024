import streamlit as st
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import duckdb
from sklearn.metrics import roc_curve, auc, classification_report


conn = duckdb.connect('/data/data.duckdb')

query = """
SELECT 
    fy.*, 
    ed.emtak, ed.status, ed.type, ed.legal_form,
    fp.current_assets, fp.current_liabilities, fp.non_current_assets, 
    fp.non_current_liabilities, fp.revenue, fp.profit_loss, 
    fp.total_profit_loss, fp.labour_expense, 
    fp.cash, fp.equity, fp.issued_capital, fp.assets
FROM fy_report_dim fy
LEFT JOIN entity_dim ed ON fy.entity_id = ed.entity_id
LEFT JOIN financial_performance_fact fp ON fy.fy_report_id = fp.fy_report_id
"""

df = conn.execute(query).fetchdf()

def app():




    # Load the test set, model, and preprocessor
    with open('test_set.pkl', 'rb') as file:
        test_set = pickle.load(file)
        X_test = test_set['X_test']
        y_test = test_set['y_test']

    with open('trained_model.pkl', 'rb') as file:
        loaded_model = pickle.load(file)

    with open('preprocessor.pkl', 'rb') as file:
        loaded_preprocessor = pickle.load(file)  

    # Input fields for features
    legal_form = st.selectbox('Legal Form', X_test['legal_form'].unique())
    assets = st.number_input('Assets')
    revenue = st.number_input('Revenue')
 
    # Create a DataFrame from user input
    input_data = pd.DataFrame([[legal_form, assets, revenue]], 
                            columns=['legal_form', 'assets', 'revenue'])
    
    numerical_cols = X_test.select_dtypes(include=['number']).columns
    categorical_cols = X_test.select_dtypes(include=['object', 'category']).columns
    
    expected_columns = numerical_cols.tolist() + categorical_cols.tolist()
    for col in expected_columns:
        if col not in input_data.columns:
            input_data[col] = np.nan

    # Preprocess the input data
    input_data_processed = loaded_preprocessor.transform(input_data)

    # Make prediction
    prediction = loaded_model.predict(input_data_processed)
    prediction_proba = loaded_model.predict_proba(input_data_processed)

    # Map predictions to labels
    labels = {1: "Likely to be audited", 0: "Unlikely to be audited"}
    st.write(f"Prediction: {labels[prediction[0]]}")
    st.write(f"Prediction Probability: {prediction_proba[0][1]:.4f}")

    # --- Display Graphs and Classification Report (using loaded test set) ---

    # ROC AUC Curve
    st.subheader("ROC AUC Curve")
    X_test_processed = loaded_preprocessor.transform(X_test)
    y_pred_proba_test = loaded_model.predict_proba(X_test_processed)[:, 1]
    fpr, tpr, thresholds = roc_curve(y_test, y_pred_proba_test)
    roc_auc = auc(fpr, tpr)

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.plot(fpr, tpr, color='darkorange', lw=2, label='ROC curve (area = %0.2f)' % roc_auc)
    ax.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
    ax.set_xlim([0.0, 1.0])
    ax.set_ylim([0.0, 1.05])
    ax.set_xlabel('False Positive Rate')
    ax.set_ylabel('True Positive Rate')
    ax.set_title('Receiver Operating Characteristic')
    ax.legend(loc="lower right")
    st.pyplot(fig)

    # Feature Importance (Top 10)
    st.subheader("Feature Importance (Top 10)")
    
    try:
        feature_importance = loaded_model.feature_importances_
        feature_names_out = loaded_preprocessor.get_feature_names_out()

        # Map original features to their transformed counterparts
        original_feature_importance_map = {}
        for i, name in enumerate(feature_names_out):
            for orig_name in expected_columns:
                if orig_name in name:
                    if orig_name not in original_feature_importance_map:
                        original_feature_importance_map[orig_name] = 0
                    original_feature_importance_map[orig_name] += feature_importance[i]

        sorted_features = sorted(original_feature_importance_map.items(), key=lambda x: x[1], reverse=True)
        top_features_names, top_features_values = zip(*sorted_features[:10])

        fig, ax2 = plt.subplots(figsize=(10, 6))
        ax2.barh(top_features_names, top_features_values)
        ax2.set_xlabel('Feature Importance')
        ax2.set_ylabel('Feature Name')
        ax2.set_title('XGBoost Feature Importance (Top 10 Original Features)')
        st.pyplot(fig)

    except Exception as e:
        st.write("Error plotting feature importance:", e)

# Feature Importance (Top 10 Direct from Model)
    st.subheader("Feature Importance (Top 10 Direct from Model)")

    try:
        direct_feature_importance = loaded_model.feature_importances_
        feature_names_out = loaded_preprocessor.get_feature_names_out()

        # Sort features by importance
        sorted_idx = np.argsort(direct_feature_importance)[::-1]
        top_10_indices = sorted_idx[:10]
        top_10_features = [feature_names_out[i] for i in top_10_indices]
        top_10_values = direct_feature_importance[top_10_indices]

        fig, ax3 = plt.subplots(figsize=(10, 6))
        ax3.barh(top_10_features[::], top_10_values[::]) 
        ax3.set_xlabel('Feature Importance')
        ax3.set_ylabel('Feature Name')
        ax3.set_title('XGBoost Feature Importance (Top 10 Direct from Model)')
        st.pyplot(fig)

    except Exception as e:
        st.write("Error plotting top 10 direct feature importance:", e)

    # Classification Report as Table (Middle Aligned)
    st.subheader("Classification Report")
    y_pred_test = loaded_model.predict(X_test_processed)
    report_dict = classification_report(y_test, y_pred_test, output_dict=True)
    report_df = pd.DataFrame(report_dict).transpose()

    # Convert DataFrame to HTML with inline styles for center alignment
    html_table = report_df.to_html(classes='center', index=True)

    # Add CSS for center alignment
    st.markdown(
        """
        <style>
        .center {
            margin-left: auto;
            margin-right: auto;
            text-align: center;
        }
        .center th, .center td {
            text-align: center !important;
        }
        </style>
        """, unsafe_allow_html=True
    )
    
    # Display table
    st.markdown(html_table, unsafe_allow_html=True)