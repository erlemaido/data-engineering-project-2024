import streamlit as st
import zipfile
import os
import io

def app():
    # Initialize session state for selected option
    if "selected_option" not in st.session_state:
        st.session_state.selected_option = "Raw data"

    # Layout for Data Export page
    col1, col2, col3 = st.columns([1, 2, 1])  # Adjust column widths as needed

    with col1:
        st.subheader("Options")
        # Custom mutually exclusive buttons
        if st.button("Raw data", key="raw_data"):
            st.session_state.selected_option = "Raw data"
        if st.button("Parquet", key="parquet_btn"):
            st.session_state.selected_option = "Parquet"
        if st.button("Aggregated data", key="aggregated_data"):
            st.session_state.selected_option = "Aggregated data"

    with col2:
        st.markdown("<h3 style='text-align: center;'>Files</h3>", unsafe_allow_html=True)

        st.write(f"Selected: **{st.session_state.selected_option}**")

        if st.session_state.selected_option == "Raw data":
            archive_path = "/data/archive"
            try:
                raw_data_options = [
                    folder for folder in os.listdir(archive_path) 
                    if os.path.isdir(os.path.join(archive_path, folder))
                ]
            except FileNotFoundError:
                raw_data_options = []

            selected_raw_data_options = st.multiselect(
                "Select Raw Data Types",
                raw_data_options,
                key="raw_data_multiselect"
            )

            if selected_raw_data_options:
                all_files = []
                for folder in selected_raw_data_options:
                    folder_path = os.path.join(archive_path, folder)
                    all_files.extend([
                        f for f in os.listdir(folder_path) 
                        if os.path.isfile(os.path.join(folder_path, f))
                    ])

                select_all = st.checkbox("Select All Files", key="select_all_files")

                if select_all:
                    selected_files = st.multiselect(
                        "Select Files", all_files, default=all_files, key="files_multiselect"
                    )
                else:
                    selected_files = st.multiselect(
                        "Select Files", all_files, key="files_multiselect"
                    )

        elif st.session_state.selected_option == "Parquet":
            data_folder = "/data"
            try:
                parquet_files = [
                    f for f in os.listdir(data_folder) 
                    if f.endswith(".parquet") and os.path.isfile(os.path.join(data_folder, f))
                ]
            except FileNotFoundError:
                parquet_files = []

            select_all_parquet = st.checkbox("Select All Parquet Files", key="select_all_parquet")

            if select_all_parquet:
                selected_parquet_files = st.multiselect(
                    "Select Parquet Files", parquet_files, default=parquet_files, key="parquet_multiselect"
                )
            else:
                selected_parquet_files = st.multiselect(
                    "Select Parquet Files", parquet_files, key="parquet_multiselect"
                )

        elif st.session_state.selected_option == "Aggregated data":
            # Display placeholder message for aggregated data
            st.write("Feature pending implementation")

    with col3:
        st.subheader("Download")

        if (
            st.session_state.selected_option == "Raw data" and 
            'files_multiselect' in st.session_state and 
            st.session_state.files_multiselect
        ):
            selected_files = st.session_state.files_multiselect
            
            # Create a ZIP file in memory with selected files
            archive_path = "/data/archive"
            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, "w") as zf:
                for file_name in selected_files:
                    # Find the folder containing each file
                    for folder in st.session_state.raw_data_multiselect:
                        file_path = os.path.join(archive_path, folder, file_name)
                        if os.path.exists(file_path):
                            zf.write(file_path, arcname=file_name)  # Add to ZIP
            
            zip_buffer.seek(0)  # Move cursor to the start of the buffer

            # Replace the original button with the download button
            st.download_button(
                label="Download",
                data=zip_buffer,
                file_name="selected_files.zip",
                mime="application/zip"
            )

        elif (
            st.session_state.selected_option == "Parquet" and 
            'parquet_multiselect' in st.session_state and 
            st.session_state.parquet_multiselect
        ):
            selected_parquet_files = st.session_state.parquet_multiselect
            
            data_folder = "/data"
            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, "w") as zf:
                for file_name in selected_parquet_files:
                    file_path = os.path.join(data_folder, file_name)
                    if os.path.exists(file_path):
                        zf.write(file_path, arcname=file_name) 
            
            zip_buffer.seek(0)  

            st.download_button(
                label="Download",
                data=zip_buffer,
                file_name="selected_parquet_files.zip",
                mime="application/zip"
            )

        else:
            st.write("No files selected.")