import os
import boto3
import pandas as pd
import streamlit as st
import plotly.express as px

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# ====================================
# STREAMLIT PAGE CONFIG
# ====================================
st.set_page_config(
    page_title="Lakehouse Prototype",
    layout="wide"
)

st.title("Lakehouse Medallion Architecture")

# ====================================
# SPARK SESSION (Cached)
# ====================================
@st.cache_resource
def get_spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("Lakehouse")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

spark = get_spark()

# ====================================
# MINIO CLIENT
# ====================================
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="password123"
)

buckets = ["bronze", "silver", "gold"]
for bucket in buckets:
    try:
        s3.create_bucket(Bucket=bucket)
    except:
        pass

# ====================================
# UPLOAD FILE (BRONZE)
# ====================================
uploaded_files = st.file_uploader(
    "Upload CSV Files",
    type=["csv"],
    accept_multiple_files=True
)

dataset_names = []
if uploaded_files:
    os.makedirs("uploads", exist_ok=True)
    for uploaded in uploaded_files:
        name_without_ext = uploaded.name.replace(".csv", "")
        dataset_names.append(name_without_ext)

        file_path = f"uploads/{uploaded.name}"
        with open(file_path, "wb") as f:
            f.write(uploaded.getbuffer())

        s3.upload_file(file_path, "bronze", uploaded.name)
    st.success("All datasets uploaded to Bronze Layer!")  

# ====================================
# DATASET SELECTOR
# ====================================
st.header("Dataset Selector")
selected_dataset = st.selectbox(
    "Pilih Dataset untuk Diproses:",
    dataset_names if dataset_names else ["Tidak ada file yang diunggah"]
)

# ====================================
# RUN ETL PIPELINE
# ====================================
if st.button("Run ETL Pipeline") and dataset_names and selected_dataset != "Tidak ada file yang diunggah":

    # ------------------------------------
    # 1. BRONZE LAYER (Data Mentah Apa Adanya)
    # ------------------------------------
    st.header("🟫 Bronze Layer")
    
    bronze_df = spark.read.csv(
        f"s3a://bronze/{selected_dataset}.csv",
        header=True,
        inferSchema=True
    )
    
    st.write(f"**Total Rows Mentah:** {bronze_df.count()} | **Total Columns Mentah:** {len(bronze_df.columns)}")
    st.dataframe(bronze_df.limit(1000).toPandas())

    # ------------------------------------
    # 2. SILVER LAYER (Pembersihan Kolom & Standarisasi)
    # ------------------------------------
    st.header("⬜ Silver Layer")
    with st.spinner("Cleaning data to Silver..."):
        
        silver_df = bronze_df
        # Rapiin nama kolom
        for col_name in silver_df.columns:
            clean_col = (
                col_name.strip()
                .lower()
                .replace(" ", "_")
                .replace("-", "_")
                .replace(".", "_")
                .replace("/", "_")
            )
            silver_df = silver_df.withColumnRenamed(col_name, clean_col)

        for col in silver_df.columns:
            silver_df = silver_df.withColumn(
                col, 
                F.when(F.col(col) == "NA", None)
                 .when(F.col(col) == "None", None)
                 .otherwise(F.col(col))
            )

        silver_df = silver_df.dropDuplicates()
        
        silver_path = f"s3a://silver/{selected_dataset}/"
        silver_df.write.mode("overwrite").parquet(silver_path)

    st.success("Data Silver sukses disimpan!")
    st.dataframe(silver_df.limit(1000).toPandas())

    # ------------------------------------
    # 3. GOLD LAYER (Dual-Table: Master Data & Summary Data)
    # ------------------------------------
    st.header("🟨 Gold Layer")
    with st.spinner("Memproses dua tipe tabel Gold ke MinIO..."):
        
        # TABEL Gold Full Data 
       
        playtype_col = next((c for c in silver_df.columns if "playtype" in c.lower()), None)
        
        if playtype_col:
            gold_full_df = silver_df.filter(
                (F.col(playtype_col).isNotNull()) & 
                (F.col(playtype_col) != "None") &
                (F.col(playtype_col) != "NA") &
                (F.col(playtype_col) != "-")
            )
        else:
            gold_full_df = silver_df

        gold_full_path = f"s3a://gold/{selected_dataset}/full/"
        gold_full_df.write.mode("overwrite").parquet(gold_full_path)
        
        # ssumarry gold tabel
        gold_agg_pd = None 
        gold_agg_path = f"s3a://gold/{selected_dataset}/summary/"
        
        # Deteksi otomatis kolom kategori dan angka yang tersedia di dalam data
        cols_structure = gold_full_df.dtypes
        cat_cols = [c[0] for c in cols_structure if c[1] == "string"]
        num_cols = [c[0] for c in cols_structure if c[1] in ["int", "bigint", "double", "float", "integer"]]

        
        group_col = next((c for c in cat_cols if "playtype" in c.lower()), cat_cols[0] if cat_cols else None)
    
        metric_col = next((c for c in num_cols if "yard" in c.lower() or "gain" in c.lower()), num_cols[0] if num_cols else None)

        if group_col and metric_col:
            
            gold_full_df = gold_full_df.withColumn(metric_col, F.col(metric_col).cast("double"))
            
            gold_agg_df = gold_full_df.groupBy(group_col).agg(
                F.sum(metric_col).alias(f"total_{metric_col}"),
                F.avg(metric_col).alias(f"avg_{metric_col}"),
                F.count(metric_col).alias("total_transaksi")
            )
            gold_agg_df.write.mode("overwrite").parquet(gold_agg_path)
            gold_agg_pd = gold_agg_df.toPandas()

        # baca data
        gold_pd = gold_full_df.limit(1000).toPandas()
        st.session_state["gold_pd"] = gold_pd
        
        st.success("✨ Dua model tabel Gold berhasil dibuat dan disimpan di S3 MinIO!")

    # tampil tabel
    st.subheader("📋 1. Gold Layer: Master Data (Sumber Utama Chart Builder)")
    st.caption(f"Path S3: `{gold_full_path}`")
    st.dataframe(gold_pd) 

    if gold_agg_pd is not None and not gold_agg_pd.empty:
        st.subheader("📊 2. Gold Layer: Business Summary Table (Hasil Agregasi)")
        st.caption(f"Path S3: `{gold_agg_path}`")
        st.dataframe(gold_agg_pd) # SEKARANG DIJAMIN MUNCUL KARENA DETEKSI OTOMATIS
    else:
        st.warning("⚠️ Gagal membuat Business Summary karena kolom kategori/angka tidak terdeteksi.")

# ====================================
# LOAD GOLD DATA FOR DASHBOARD
# ====================================
gold_pd = None
if "gold_pd" in st.session_state:
    gold_pd = st.session_state["gold_pd"]
else:
    try:
        # Arahkan ke subfolder /full/ agar sinkron saat halaman di-refresh
        existing_gold = spark.read.parquet(f"s3a://gold/{selected_dataset}/full/")
        gold_pd = existing_gold.limit(1000).toPandas()
        st.session_state["gold_pd"] = gold_pd
    except:
        st.info("Silakan pilih dataset, lalu klik 'Run ETL Pipeline' untuk memproses data.")
        st.stop()

# ====================================
# POWER BI STYLE CHART BUILDER (SEMUA GRAFIK LAMA BALIK)
# ====================================
if gold_pd is not None and not gold_pd.empty:
    st.markdown("---")
    st.header("📊 Interactive Chart Builder")

    # Ambil metadata kolom secara dinamis dari pandas dataframe
    all_columns = gold_pd.columns.tolist()
    numeric_columns = gold_pd.select_dtypes(include=['number']).columns.tolist()
    category_columns = gold_pd.select_dtypes(include=['object', 'category']).columns.tolist()

    if not category_columns:
        category_columns = all_columns

    chart_type = st.selectbox(
        "Chart Type",
        ["Bar Chart", "Line Chart", "Pie Chart", "Scatter Plot", "Bubble Chart", "Treemap", "Sunburst", "3D Scatter Plot"]
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        x_axis = st.selectbox("X Axis", all_columns, key="x")
    with c2:
        y_axis = st.selectbox("Y Axis", numeric_columns if numeric_columns else all_columns, key="y")
    with c3:
        color_col = st.selectbox("Color Grouping", ["None"] + category_columns, key="color")

    plot_df = gold_pd.copy()
    color_param = None if color_col == "None" else color_col

    # Batasi data untuk chart builder agar tidak tumpang tindih berantakan di layar
    plot_df_chart = plot_df.head(1000)

    # Render Grafik menggunakan Plotly
    fig = None
    if chart_type == "Bar Chart":
        fig = px.bar(plot_df_chart, x=x_axis, y=y_axis, color=color_param)
    elif chart_type == "Line Chart":
        fig = px.line(plot_df_chart, x=x_axis, y=y_axis, color=color_param)
    elif chart_type == "Pie Chart":
        fig = px.pie(plot_df_chart, names=x_axis, values=y_axis)
    elif chart_type == "Scatter Plot":
        fig = px.scatter(plot_df_chart, x=x_axis, y=y_axis, color=color_param)
    elif chart_type == "Bubble Chart":
        fig = px.scatter(plot_df_chart, x=x_axis, y=y_axis, size=y_axis, color=color_param)
    elif chart_type == "Treemap":
        fig = px.treemap(plot_df_chart, path=[x_axis], values=y_axis)
    elif chart_type == "Sunburst":
        fig = px.sunburst(plot_df_chart, path=[x_axis], values=y_axis)
    elif chart_type == "3D Scatter Plot":
        z_axis = st.selectbox("Z Axis", numeric_columns if numeric_columns else all_columns, key="z")
        fig = px.scatter_3d(plot_df_chart, x=x_axis, y=y_axis, z=z_axis, color=color_param)

    if fig:
        st.plotly_chart(fig, use_container_width=True)

    # ====================================
    # KPI CARDS
    # ====================================
    st.header("📌 Quick Summary Matriks")
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        st.metric("Rows", plot_df.shape[0])
    with kpi2:
        st.metric("Columns", plot_df.shape[1])
    with kpi3:
        if numeric_columns:
            st.metric("Mean (Y)", round(plot_df[y_axis].mean(), 2))
    with kpi4:
        if numeric_columns:
            st.metric("Max (Y)", round(plot_df[y_axis].max(), 2))