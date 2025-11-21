"""
chart_utils.py
Các hàm để tính toán dữ liệu biểu đồ dựa trên Spark/Pandas DataFrame
"""

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, round as spark_round


def prepare_dataframe(df):
    """
    Chuyển đổi Spark DataFrame sang Pandas nếu cần
    
    Input:
        df: Spark DataFrame hoặc Pandas DataFrame
    Output:
        pdf: Pandas DataFrame
    """
    if hasattr(df, "toPandas"):
        return df.toPandas()
    return df


def plot_annual_income_hist(df, column="annual_inc", max_income=300000, bins=50):
    """
    Chức năng: 
    - Tính phân phối thu nhập hàng năm (annual_inc)
    - Trả về dữ liệu dạng dictionary để vẽ biểu đồ frontend

    Input:
    df : Spark DataFrame hoặc Pandas DataFrame
        DataFrame chứa cột thu nhập hàng năm.
    column : str, optional
        Tên cột thu nhập trong DataFrame (mặc định: "annual_inc").
    max_income : int, optional
        Ngưỡng lọc giá trị thu nhập tối đa để loại bỏ outliers (mặc định: 300000).
    bins : int, optional
        Số lượng bins cho histogram (mặc định: 50).

    Output:
    dict
        {
            "labels": [list các khoảng thu nhập],
            "data": [list số lượng người trong mỗi khoảng]
        }
    """
    try:
        pdf = prepare_dataframe(df)
        
        # Lọc dữ liệu
        pdf_filtered = pdf[[column]].dropna()
        pdf_filtered = pdf_filtered[pdf_filtered[column] < max_income]
        
        if len(pdf_filtered) == 0:
            return {"labels": [], "data": []}
        
        # Tạo histogram
        hist, bin_edges = np.histogram(pdf_filtered[column], bins=bins)
        
        # Tạo labels cho mỗi bin
        labels = [
            f"${int(bin_edges[i]):,}-${int(bin_edges[i+1]):,}" 
            for i in range(len(bin_edges)-1)
        ]
        
        return {
            "labels": labels,
            "data": [int(h) for h in hist]
        }
    except Exception as e:
        print(f"Error in plot_annual_income_hist: {e}")
        return {"labels": [], "data": []}


def hist_with_numbers(series, title="Histogram", xlabel="Value", bins=50):
    """
    Chức năng:
    - Tính phân phối của một cột dữ liệu số
    - Trả về dữ liệu dạng dictionary để vẽ biểu đồ frontend

    Input:
    series : pandas.Series hoặc list
        Cột dữ liệu số cần vẽ histogram.
    title : str, optional
        Tiêu đề cho biểu đồ (mặc định: "Histogram").
    xlabel : str, optional
        Nhãn trục X (mặc định: "Value").
    bins : int, optional
        Số lượng bins (mặc định: 50).

    Output:
    dict
        {
            "title": title,
            "xlabel": xlabel,
            "labels": [list các khoảng],
            "data": [list số lượng]
        }
    """
    try:
        # Chuyển sang Series nếu cần
        if isinstance(series, (list, np.ndarray)):
            series = pd.Series(series)
        
        # Loại bỏ NaN
        series = series.dropna()
        
        if len(series) == 0:
            return {
                "title": title,
                "xlabel": xlabel,
                "labels": [],
                "data": []
            }
        
        # Tạo histogram
        hist, bin_edges = np.histogram(series, bins=bins)
        
        # Tạo labels
        labels = [
            f"{bin_edges[i]:.1f}-{bin_edges[i+1]:.1f}" 
            for i in range(len(bin_edges)-1)
        ]
        
        return {
            "title": title,
            "xlabel": xlabel,
            "labels": labels,
            "data": [int(h) for h in hist]
        }
    except Exception as e:
        print(f"Error in hist_with_numbers: {e}")
        return {
            "title": title,
            "xlabel": xlabel,
            "labels": [],
            "data": []
        }


def plot_loan_amount_distribution(df, column="loan_amnt", bins=20):
    """
    Chức năng:
    - Tính phân phối số tiền vay (loan_amnt)
    - Trả về dữ liệu cho biểu đồ frontend

    Input:
    df : Spark DataFrame hoặc Pandas DataFrame
        DataFrame chứa cột loan_amnt
    column : str
        Tên cột số tiền vay (mặc định: "loan_amnt")
    bins : int
        Số lượng bins (mặc định: 20)

    Output:
    dict
        {
            "labels": [khoảng tiền],
            "data": [số lượng]
        }
    """
    try:
        pdf = prepare_dataframe(df)
        
        # Lọc dữ liệu hợp lệ
        values = pdf[[column]].dropna()
        values = values[values[column] > 0]
        
        if len(values) == 0:
            return {"labels": [], "data": []}
        
        # Tạo histogram
        hist, bin_edges = np.histogram(values[column], bins=bins)
        
        # Tạo labels
        labels = [
            f"${int(bin_edges[i]):,}-${int(bin_edges[i+1]):,}" 
            for i in range(len(bin_edges)-1)
        ]
        
        return {
            "labels": labels,
            "data": [int(h) for h in hist]
        }
    except Exception as e:
        print(f"Error in plot_loan_amount_distribution: {e}")
        return {"labels": [], "data": []}


def plot_repayment_efficiency(df):
    """
    Tính hiệu quả hoàn trả (repayment_efficiency) và trả về dữ liệu phân phối.

    Chức năng:
    - Tính repayment_efficiency = total_rec_prncp / total_pymnt
    - Chia thành các khoảng theo bước 0.1
    - Đếm số lượng khoản vay theo từng khoảng hiệu quả

    Input:
    df : Spark DataFrame hoặc Pandas DataFrame
        DataFrame chứa các cột:
        - total_rec_prncp : tổng số tiền gốc đã thu hồi
        - total_pymnt     : tổng số tiền đã thanh toán

    Output:
    dict
        {
            "labels": [khoảng hiệu quả],
            "data": [số lượng khoản vay]
        }
    """
    try:
        pdf = prepare_dataframe(df)
        
        # Tính repayment_efficiency
        pdf_copy = pdf.copy()
        pdf_copy['total_pymnt'] = pdf_copy['total_pymnt'].replace(0, np.nan)
        pdf_copy = pdf_copy.dropna(subset=['total_pymnt', 'total_rec_prncp'])
        
        if len(pdf_copy) == 0:
            return {"labels": [], "data": []}
        
        pdf_copy['repayment_efficiency'] = (
            pdf_copy['total_rec_prncp'] / pdf_copy['total_pymnt']
        ).round(1)
        
        # Đếm theo efficiency range
        efficiency_counts = pdf_copy['repayment_efficiency'].value_counts().sort_index()
        
        labels = [str(eff) for eff in efficiency_counts.index]
        data = efficiency_counts.values.tolist()
        
        return {
            "labels": labels,
            "data": data
        }
    except Exception as e:
        print(f"Error in plot_repayment_efficiency: {e}")
        return {"labels": [], "data": []}


def plot_grade_distribution(df, column="grade"):
    """
    Chức năng:
    - Tính phân phối grade khoản vay (A, B, C, D, E, F, G)

    Input:
    df : Spark DataFrame hoặc Pandas DataFrame
        DataFrame chứa cột grade
    column : str
        Tên cột grade (mặc định: "grade")

    Output:
    dict
        {
            "labels": [grade values],
            "data": [số lượng]
        }
    """
    try:
        pdf = prepare_dataframe(df)
        
        # Đếm theo grade
        grade_counts = pdf[column].value_counts().sort_index()
        
        labels = grade_counts.index.tolist()
        data = grade_counts.values.tolist()
        
        return {
            "labels": labels,
            "data": data
        }
    except Exception as e:
        print(f"Error in plot_grade_distribution: {e}")
        return {"labels": [], "data": []}


def plot_risk_score_distribution(df, column="risk_score", bins=10):
    """
    Chức năng:
    - Tính phân phối điểm rủi ro (risk_score từ 0 đến 1)

    Input:
    df : Spark DataFrame hoặc Pandas DataFrame
        DataFrame chứa cột risk_score
    column : str
        Tên cột risk_score (mặc định: "risk_score")
    bins : int
        Số lượng bins (mặc định: 10)

    Output:
    dict
        {
            "labels": [khoảng risk],
            "data": [số lượng]
        }
    """
    try:
        pdf = prepare_dataframe(df)
        
        # Lọc dữ liệu hợp lệ
        values = pdf[[column]].dropna()
        values = values[(values[column] >= 0) & (values[column] <= 1)]
        
        if len(values) == 0:
            return {"labels": [], "data": []}
        
        # Tạo histogram với range 0-1
        hist, bin_edges = np.histogram(values[column], bins=bins, range=(0, 1))
        
        # Tạo labels
        labels = [
            f"{bin_edges[i]:.2f}-{bin_edges[i+1]:.2f}" 
            for i in range(len(bin_edges)-1)
        ]
        
        return {
            "labels": labels,
            "data": [int(h) for h in hist]
        }
    except Exception as e:
        print(f"Error in plot_risk_score_distribution: {e}")
        return {"labels": [], "data": []}


def plot_status_distribution(df, column="loan_status"):
    """
    Chức năng:
    - Tính phân phối trạng thái khoản vay

    Input:
    df : Spark DataFrame hoặc Pandas DataFrame
        DataFrame chứa cột loan_status
    column : str
        Tên cột trạng thái (mặc định: "loan_status")

    Output:
    dict
        {
            "labels": [trạng thái],
            "data": [số lượng]
        }
    """
    try:
        pdf = prepare_dataframe(df)
        
        # Đếm theo status
        status_counts = pdf[column].value_counts()
        
        # Sắp xếp giảm dần
        status_counts = status_counts.sort_values(ascending=False)
        
        labels = status_counts.index.tolist()
        data = status_counts.values.tolist()
        
        return {
            "labels": labels,
            "data": data
        }
    except Exception as e:
        print(f"Error in plot_status_distribution: {e}")
        return {"labels": [], "data": []}


def plot_label_distribution(df, column="label"):
    """
    Chức năng:
    - Tính phân phối nhãn (Good/Bad)

    Input:
    df : Spark DataFrame hoặc Pandas DataFrame
        DataFrame chứa cột label
    column : str
        Tên cột label (mặc định: "label")

    Output:
    dict
        {
            "labels": ["Good", "Bad"],
            "data": [số lượng Good, số lượng Bad],
            "colors": ["#10b981", "#ef4444"]
        }
    """
    try:
        pdf = prepare_dataframe(df)
        
        # Đếm theo label
        label_counts = pdf[column].value_counts()
        
        labels = []
        data = []
        colors = []
        
        # Đảm bảo thứ tự: Good trước, Bad sau
        if "Good" in label_counts.index:
            labels.append("Good")
            data.append(int(label_counts["Good"]))
            colors.append("#10b981")
        
        if "Bad" in label_counts.index:
            labels.append("Bad")
            data.append(int(label_counts["Bad"]))
            colors.append("#ef4444")
        
        return {
            "labels": labels,
            "data": data,
            "colors": colors
        }
    except Exception as e:
        print(f"Error in plot_label_distribution: {e}")
        return {
            "labels": [],
            "data": [],
            "colors": []
        }


def plot_purpose_distribution(df, column="purpose", top_n=10):
    """
    Chức năng:
    - Tính phân phối mục đích vay (top N)

    Input:
    df : Spark DataFrame hoặc Pandas DataFrame
        DataFrame chứa cột purpose
    column : str
        Tên cột mục đích (mặc định: "purpose")
    top_n : int
        Chỉ lấy top N mục đích (mặc định: 10)

    Output:
    dict
        {
            "labels": [các mục đích vay],
            "data": [số lượng]
        }
    """
    try:
        pdf = prepare_dataframe(df)
        
        # Đếm theo purpose
        purpose_counts = pdf[column].value_counts().head(top_n)
        
        labels = purpose_counts.index.tolist()
        data = purpose_counts.values.tolist()
        
        return {
            "labels": labels,
            "data": data
        }
    except Exception as e:
        print(f"Error in plot_purpose_distribution: {e}")
        return {"labels": [], "data": []}


def plot_interest_rate_distribution(df, column="int_rate", bins=20):
    """
    Chức năng:
    - Tính phân phối lãi suất (interest rate)

    Input:
    df : Spark DataFrame hoặc Pandas DataFrame
        DataFrame chứa cột int_rate
    column : str
        Tên cột lãi suất (mặc định: "int_rate")
    bins : int
        Số lượng bins (mặc định: 20)

    Output:
    dict
        {
            "labels": [khoảng lãi suất],
            "data": [số lượng]
        }
    """
    try:
        pdf = prepare_dataframe(df)
        
        # Lọc dữ liệu hợp lệ
        values = pdf[[column]].dropna()
        
        if len(values) == 0:
            return {"labels": [], "data": []}
        
        # Tạo histogram
        hist, bin_edges = np.histogram(values[column], bins=bins)
        
        # Tạo labels
        labels = [
            f"{bin_edges[i]:.1f}%-{bin_edges[i+1]:.1f}%" 
            for i in range(len(bin_edges)-1)
        ]
        
        return {
            "labels": labels,
            "data": [int(h) for h in hist]
        }
    except Exception as e:
        print(f"Error in plot_interest_rate_distribution: {e}")
        return {"labels": [], "data": []}


# Test functions (chạy khi import trực tiếp)
if __name__ == "__main__":
    print("chart_utils.py loaded successfully!")
    print("Available functions:")
    print("  - plot_annual_income_hist()")
    print("  - hist_with_numbers()")
    print("  - plot_loan_amount_distribution()")
    print("  - plot_repayment_efficiency()")
    print("  - plot_grade_distribution()")
    print("  - plot_risk_score_distribution()")
    print("  - plot_status_distribution()")
    print("  - plot_label_distribution()")
    print("  - plot_purpose_distribution()")
    print("  - plot_interest_rate_distribution()")