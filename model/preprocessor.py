import os
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import StandardScaler
import joblib

class LoanPreprocessor(BaseEstimator, TransformerMixin):
    def __init__(self, do_scale=True):
        self.do_scale = do_scale
        self.scaler = StandardScaler() if do_scale else None
        self.feature_names_ = None
        
        # Define column groups (match với Spark code)
        self.categorical_cols = [
            'initial_list_status_w', 'is_60_months', 
            'purpose_debt_consolidation', 'purpose_credit_card',
            'purpose_home_improvement', 'purpose_major_purchase', 
            'purpose_small_business', 'purpose_car', 'purpose_medical', 
            'purpose_other', 'is_mortgage', 'is_rent', 'is_own', 
            'is_source_verified', 'is_verified', 'is_not_verified', 
            'emp_length_num'
        ]
        self.unskewed_cols = ['loan_amnt']
        self.skewed_cols = None  # Will be computed during fit
        self.has_neg_cols = [
            'mths_since_last_delinq', 
            'mths_since_last_record', 
            'mths_since_last_major_derog'
        ]

    def fit(self, X, y=None):
        X_ = self._to_df(X)
        X_proc = self._apply_rules(X_.copy(), fit_mode=True)

        # Determine skewed columns (all numeric except categorical and unskewed)
        all_numeric = X_proc.select_dtypes(include=[np.number]).columns.tolist()
        self.skewed_cols = [
            c for c in all_numeric 
            if c not in self.categorical_cols and c not in self.unskewed_cols
        ]
        
        # Apply log transform to skewed columns
        X_transformed = X_proc.copy()
        for c in self.skewed_cols:
            if c in X_transformed.columns:
                X_transformed[c] = np.log1p(X_transformed[c])
        
        # Scale numeric columns (skewed + unskewed, excluding categorical)
        if self.do_scale:
            scale_cols = self.skewed_cols + self.unskewed_cols
            scale_cols = [c for c in scale_cols if c in X_transformed.columns]
            if len(scale_cols) > 0:
                self.scaler.fit(X_transformed[scale_cols].values)
        
        # Set feature names: scaled numeric cols first, then categorical
        scale_cols = self.skewed_cols + self.unskewed_cols
        scale_cols = [c for c in scale_cols if c in X_proc.columns]
        cat_cols = [c for c in self.categorical_cols if c in X_proc.columns]
        self.feature_names_ = scale_cols + cat_cols
        
        return self

    def transform(self, X):
        X_ = self._to_df(X)
        X_proc = self._apply_rules(X_.copy(), fit_mode=False)
        
        # Apply log transform to skewed columns
        X_transformed = X_proc.copy()
        for c in self.skewed_cols:
            if c in X_transformed.columns:
                X_transformed[c] = np.log1p(X_transformed[c])
        
        # Scale numeric columns
        if self.do_scale and self.scaler is not None:
            scale_cols = self.skewed_cols + self.unskewed_cols
            scale_cols = [c for c in scale_cols if c in X_transformed.columns]
            if len(scale_cols) > 0:
                X_transformed[scale_cols] = self.scaler.transform(X_transformed[scale_cols].values)
        
        # Ensure column order matches fitted order
        if self.feature_names_ is not None:
            # Add missing columns as zeros
            for c in self.feature_names_:
                if c not in X_transformed.columns:
                    X_transformed[c] = 0
            # Reorder to match training
            X_transformed = X_transformed[self.feature_names_]
        
        return X_transformed.values

    def _to_df(self, X):
        if isinstance(X, pd.DataFrame):
            return X.copy()
        else:
            return pd.DataFrame(X)

    def _apply_rules(self, df, fit_mode=False):
        df.replace("", np.nan, inplace=True)
        cols_to_drop = [
            "id",
            "member_id",
            "dti",
            "policy_code",
            "annual_inc_joint",
            "dti_joint",
            "acc_now_delinq",
            "INDIVIDUAL",
            "JOINT"
        ]
        for c in cols_to_drop:
            if c in df.columns:
                df.drop(columns=[c], inplace=True)

        cols_to_fill = [
            "tot_coll_amt", "tot_cur_bal",
            "open_acc_6m", "open_il_6m", "open_il_12m", "open_il_24m",
            "mths_since_rcnt_il", "total_bal_il", "il_util", "open_rv_12m",
            "open_rv_24m", "max_bal_bc", "all_util", "total_rev_hi_lim",
            "inq_fi", "total_cu_tl", "inq_last_12m"
        ]
        for c in cols_to_fill:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0).astype(float)

        # Fix inq_last_12m < 0
        if "inq_last_12m" in df.columns:
            df["inq_last_12m"] = pd.to_numeric(df["inq_last_12m"], errors='coerce').fillna(0.0)
            df.loc[df["inq_last_12m"] < 0, "inq_last_12m"] = 0.0
            df["inq_last_12m"] = df["inq_last_12m"].astype(float)

        if "verification_status_joint" in df.columns:
            df["verification_status_joint"] = df["verification_status_joint"].fillna("Not joint")

        # desc and emp_title
        if "desc" in df.columns:
            df["desc"] = df["desc"].fillna("No description")
        if "emp_title" in df.columns:
            df["emp_title"] = df["emp_title"].fillna("Unknown")

        # mths_since_* fill -1
        for c in ["mths_since_last_record", "mths_since_last_major_derog", "mths_since_last_delinq"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(-1)

        # Payment dates
        if "last_pymnt_d" in df.columns:
            df["last_pymnt_d"] = df["last_pymnt_d"].fillna("No payment yet")
        if "next_pymnt_d" in df.columns:
            df["next_pymnt_d"] = df["next_pymnt_d"].fillna("No future payment")

        # emp_length
        if "emp_length" in df.columns:
            mapping = {
                "< 1 year": 0.5, "1 year": 1.0, "2 years": 2.0, "3 years": 3.0,
                "4 years": 4.0, "5 years": 5.0, "6 years": 6.0, "7 years": 7.0,
                "8 years": 8.0, "9 years": 9.0, "10+ years": 10.0
            }
            df["emp_length_num"] = df["emp_length"].map(mapping).fillna(0.0)

        # home_ownership flags
        if "home_ownership" in df.columns:
            df["is_mortgage"] = (df["home_ownership"] == "MORTGAGE").astype(int)
            df["is_rent"] = (df["home_ownership"] == "RENT").astype(int)
            df["is_own"] = (df["home_ownership"] == "OWN").astype(int)

        # verification_status flags
        if "verification_status" in df.columns:
            df["is_source_verified"] = (df["verification_status"] == "Source Verified").astype(int)
            df["is_verified"] = (df["verification_status"] == "Verified").astype(int)
            df["is_not_verified"] = (df["verification_status"] == "Not Verified").astype(int)

        # term flags
        if "term" in df.columns:
            df["is_36_months"] = (df["term"] == "36 months").astype(int)
            df["is_60_months"] = (df["term"] == "60 months").astype(int)

        # sub_grade numeric
        if "sub_grade" in df.columns:
            grade_order = [
                "A1","A2","A3","A4","A5",
                "B1","B2","B3","B4","B5",
                "C1","C2","C3","C4","C5",
                "D1","D2","D3","D4","D5",
                "E1","E2","E3","E4","E5",
                "F1","F2","F3","F4","F5",
                "G1","G2","G3","G4","G5"
            ]
            mapping = {g: i+1 for i, g in enumerate(grade_order)}
            df["sub_grade_num"] = df["sub_grade"].map(mapping).astype('float').where(df["sub_grade"].notna(), np.nan)

        # purpose flags
        if "purpose" in df.columns:
            purposes = [
                "debt_consolidation", "credit_card", "home_improvement",
                "major_purchase", "small_business", "car", "medical", "other"
            ]
            for p in purposes:
                df[f"purpose_{p}"] = (df["purpose"] == p).astype(int)

        # initial_list_status flags
        if "initial_list_status" in df.columns:
            df["initial_list_status_f"] = (df["initial_list_status"] == "f").astype(int)
            df["initial_list_status_w"] = (df["initial_list_status"] == "w").astype(int)

        shift_cols = ['mths_since_last_delinq', 'mths_since_last_record', 'mths_since_last_major_derog']
        for c in shift_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(-1)
                df[c] = df[c] + 1  # shift: -1 -> 0

        removed_cols = [
            "initial_list_status_f",
            "is_36_months",
            "out_prncp_inv",
            "funded_amnt",
            "funded_amnt_inv",
            "total_pymnt_inv",
            "total_rec_prncp",
            "installment",
            "open_il_6m",
            "revol_bal",
            "max_bal_bc",
            "open_il_12m",
            "open_rv_12m",
            "sub_grade_num",
            "collection_recovery_fee",
            "il_util"
        ]
        for c in removed_cols:
            if c in df.columns:
                df.drop(columns=[c], inplace=True)

        num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        for c in num_cols:
            df[c] = pd.to_numeric(df[c], errors='coerce')
            valid_mask = df[c].notna() & (df[c] != -1)
            median = df.loc[valid_mask, c].median() if valid_mask.any() else 0.0
            df[c] = df[c].fillna(median)

        # Fill remaining object columns
        obj_cols = df.select_dtypes(include=['object']).columns.tolist()
        for c in obj_cols:
            df[c] = df[c].fillna("missing")

        return df


# Utility to create and save preprocessor
def create_and_save_preprocessor(train_df, path="preprocessor.pkl", do_scale=True):
    proc = LoanPreprocessor(do_scale=do_scale)
    proc.fit(train_df)
    joblib.dump(proc, path)

def load_preprocessor(path="preprocessor.pkl"):
    """Load saved preprocessor from disk"""
    return joblib.load(path)

if __name__ == "__main__":
    csv_path = '/home/jovyan/work/data/loan.csv'
    output_path = "/home/jovyan/work/model/preprocessor.pkl"
    if os.path.exists(csv_path):
        try:
            print(f"Đang đọc dữ liệu từ {csv_path}...")
            df = pd.read_csv(csv_path)
            create_and_save_preprocessor(df, path=output_path, do_scale=True)

        except Exception as e:
            print(f"Error: {e}")
    else:
        print(f"Error: Cann't find '{csv_path}'.")