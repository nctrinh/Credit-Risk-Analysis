import joblib
import os
import pandas as pd
import numpy as np
import sys


import os
os.environ["PYSPARK_PYTHON"] = "/opt/conda/envs/py37/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/conda/envs/py37/bin/python"

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from preprocessor import LoanPreprocessor

# Đường dẫn model và preprocessor
model_path = os.path.join(os.path.dirname(__file__), "trained_models/logistic_model.pkl")
preprocessor_path = os.path.join(os.path.dirname(__file__), "trained_models/preprocessor.pkl")

# Load model và preprocessor một lần khi module được import
print("Loading model and preprocessor...")

try:
    model = joblib.load(model_path)
    print(f"✓ Model loaded from: {model_path}")
except Exception as e:
    print(f"✗ Error loading model: {e}")
    model = None

try:
    # Try to load saved preprocessor first
    if os.path.exists(preprocessor_path):
        preprocessor = joblib.load(preprocessor_path)
        print(f"✓ Preprocessor loaded from: {preprocessor_path}")
    else:
        # Create new preprocessor (will need to be fitted)
        preprocessor = LoanPreprocessor(do_scale=True)
        print("⚠ Warning: Using unfitted preprocessor. Results may be inaccurate.")
except Exception as e:
    print(f"✗ Error loading preprocessor: {e}")
    preprocessor = None

def predict_loan(raw_data: dict):
    """
    Predict credit risk from raw loan data
    
    Args:
        raw_data: Dictionary containing loan features
        
    Returns:
        Dictionary with prediction results
    """
    try:
        if model is None or preprocessor is None:
            raise ValueError("Model or preprocessor not loaded")
        
        # Convert dict to DataFrame
        df_row = pd.DataFrame([raw_data])
        
        # Preprocess data - returns numpy array
        try:
            X_processed = preprocessor.transform(df_row)
        except Exception as prep_error:
            print(f"Preprocessing error: {prep_error}")
            raise ValueError(f"Preprocessing failed: {prep_error}")
        
        # Check if preprocessing was successful
        if X_processed is None:
            raise ValueError("Preprocessing returned None")
        
        if isinstance(X_processed, np.ndarray):
            if X_processed.size == 0:
                raise ValueError("Preprocessing returned empty array")
        else:
            raise ValueError(f"Preprocessing returned unexpected type: {type(X_processed)}")
        
        # Make prediction
        try:
            proba = model.predict_proba(X_processed)[0][1]
            label = "Bad" if proba > 0.5 else "Good"
        except Exception as pred_error:
            print(f"Prediction error: {pred_error}")
            raise ValueError(f"Prediction failed: {pred_error}")
        
        return {
            "id": int(raw_data.get("id", 0)),
            "member_id": int(raw_data.get("member_id", 0)),
            "label": label,
            "risk_score": float(proba),
            "loan_amnt": float(raw_data.get("loan_amnt", 0)),
            "grade": str(raw_data.get("grade", "Unknown")),
            "int_rate": float(raw_data.get("int_rate", 0)),
            "annual_inc": float(raw_data.get("annual_inc", 0)),
            "status": "success"
        }
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error in predict_loan: {error_msg}")
        import traceback
        traceback.print_exc()
        
        # Return error result with same structure
        return {
            "id": int(raw_data.get("id", 0)),
            "member_id": int(raw_data.get("member_id", 0)),
            "label": None,
            "risk_score": None,
            "loan_amnt": float(raw_data.get("loan_amnt", 0)),
            "grade": str(raw_data.get("grade", "Unknown")),
            "error": error_msg,
            "status": "error"
        }

def predict_from_dict(raw_data: dict):
    """
    Alias for predict_loan for backward compatibility
    """
    return predict_loan(raw_data)