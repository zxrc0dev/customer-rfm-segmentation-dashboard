PRE_WRANGLING_SCHEMA = {
    "expected_columns": {
        "Churn",
        "Contract",
        "customerID",
        "Dependents",
        "DeviceProtection",
        "gender",
        "InternetService",
        "MonthlyCharges",
        "MultipleLines",
        "OnlineBackup",
        "OnlineSecurity",
        "PaperlessBilling",
        "Partner",
        "PaymentMethod",
        "PhoneService",
        "SeniorCitizen",
        "StreamingMovies",
        "StreamingTV",
        "TechSupport",
        "tenure",
        "TotalCharges",
    },
    "min_rows": 7000,
}

POST_WRANGLING_SCHEMA = {
    "expected_columns": {
        "churn",
        "contract",
        "dependents",
        "device_protection",
        "gender",
        "internet_service",
        "monthly_charges",
        "multiple_lines",
        "online_backup",
        "online_security",
        "paperless_billing",
        "partner",
        "payment_method",
        "phone_service",
        "senior_citizen",
        "streaming_movies",
        "streaming_tv",
        "tech_support",
        "tenure",
        "total_charges",
    },
    "min_rows": 5000
}

POST_FEATURE_ENGINEERING_SCHEMA = {
    "expected_columns": {
        "churn",
        "contract_stability",      # Engineered
        "dependents",
        "device_protection",
        "fiber_no_support",        # Engineered
        "high_risk_new_monthly",   # Engineered
        "high_risk_tenure",        # Engineered
        "internet_service",
        "manual_payment_early",    # Engineered
        "monthly_charges",
        "multiple_lines",
        "online_backup",
        "online_security",
        "paperless_billing",
        "partner",
        "payment_method",
        "senior_citizen",
        "streaming_movies",
        "streaming_tv",
        "tech_support",
        "tenure",
    },
    "min_rows": 5000
}

MODEL_INPUT_SCHEMA = {
    "expected_columns": {
        "contract_stability",      # Engineered
        "dependents",
        "device_protection",
        "fiber_no_support",        # Engineered
        "high_risk_new_monthly",   # Engineered
        "high_risk_tenure",        # Engineered
        "internet_service",
        "manual_payment_early",    # Engineered
        "monthly_charges",
        "multiple_lines",
        "online_backup",
        "online_security",
        "paperless_billing",
        "partner",
        "payment_method",
        "senior_citizen",
        "streaming_movies",
        "streaming_tv",
        "tech_support",
        "tenure",
    },
    "min_rows": 0
}