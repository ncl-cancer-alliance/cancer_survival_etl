--Snowflake SQL to create the Index table
CREATE TABLE DATA_LAB_NCL_TRAINING_TEMP.CANCER__SURVIVAL.INDEX(
    "Area_Code" VARCHAR,
    "Area_Name" VARCHAR,
    "Cancer_Site" VARCHAR,
    "Gender" VARCHAR,
    "Age_At_Diagnosis" VARCHAR,
    "Standardisation_Type" VARCHAR,
    "Diagnosis_Year" NUMBER,
    "Years_Since_Diagnosis" NUMBER,
    "Patient_Numbers" NUMBER,
    "Survival_Per" FLOAT,
    "Lower_CI" FLOAT,
    "Upper_CI" FLOAT,
    "Precision" FLOAT,
    "Standard_Error" FLOAT,
    "Data_Substituted" BOOLEAN,
    "_timestamp" TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)