--Snowflake SQL to create the Adult 4 table
CREATE TABLE DATA_LAB_NCL_TRAINING_TEMP.CANCER__SURVIVAL.ADULT_4 (
    "Area_Type" VARCHAR,
    "Area_Code" VARCHAR,
    "Area_Name" VARCHAR,
    "Area_Core" BOOLEAN,
    "Cancer_Site" VARCHAR,
    "Gender" VARCHAR,
    "Standardisation_Type" VARCHAR,
    "Standardisation_Type_Subcategory" VARCHAR,
    "Years_Since_Diagnosis" NUMBER,
    "Patient_Numbers" NUMBER,
    "Survival_Metric" VARCHAR,
    "Survival_Per" FLOAT,
    "Date_Diagnosis_Window" VARCHAR,
    "Date_Snapshot" VARCHAR,
    "_timestamp" TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)