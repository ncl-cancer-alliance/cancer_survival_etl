--Script to pull in the data for core areas in the adult4 data
SELECT *, 
	CONCAT("Cancer_Site", "Gender", "Years_Since_Diagnosis", "Date_Diagnosis_Window") AS JOIN_KEY
FROM DATA_LAB_NCL_TRAINING_TEMP.CANCER__SURVIVAL.ADULT_4
WHERE "Area_Core" = 1
--Only include age-standardised data and the net survival metric for non-NCL data
AND (
	("Standardisation_Type" = 'Age-standardised' AND "Survival_Metric" = 'Net Survival')
	OR "Area_Code" = 'E56000027'
)