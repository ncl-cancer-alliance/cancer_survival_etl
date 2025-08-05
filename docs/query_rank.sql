WITH CA_RANK ("Area_Code", JOIN_KEY, "Survival_Per", RANK_CA) AS (
    --Rank each CA for each JOIN KEY combination
    SELECT "Area_Code",
    	CONCAT("Cancer_Site", "Gender", "Years_Since_Diagnosis", "Date_Diagnosis_Window") AS JOIN_KEY,
        "Survival_Per",
        RANK() OVER (PARTITION BY JOIN_KEY ORDER BY "Survival_Per" DESC) AS RANK_CA
    FROM DATA_LAB_NCL_TRAINING_TEMP.CANCER__SURVIVAL.ADULT_4
    WHERE ("Standardisation_Type" = 'Age-standardised' 
        AND "Survival_Metric" = 'Net Survival' 
        AND "Area_Type" = 'Cancer Alliance'
    )
    AND "Survival_Per" IS NOT NULL
),
CA_BASE AS (
    --Get the denominator for each JOIN KEY (CA's with non-null data)
    SELECT
        "Cancer_Site",
        CONCAT("Cancer_Site", "Gender", "Years_Since_Diagnosis", "Date_Diagnosis_Window") AS JOIN_KEY,
        COUNT(1) AS RANK_BASE
    FROM DATA_LAB_NCL_TRAINING_TEMP.CANCER__SURVIVAL.ADULT_4
    WHERE ("Standardisation_Type" = 'Age-standardised' 
        AND "Survival_Metric" = 'Net Survival' 
        AND "Area_Type" = 'Cancer Alliance'
    )
    AND "Survival_Per" IS NOT NULL
    GROUP BY "Cancer_Site", "Gender", "Years_Since_Diagnosis", "Date_Diagnosis_Window"
)
SELECT 
    CA_BASE.JOIN_KEY,
    CA_BASE."Cancer_Site",
    CA_RANK."Survival_Per",
    CA_RANK.RANK_CA,
    RANK_BASE,
    CASE
        WHEN RANK_CA IS NULL THEN NULL
        WHEN RANK_BASE < 4 THEN '-'
        WHEN RANK_CA / RANK_BASE < 0.25 THEN '1st'
        WHEN RANK_CA / RANK_BASE < 0.5 THEN '2nd'
        WHEN RANK_CA / RANK_BASE < 0.75 THEN '3rd'
        ELSE '4th'
    END AS NCL_QUARTILE
    
FROM CA_BASE

LEFT JOIN CA_RANK 
ON CA_RANK.JOIN_KEY = CA_BASE.JOIN_KEY
AND "Area_Code" = 'E56000027'