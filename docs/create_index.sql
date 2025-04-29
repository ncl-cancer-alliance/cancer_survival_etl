--Create table statement for the Index data
CREATE TABLE [Data_Lab_NCL_Dev].[GrahamR].[cancer_survival_index](
	area_code CHAR(9) NOT NULL,
	area_name nVARCHAR(100) NOT NULL,
	cancer_site nVARCHAR(40) NOT NULL,
	gender nVARCHAR(7) NOT NULL,
	age_at_diagnosis nVARCHAR(8) NOT NULL,
	standardisation_type nVARCHAR(40) NOT NULL,
	diagnosis_year SMALLINT NOT NULL,
	years_since_diagnosis TINYINT NOT NULL,
	patient_numbers INT,
	survival_per FLOAT,
	lower_ci FLOAT,
	upper_ci FLOAT,
	precision FLOAT,
	standard_error FLOAT,
	data_substituted TINYINT NOT NULL,
	date_upload DATE NOT NULL,

	PRIMARY KEY (area_code, cancer_site, 
		gender, age_at_diagnosis, diagnosis_year, years_since_diagnosis)
);

