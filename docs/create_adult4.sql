--Create table statement for the Index data
CREATE TABLE [Data_Lab_NCL_Dev].[GrahamR].[cancer_survival_adult4](
	area_code CHAR(9) NOT NULL,
	area_name nVARCHAR(100) NOT NULL,
	cancer_site nVARCHAR(40) NOT NULL,
	gender nVARCHAR(7) NOT NULL,
	standardisation_type nVARCHAR(40) NOT NULL,
	standardisation_type_subcategory nVARCHAR(40),
	years_since_diagnosis TINYINT NOT NULL,
	patient_numbers INT NOT NULL,
	survival_metric nVARCHAR(40) NOT NULL,
	survival_per FLOAT NOT NULL,
	data_diagnosis_window CHAR(9) NOT NULL,
	data_snapshot nVARCHAR(14),
	date_upload DATE NOT NULL,

	PRIMARY KEY (area_code, cancer_site, 
		gender, standardisation_type, years_since_diagnosis, 
		survival_metric, data_diagnosis_window)
);

