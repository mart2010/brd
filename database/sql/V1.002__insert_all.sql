-- author = 'mart2010'
-- copyright = "Copyright 2016, The BRD Project"


-------------------------------------- Data insertion -------------------------------------

-- the goal here is to pre-load all reference data and static content....

-------------------------------------------------------------------------------------------


insert into integration.site(id, logical_name, status, create_dts)
values
(1, 'librarything', 'active', now())
,(2, 'goodreads', 'active', now())
,(3, 'critiqueslibres', 'active', now())
,(4, 'babelio', 'active', now())
,(5, 'amazon.com', 'active', now())
;

insert into integration.site_identifier(site_id, hostname, valid_from, valid_to, create_dts)
values
(1, 'www.librarything.com', now(), '''infinity'''::timestamp, now())
,(2, 'www.goodreads.com', now(), '''infinity'''::timestamp, now())
,(3, 'www.critiqueslibres.com', now(), '''infinity'''::timestamp, now())
,(4, 'www.babelio.com', now(), '''infinity'''::timestamp, now())
,(5, 'www.amazon.com', now(), '''infinity'''::timestamp, now())
;


-- 'copy' requires root privilege and the use of plsql '\copy' is only possible in psql shell env!
-- now done in python!
--copy integration.language(code3,code3_term,code2,code,english_iso_name,english_name,french_iso_name,french_name)
--from '/Users/mart/Google Drive/brd/ref_data/Iso_639_and_Marc_code - ISO-639-2_utf-8.tsv'
--with delimiter '\t';


--insert into integration.language(code, code2, name, native_name, create_dts)
--values
--('ENG', 'EN', 'English', 'English', now())
--,('FRE', 'FR', 'French', 'Français', now())
--,('GER', 'DE', 'German', '', now())
--,('SPA', '', 'Spanish', '', now())
--,('DUT', '', 'Dutch', '', now())
--,('ITA', '', 'Italian', '', now())
--,('POR1', '', 'Portuguese (Brazil)', '', now())
--,('POR2', '', 'Portuguese (Portugal)', '', now())
--,('SWE', '', 'Swedish', '', now())
--,('DAN', '', 'Danish', '', now())
--,('FIN', '', 'Finnish', '', now())
--,('NOR', '', 'Norwegian', '', now())
--,('POL', '', 'Polish', '', now())
--,('CAT', '', 'Catalan', '', now())
--,('HUN', '', 'Hungarian', '', now())
--,('TUR', '', 'Turkish', '', now())
--,('RUS', '', 'Russian', '', now())
--,('GRE', '', 'Greek', '', now())
--,('HIN', '', 'Hindi', '', now())
--,('CZE', '', 'Czech', '', now())
--,('PIR', '', 'Piratical', '', now())
--,('JPN', '', 'Japanese', '', now())
--,('LIT', '', 'Lithuanian', '', now())
--,('ALB', '', 'Albanian', '', now())
--,('LAT', '', 'Latin', '', now())
--,('BUL', '', 'Bulgarian', '', now())
--,('RUM', '', 'Romanian', '', now())
--,('CHI1', '', 'Chinese, traditional', '', now())
--,('SCR', '', 'Croatian', '', now())
--,('CHI2', '', 'Chinese, simplified', '', now())
--,('EST', '', 'Estonian', '', now())
--,('SLO', '', 'Slovak', '', now())
--,('LAV', '', 'Latvian', '', now())
--,('ARA', '', 'Arabic', '', now())
--,('BAQ', '', 'Basque', '', now())
--,('POR', '', 'Portuguese', '', now())
--,('ICE', '', 'Icelandic', '', now())
--,('WEL', '', 'Welsh', '', now())
--,('HEB', '', 'Hebrew', '', now())
--,('SCC', '', 'Serbian', '', now())
--,('IND', '', 'Indonesian', '', now())
--,('GLE', '', 'Irish', '', now())
--,('KOR', '', 'Korean', '', now())
--,('PER', '', 'Persian', '', now())
--,('AFR', '', 'Afrikaans', '', now())
--,('TGL', '', 'Tagalog', '', now())
--,('ARM', '', 'Armenian', '', now())
--,('SLV', '', 'Slovenian', '', now())
--,('EPO', '', 'Esperanto', '', now())
--,('GEO', '', 'Georgian', '', now())
--,('GLG', '', 'Galician', '', now())
--,('URD', '', 'Urdu', '', now())
--,('MAC', '', 'Macedonian', '', now())
--,('YID', '', 'Yiddish', '', now())
--,('ENM', '', 'English (Middle)', '', now())
--,('CHI', '', 'Chinese', '', now())
--,('NOB', '', 'Norwegian (Bokmål)', '', now())
--;


-- Dim_Date population dml

SELECT
	datum AS DATE,
	EXTRACT(YEAR FROM datum) AS YEAR,
	EXTRACT(MONTH FROM datum) AS MONTH,
	-- Localized month name
	to_char(datum, 'TMMonth') AS MonthName,
	EXTRACT(DAY FROM datum) AS DAY,
	EXTRACT(doy FROM datum) AS DayOfYear,
	-- Localized weekday
	to_char(datum, 'TMDay') AS WeekdayName,
	-- ISO calendar week
	EXTRACT(week FROM datum) AS CalendarWeek,
	to_char(datum, 'dd. mm. yyyy') AS FormattedDate,
	'Q' || to_char(datum, 'Q') AS Quarter,
	to_char(datum, 'yyyy/"Q"Q') AS YearQuarter,
	to_char(datum, 'yyyy/mm') AS YearMonth,
	-- ISO calendar year and week
	to_char(datum, 'iyyy/IW') AS YearCalendarWeek,
	-- Weekend
	CASE WHEN EXTRACT(isodow FROM datum) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS Weekend,
	-- ISO start and end of the week of this date
	datum + (1 - EXTRACT(isodow FROM datum))::INTEGER AS CWStart,
	datum + (7 - EXTRACT(isodow FROM datum))::INTEGER AS CWEnd,
	-- Start and end of the month of this date
	datum + (1 - EXTRACT(DAY FROM datum))::INTEGER AS MonthStart,
	(datum + (1 - EXTRACT(DAY FROM datum))::INTEGER + '1 month'::INTERVAL)::DATE - '1 day'::INTERVAL AS MonthEnd
FROM (
	-- starting from min review_date + 50 year + 12..missing days for leap year: 365*50+12=18262
	SELECT '1969-01-01'::DATE + SEQUENCE.DAY AS datum
	FROM generate_series(0,18262) AS SEQUENCE(DAY)
	GROUP BY SEQUENCE.DAY
     ) DQ
ORDER BY 1;






