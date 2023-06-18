/*
 * RAW_OBSERVATIONS
 */

CREATE TABLE IF NOT EXISTS RAW_OBSERVATIONS (
	RUN_ID TEXT,
	SYS_LOAD_TIME TEXT,
	FILENAME TEXT,
	OBSERVATION TEXT
);


/*
 * VW_PROPERTIES
 */

DROP VIEW IF EXISTS VW_PROPERTIES;

CREATE VIEW VW_PROPERTIES AS
WITH CTE_PROPERTIES AS
(
	SELECT json_extract(OBSERVATION, "$.properties") AS PROPERTIES
	FROM RAW_OBSERVATIONS
)
SELECT
	json_extract(PROPERTIES, "$.timestamp") AS "TIMESTAMP",
	json_extract(PROPERTIES, "$.rawMessage") AS "RAW_MESSAGE",
	json_extract(PROPERTIES, "$.textDescription") AS "TEXT_DESCRIPTION",
	json_extract(PROPERTIES, "$.temperature.value") AS "TEMPERATURE",
	json_extract(PROPERTIES, "$.dewpoint.value") AS "DEWPOINT",
	json_extract(PROPERTIES, "$.windDirection.value") AS "WIND_DIRECTION",
	json_extract(PROPERTIES, "$.windSpeed.value") AS "WIND_SPEED",
	json_extract(PROPERTIES, "$.windGust.value") AS "WIND_GUST",
	json_extract(PROPERTIES, "$.barometricPressure.value") AS "BAROMETRIC_PRESSURE",
	json_extract(PROPERTIES, "$.seaLevelPressure.value") AS "SEA_LEVEL_PRESSURE",
	json_extract(PROPERTIES, "$.visibility.value") AS "VISIBILITY",
	json_extract(PROPERTIES, "$.relativeHumidity.value") AS "RELATIVE_HUMIDITY",
	json_extract(PROPERTIES, "$.windChill.value") AS "WIND_CHILL",
	json_extract(PROPERTIES, "$.heatIndex.value") AS "HEAT_INDEX"
FROM CTE_PROPERTIES
ORDER BY TIMESTAMP;