CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS serving;
GRANT ALL PRIVILEGES ON serving.* TO 'etl'@'%';

CREATE TABLE IF NOT EXISTS staging.weather_raw (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  city        VARCHAR(64)  NOT NULL,
  obs_time    DATETIME     NOT NULL,
  payload     JSON         NOT NULL,
  ingested_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                           ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_city_time (city, obs_time),
  KEY idx_ingested (ingested_at)
);

CREATE TABLE IF NOT EXISTS staging.etl_watermark (
  pipeline_name  VARCHAR(64) PRIMARY KEY,
  last_loaded_at TIMESTAMP(6) NOT NULL
);