SELECT file, jsonb_array_length(payload) FROM bronze.comtrade_raw;

SELECT SUM(jsonb_array_length(payload)) FROM bronze.comtrade_raw;

SELECT file, payload->0 FROM bronze.comtrade_raw;

