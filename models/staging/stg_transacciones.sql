{{ config(materialized='view') }}

WITH source AS (
    -- Leemos de la fuente declarada en el archivo .yml
    SELECT * FROM {{ source('remesas_dataset', 'transacciones_realistas') }}
),

renamed_and_casted AS (
    SELECT 
        CAST(transaction_id AS STRING) AS id_transaccion,
        CAST(timestamp AS DATETIME) AS fecha_operacion,
        CAST(origen AS STRING) AS pais_origen,
        CAST(destino AS STRING) AS sucursal_destino,
        CAST(monto_usd AS FLOAT64) AS monto_usd,
        CAST(remitente_nombre AS STRING) AS nombre_remitente,
        CAST(estado AS STRING) AS estado_transaccion
    FROM source
)

SELECT * FROM renamed_and_casted