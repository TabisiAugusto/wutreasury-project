{{ config(materialized='table') }}
WITH transacciones_validas AS (
    SELECT 
        fecha_operacion,      
        sucursal_destino,    
        monto_usd
    FROM {{ ref('stg_transacciones') }}
    WHERE estado_transaccion = 'Completado' 
),

retiros_por_hora AS (
    SELECT 
        DATETIME_TRUNC(fecha_operacion, HOUR) AS hora_operacion,
        sucursal_destino,
        SUM(monto_usd) AS total_retirado_usd
    FROM transacciones_validas
    GROUP BY 1, 2
),

metricas_liquidez AS (
    SELECT 
        hora_operacion,
        sucursal_destino,
        total_retirado_usd,
        AVG(total_retirado_usd) OVER (
            PARTITION BY sucursal_destino 
            ORDER BY hora_operacion 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS velocidad_retiro_promedio_usd
    FROM retiros_por_hora
),

saldo_disponible AS (
    SELECT 'Estados Unidos' AS sucursal_destino, 250000.0 AS saldo_usd UNION ALL
    SELECT 'Argentina'      AS sucursal_destino, 180000.0 AS saldo_usd UNION ALL
    SELECT 'Brasil'         AS sucursal_destino, 120000.0 AS saldo_usd
),

snapshot_actual AS (
    SELECT *
    FROM metricas_liquidez
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY sucursal_destino 
        ORDER BY hora_operacion DESC
    ) = 1
),

runway AS (
    SELECT
        s.hora_operacion,
        s.sucursal_destino,
        sd.saldo_usd                                                        AS saldo_disponible_usd,
        s.velocidad_retiro_promedio_usd                                     AS burn_rate_usd_por_hora,
        SAFE_DIVIDE(sd.saldo_usd, s.velocidad_retiro_promedio_usd)          AS liquidity_runway_horas,
        CASE 
            WHEN SAFE_DIVIDE(sd.saldo_usd, s.velocidad_retiro_promedio_usd) < 6  THEN 'CRITICO'
            WHEN SAFE_DIVIDE(sd.saldo_usd, s.velocidad_retiro_promedio_usd) < 24 THEN 'ALERTA'
            ELSE 'NORMAL'
        END                                                                 AS estado_liquidez,
        DATETIME_ADD(
            s.hora_operacion, 
            INTERVAL CAST(
                SAFE_DIVIDE(sd.saldo_usd, s.velocidad_retiro_promedio_usd) 
            AS INT64) HOUR
        )                                                                   AS eta_agotamiento
    FROM snapshot_actual s
    JOIN saldo_disponible sd USING (sucursal_destino)
)

SELECT * FROM runway
ORDER BY liquidity_runway_horas ASC
