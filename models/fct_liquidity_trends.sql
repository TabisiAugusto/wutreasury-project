{{ config(materialized='table') }}

WITH transacciones_validas AS (
    -- Leemos de tu capa de staging (que ya creamos y testeamos)
    SELECT * FROM {{ ref('stg_transacciones') }}
    WHERE estado_transaccion = 'Completado' 
),

retiros_por_hora AS (
    -- Agrupamos por hora y sucursal para ver la evolución
    SELECT 
        DATETIME_TRUNC(fecha_operacion, HOUR) AS hora_operacion,
        sucursal_destino,
        SUM(monto_usd) AS total_retirado_usd
    FROM transacciones_validas
    GROUP BY 1, 2
),

metricas_historicas AS (
    -- Calculamos la velocidad de retiro (burn rate) para cada punto en el tiempo
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
    -- Saldo base para el cálculo del runway (esto se mantiene igual)
    SELECT 'Estados Unidos' AS sucursal_destino, 250000.0 AS saldo_usd UNION ALL
    SELECT 'Argentina'      AS sucursal_destino, 180000.0 AS saldo_usd UNION ALL
    SELECT 'Brasil'         AS sucursal_destino, 120000.0 AS saldo_usd
),

final_historico AS (
    SELECT
        m.hora_operacion,
        m.sucursal_destino,
        sd.saldo_usd                                                        AS saldo_actual_usd,
        m.velocidad_retiro_promedio_usd                                     AS burn_rate_usd_por_hora,
        -- Cálculo de runway para cada punto histórico
        SAFE_DIVIDE(sd.saldo_usd, m.velocidad_retiro_promedio_usd)          AS liquidity_runway_horas
    FROM metricas_historicas m
    JOIN saldo_disponible sd USING (sucursal_destino)
)

SELECT * FROM final_historico
ORDER BY sucursal_destino, hora_operacion ASC