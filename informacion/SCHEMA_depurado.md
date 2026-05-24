## Descripción de la tabla

Esta tabla, llamada haciendas,  registra **mensualmente** los indicadores operativos, de costos y ambientales de cada hacienda bananera.

**Granularidad:** un registro = una hacienda + un mes.

**Identificación de la hacienda:** columna `Unidad` (código) y `Nombre_Unidad` (nombre legible).

**Identificación del período:** columna `FECHA` en formato `DD/MM/YYYY`. Aunque el campo se llama FECHA, el reporte se emite **una vez por mes**, por lo que la fecha representa el mes completo de actividad. La columna `Semana` indica la semana del año en que se generó el reporte, no implica frecuencia semanal de los datos.

**Cobertura temporal:** datos desde enero de 2020 hasta junio de 2025.

**Regla de agregación:** para analizar lo ocurrido durante un año completo en una hacienda se deben **agrupar** los registros por año (extraído de `FECHA` o de `FECHA_ano`) y por `Unidad`, sumando o promediando las métricas según corresponda. No existe un registro anual consolidado; el año se reconstruye acumulando los doce registros mensuales de cada hacienda.

**Columnas derivadas:** al cargar el archivo, el sistema agrega automáticamente `FECHA_mes` y `FECHA_ano` para facilitar filtros y agrupaciones por período sin operar sobre la fecha completa.

---

| # | Columna | Tipo | Descripción |
| :--- | :--- | :--- | :--- |
| 1 | **FECHA** | date | Fecha del registro en formato dd/mm/yyyy. Representa el mes completo de actividad. |
| 2 | **FECHA_mes** | int | Mes extraído de FECHA (1 = enero … 12 = diciembre). Columna derivada, siempre disponible. |
| 3 | **FECHA_ano** | int | Año extraído de FECHA (ej. 2024). Columna derivada, siempre disponible. |
| 4 | **Semana** | int | Número de semana del año. |
| 5 | **Zona** | text | Región geográfica donde se ubica la hacienda. |
| 6 | **Unidad** | text | Código identificador de la hacienda. |
| 7 | **Nombre_Unidad** | text | Nombre de la hacienda. |
| 8 | **Real** | numeric | Indicador de rendimiento o ratio de producción real. |
| 9 | **Costo_Ha** | numeric | Costo total acumulado por hectárea. |
| 10 | **Atencion_Plantacion** | numeric | Costos generales de mantenimiento del cultivo. |
| 11 | **C_Riego** | numeric | Costo total por actividades de riego. |
| 12 | **C_Mano_Obra_Riego** | numeric | Costo de personal para labores de riego. |
| 13 | **C_Mantenimiento_Riego** | numeric | Costo en reparaciones de infraestructura para riego. |
| 14 | **C_Combustible** | numeric | Costo de combustible. |
| 15 | **C_Control_Sigatoca** | numeric | Costo del programa fitosanitario contra la Sigatoka. |
| 16 | **C_Aplicacion_Aerea** | numeric | Costo específico en fumigación aérea. |
| 17 | **C_Deshoje** | numeric | Costo de la labor manual de retirar hojas enfermas o secas. |
| 18 | **C_Costos_Productos** | numeric | Costo en insumos químicos y fertilizantes. |
| 19 | **C_Fertilizacion** | numeric | Costo total del proceso de fertilización. |
| 20 | **C_Sacos_Fert** | numeric | Costo de compra de sacos de fertilizante. |
| 21 | **C_ManodeObra_Fert** | numeric | Costo de la aplicación del fertilizante en campo. |
| 22 | **C_Transporte_Fert** | numeric | Costo de transporte del fertilizante. |
| 23 | **C_Administracion_Hacienda** | numeric | Costos administrativos de la hacienda. |
| 24 | **Sueldos** | numeric | Nómina de empleados fijos. |
| 25 | **Servicios_Basicos** | numeric | Pagos de luz, agua y otros servicios. |
| 26 | **C_Empaque_Fijo** | numeric | Costos fijos asociados al empaque. |
| 27 | **Mantenimiento_Empacadora** | numeric | Costos de mantenimiento de la empacadora. |
| 28 | **Mantenimiento_Equipo** | numeric | Costo de mantenimiento de equipo. |
| 29 | **C_Logistica** | numeric | Costo total del movimiento de la fruta y materiales. |
| 30 | **Transporte** | numeric | Gasto en fletes y acarreo. |
| 31 | **Materiales** | numeric | Inversión en cartón, plástico y otros insumos de empaque. |
| 32 | **Reclasificaciones_Transporte** | numeric | Ajustes contables internos sobre transporte. |
| 33 | **Reclasificaciones_Materiales** | numeric | Ajustes contables internos sobre materiales. |
| 34 | **C_Empaque_Variable** | numeric | Costos de empaque que varían según el volumen. |
| 35 | **C_Cosecha** | numeric | Costo de la labor de cosecha. |
| 36 | **C_Transporte** | numeric | Otros costos relacionados con el transporte. |
| 37 | **C_Depreciaciones** | numeric | Costo de depreciación de activos fijos. |
| 38 | **Total_Cajas** | numeric | Volumen total de cajas producidas. |
| 39 | **Total_Hectareas** | numeric | Superficie productiva de la unidad en hectáreas. |
| 40 | **Racimo_Rechazado** | numeric | Cantidad de fruta que fue rechazada. |
| 41 | **Total_Peso_Caja** | numeric | Sumatoria total del peso de las cajas. |
| 42 | **Promedio_Peso_Caja** | numeric | Peso medio por unidad de caja. |
| 43 | **Tipo_Suelo** | text | Clasificación técnica del terreno. |
| 44 | **Incidencia_Sigatoka** | numeric | Nivel de presencia de la plaga en el cultivo. |
| 45 | **Temperatura_C** | numeric | Temperatura media registrada en grados centígrados (°C). |
| 46 | **Precipitacion_mm** | numeric | Lluvia caída acumulada (milímetros). |
| 47 | **Evotranspiracion** | numeric | Tasa de evaporación y transpiración biológica. |
| 48 | **Humedad** | numeric | Porcentaje de humedad relativa ambiente. |
| 49 | **Ausentismo_Agricola** | numeric | Total de inasistencias del personal de campo. |
| 50 | **Ausentismo_Justificado_Agricola** | numeric | Inasistencias del personal con soporte legal o médico. |
| 51 | **Ausentismo_Injustificado_Agricola** | numeric | Inasistencias del personal sin justificación. |
| 52 | **RotPerson_Salida_Todos_Motivos_Agricola** | numeric | Índice de rotación o salidas de personal por todos los motivos. |
| 53 | **Pago_Labor_Persona** | numeric | Indicador de pago por jornada o labor. |
| 54 | **Pago_Por_Cuenta** | numeric | Indicador de pagos bajo modalidades de cuentas. |
| 55 | **Vacante_Labor** | numeric | Número de puestos de trabajo por cubrir. |
