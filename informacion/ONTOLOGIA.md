# Ontología del Negocio — Reybanpac

## Descripción general

Reybanpac es una empresa ecuatoriana productora y exportadora de banano. Opera un conjunto de haciendas bananeras distribuidas en distintas zonas geográficas del país. El sistema registra mensualmente los indicadores operativos, de costos, ambientales y laborales de cada hacienda, con el objetivo de monitorear la eficiencia productiva y controlar los costos de operación.

## Cobertura temporal

<!-- COBERTURA_INICIO -->
Los datos disponibles cubren desde **enero 2020** hasta **enero 2025**.
<!-- COBERTURA_FIN -->

## Referencia temporal para el análisis

El sistema utiliza como **fecha de referencia** (equivalente al "hoy") el **último mes disponible en los datos**, no la fecha real del sistema. Cuando un usuario pregunta por "los últimos 6 meses", "el último trimestre", "el mes actual" o cualquier expresión temporal relativa, el período debe calcularse hacia atrás desde ese último mes presente en el archivo.

Ejemplo: si el último mes en los datos es enero 2025, "los últimos 6 meses" corresponde a agosto 2024 – enero 2025, y "el año en curso" corresponde a enero 2025.

Este principio aplica tanto al planificador (que define los pasos) como al codificador (que genera el código pandas).

## Columnas derivadas

Al cargar un nuevo archivo de datos, el sistema genera automáticamente las siguientes columnas adicionales a partir de la columna `FECHA`:

- **FECHA_mes** (int): número del mes del registro, con valores del 1 (enero) al 12 (diciembre). Permite filtrar o agrupar por mes sin necesidad de operar sobre la fecha completa.
- **FECHA_ano** (int): año del registro, por ejemplo 2024. Permite filtrar o agrupar por año de forma directa.

Estas columnas están siempre disponibles en `DF_GLOBAL` y pueden usarse en cualquier consulta.

---

## Entidades principales

### Hacienda
Unidad productiva de cultivo de banano. Cada hacienda tiene un código único (`Unidad`), un nombre (`Nombre_Unidad`), una zona geográfica (`Zona`), una superficie en hectáreas (`Total_Hectareas`) y un tipo de suelo (`Tipo_Suelo`). Es la entidad de análisis central del sistema.

### Zona
Agrupación geográfica de haciendas. Permite comparar el desempeño regional y tomar decisiones de inversión por área geográfica.

---

## Procesos productivos

### Riego
Suministro de agua al cultivo. Sus costos incluyen mano de obra (`C_Mano_Obra_Riego`), mantenimiento de infraestructura hidráulica (`C_Mantenimiento_Riego`) y combustible para las bombas. Un déficit o exceso hídrico afecta directamente el rendimiento.

### Control de Sigatoka
La Sigatoka negra es la principal enfermedad fungosa del banano. Su control demanda fumigación aérea (`C_Aplicacion_Aerea`) y labores de deshoje (`C_Deshoje`). El indicador `Incidencia_Sigatoka` mide el nivel de presencia de la plaga: valores altos reducen la producción y elevan los costos fitosanitarios.

### Fertilización
Aplicación de nutrientes al suelo para maximizar el rendimiento. Incluye el costo de los insumos (`C_Sacos_Fert`), la mano de obra de aplicación (`C_ManodeObra_Fert`) y el transporte (`C_Transporte_Fert`).

### Cosecha
Corte y recolección del racimo de banano. El `Racimo_Rechazado` indica la fruta que no cumple estándares de calidad de exportación y representa una pérdida directa.

### Empaque
Selección, clasificación y empaque de la fruta para exportación. Tiene costos fijos asociados a la infraestructura de la empacadora y costos variables que dependen del volumen producido (`C_Empaque_Variable`).

### Logística
Transporte de la fruta desde la hacienda hasta el puerto o centro de acopio. Incluye fletes, acarreo y ajustes contables por reclasificaciones.

---

## Métricas clave

### Real
Indicador del rendimiento productivo real de la hacienda respecto a su potencial. Valores cercanos a 1 indican operación óptima. Valores bajos señalan problemas fitosanitarios, climáticos o laborales.

### Costo por hectárea (`Costo_Ha`)
Métrica principal de eficiencia de costos. Normaliza el costo total por la superficie cultivada, permitiendo comparar haciendas de distinto tamaño.

### Total de cajas (`Total_Cajas`)
Volumen de producción exportable. Una caja estándar de banano pesa aproximadamente 18.14 kg. Es el indicador de output productivo más directo.

### Costo por caja (métrica derivada)
Se calcula como `(Costo_Ha × Total_Hectareas) / Total_Cajas`. Combina eficiencia de costos y volumen; es la métrica comparativa más relevante entre haciendas.

### Peso promedio por caja (`Promedio_Peso_Caja`)
Indicador de calidad del fruto. Cajas con peso por debajo del estándar pueden ser rechazadas por el comprador.

---

## Factores ambientales

La producción de banano es altamente sensible a las condiciones climáticas:

- **Temperatura** (`Temperatura_C`): temperaturas extremas alteran el desarrollo del fruto y aceleran la proliferación de Sigatoka.
- **Precipitación** (`Precipitacion_mm`): el exceso hídrico favorece enfermedades; el déficit eleva los costos de riego.
- **Humedad** (`Humedad`): niveles altos de humedad relativa aceleran la propagación de hongos.
- **Evapotranspiración** (`Evotranspiracion`): indica la demanda hídrica del cultivo; se usa para dimensionar el riego.

---

## Factores laborales

El cultivo de banano es intensivo en mano de obra. Los indicadores laborales son críticos para la planificación operativa:

- **Ausentismo** (`Ausentismo_Agricola`): inasistencias totales del personal de campo. Se desglosa en justificado e injustificado.
- **Rotación** (`RotPerson_Salida_Todos_Motivos_Agricola`): salidas de personal por cualquier motivo. Alta rotación eleva los costos de capacitación.
- **Vacantes** (`Vacante_Labor`): puestos sin cubrir que limitan la capacidad operativa.
- **Pago por labor** (`Pago_Labor_Persona`): modalidad de pago por jornada o tarea.
