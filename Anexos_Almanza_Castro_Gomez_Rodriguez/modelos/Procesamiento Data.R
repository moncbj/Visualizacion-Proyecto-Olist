# Preparacion de datos

need <- c("tidyverse","readxl","lubridate","janitor","writexl")
inst <- need[!need %in% rownames(installed.packages())]
if (length(inst)) install.packages(inst, repos = "https://cloud.r-project.org")
library(tidyverse); library(readxl); library(lubridate); library(janitor); library(writexl)

RAW <- "data/raw"
OUT <- "data/processed"
dir.create(OUT, recursive = TRUE, showWarnings = FALSE)

orders    <- read_excel(file.path(RAW, "olist_orders_dataset.xlsx"))       |> clean_names()
items     <- read_excel(file.path(RAW, "olist_order_items_dataset.xlsx"))  |> clean_names()
payments  <- read_excel(file.path(RAW, "olist_order_payments_dataset.xlsx")) |> clean_names()
customers <- read_excel(file.path(RAW, "olist_customers_dataset.xlsx"))    |> clean_names()

# Lista de columnas con fechas para convertir a formato fecha-hora
date_cols <- c("order_purchase_timestamp","order_approved_at",
               "order_delivered_carrier_date","order_delivered_customer_date",
               "order_estimated_delivery_date")

# Conversion de columnas de texto a formato datatime
orders <- orders |> mutate(across(all_of(date_cols), ~ ymd_hms(.x, quiet = TRUE)))


# Agregacion de items y pagos a nivel orden
# Agrupa items por order_id y calcula totales por orden
items_agg <- items |>
  group_by(order_id) |>
  summarise(
    total_price   = sum(price, na.rm = TRUE),
    total_freight = sum(freight_value, na.rm = TRUE),
    total_items   = n(),
    .groups = "drop"
  )

# Conversion de columnas numericas en payments
payments <- payments |>
  mutate(
    payment_installments = suppressWarnings(as.integer(payment_installments)),
    payment_value        = suppressWarnings(as.numeric(payment_value))
  )

# Agrupacion por tipo de pago para obtener valores y cuotas maxima
p_by_type <- payments |>
  group_by(order_id, payment_type) |>
  summarise(
    value_by_type = sum(payment_value, na.rm = TRUE),
    inst_max      = max(payment_installments, na.rm = TRUE),
    .groups = "drop_last"
  )

# Selección del metodo principal de pago (mayor valor)
p_main <- p_by_type |>
  slice_max(order_by = value_by_type, n = 1, with_ties = FALSE) |>
  ungroup() |>
  transmute(
    order_id,
    main_payment_type = payment_type,
    installments      = ifelse(is.infinite(inst_max), NA_integer_, inst_max),
    is_credit_card    = main_payment_type == "credit_card",
    installments_category = case_when(
      is.na(installments) ~ NA_character_,
      installments <= 1   ~ "1",
      installments <= 3   ~ "2-3",
      installments <= 6   ~ "4-6",
      installments <= 12  ~ "7-12",
      TRUE                ~ "13+"
    )
  )


# Total pagado por orden
p_total <- payments |>
  group_by(order_id) |>
  summarise(payment_value_total = sum(payment_value, na.rm = TRUE), .groups = "drop")

# Combina metodo principal y total pagado
payments_agg <- p_main |>
  left_join(p_total, by = "order_id")

# Agregacion para Customers
# Diccionario de regiones por estado (UF)
uf_region <- c(
  "AC"="Norte","AP"="Norte","AM"="Norte","PA"="Norte","RO"="Norte","RR"="Norte","TO"="Norte",
  "AL"="Nordeste","BA"="Nordeste","CE"="Nordeste","MA"="Nordeste","PB"="Nordeste","PE"="Nordeste","PI"="Nordeste","RN"="Nordeste","SE"="Nordeste",
  "DF"="Centro-Oeste","GO"="Centro-Oeste","MT"="Centro-Oeste","MS"="Centro-Oeste",
  "ES"="Sudeste","MG"="Sudeste","RJ"="Sudeste","SP"="Sudeste",
  "PR"="Sul","RS"="Sul","SC"="Sul"
)

# Agrega ciudad, estado y region del cliente
dim_customer <- customers |>
  distinct(customer_id, customer_unique_id, customer_city, customer_state) |>
  mutate(region = uf_region[customer_state] |> as.character())


# Dimension temporal
# Determina rango de fechas en orders
start_date <- floor_date(min(orders$order_purchase_timestamp, na.rm = TRUE), "day")
end_date   <- ceiling_date(max(orders$order_purchase_timestamp, na.rm = TRUE), "day")

# Crea una tabla calendario (dimension de date)
dim_date <- tibble(date = seq.Date(as_date(start_date), as_date(end_date), by = "day")) |>
  mutate(
    year       = year(date),
    month      = month(date, label = TRUE, abbr = TRUE),
    yearmonth  = format(date, "%Y-%m")
  )

# Creacion de tablas y atributos hechos
# Tabla de dimension orders
dim_orderattrs <- orders |>
  transmute(
    order_id,
    order_status,
    purchase_date           = as_date(order_purchase_timestamp),
    approved_date           = as_date(order_approved_at),
    delivered_date          = as_date(order_delivered_customer_date),
    estimated_delivery_date = as_date(order_estimated_delivery_date)
  )

# Tabla de hechos principales con variables derivadas
fact_orders <- orders |>
  select(order_id, customer_id, order_status,
         order_purchase_timestamp, order_approved_at,
         order_delivered_customer_date, order_estimated_delivery_date) |>
  left_join(items_agg,    by = "order_id") |>
  left_join(payments_agg, by = "order_id") |>
  left_join(dim_customer, by = "customer_id") |>
  mutate(
    purchase_date     = as_date(order_purchase_timestamp),
    total_order_value = coalesce(total_price,0) + coalesce(total_freight,0),
    freight_ratio     = ifelse(total_price > 0, total_freight/total_price, NA_real_),
    delivered_flag    = order_status == "delivered" & !is.na(order_delivered_customer_date),
    ontime_flag       = delivered_flag & (order_delivered_customer_date <= order_estimated_delivery_date),
    approval_hours    = as.numeric(difftime(order_approved_at, order_purchase_timestamp, units = "hours")),
    delivery_days     = as.numeric(difftime(order_delivered_customer_date, order_purchase_timestamp, units = "days"))
  )

# Exportacion de datos preparados
# Exporta cada dimension del modelo
write_csv(fact_orders,   file.path(OUT, "FactOrders.csv"))
write_csv(dim_date,      file.path(OUT, "DimDate.csv"))
write_csv(dim_customer,  file.path(OUT, "DimCustomer.csv"))
write_csv(payments_agg,  file.path(OUT, "DimPayment.csv"))
write_csv(dim_orderattrs,file.path(OUT, "DimOrderAttrs.csv"))

# Exporta las tablas en un arhcivo excel
write_xlsx(list(
  FactOrders    = fact_orders,
  DimDate       = dim_date,
  DimCustomer   = dim_customer,
  DimPayment    = payments_agg,
  DimOrderAttrs = dim_orderattrs
), path = file.path(OUT, "olist_modelo_estrella.xlsx"))

fact_orders |>
  summarise(
    n_orders       = n_distinct(order_id),
    delivered_rate = mean(delivered_flag, na.rm = TRUE),
    ontime_rate    = mean(ontime_flag, na.rm = TRUE),
    pct_ratio_gt1  = mean(freight_ratio > 1, na.rm = TRUE)
  ) |>
  print()

payments_agg |>
  count(main_payment_type, sort = TRUE) |>
  print(n = 10)
