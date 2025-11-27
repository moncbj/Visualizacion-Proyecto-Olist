let
    Origen = Csv.Document(File.Contents("C:\Users\Linda\Downloads\archive (1)\olist_orders_dataset.csv"),[Delimiter=",", Columns=8, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Encabezados promovidos" = Table.PromoteHeaders(Origen, [PromoteAllScalars=true]),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"order_id", type text}, {"customer_id", type text}, {"order_status", type text}, {"order_purchase_timestamp", type datetime}, {"order_approved_at", type datetime}, {"order_delivered_carrier_date", type datetime}, {"order_delivered_customer_date", type datetime}, {"order_estimated_delivery_date", type datetime}}),
    #"Personalizada agregada" = Table.AddColumn(#"Tipo cambiado", "tiempo_entrega_dias", each if [order_delivered_customer_date] <> null and [order_purchase_timestamp] <> null
then Duration.Days([order_delivered_customer_date] - [order_purchase_timestamp]) 
else null),
    #"Personalizada agregada1" = Table.AddColumn(#"Personalizada agregada", "tiempo_aprobacion_dias", each if [order_approved_at] <> null and [order_purchase_timestamp] <> null
then Duration.Days([order_approved_at] - [order_purchase_timestamp])
else null),
    #"Fecha extraída" = Table.TransformColumns(#"Personalizada agregada1",{{"order_purchase_timestamp", DateTime.Date, type date}}),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Fecha extraída",{{"order_purchase_timestamp", "Fecha_pedido"}}),
    #"Columnas quitadas" = Table.RemoveColumns(#"Columnas con nombre cambiado",{"order_approved_at", "order_delivered_carrier_date"}),
    #"Personalizada agregada2" = Table.AddColumn(#"Columnas quitadas", "tiempo_estimado_dias", each Duration.Days([order_estimated_delivery_date] - [order_purchase_timestamp])),
    #"Columnas quitadas1" = Table.RemoveColumns(#"Personalizada agregada2",{"tiempo_estimado_dias", "order_estimated_delivery_date", "order_delivered_customer_date", "order_status"})
in
    #"Columnas quitadas1"