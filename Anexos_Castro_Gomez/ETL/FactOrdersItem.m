let
    Origen = Csv.Document(File.Contents("C:\Users\Linda\Downloads\archive (1)\olist_order_items_dataset.csv"),[Delimiter=",", Columns=7, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Encabezados promovidos" = Table.PromoteHeaders(Origen, [PromoteAllScalars=true]),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"order_id", type text}, {"order_item_id", Int64.Type}, {"product_id", type text}, {"seller_id", type text}, {"shipping_limit_date", type datetime}, {"price", Int64.Type}, {"freight_value", Int64.Type}}),
    #"Consultas combinadas" = Table.NestedJoin(#"Tipo cambiado", {"order_id"}, Orders, {"order_id"}, "Orders", JoinKind.LeftOuter),
    #"Se expandió Orders" = Table.ExpandTableColumn(#"Consultas combinadas", "Orders", {"customer_id", "Fecha_pedido"}, {"Orders.customer_id", "Orders.Fecha_pedido"}),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Se expandió Orders",{{"Orders.customer_id", "customer_id"}, {"Orders.Fecha_pedido", "Fecha_pedido"}}),
    #"Tipo cambiado1" = Table.TransformColumnTypes(#"Columnas con nombre cambiado",{{"order_item_id", type text}}),
    #"Personalizada agregada" = Table.AddColumn(#"Tipo cambiado1", "order_product_key", each [order_id] & "-" & [order_item_id]),
    #"Tipo cambiado2" = Table.TransformColumnTypes(#"Personalizada agregada",{{"order_product_key", type text}}),
    #"Personalizada agregada1" = Table.AddColumn(#"Tipo cambiado2", "valor_item", each [price] + [freight_value]),
    #"Tipo cambiado3" = Table.TransformColumnTypes(#"Personalizada agregada1",{{"valor_item", type number}}),
    #"Columnas reordenadas" = Table.ReorderColumns(#"Tipo cambiado3",{"order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "Fecha_pedido", "price", "freight_value", "customer_id", "order_product_key", "valor_item"}),
    #"Fecha extraída" = Table.TransformColumns(#"Columnas reordenadas",{{"shipping_limit_date", DateTime.Date, type date}}),
    #"Columnas quitadas" = Table.RemoveColumns(#"Fecha extraída",{"seller_id"}),
    #"Columnas reordenadas1" = Table.ReorderColumns(#"Columnas quitadas",{"order_id", "order_item_id", "product_id", "customer_id", "order_product_key", "shipping_limit_date", "Fecha_pedido", "price", "freight_value", "valor_item"}),
    #"Columnas quitadas1" = Table.RemoveColumns(#"Columnas reordenadas1",{"order_item_id"}),
    #"Columnas con nombre cambiado1" = Table.RenameColumns(#"Columnas quitadas1",{{"valor_item", "total_item"}}),
    #"Columnas quitadas2" = Table.RemoveColumns(#"Columnas con nombre cambiado1",{"shipping_limit_date", "price", "freight_value", "order_product_key"})
in
    #"Columnas quitadas2"