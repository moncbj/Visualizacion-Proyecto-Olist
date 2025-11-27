let
    Origen = Csv.Document(File.Contents("C:\Users\Linda\Downloads\archive (1)\olist_order_items_dataset.csv"),[Delimiter=",", Columns=7, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Encabezados promovidos" = Table.PromoteHeaders(Origen, [PromoteAllScalars=true]),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"order_id", type text}, {"order_item_id", Int64.Type}, {"product_id", type text}, {"seller_id", type text}, {"shipping_limit_date", type datetime}, {"price", type number}, {"freight_value", type number}}),
    #"Filas filtradas" = Table.SelectRows(#"Tipo cambiado", each true),
    #"Personalizada agregada" = Table.AddColumn(#"Filas filtradas", "valor_total_item", each [price] + [freight_value]),
    #"Personalizada agregada1" = Table.AddColumn(#"Personalizada agregada", "proporcion_envio_item", each if [price] <> 0 then [freight_value] / [price] else null),
    #"Tipo cambiado1" = Table.TransformColumnTypes(#"Personalizada agregada1",{{"valor_total_item", type number}, {"proporcion_envio_item", type number}}),
    #"Filas agrupadas" = Table.Group(#"Tipo cambiado1", {"order_id"}, {{"precio_total", each List.Sum([price]), type nullable number}, {"flete_total", each List.Sum([freight_value]), type nullable number}, {"valor_total", each List.Sum([valor_total_item]), type nullable number}, {"num_items", each Table.RowCount(_), Int64.Type}, {"promedio_precio_item", each List.Average([price]), type nullable number}, {"max_precio_item", each List.Max([price]), type nullable number}, {"min_precio_item", each List.Min([price]), type nullable number}})
in
    #"Filas agrupadas"