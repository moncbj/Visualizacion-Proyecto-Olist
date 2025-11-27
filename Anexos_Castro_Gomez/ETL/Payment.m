let
    Origen = Csv.Document(File.Contents("C:\Users\Linda\Downloads\archive (1)\olist_order_payments_dataset.csv"),[Delimiter=",", Columns=5, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Encabezados promovidos" = Table.PromoteHeaders(Origen, [PromoteAllScalars=true]),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"order_id", type text}, {"payment_sequential", Int64.Type}, {"payment_type", type text}, {"payment_installments", Int64.Type}, {"payment_value", Int64.Type}}),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Tipo cambiado",{{"payment_value", "valor_pago"}, {"payment_type", "tipo_pago"}, {"payment_installments", "cuotas"}}),
    #"Columna condicional agregada" = Table.AddColumn(#"Columnas con nombre cambiado", "categoria_cuotas", each if [cuotas] = 1 then 1 else if [cuotas] <= 3 then "2 a 3" else if [cuotas] <= 6 then "4 a 6" else if [cuotas] <= 12 then "7 a 12" else "> 13"),
    #"Tipo cambiado1" = Table.TransformColumnTypes(#"Columna condicional agregada",{{"categoria_cuotas", type text}}),
    #"Valor reemplazado" = Table.ReplaceValue(#"Tipo cambiado1","not_defined","Cancelado",Replacer.ReplaceText,{"tipo_pago"}),
    #"Valor reemplazado1" = Table.ReplaceValue(#"Valor reemplazado","_","",Replacer.ReplaceText,{"tipo_pago"}),
    #"Poner En Mayúsculas Cada Palabra" = Table.TransformColumns(#"Valor reemplazado1",{{"tipo_pago", Text.Proper, type text}}),
    #"Filas agrupadas" = Table.Group(#"Poner En Mayúsculas Cada Palabra", {"order_id"}, {{"valor_pago", each List.Sum([valor_pago]), type nullable number}, {"cuotas", each List.Max([cuotas]), type nullable number}, {"pagos_agrupados", each _, type table [order_id=nullable text, payment_sequential=nullable number, tipo_pago=text, cuotas=nullable number, valor_pago=nullable number, categoria_cuotas=nullable text]}, {"num_pagos", each Table.RowCount(_), Int64.Type}}),
    #"Personalizada agregada" = Table.AddColumn(#"Filas agrupadas", "Personalizado", each if Table.RowCount([pagos_agrupados]) > 0 then [pagos_agrupados]{0}[tipo_pago] else null),
    #"Filas filtradas" = Table.SelectRows(#"Personalizada agregada", each true),
    #"Columnas con nombre cambiado1" = Table.RenameColumns(#"Filas filtradas",{{"Personalizado", "tipo_pago_agrupado"}}),
    #"Columnas quitadas" = Table.RemoveColumns(#"Columnas con nombre cambiado1",{"pagos_agrupados"})
in
    #"Columnas quitadas"