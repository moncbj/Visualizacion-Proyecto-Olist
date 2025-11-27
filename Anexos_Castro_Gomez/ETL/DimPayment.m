let
    Origen = Csv.Document(File.Contents("C:\Users\Linda\Downloads\archive (1)\olist_order_payments_dataset.csv"),[Delimiter=",", Columns=5, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Encabezados promovidos" = Table.PromoteHeaders(Origen, [PromoteAllScalars=true]),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"order_id", type text}, {"payment_sequential", Int64.Type}, {"payment_type", type text}, {"payment_installments", Int64.Type}, {"payment_value", Int64.Type}}),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Tipo cambiado",{{"payment_value", "valor_pago"}, {"payment_type", "tipo_pago"}, {"payment_installments", "cuotas"}}),
    #"Columna condicional agregada" = Table.AddColumn(#"Columnas con nombre cambiado", "categoria_cuotas", each if [cuotas] = 1 then 1 else if [cuotas] <= 3 then #date(2025, 3, 2) else if [cuotas] <= 6 then #date(2025, 6, 4) else if [cuotas] <= 10 then #date(2025, 10, 7) else "11 - 13"),
    #"Filas filtradas" = Table.SelectRows(#"Columna condicional agregada", each true),
    #"Valor reemplazado" = Table.ReplaceValue(#"Filas filtradas","not_defined","Cancelado",Replacer.ReplaceText,{"tipo_pago"}),
    #"Valor reemplazado1" = Table.ReplaceValue(#"Valor reemplazado","_","",Replacer.ReplaceText,{"tipo_pago"}),
    #"Poner En Mayúsculas Cada Palabra" = Table.TransformColumns(#"Valor reemplazado1",{{"tipo_pago", Text.Proper, type text}}),
    #"Otras columnas quitadas" = Table.SelectColumns(#"Poner En Mayúsculas Cada Palabra",{"tipo_pago"}),
    #"Duplicados quitados" = Table.Distinct(#"Otras columnas quitadas")
in
    #"Duplicados quitados"