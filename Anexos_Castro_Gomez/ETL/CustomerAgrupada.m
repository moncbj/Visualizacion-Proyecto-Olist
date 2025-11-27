let
    Origen = Csv.Document(File.Contents("C:\Users\Linda\Downloads\archive (1)\olist_customers_dataset.csv"),[Delimiter=",", Columns=5, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Encabezados promovidos" = Table.PromoteHeaders(Origen, [PromoteAllScalars=true]),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"customer_id", type text}, {"customer_unique_id", type text}, {"customer_zip_code_prefix", Int64.Type}, {"customer_city", type text}, {"customer_state", type text}}),
    #"Personalizada agregada" = Table.AddColumn(#"Tipo cambiado", "region", each if List.Contains({"SP","RJ","MG","ES"}, [customer_state]) then "Sudeste"
else if List.Contains({"RS","SC","PR"}, [customer_state]) then "Sur"
else if List.Contains({"DF","GO","MT","MS"}, [customer_state]) then "Centro-Oeste"
else if List.Contains({"BA","PE","CE","RN","AL","PB","PI","SE","MA"}, [customer_state]) then "Nordeste"
else "Norte"),
    #"Filas filtradas" = Table.SelectRows(#"Personalizada agregada", each true),
    #"Tipo cambiado1" = Table.TransformColumnTypes(#"Filas filtradas",{{"region", type text}}),
    #"Poner En Mayúsculas Cada Palabra" = Table.TransformColumns(#"Tipo cambiado1",{{"customer_city", Text.Proper, type text}}),
    #"Columnas quitadas" = Table.RemoveColumns(#"Poner En Mayúsculas Cada Palabra",{"customer_zip_code_prefix"}),
    #"Filas agrupadas" = Table.Group(#"Columnas quitadas", {"customer_unique_id"}, {{"num_pedidos", each Table.RowCount(_), Int64.Type}}),
    #"Columna condicional agregada" = Table.AddColumn(#"Filas agrupadas", "cliente_recurrente", each if [num_pedidos] > 1 then "Sí" else "No"),
    #"Filas filtradas1" = Table.SelectRows(#"Columna condicional agregada", each true)
in
    #"Filas filtradas1"