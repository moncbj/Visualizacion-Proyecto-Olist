let
    Origen = Csv.Document(File.Contents("C:\Users\Linda\Downloads\archive (1)\product_category_name_translation.csv"),[Delimiter=",", Columns=2, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    #"Tipo cambiado" = Table.TransformColumnTypes(Origen,{{"Column1", type text}, {"Column2", type text}}),
    #"Encabezados promovidos" = Table.PromoteHeaders(#"Tipo cambiado", [PromoteAllScalars=true]),
    #"Tipo cambiado1" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"product_category_name", type text}, {"product_category_name_english", type text}})
in
    #"Tipo cambiado1"