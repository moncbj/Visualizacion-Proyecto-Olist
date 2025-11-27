let
    Origen = Table.NestedJoin(Customer, {"customer_unique_id"}, CustomerAgrupada, {"customer_unique_id"}, "Customer", JoinKind.LeftOuter),
    #"Se expandió Customer" = Table.ExpandTableColumn(Origen, "Customer", {"num_pedidos", "cliente_recurrente"}, {"num_pedidos", "cliente_recurrente"}),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Se expandió Customer",{{"cliente_recurrente", type text}}),
    #"Columnas quitadas" = Table.RemoveColumns(#"Tipo cambiado",{"customer_zip_code_prefix", "customer_unique_id", "num_pedidos", "customer_state"})
in
    #"Columnas quitadas"