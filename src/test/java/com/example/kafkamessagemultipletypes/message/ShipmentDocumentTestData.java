package com.example.kafkamessagemultipletypes.message;

public final class ShipmentDocumentTestData {
    private ShipmentDocumentTestData() {}

    public static ShipmentDocument fromBiaToWaw() {
        return new ShipmentDocument("BIA-WAW-1", "BIA", "WAW");
    };
}
