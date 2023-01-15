package com.example.kafkamessagemultipletypes.message;

public final class ShipmentLeftTestData {
    private ShipmentLeftTestData() {}

    public static ShipmentLeftEvent fromBiaToWawLeft() {
        return new ShipmentLeftEvent("BIA-WAW-1");
    };
}
