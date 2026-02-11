use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryPosition {
    pub item_code: String,
    pub quantity_on_hand: Decimal,
    pub average_cost: Decimal,
}

impl InventoryPosition {
    pub fn receive(&mut self, quantity: Decimal, unit_cost: Decimal) {
        let current_value = self.quantity_on_hand * self.average_cost;
        let incoming_value = quantity * unit_cost;
        let new_qty = self.quantity_on_hand + quantity;

        if new_qty.is_zero() {
            self.average_cost = Decimal::ZERO;
            self.quantity_on_hand = Decimal::ZERO;
            return;
        }

        self.average_cost = (current_value + incoming_value) / new_qty;
        self.quantity_on_hand = new_qty;
    }

    pub fn issue(&mut self, quantity: Decimal) -> Decimal {
        let cogs = quantity * self.average_cost;
        self.quantity_on_hand -= quantity;
        cogs
    }
}
