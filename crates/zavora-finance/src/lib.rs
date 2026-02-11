use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zavora_core::{IfrsLiteProfile, StandardsProfile};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalLine {
    pub account: String,
    pub debit: Decimal,
    pub credit: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    pub id: Uuid,
    pub memo: String,
    pub lines: Vec<JournalLine>,
}

pub fn invoice_journal(amount: Decimal) -> JournalEntry {
    let profile = IfrsLiteProfile;
    let coa = profile.chart_of_accounts();

    JournalEntry {
        id: Uuid::new_v4(),
        memo: "Invoice posted".to_string(),
        lines: vec![
            JournalLine {
                account: coa.accounts_receivable,
                debit: amount,
                credit: Decimal::ZERO,
            },
            JournalLine {
                account: coa.revenue,
                debit: Decimal::ZERO,
                credit: amount,
            },
        ],
    }
}
