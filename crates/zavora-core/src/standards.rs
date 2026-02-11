use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChartOfAccounts {
    pub cash: String,
    pub accounts_receivable: String,
    pub inventory: String,
    pub accounts_payable: String,
    pub revenue: String,
    pub cogs: String,
}

pub trait StandardsProfile {
    fn name(&self) -> &'static str;
    fn chart_of_accounts(&self) -> ChartOfAccounts;
    fn inventory_valuation_method(&self) -> &'static str;
}

#[derive(Debug, Clone, Default)]
pub struct IfrsLiteProfile;

impl StandardsProfile for IfrsLiteProfile {
    fn name(&self) -> &'static str {
        "IFRS-lite"
    }

    fn chart_of_accounts(&self) -> ChartOfAccounts {
        ChartOfAccounts {
            cash: "1000".to_string(),
            accounts_receivable: "1100".to_string(),
            inventory: "1300".to_string(),
            accounts_payable: "2100".to_string(),
            revenue: "4000".to_string(),
            cogs: "5000".to_string(),
        }
    }

    fn inventory_valuation_method(&self) -> &'static str {
        "AVCO"
    }
}
