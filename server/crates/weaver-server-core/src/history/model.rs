#[derive(Debug, Default)]
pub struct HistoryFilter {
    pub status: Option<String>,
    pub category: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}
