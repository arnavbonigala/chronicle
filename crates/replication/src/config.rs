#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub broker_id: u32,
    pub brokers: Vec<BrokerInfo>,
}

#[derive(Debug, Clone)]
pub struct BrokerInfo {
    pub id: u32,
    pub addr: String,
}

impl ClusterConfig {
    pub fn broker_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self.brokers.iter().map(|b| b.id).collect();
        ids.sort();
        ids
    }

    pub fn peer_brokers(&self) -> Vec<&BrokerInfo> {
        self.brokers
            .iter()
            .filter(|b| b.id != self.broker_id)
            .collect()
    }

    pub fn broker_addr(&self, id: u32) -> Option<&str> {
        self.brokers
            .iter()
            .find(|b| b.id == id)
            .map(|b| b.addr.as_str())
    }

    pub fn is_single_broker(&self) -> bool {
        self.brokers.len() <= 1
    }
}
