use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chronicle_storage::{Record, RecordHeader, TopicStore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::replica_manager::ReplicaManager;

mod proto {
    tonic::include_proto!("chronicle");
}

use proto::chronicle_client::ChronicleClient;

pub struct FollowerFetcher {
    pub broker_id: u32,
    pub topic: String,
    pub partition: u32,
    pub leader_addr: String,
    pub store: Arc<TopicStore>,
    pub replica_manager: Arc<ReplicaManager>,
    pub fetch_interval: Duration,
    pub cancel: CancellationToken,
}

impl FollowerFetcher {
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(&self) {
        tracing::info!(
            broker_id = self.broker_id,
            topic = %self.topic,
            partition = self.partition,
            leader = %self.leader_addr,
            "starting follower fetcher"
        );

        let mut backoff = Duration::from_millis(100);
        let max_backoff = Duration::from_secs(5);

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    tracing::info!(
                        topic = %self.topic,
                        partition = self.partition,
                        "follower fetcher cancelled"
                    );
                    return;
                }
                result = self.connect_and_fetch(&mut backoff, max_backoff) => {
                    match result {
                        Ok(()) => {}
                        Err(e) => {
                            tracing::warn!(
                                topic = %self.topic,
                                partition = self.partition,
                                error = %e,
                                "follower fetch error, backing off"
                            );
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(max_backoff);
                        }
                    }
                }
            }
        }
    }

    async fn connect_and_fetch(
        &self,
        backoff: &mut Duration,
        max_backoff: Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut client = ChronicleClient::connect(self.leader_addr.clone()).await?;
        tracing::info!(
            topic = %self.topic,
            partition = self.partition,
            "connected to leader"
        );

        loop {
            if self.cancel.is_cancelled() {
                return Ok(());
            }

            let local_leo = self.get_local_leo();
            let leader_epoch = self
                .replica_manager
                .leader_epoch(&self.topic, self.partition);

            let resp = client
                .replicate_fetch(proto::ReplicateFetchRequest {
                    broker_id: self.broker_id,
                    topic: self.topic.clone(),
                    partition: self.partition,
                    fetch_offset: local_leo,
                    leader_epoch,
                })
                .await?
                .into_inner();

            if let Some(ref err) = resp.error {
                if err.code == proto::ErrorCode::NotLeaderForPartition as i32 {
                    tracing::warn!(
                        topic = %self.topic,
                        partition = self.partition,
                        "leader reports not leader, backing off"
                    );
                    tokio::time::sleep(*backoff).await;
                    *backoff = (*backoff * 2).min(max_backoff);
                    return Ok(());
                }
                if err.code != proto::ErrorCode::None as i32 {
                    tracing::warn!(
                        topic = %self.topic,
                        partition = self.partition,
                        error = %err.message,
                        "replicate fetch error"
                    );
                    tokio::time::sleep(self.fetch_interval).await;
                    continue;
                }
            }

            let records_fetched = resp.records.len();
            if records_fetched > 0 {
                self.append_records(&resp.records)?;
                *backoff = Duration::from_millis(100);
            }

            self.replica_manager.update_follower_hwm(
                &self.topic,
                self.partition,
                resp.high_watermark,
            );

            if records_fetched == 0 {
                tokio::time::sleep(self.fetch_interval).await;
            }
        }
    }

    fn get_local_leo(&self) -> u64 {
        self.store
            .topic(&self.topic)
            .and_then(|t| {
                t.partition(self.partition)
                    .map(|l| l.read().unwrap().latest_offset())
            })
            .unwrap_or(0)
    }

    fn append_records(
        &self,
        records: &[proto::Record],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let topic = self
            .store
            .topic(&self.topic)
            .ok_or("topic not found locally")?;
        let log_lock = topic
            .partition(self.partition)
            .ok_or("partition not found locally")?;
        let mut log = log_lock.write().unwrap();

        for r in records {
            let record = Record {
                offset: r.offset,
                timestamp_ms: r.timestamp_ms,
                key: Bytes::copy_from_slice(&r.key),
                value: Bytes::copy_from_slice(&r.value),
                headers: r
                    .headers
                    .iter()
                    .map(|h| RecordHeader {
                        key: h.key.clone(),
                        value: Bytes::copy_from_slice(&h.value),
                    })
                    .collect(),
                producer_id: r.producer_id,
                producer_epoch: r.producer_epoch as u16,
                sequence_number: r.sequence_number,
                is_transactional: r.is_transactional,
                is_control: r.is_control,
            };
            log.append_record(&record)?;
        }
        Ok(())
    }
}
