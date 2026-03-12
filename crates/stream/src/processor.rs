use std::time::Duration;

use bytes::Bytes;
use tonic::transport::Channel;
use tracing;

use crate::operator::{Operator, StreamRecord};

pub mod proto {
    tonic::include_proto!("chronicle");
}

use proto::chronicle_client::ChronicleClient;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StreamJobConfig {
    pub job_name: String,
    pub input_topic: String,
    pub input_partition: u32,
    pub output_topic: String,
    pub output_partition: u32,
    pub server_addr: String,
    pub poll_interval_ms: u64,
}

pub struct StreamJob {
    pub config: StreamJobConfig,
    operators: Vec<Box<dyn Operator>>,
}

impl StreamJob {
    pub fn builder(config: StreamJobConfig) -> StreamJobBuilder {
        StreamJobBuilder {
            config,
            operators: Vec::new(),
        }
    }
}

pub struct StreamJobBuilder {
    config: StreamJobConfig,
    operators: Vec<Box<dyn Operator>>,
}

impl StreamJobBuilder {
    pub fn add_operator(mut self, op: Box<dyn Operator>) -> Self {
        self.operators.push(op);
        self
    }

    pub fn build(self) -> StreamJob {
        StreamJob {
            config: self.config,
            operators: self.operators,
        }
    }
}

pub struct StreamProcessor {
    job: StreamJob,
    client: ChronicleClient<Channel>,
    current_offset: u64,
}

impl StreamProcessor {
    pub async fn new(job: StreamJob) -> Result<Self, tonic::transport::Error> {
        let client = ChronicleClient::connect(job.config.server_addr.clone()).await?;
        Ok(Self {
            job,
            client,
            current_offset: 0,
        })
    }

    pub async fn run(&mut self, cancel: tokio_util::sync::CancellationToken) {
        let poll_interval = Duration::from_millis(self.job.config.poll_interval_ms);

        tracing::info!(
            job = %self.job.config.job_name,
            input = %self.job.config.input_topic,
            output = %self.job.config.output_topic,
            "stream processor starting"
        );

        loop {
            if cancel.is_cancelled() {
                tracing::info!(job = %self.job.config.job_name, "stream processor stopping");
                break;
            }

            match self.poll_and_process().await {
                Ok(0) => {
                    tokio::select! {
                        _ = tokio::time::sleep(poll_interval) => {}
                        _ = cancel.cancelled() => break,
                    }
                }
                Ok(n) => {
                    tracing::debug!(job = %self.job.config.job_name, processed = n, "batch complete");
                }
                Err(e) => {
                    tracing::error!(job = %self.job.config.job_name, error = %e, "processing error");
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        _ = cancel.cancelled() => break,
                    }
                }
            }
        }
    }

    async fn poll_and_process(&mut self) -> Result<usize, Box<dyn std::error::Error>> {
        let fetch_resp = self
            .client
            .fetch(proto::FetchRequest {
                topic: self.job.config.input_topic.clone(),
                partition: self.job.config.input_partition,
                offset: self.current_offset,
                max_records: 100,
                isolation_level: proto::IsolationLevel::ReadCommitted.into(),
            })
            .await?
            .into_inner();

        if let Some(ref err) = fetch_resp.error {
            if err.code != proto::ErrorCode::None as i32 {
                return Err(err.message.clone().into());
            }
        }

        let mut processed = 0;

        for record in &fetch_resp.records {
            let input = StreamRecord {
                key: Bytes::copy_from_slice(&record.key),
                value: Bytes::copy_from_slice(&record.value),
                timestamp_ms: record.timestamp_ms,
                offset: record.offset,
                topic: self.job.config.input_topic.clone(),
                partition: self.job.config.input_partition,
            };

            let mut current = vec![input];
            for op in &self.job.operators {
                let mut next = Vec::new();
                for rec in &current {
                    next.extend(op.process(rec));
                }
                current = next;
            }

            for output in &current {
                let produce_resp = self
                    .client
                    .produce(proto::ProduceRequest {
                        topic: self.job.config.output_topic.clone(),
                        partition: Some(self.job.config.output_partition),
                        key: output.key.to_vec(),
                        value: output.value.to_vec(),
                        acks: proto::Acks::Leader.into(),
                        producer_id: 0,
                        producer_epoch: 0,
                        first_sequence: 0,
                        headers: vec![],
                        is_transactional: false,
                    })
                    .await?
                    .into_inner();

                if let Some(ref err) = produce_resp.error {
                    if err.code != proto::ErrorCode::None as i32 {
                        return Err(err.message.clone().into());
                    }
                }
            }

            self.current_offset = record.offset + 1;
            processed += 1;
        }

        Ok(processed)
    }
}
