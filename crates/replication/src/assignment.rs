use chronicle_storage::PartitionAssignment;

pub fn compute_assignments(
    partition_count: u32,
    replication_factor: u32,
    broker_ids: &[u32],
) -> Vec<PartitionAssignment> {
    let n = broker_ids.len();
    let rf = (replication_factor as usize).min(n);
    (0..partition_count)
        .map(|p| {
            let replicas: Vec<u32> = (0..rf).map(|i| broker_ids[(p as usize + i) % n]).collect();
            PartitionAssignment {
                partition_id: p,
                replicas,
            }
        })
        .collect()
}

pub fn local_partitions(assignments: &[PartitionAssignment], broker_id: u32) -> Vec<u32> {
    assignments
        .iter()
        .filter(|a| a.replicas.contains(&broker_id))
        .map(|a| a.partition_id)
        .collect()
}

pub fn leader_for(assignments: &[PartitionAssignment], partition_id: u32) -> Option<u32> {
    assignments
        .iter()
        .find(|a| a.partition_id == partition_id)
        .and_then(|a| a.replicas.first().copied())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balanced_distribution() {
        let ids = vec![1, 2, 3];
        let assignments = compute_assignments(6, 2, &ids);
        assert_eq!(assignments.len(), 6);

        let mut counts = std::collections::HashMap::new();
        for a in &assignments {
            for &r in &a.replicas {
                *counts.entry(r).or_insert(0u32) += 1;
            }
        }
        for &id in &ids {
            assert_eq!(counts[&id], 4, "broker {id} should have 4 replicas");
        }
    }

    #[test]
    fn correct_replication_factor() {
        let ids = vec![1, 2, 3];
        let assignments = compute_assignments(4, 3, &ids);
        for a in &assignments {
            assert_eq!(a.replicas.len(), 3);
        }
    }

    #[test]
    fn rf_capped_at_broker_count() {
        let ids = vec![1, 2];
        let assignments = compute_assignments(3, 5, &ids);
        for a in &assignments {
            assert_eq!(a.replicas.len(), 2);
        }
    }

    #[test]
    fn no_duplicate_replicas() {
        let ids = vec![1, 2, 3, 4];
        let assignments = compute_assignments(8, 3, &ids);
        for a in &assignments {
            let mut seen = std::collections::HashSet::new();
            for &r in &a.replicas {
                assert!(
                    seen.insert(r),
                    "duplicate replica {r} in partition {}",
                    a.partition_id
                );
            }
        }
    }

    #[test]
    fn leader_is_first_replica() {
        let ids = vec![1, 2, 3];
        let assignments = compute_assignments(3, 2, &ids);
        assert_eq!(leader_for(&assignments, 0), Some(1));
        assert_eq!(leader_for(&assignments, 1), Some(2));
        assert_eq!(leader_for(&assignments, 2), Some(3));
    }

    #[test]
    fn local_partitions_subset() {
        let ids = vec![1, 2, 3];
        let assignments = compute_assignments(3, 2, &ids);
        let local = local_partitions(&assignments, 1);
        assert!(local.contains(&0));
        assert!(local.contains(&2));
    }
}
