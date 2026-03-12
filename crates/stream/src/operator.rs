use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct StreamRecord {
    pub key: Bytes,
    pub value: Bytes,
    pub timestamp_ms: u64,
    pub offset: u64,
    pub topic: String,
    pub partition: u32,
}

pub trait Operator: Send + Sync {
    fn process(&self, record: &StreamRecord) -> Vec<StreamRecord>;
    fn name(&self) -> &str;
}

pub struct Filter<F: Fn(&StreamRecord) -> bool + Send + Sync> {
    predicate: F,
}

impl<F: Fn(&StreamRecord) -> bool + Send + Sync> Filter<F> {
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }
}

impl<F: Fn(&StreamRecord) -> bool + Send + Sync> Operator for Filter<F> {
    fn process(&self, record: &StreamRecord) -> Vec<StreamRecord> {
        if (self.predicate)(record) {
            vec![record.clone()]
        } else {
            vec![]
        }
    }

    fn name(&self) -> &str {
        "Filter"
    }
}

pub struct Map<F: Fn(&StreamRecord) -> StreamRecord + Send + Sync> {
    transform: F,
}

impl<F: Fn(&StreamRecord) -> StreamRecord + Send + Sync> Map<F> {
    pub fn new(transform: F) -> Self {
        Self { transform }
    }
}

impl<F: Fn(&StreamRecord) -> StreamRecord + Send + Sync> Operator for Map<F> {
    fn process(&self, record: &StreamRecord) -> Vec<StreamRecord> {
        vec![(self.transform)(record)]
    }

    fn name(&self) -> &str {
        "Map"
    }
}

pub struct FlatMap<F: Fn(&StreamRecord) -> Vec<StreamRecord> + Send + Sync> {
    transform: F,
}

impl<F: Fn(&StreamRecord) -> Vec<StreamRecord> + Send + Sync> FlatMap<F> {
    pub fn new(transform: F) -> Self {
        Self { transform }
    }
}

impl<F: Fn(&StreamRecord) -> Vec<StreamRecord> + Send + Sync> Operator for FlatMap<F> {
    fn process(&self, record: &StreamRecord) -> Vec<StreamRecord> {
        (self.transform)(record)
    }

    fn name(&self) -> &str {
        "FlatMap"
    }
}

pub struct RegexFilter {
    pattern: regex::Regex,
}

impl RegexFilter {
    pub fn new(pattern: &str) -> Result<Self, regex::Error> {
        Ok(Self {
            pattern: regex::Regex::new(pattern)?,
        })
    }
}

impl Operator for RegexFilter {
    fn process(&self, record: &StreamRecord) -> Vec<StreamRecord> {
        let value_str = String::from_utf8_lossy(&record.value);
        if self.pattern.is_match(&value_str) {
            vec![record.clone()]
        } else {
            vec![]
        }
    }

    fn name(&self) -> &str {
        "RegexFilter"
    }
}

pub struct Passthrough;

impl Operator for Passthrough {
    fn process(&self, record: &StreamRecord) -> Vec<StreamRecord> {
        vec![record.clone()]
    }

    fn name(&self) -> &str {
        "Passthrough"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_record(value: &str) -> StreamRecord {
        StreamRecord {
            key: Bytes::from_static(b"k"),
            value: Bytes::from(value.to_string()),
            timestamp_ms: 1000,
            offset: 0,
            topic: "test".into(),
            partition: 0,
        }
    }

    #[test]
    fn filter_passes_matching() {
        let op = Filter::new(|r: &StreamRecord| r.value.len() > 3);
        let results = op.process(&test_record("hello"));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn filter_drops_non_matching() {
        let op = Filter::new(|r: &StreamRecord| r.value.len() > 10);
        let results = op.process(&test_record("hi"));
        assert!(results.is_empty());
    }

    #[test]
    fn map_transforms() {
        let op = Map::new(|r: &StreamRecord| {
            let mut out = r.clone();
            out.value = Bytes::from(format!("mapped:{}", String::from_utf8_lossy(&r.value)));
            out
        });
        let results = op.process(&test_record("data"));
        assert_eq!(results.len(), 1);
        assert_eq!(&results[0].value[..], b"mapped:data");
    }

    #[test]
    fn flat_map_expands() {
        let op = FlatMap::new(|r: &StreamRecord| {
            let val = String::from_utf8_lossy(&r.value);
            val.split(',')
                .map(|part| {
                    let mut out = r.clone();
                    out.value = Bytes::from(part.to_string());
                    out
                })
                .collect()
        });
        let results = op.process(&test_record("a,b,c"));
        assert_eq!(results.len(), 3);
        assert_eq!(&results[0].value[..], b"a");
        assert_eq!(&results[1].value[..], b"b");
        assert_eq!(&results[2].value[..], b"c");
    }

    #[test]
    fn regex_filter_matches() {
        let op = RegexFilter::new(r"^\d+$").unwrap();
        assert_eq!(op.process(&test_record("12345")).len(), 1);
        assert!(op.process(&test_record("abc")).is_empty());
    }

    #[test]
    fn passthrough_passes_all() {
        let op = Passthrough;
        assert_eq!(op.process(&test_record("anything")).len(), 1);
    }
}
