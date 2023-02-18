use serde::Deserialize;

use {
  log::*,
  std::collections::HashMap,
  std::fmt,
  std::sync::{atomic, Arc, Mutex, RwLock},
  tokio::time,
  warp::{Filter, Rejection, Reply},
};

#[derive(Debug)]
enum Value {
  U64 {
      value: Arc<atomic::AtomicU64>,
      metric_type: MetricType,
  },
  I64 {
      value: Arc<atomic::AtomicI64>,
      metric_type: MetricType,
  },
  Bool {
      value: Arc<Mutex<bool>>,
      metric_type: MetricType,
  },
}

#[derive(Debug, Clone)]
pub enum MetricType {
  Counter,
  Gauge,
}

impl fmt::Display for MetricType {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
      match self {
          MetricType::Counter => {
              write!(f, "counter")
          }
          MetricType::Gauge => {
              write!(f, "gauge")
          }
      }
  }
}

#[derive(Debug)]
enum PrevValue {
  U64(u64),
  I64(i64),
  Bool(bool),
}

#[derive(Clone)]
pub struct MetricU64 {
  value: Arc<atomic::AtomicU64>,
}
impl MetricU64 {
  pub fn value(&self) -> u64 {
      self.value.load(atomic::Ordering::Acquire)
  }

  pub fn set(&mut self, value: u64) {
      self.value.store(value, atomic::Ordering::Release);
  }

  pub fn set_max(&mut self, value: u64) {
      self.value.fetch_max(value, atomic::Ordering::AcqRel);
  }

  pub fn add(&mut self, value: u64) {
      self.value.fetch_add(value, atomic::Ordering::AcqRel);
  }

  pub fn increment(&mut self) {
      self.value.fetch_add(1, atomic::Ordering::AcqRel);
  }

  pub fn decrement(&mut self) {
      self.value.fetch_sub(1, atomic::Ordering::AcqRel);
  }
}

#[derive(Clone)]
pub struct MetricI64 {
  value: Arc<atomic::AtomicI64>,
}
impl MetricI64 {
  pub fn set(&mut self, value: i64) {
      self.value.store(value, atomic::Ordering::Release);
  }

  pub fn increment(&mut self) {
      self.value.fetch_add(1, atomic::Ordering::AcqRel);
  }

  pub fn decrement(&mut self) {
      self.value.fetch_sub(1, atomic::Ordering::AcqRel);
  }
}

#[derive(Clone)]
pub struct MetricBool {
  value: Arc<Mutex<bool>>,
}

impl MetricBool {
  pub fn set(&self, value: bool) {
      *self.value.lock().unwrap() = value;
  }
}

#[derive(Clone)]
pub struct Metrics {
  registry: Arc<RwLock<HashMap<String, Value>>>,
  labels: HashMap<String, String>,
}

impl Metrics {
  pub fn register_u64(&self, name: String, metric_type: MetricType) -> MetricU64 {
      let mut registry = self.registry.write().unwrap();
      let value = registry.entry(name).or_insert(Value::U64 {
          value: Arc::new(atomic::AtomicU64::new(0)),
          metric_type: metric_type,
      });
      MetricU64 {
          value: match value {
              Value::U64 {
                  value: v,
                  metric_type: _,
              } => v.clone(),
              _ => panic!("bad metric type"),
          },
      }
  }

  pub fn register_i64(&self, name: String, metric_type: MetricType) -> MetricI64 {
      let mut registry = self.registry.write().unwrap();
      let value = registry.entry(name).or_insert(Value::I64 {
          value: Arc::new(atomic::AtomicI64::new(0)),
          metric_type: metric_type,
      });
      MetricI64 {
          value: match value {
              Value::I64 {
                  value: v,
                  metric_type: _,
              } => v.clone(),
              _ => panic!("bad metric type"),
          },
      }
  }

  pub fn register_bool(&self, name: String) -> MetricBool {
      let mut registry = self.registry.write().unwrap();
      let value = registry.entry(name).or_insert(Value::Bool {
          value: Arc::new(Mutex::new(false)),
          metric_type: MetricType::Gauge,
      });
      MetricBool {
          value: match value {
              Value::Bool {
                  value: v,
                  metric_type: _,
              } => v.clone(),
              _ => panic!("bad metric type"),
          },
      }
  }

  pub fn get_registry_vec(&self) -> Vec<(String, String, String)> {
      let mut vec: Vec<(String, String, String)> = Vec::new();
      let metrics = self.registry.read().unwrap();
      for (name, value) in metrics.iter() {
          let (value_str, type_str) = match value {
              Value::U64 {
                  value: v,
                  metric_type: t,
              } => (
                  format!("{}", v.load(atomic::Ordering::Acquire)),
                  t.to_string(),
              ),
              Value::I64 {
                  value: v,
                  metric_type: t,
              } => (
                  format!("{}", v.load(atomic::Ordering::Acquire)),
                  t.to_string(),
              ),
              Value::Bool {
                  value: v,
                  metric_type: t,
              } => {
                  let bool_to_int = if *v.lock().unwrap() { 1 } else { 0 };
                  (format!("{}", bool_to_int), t.to_string())
              }
          };
          vec.push((name.clone(), value_str, type_str));
      }
      vec
  }
}

async fn handle_prometheus_poll(metrics: Metrics) -> Result<impl Reply, Rejection> {
  debug!("handle_prometheus_poll");
  let label_strings_vec: Vec<String> = metrics
      .labels
      .iter()
      .map(|(name, value)| format!("{}=\"{}\"", name, value))
      .collect();
  let lines: Vec<String> = metrics
      .get_registry_vec()
      .iter()
      .map(|(name, value, type_name)| {
          let sanitized_name = str::replace(name, "-", "_");
          format!(
              "# HELP {} \n# TYPE {} {}\n{}{{{}}} {}",
              sanitized_name,
              sanitized_name,
              type_name,
              sanitized_name,
              label_strings_vec.join(","),
              value
          )
      })
      .collect();
  Ok(format!("{}\n", lines.join("\n")))
}

pub fn with_metrics(
  metrics: Metrics,
) -> impl Filter<Extract = (Metrics,), Error = std::convert::Infallible> + Clone {
  warp::any().map(move || metrics.clone())
}

#[derive(Clone, Debug, Deserialize)]
pub struct MetricsConfig {
    pub output_stdout: bool,
    pub output_http: bool,
    // TODO: add configurable port and endpoint url
    // TODO: add configurable write interval
}


pub fn start(config: MetricsConfig, process_name: String) -> Metrics {
  let mut write_interval = time::interval(time::Duration::from_secs(60));

  let registry = Arc::new(RwLock::new(HashMap::<String, Value>::new()));
  let registry_c = Arc::clone(&registry);
  let labels = HashMap::from([(String::from("process"), process_name)]);
  let metrics_tx = Metrics { registry, labels };
  let metrics_route = warp::path!("metrics")
      .and(with_metrics(metrics_tx.clone()))
      .and_then(handle_prometheus_poll);

  if config.output_http {
      // serve prometheus metrics endpoint
      tokio::spawn(async move {
          warp::serve(metrics_route).run(([0, 0, 0, 0], 9091)).await;
      });
  }

  if config.output_stdout {
      // periodically log to stdout
      tokio::spawn(async move {
          let mut previous_values = HashMap::<String, PrevValue>::new();
          loop {
              write_interval.tick().await;

              // Nested locking! Safe because the only other user locks registry for writing and doesn't
              // acquire any interior locks.
              let metrics = registry_c.read().unwrap();
              for (name, value) in metrics.iter() {
                  let previous_value = previous_values.get_mut(name);
                  match value {
                      Value::U64 {
                          value: v,
                          metric_type: _,
                      } => {
                          let new_value = v.load(atomic::Ordering::Acquire);
                          let previous_value = if let Some(PrevValue::U64(v)) = previous_value {
                              let prev = *v;
                              *v = new_value;
                              prev
                          } else {
                              previous_values.insert(name.clone(), PrevValue::U64(new_value));
                              0
                          };
                          let diff = new_value.wrapping_sub(previous_value) as i64;
                          info!("metric: {}: {} ({:+})", name, new_value, diff);
                      }
                      Value::I64 {
                          value: v,
                          metric_type: _,
                      } => {
                          let new_value = v.load(atomic::Ordering::Acquire);
                          let previous_value = if let Some(PrevValue::I64(v)) = previous_value {
                              let prev = *v;
                              *v = new_value;
                              prev
                          } else {
                              previous_values.insert(name.clone(), PrevValue::I64(new_value));
                              0
                          };
                          let diff = new_value - previous_value;
                          info!("metric: {}: {} ({:+})", name, new_value, diff);
                      }
                      Value::Bool {
                          value: v,
                          metric_type: _,
                      } => {
                          let new_value = v.lock().unwrap();
                          let previous_value = if let Some(PrevValue::Bool(v)) = previous_value {
                              let mut prev = new_value.clone();
                              std::mem::swap(&mut prev, v);
                              prev
                          } else {
                              previous_values
                                  .insert(name.clone(), PrevValue::Bool(new_value.clone()));
                              false
                          };
                          if *new_value == previous_value {
                              info!("metric: {}: {} (unchanged)", name, &*new_value);
                          } else {
                              info!(
                                  "metric: {}: {} (before: {})",
                                  name, &*new_value, previous_value
                              );
                          }
                      }
                  }
              }
          }
      });
  }

  metrics_tx
}
