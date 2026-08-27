mod manager;
mod model;
mod scheduler;
mod settings;
mod store;

pub use manager::DownloadManager;
pub use model::{DownloadEntry, DownloadQueueDef, DownloadSchedule, DownloadStatus};
pub use scheduler::run_forever as run_scheduler_forever;
pub use settings::{default_settings_path, Settings, SettingsStore};
pub use store::{default_data_dir, default_store_path, DownloadStore};
