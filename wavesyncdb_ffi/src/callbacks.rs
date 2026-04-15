use crate::types::{FfiChangeNotification, FfiNetworkEvent};

#[uniffi::export(callback_interface)]
pub trait ChangeListener: Send + Sync {
    fn on_change(&self, notification: FfiChangeNotification);
}

#[uniffi::export(callback_interface)]
pub trait NetworkEventListener: Send + Sync {
    fn on_event(&self, event: FfiNetworkEvent);
}
