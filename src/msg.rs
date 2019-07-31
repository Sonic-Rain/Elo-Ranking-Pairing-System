use serde_derive::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MqttMsg {
    pub topic: String,
    pub msg: String,
}


