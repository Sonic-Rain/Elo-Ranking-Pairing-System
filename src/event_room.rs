

use mqtt;
use mqtt::packet::*;
use serde_json::{self, Result, Value};
use mqtt::{Decodable, Encodable, QualityOfService};
use mqtt::{TopicFilter, TopicName};
use std::env;
use std::io::{self, Write};
use serde_derive::{Serialize, Deserialize};
use std::io::{Error, ErrorKind};
use log::{info, warn, error, trace};
use std::thread;
use std::time::{Duration, Instant};

use ::futures::Future;
use mysql;
use std::sync::{Arc, Mutex, Condvar, RwLock};
use crossbeam_channel::{bounded, tick, Sender, Receiver, select};

use crate::room::*;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateRoomData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CloseRoomData {
    pub rid: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserLoginData {
    pub u: User,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserLogoutData {
    pub id: String,
}

pub enum RoomEventData {
    Login(UserLoginData),
    Logout(UserLogoutData),
    Create(CreateRoomData),
    Close(CloseRoomData),
}

// Prints the elapsed time.
fn show(dur: Duration) {
    println!(
        "Elapsed: {}.{:03} sec",
        dur.as_secs(),
        dur.subsec_nanos() / 1_000_000
    );
}

pub fn init() -> Sender<RoomEventData> {
    let mut TotalRoom: Vec<RoomData> = vec![];
    let mut TotalUsers: Vec<User> = vec![];
    let (tx, rx):(Sender<RoomEventData>, Receiver<RoomEventData>) = bounded(1000);
    let start = Instant::now();
    let update = tick(Duration::from_secs(1));
    thread::spawn(move || {
        let mut roomCount = 0;
        loop {
            select! {
                recv(update) -> _ => {
                    //show(start.elapsed());
                }
                recv(rx) -> d => {
                    if let Ok(d) = d {
                        match d {
                            RoomEventData::Login(x) => {
                                println!("{:?}", x);
                                TotalUsers.push(x.u);
                                println!("{:?}", TotalUsers);
                            },
                            RoomEventData::Logout(x) => {
                                println!("{:?}", x);
                                for i in 0..TotalUsers.len() {
                                    if TotalUsers[i].id == x.id {
                                        TotalUsers.remove(i);
                                        break;
                                    }
                                }
                                println!("{:?}", TotalUsers);
                            },
                            RoomEventData::Create(x) => {
                                println!("{:?}", x);
                                roomCount += 1;
                                let mut new_room = RoomData {
                                    rid: roomCount,
                                    users: vec![],
                                    avg_ng: 0,
                                    avg_rk: 0,
                                };
                                for i in 0..TotalUsers.len() {
                                    if TotalUsers[i].id == x.id {
                                        new_room.add_user(&TotalUsers[i]);
                                    }
                                }
                                TotalRoom.push(new_room);
                                println!("TotalRoom {:?}", TotalRoom);
                            },
                            RoomEventData::Close(x) => {
                                println!("{:?}", x);
                                let mut i = 0;
                                while i != TotalRoom.len() {
                                    if TotalRoom[i].rid == x.rid {
                                        TotalRoom.remove(i);
                                    } else {
                                        i += 1;
                                    }
                                }
                                println!("{:?}", TotalRoom);
                            },
                        }
                    }
                }
            }
        }
    });
    tx
}

pub fn create(stream: &mut std::net::TcpStream, id: String, v: Value, pool: mysql::Pool, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: CreateRoomData = serde_json::from_value(v).unwrap();
    let mut conn = pool.get_conn().unwrap();
    let qres = conn.query(format!("update user set status='online' where userid='{}';", data.id));
    let publish_packet = match qres {
        Ok(_) => {
            PublishPacket::new(TopicName::new(id.clone()).unwrap(), QoSWithPacketIdentifier::Level0, "{\"msg\":\"ok\"}".to_string());
            sender.send(RoomEventData::Create(CreateRoomData{id: id}));
        },
        _=> {
            PublishPacket::new(TopicName::new(id.clone()).unwrap(), QoSWithPacketIdentifier::Level0, "{\"msg\":\"fail\"}".to_string());
        }
    };
    let mut buf = Vec::new();
    publish_packet.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();
    Ok(())
}

