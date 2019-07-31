

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
use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;

use crate::room::*;
use crate::msg::*;

const TEAM_SIZE: u16 = 5;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateRoomData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CloseRoomData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserLoginData {
    pub u: User,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserLogoutData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StartQueueData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CancelQueueData {
    pub id: String,
}

pub enum RoomEventData {
    Login(UserLoginData),
    Logout(UserLogoutData),
    Create(CreateRoomData),
    Close(CloseRoomData),
    StartQueue(StartQueueData),
    CancelQueue(CancelQueueData),
}

// Prints the elapsed time.
fn show(dur: Duration) {
    println!(
        "Elapsed: {}.{:03} sec",
        dur.as_secs(),
        dur.subsec_nanos() / 1_000_000
    );
}

pub fn init(msgtx: Sender<MqttMsg>) -> Sender<RoomEventData> {
    
    let (tx, rx):(Sender<RoomEventData>, Receiver<RoomEventData>) = bounded(1000);
    let start = Instant::now();
    let update = tick(Duration::from_secs(1));
    
    thread::spawn(move || {
        let mut TotalRoom: Vec<Rc<RefCell<RoomData>>> = vec![];
        let mut QueueRoom: Vec<Rc<RefCell<RoomData>>> = vec![];
        let mut GameGroups: Vec<FightGroup> = vec![];
        let mut RoomMap: HashMap<String, u32> = HashMap::new();
        let mut TotalUsers: Vec<User> = vec![];
        let mut roomCount: u32 = 0;
        loop {
            let msgtx = msgtx.clone();
            select! {
                recv(update) -> _ => {
                    //show(start.elapsed());
                    if QueueRoom.len() > 2 {
                        QueueRoom.sort_by_key(|x| x.borrow().avg_rk);
                        let mut g: FightGroup = Default::default();
                        for i in 0..QueueRoom.len() {
                            if !QueueRoom[i].borrow().ready &&
                                QueueRoom[i].borrow().users.len() as u16 + g.user_count <= TEAM_SIZE {
                                g.add_room(Rc::clone(&QueueRoom[i]));
                            }
                            if g.user_count == TEAM_SIZE {
                                for r in &mut g.rooms {
                                    r.borrow_mut().ready = true;
                                    for u in &r.borrow().users {
                                        msgtx.send(MqttMsg{topic:format!("room/()/res/prestart"), msg: r#"{"msg":"go"}"#.to_string()});
                                    }
                                }
                                GameGroups.push(g.clone());
                                g = Default::default();
                            }
                        }
                    }
                }
                recv(rx) -> d => {
                    if let Ok(d) = d {
                        match d {
                            RoomEventData::StartQueue(x) => {
                                for r in &QueueRoom {
                                    if r.borrow().master == x.id {
                                        return;
                                    }
                                }
                                for r in &TotalRoom {
                                    if r.borrow().master == x.id {
                                        QueueRoom.push((*r).clone());
                                        break;
                                    }
                                }
                                println!("{:#?}", QueueRoom);
                            },
                            RoomEventData::CancelQueue(x) => {
                                for i in 0..QueueRoom.len() {
                                    if QueueRoom[i].borrow().master == x.id {
                                        QueueRoom.remove(i);
                                        break;
                                    }
                                }
                                println!("{:#?}", QueueRoom);
                            },
                            RoomEventData::Login(x) => {
                                if !TotalUsers.contains(&x.u) {
                                    TotalUsers.push(x.u);
                                }
                                println!("{:#?}", TotalUsers);
                            },
                            RoomEventData::Logout(x) => {
                                for i in 0..TotalUsers.len() {
                                    if TotalUsers[i].id == x.id {
                                        TotalUsers.remove(i);
                                        break;
                                    }
                                }
                                println!("{:#?}", TotalUsers);
                            },
                            RoomEventData::Create(x) => {
                                roomCount += 1;
                                RoomMap.insert(
                                    x.id.clone(),
                                    roomCount,
                                );
                                let mut new_room = RoomData {
                                    rid: roomCount,
                                    users: vec![],
                                    master: x.id.clone(),
                                    avg_ng: 0,
                                    avg_rk: 0,
                                    ready: false,
                                };
                                for i in 0..TotalUsers.len() {
                                    if TotalUsers[i].id == x.id {
                                        new_room.add_user(&TotalUsers[i]);
                                    }
                                }
                                //TotalRoom.push(new_room);
                                TotalRoom.push(Rc::new(RefCell::new(new_room)));
                                println!("TotalRoom {:#?}", TotalRoom);
                            },
                            RoomEventData::Close(x) => {
                                if let Some(y) =  RoomMap.get(&x.id) {
                                    let mut i = 0;
                                    while i != TotalRoom.len() {
                                        if TotalRoom[i].borrow().rid == *y {
                                            TotalRoom.remove(i);
                                        } else {
                                            i += 1;
                                        }
                                    }
                                }
                                println!("{:#?}", TotalRoom);
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
    let data: CreateRoomData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn().unwrap();
    sender.send(RoomEventData::Create(CreateRoomData{id: id.clone()}));
    let publish_packet = PublishPacket::new(TopicName::new(format!("room/{}/res/create", id)).unwrap(), QoSWithPacketIdentifier::Level0, "{\"msg\":\"ok\"}".to_string());
    let mut buf = Vec::new();
    publish_packet.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();
    Ok(())
}

pub fn close(stream: &mut std::net::TcpStream, id: String, v: Value, pool: mysql::Pool, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: CreateRoomData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn().unwrap();
    sender.send(RoomEventData::Close(CloseRoomData{id: id.clone()}));
    let publish_packet = PublishPacket::new(TopicName::new(format!("room/{}/res/close", id)).unwrap(), QoSWithPacketIdentifier::Level0, "{\"msg\":\"ok\"}".to_string());
    let mut buf = Vec::new();
    publish_packet.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();
    Ok(())
}

pub fn start_queue(stream: &mut std::net::TcpStream, id: String, v: Value, pool: mysql::Pool, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: CreateRoomData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn().unwrap();
    sender.send(RoomEventData::StartQueue(StartQueueData{id: id.clone()}));
    let publish_packet = PublishPacket::new(TopicName::new(format!("room/{}/res/start_queue", id)).unwrap(), QoSWithPacketIdentifier::Level0, "{\"msg\":\"ok\"}".to_string());
    let mut buf = Vec::new();
    publish_packet.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();
    Ok(())
}

pub fn cancel_queue(stream: &mut std::net::TcpStream, id: String, v: Value, pool: mysql::Pool, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: CreateRoomData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn().unwrap();
    sender.send(RoomEventData::CancelQueue(CancelQueueData{id: id.clone()}));
    let publish_packet = PublishPacket::new(TopicName::new(format!("room/{}/res/cancel_queue", id)).unwrap(), QoSWithPacketIdentifier::Level0, "{\"msg\":\"ok\"}".to_string());
    let mut buf = Vec::new();
    publish_packet.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();
    Ok(())
}

