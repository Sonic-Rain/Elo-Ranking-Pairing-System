

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
const MATCH_SIZE: usize = 2;

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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PreStartData {
    pub room: String,
    pub id: String,
}

pub enum RoomEventData {
    Login(UserLoginData),
    Logout(UserLogoutData),
    Create(CreateRoomData),
    Close(CloseRoomData),
    StartQueue(StartQueueData),
    CancelQueue(CancelQueueData),
    PreStart(PreStartData),
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
    let update1s = tick(Duration::from_secs(1));
    let update300ms = tick(Duration::from_millis(300));
    
    thread::spawn(move || {
        let mut TotalRoom: Vec<Rc<RefCell<RoomData>>> = vec![];
        let mut QueueRoom: Vec<Rc<RefCell<RoomData>>> = vec![];
        let mut ReadyGroups: Vec<Rc<RefCell<FightGroup>>> = vec![];
        let mut PreStartGroups: Vec<FightGame> = vec![];
        let mut GameingGroups: Vec<FightGame> = vec![];
        let mut RoomMap: HashMap<String, u32> = HashMap::new();
        let mut TotalUsers: Vec<User> = vec![];
        let mut roomCount: u32 = 0;
        loop {
            let msgtx = msgtx.clone();
            select! {
                recv(update1s) -> _ => {
                    //show(start.elapsed());
                    if QueueRoom.len() > 2 {
                        QueueRoom.sort_by_key(|x| x.borrow().avg_rk);
                        let mut g: FightGroup = Default::default();
                        for i in 0..QueueRoom.len() {
                            if QueueRoom[i].borrow().ready == 0 &&
                                QueueRoom[i].borrow().users.len() as u16 + g.user_count <= TEAM_SIZE {
                                QueueRoom[i].borrow_mut().ready = 1;
                                g.add_room(Rc::clone(&QueueRoom[i]));
                            }
                            if g.user_count == TEAM_SIZE {
                                g.prestart();
                                ReadyGroups.push(Rc::new(RefCell::new(g.clone())));
                                g = Default::default();
                            }
                        }
                    }
                    if ReadyGroups.len() >= 2 {
                        let mut fg: FightGame = Default::default();
                        for rg in &mut ReadyGroups {
                            if rg.borrow().game_status == 0 && fg.teams.len() < MATCH_SIZE {
                                fg.teams.push(Rc::clone(rg));
                            }
                            if fg.teams.len() == MATCH_SIZE {
                                for g in &mut fg.teams {
                                    let gstatus = g.borrow().game_status;
                                    if gstatus == 0 {
                                        for r in &mut g.borrow_mut().rooms {
                                            r.borrow_mut().ready = 2;
                                        }
                                    }
                                    g.borrow_mut().game_status = 1;
                                }
                                fg.update_names();
                                for r in &fg.room_names {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/prestart", r), msg: r#"{"msg":"go"}"#.to_string()});
                                }
                                PreStartGroups.push(fg.clone());
                                fg = Default::default();
                            }
                        }
                    }
                }
                recv(update300ms) -> _ => {
                    let mut i = 0;
                    while i != PreStartGroups.len() {
                        if PreStartGroups[i].check_prestart() {
                            let mut start_group = PreStartGroups.remove(i);
                            start_group.ready();
                            start_group.update_names();
                            for r in &start_group.room_names {
                                msgtx.send(MqttMsg{topic:format!("room/{}/res/start", r), msg: r#"{"msg":"go"}"#.to_string()});
                            }
                            GameingGroups.push(start_group);
                            println!("{:#?}", GameingGroups);
                        }
                        else {
                            i += 1;
                        }
                    }
                }
                recv(rx) -> d => {
                    if let Ok(d) = d {
                        match d {
                            RoomEventData::PreStart(x) => {
                                for r in &mut ReadyGroups {
                                    let mut rr = r.borrow_mut();
                                    if rr.check_has_room(&x.room) {
                                        rr.user_ready(&x.id);
                                        break;
                                    }
                                }
                                //println!("{:#?}", ReadyGroups);
                            },
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
                                //println!("{:#?}", QueueRoom);
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
                                //println!("{:#?}", TotalUsers);
                            },
                            RoomEventData::Logout(x) => {
                                for i in 0..TotalUsers.len() {
                                    if TotalUsers[i].id == x.id {
                                        TotalUsers.remove(i);
                                        break;
                                    }
                                }
                                //println!("{:#?}", TotalUsers);
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
                                    ready: 0,
                                };
                                for i in 0..TotalUsers.len() {
                                    if TotalUsers[i].id == x.id {
                                        new_room.add_user(&TotalUsers[i]);
                                    }
                                }
                                //TotalRoom.push(new_room);
                                TotalRoom.push(Rc::new(RefCell::new(new_room)));
                                //println!("TotalRoom {:#?}", TotalRoom);
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
                                //println!("{:#?}", TotalRoom);
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
    sender.send(RoomEventData::Create(CreateRoomData{id: data.id.clone()}));
    let publish_packet = PublishPacket::new(TopicName::new(format!("room/{}/res/create", id)).unwrap(), QoSWithPacketIdentifier::Level0, "{\"msg\":\"ok\"}".to_string());
    let mut buf = Vec::new();
    publish_packet.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();
    Ok(())
}

pub fn close(stream: &mut std::net::TcpStream, id: String, v: Value, pool: mysql::Pool, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: CloseRoomData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn().unwrap();
    sender.send(RoomEventData::Close(CloseRoomData{id: data.id.clone()}));
    let publish_packet = PublishPacket::new(TopicName::new(format!("room/{}/res/close", id)).unwrap(), QoSWithPacketIdentifier::Level0, "{\"msg\":\"ok\"}".to_string());
    let mut buf = Vec::new();
    publish_packet.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();
    Ok(())
}

pub fn start_queue(stream: &mut std::net::TcpStream, id: String, v: Value, pool: mysql::Pool, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: StartQueueData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn().unwrap();
    sender.send(RoomEventData::StartQueue(StartQueueData{id: data.id.clone()}));
    let publish_packet = PublishPacket::new(TopicName::new(format!("room/{}/res/start_queue", id)).unwrap(), QoSWithPacketIdentifier::Level0, "{\"msg\":\"ok\"}".to_string());
    let mut buf = Vec::new();
    publish_packet.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();
    Ok(())
}

pub fn cancel_queue(stream: &mut std::net::TcpStream, id: String, v: Value, pool: mysql::Pool, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: CancelQueueData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn().unwrap();
    sender.send(RoomEventData::CancelQueue(CancelQueueData{id: data.id.clone()}));
    let publish_packet = PublishPacket::new(TopicName::new(format!("room/{}/res/cancel_queue", id)).unwrap(), QoSWithPacketIdentifier::Level0, "{\"msg\":\"ok\"}".to_string());
    let mut buf = Vec::new();
    publish_packet.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();
    Ok(())
}


pub fn prestart(stream: &mut std::net::TcpStream, id: String, v: Value, pool: mysql::Pool, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: PreStartData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn().unwrap();
    sender.send(RoomEventData::PreStart(PreStartData{id: data.id.clone(), room: data.room.clone()}));
    Ok(())
}
