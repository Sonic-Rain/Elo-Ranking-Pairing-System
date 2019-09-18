use serde_json::{self, Result, Value};
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
use std::collections::{HashMap, BTreeMap};
use std::cell::RefCell;
use std::rc::Rc;

use crate::room::*;
use crate::msg::*;
use std::process::Command;

const TEAM_SIZE: u16 = 1;
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
pub struct InviteRoomData {
    pub room: String,
    pub invite: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JoinRoomData {
    pub room: String,
    pub join: String,
}

#[derive(Clone, Debug)]
pub struct UserLoginData {
    pub u: User,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserNGHeroData {
    pub id: String,
    pub hero: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserLogoutData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StartQueueData {
    pub id: String,
    pub action: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CancelQueueData {
    pub id: String,
    pub action: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PreStartData {
    pub room: String,
    pub id: String,
    pub accept: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LeaveData {
    pub room: String,
    pub id: String,
}

pub enum RoomEventData {
    Reset(),
    Login(UserLoginData),
    Logout(UserLogoutData),
    Create(CreateRoomData),
    Close(CloseRoomData),
    ChooseNGHero(UserNGHeroData),
    Invite(InviteRoomData),
    Join(JoinRoomData),
    StartQueue(StartQueueData),
    CancelQueue(CancelQueueData),
    PreStart(PreStartData),
    Leave(LeaveData),
}

// Prints the elapsed time.
fn show(dur: Duration) {
    println!(
        "Elapsed: {}.{:03} sec",
        dur.as_secs(),
        dur.subsec_nanos() / 1_000_000
    );
}

fn SendGameList(game: Rc<RefCell<FightGame>>, msgtx: Sender<MqttMsg>, conn: &mut mysql::PooledConn) {
    pub struct ListCell {
        pub id: String,
        pub name: String,
        pub hero: String,
    }
    pub struct GameCell {
        pub game: u64,  
        pub team1: Vec<ListCell>,
        pub team2: Vec<ListCell>,
    }

    for r in &game.borrow().room_names {
        let sql = format!("select userid,name from user where userid='{}';", r);
        println!("sql: {}", sql);
        let qres = conn.query(sql.clone()).unwrap();
        let mut userid: String = "".to_owned();
        let mut name: String = "".to_owned();

        let mut count = 0;
        for row in qres {
            count += 1;
            let a = row.unwrap().clone();
            userid = mysql::from_value(a.get("userid").unwrap());
            name = mysql::from_value(a.get("name").unwrap());
            break;
        }

    }
}

fn get_rid_by_id(id: &String, users: &BTreeMap<String, Rc<RefCell<User>>>) -> u32 {
    let u = users.get(id);
    if let Some(u) = u {
        return u.borrow().rid;
    }
    return 0;
}

fn get_gid_by_id(id: &String, users: &BTreeMap<String, Rc<RefCell<User>>>) -> u32 {
    let u = users.get(id);
    if let Some(u) = u {
        return u.borrow().gid;
    }
    return 0;
}

fn get_game_id_by_id(id: &String, users: &BTreeMap<String, Rc<RefCell<User>>>) -> u32 {
    let u = users.get(id);
    if let Some(u) = u {
        return u.borrow().game_id;
    }
    return 0;
}

pub fn init(msgtx: Sender<MqttMsg>, pool: mysql::Pool) -> Sender<RoomEventData> {
    let (tx, rx):(Sender<RoomEventData>, Receiver<RoomEventData>) = bounded(1000);
    let start = Instant::now();
    let update200ms = tick(Duration::from_millis(200));
    let update100ms = tick(Duration::from_millis(100));
    
    thread::spawn(move || {
        let mut conn = pool.get_conn().unwrap();
        let mut TotalRoom: BTreeMap<u32, Rc<RefCell<RoomData>>> = BTreeMap::new();
        let mut QueueRoom: BTreeMap<u32, Rc<RefCell<RoomData>>> = BTreeMap::new();
        let mut ReadyGroups: BTreeMap<u32, Rc<RefCell<FightGroup>>> = BTreeMap::new();
        let mut PreStartGroups: BTreeMap<u32, Rc<RefCell<FightGame>>> = BTreeMap::new();
        let mut GameingGroups: BTreeMap<u32, Rc<RefCell<FightGame>>> = BTreeMap::new();
        let mut TotalUsers: BTreeMap<String, Rc<RefCell<User>>> = BTreeMap::new();
        let mut room_id: u32 = 0;
        let mut group_id: u32 = 0;
        let mut game_id: u32 = 0;
        let mut game_port: u16 = 7777;
        loop {
            select! {
                recv(update200ms) -> _ => {
                    //show(start.elapsed());
                    if QueueRoom.len() >= MATCH_SIZE {
                        let mut g: FightGroup = Default::default();
                        let mut tq: Vec<Rc<RefCell<RoomData>>> = vec![];
                        tq = QueueRoom.iter().map(|x|Rc::clone(x.1)).collect();
                        tq.sort_by_key(|x| x.borrow().avg_rk);
                        for (k, v) in &mut QueueRoom {
                            if v.borrow().ready == 0 &&
                                v.borrow().users.len() as u16 + g.user_count <= TEAM_SIZE {
                                g.add_room(Rc::clone(&v));
                            }
                            if g.user_count == TEAM_SIZE {
                                g.prestart();
                                group_id += 1;
                                g.set_group_id(group_id);
                                ReadyGroups.insert(group_id, Rc::new(RefCell::new(g.clone())));
                                g = Default::default();
                            }
                        }
                    }
                    if ReadyGroups.len() >= MATCH_SIZE {
                        let mut fg: FightGame = Default::default();
                        for (id, rg) in &mut ReadyGroups {
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
                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/prestart", r), msg: r#"{"msg":"prestart"}"#.to_string()}).unwrap();
                                }
                                game_id += 1;
                                fg.set_game_id(game_id);
                                PreStartGroups.insert(game_id, Rc::new(RefCell::new(fg)));
                                fg = Default::default();
                            }
                        }
                    }
                    // update prestart groups
                    let mut rm_ids: Vec<u32> = vec![];
                    for (id, group) in &mut PreStartGroups {
                        let res = group.borrow().check_prestart();
                        match res {
                            PrestartStatus::Ready => {
                                rm_ids.push(*id);
                                game_port += 1;
                                if game_port > 65500 {
                                    game_port = 7777;
                                }
                                group.borrow_mut().ready();
                                group.borrow_mut().update_names();
                                for r in &group.borrow().room_names {
                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/start", r), 
                                        msg: format!(r#"{{"room":"{}","msg":"start","server":"59.126.81.58:{}","game":{}}}"#, 
                                            r, game_port, id.clone())}).unwrap();
                                }
                                GameingGroups.insert(id.clone(), group.clone());
                                println!("GameingGroups {:#?}", GameingGroups);
                                println!("game_port: {}", game_port);
                                let cmd = Command::new("/home/damody/LinuxNoEditor/CF1/Binaries/Linux/CF1Server")
                                        .arg(format!("-Port={}", game_port))
                                        .spawn();
                                std::thread::sleep_ms(1000);
                                SendGameList(Rc::clone(group), msgtx.clone(), &mut conn);
                            },
                            PrestartStatus::Cancel => {
                                group.borrow_mut().update_names();
                                group.borrow_mut().clear_queue();
                                for r in &group.borrow().room_names {
                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/prestart", r), 
                                        msg: format!(r#"{{"msg":"stop queue"}}"#)}).unwrap();
                                }
                                rm_ids.push(*id);
                            },
                            PrestartStatus::Wait => {
                                
                            }
                        }
                    }
                    for k in rm_ids {
                        PreStartGroups.remove(&k);
                    }
                }
                recv(rx) -> d => {
                    if let Ok(d) = d {
                        match d {
                            RoomEventData::Leave(x) => {
                                let u = TotalUsers.get(&x.room);
                                if let Some(u) = u {
                                        let r = TotalRoom.get(&u.borrow().rid);
                                        if let Some(r) = r {
                                            r.borrow_mut().rm_user(&x.id);
                                            r.borrow().publish_update(&msgtx);
                                            msgtx.try_send(MqttMsg{topic:format!("room/{}/res/leave", x.id), 
                                                msg: format!(r#"{{"msg":"ok"}}"#)}).unwrap();
                                        }
                                }
                            },
                            RoomEventData::ChooseNGHero(x) => {
                                let u = TotalUsers.get(&x.id);
                                if let Some(u) = u {
                                    u.borrow_mut().hero = x.hero;
                                    msgtx.try_send(MqttMsg{topic:format!("member/{}/res/choose_hero", u.borrow().id), 
                                        msg: format!(r#"{{"id":"{}", "hero":"{}"}}"#, u.borrow().id, u.borrow().hero)}).unwrap();
                                }
                            },
                            RoomEventData::Invite(x) => {
                                if TotalUsers.contains_key(&x.invite) {
                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/invite", x.invite.clone()), 
                                        msg: format!(r#"{{"room":"{}","invite":"{}"}}"#, x.room.clone(), x.invite.clone())}).unwrap();
                                }
                                println!("Invite {:#?}", x);
                            },
                            RoomEventData::Join(x) => {
                                let u = TotalUsers.get(&x.room);
                                let j = TotalUsers.get(&x.join);
                                if let Some(u) = u {
                                    if let Some(j) = j {
                                        let r = TotalRoom.get(&u.borrow().rid);
                                        if let Some(r) = r {
                                            r.borrow_mut().add_user(Rc::clone(j));
                                            r.borrow().publish_update(&msgtx);
                                            msgtx.try_send(MqttMsg{topic:format!("room/{}/res/join", x.join.clone()), 
                                                msg: format!(r#"{{"room":"{}","msg":"ok"}}"#, x.room.clone())}).unwrap();
                                        }
                                    }
                                }
                                println!("TotalRoom {:#?}", TotalRoom);
                            },
                            RoomEventData::Reset() => {
                                TotalRoom.clear();
                                QueueRoom.clear();
                                ReadyGroups.clear();
                                PreStartGroups.clear();
                                GameingGroups.clear();
                                TotalUsers.clear();
                                room_id = 0;
                            },
                            RoomEventData::PreStart(x) => {
                                let u = TotalUsers.get(&x.room);
                                if let Some(u) = u {
                                    let gid = u.borrow().gid;
                                    if gid != 0 {
                                        let g = ReadyGroups.get(&gid);
                                        if let Some(gr) = g {
                                            if x.accept == true {
                                                gr.borrow_mut().user_ready(&x.id);
                                            } else {
                                                gr.borrow_mut().user_cancel(&x.id);
                                                ReadyGroups.remove(&gid);
                                                let r = QueueRoom.remove(&u.borrow().rid);
                                                if let Some(r) = r {
                                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master), 
                                                        msg: format!(r#"{{"msg":"ok"}}"#)}).unwrap();
                                                }
                                            }
                                        }
                                        info!("ReadyGroups: {:#?}", ReadyGroups);
                                    }
                                }
                            },
                            RoomEventData::StartQueue(x) => {
                                let mut success = false;
                                let mut hasRoom = false;
                                let u = TotalUsers.get(&x.id);
                                let mut rid = 0;
                                if let Some(u) = u {
                                    if u.borrow().rid != 0 {
                                        hasRoom = true;
                                        rid = u.borrow().rid;
                                    }
                                }
                                if hasRoom {
                                    let r = TotalRoom.get(&rid);
                                    if let Some(y) = r {
                                        QueueRoom.insert(
                                            y.borrow().rid,
                                            Rc::clone(y)
                                        );
                                        success = true;
                                        if success {
                                            msgtx.try_send(MqttMsg{topic:format!("room/{}/res/start_queue", y.borrow().master.clone()), 
                                                msg: format!(r#"{{"msg":"ok"}}"#)}).unwrap();
                                        } else {
                                            msgtx.try_send(MqttMsg{topic:format!("room/{}/res/start_queue", y.borrow().master.clone()), 
                                                msg: format!(r#"{{"msg":"fail"}}"#)}).unwrap();
                                        }
                                    }
                                }
                                info!("QueueRoom: {:#?}", QueueRoom);
                            },
                            RoomEventData::CancelQueue(x) => {
                                let mut success = false;
                                let u = TotalUsers.get(&x.id);
                                if let Some(u) = u {
                                    let r = QueueRoom.remove(&u.borrow().rid);
                                    if let Some(r) = r {
                                        success = true;
                                        if success {
                                            msgtx.try_send(MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master.clone()), 
                                                msg: format!(r#"{{"msg":"ok"}}"#)}).unwrap();
                                        } else {
                                            msgtx.try_send(MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master.clone()), 
                                                msg: format!(r#"{{"msg":"fail"}}"#)}).unwrap();
                                        }
                                    }
                                }
                                info!("QueueRoom: {:#?}", QueueRoom);
                            },
                            RoomEventData::Login(x) => {
                                let mut success = true;
                                if TotalUsers.contains_key(&x.u.id) {
                                    success = false;
                                }
                                if success {
                                    TotalUsers.insert(x.u.id.clone(), Rc::new(RefCell::new(x.u.clone())));
                                    msgtx.try_send(MqttMsg{topic:format!("member/{}/res/login", x.u.id.clone()), 
                                        msg: format!(r#"{{"msg":"ok"}}"#)}).unwrap();
                                } else {
                                    msgtx.try_send(MqttMsg{topic:format!("member/{}/res/login", x.u.id.clone()), 
                                        msg: format!(r#"{{"msg":"fail"}}"#)}).unwrap();
                                }
                            },
                            RoomEventData::Logout(x) => {
                                let mut success = false;
                                let u = TotalUsers.get(&x.id);
                                if let Some(u) = u {
                                    let gid = u.borrow().gid;
                                    let rid = u.borrow().rid;
                                    if gid != 0 {
                                        let g = ReadyGroups.get(&gid);
                                        if let Some(gr) = g {
                                            gr.borrow_mut().user_cancel(&x.id);
                                            ReadyGroups.remove(&gid);
                                            let r = QueueRoom.remove(&rid);
                                            if let Some(r) = r {
                                                msgtx.try_send(MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master), 
                                                    msg: format!(r#"{{"msg":"ok"}}"#)}).unwrap();
                                            }
                                            TotalRoom.remove(&rid);
                                        }
                                    }
                                }
                                let mut u = TotalUsers.remove(&x.id);
                                if let Some(u) = u {
                                    success = true;
                                }
                                if success {
                                    msgtx.try_send(MqttMsg{topic:format!("member/{}/res/logout", x.id.clone()), 
                                        msg: format!(r#"{{"msg":"ok"}}"#)}).unwrap();
                                } else {
                                    msgtx.try_send(MqttMsg{topic:format!("member/{}/res/logout", x.id.clone()), 
                                        msg: format!(r#"{{"msg":"fail"}}"#)}).unwrap();
                                }
                            },
                            RoomEventData::Create(x) => {
                                let mut success = false;
                                if !TotalRoom.contains_key(&get_rid_by_id(&x.id, &TotalUsers)) {
                                    room_id += 1;
                                    
                                    let mut new_room = RoomData {
                                        rid: room_id,
                                        users: vec![],
                                        master: x.id.clone(),
                                        avg_ng: 0,
                                        avg_rk: 0,
                                        ready: 0,
                                    };
                                    let mut u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        new_room.add_user(Rc::clone(&u));
                                        let rid = new_room.rid;
                                        let r = Rc::new(RefCell::new(new_room));
                                        TotalRoom.insert(
                                            rid,
                                            Rc::clone(&r),
                                        );
                                        success = true;
                                    }
                                }
                                if success {
                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/create", x.id.clone()), 
                                        msg: format!(r#"{{"msg":"ok"}}"#)}).unwrap();
                                } else {
                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/create", x.id.clone()), 
                                        msg: format!(r#"{{"msg":"fail"}}"#)}).unwrap();
                                }
                            },
                            RoomEventData::Close(x) => {
                                let mut success = false;
                                if let Some(y) =  TotalRoom.remove(&get_rid_by_id(&x.id, &TotalUsers)) {
                                    let data = TotalRoom.remove(&get_rid_by_id(&x.id, &TotalUsers));
                                    y.borrow_mut().close();
                                    match data {
                                        Some(_) => {
                                            QueueRoom.remove(&get_rid_by_id(&x.id, &TotalUsers));
                                            success = true;
                                        },
                                        _ => {}
                                    }
                                }
                                if success {
                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/cancel_queue", x.id.clone()), 
                                        msg: format!(r#"{{"msg":"ok"}}"#)}).unwrap();
                                } else {
                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/cancel_queue", x.id.clone()), 
                                        msg: format!(r#"{{"msg":"fail"}}"#)}).unwrap();
                                }
                            },
                        }
                    }
                }
            }
        }
    });
    tx
}

pub fn create(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: CreateRoomData = serde_json::from_value(v)?;
    sender.send(RoomEventData::Create(CreateRoomData{id: data.id.clone()}));
    Ok(())
}

pub fn close(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: CloseRoomData = serde_json::from_value(v)?;
    sender.send(RoomEventData::Close(CloseRoomData{id: data.id.clone()}));
    Ok(())
}

pub fn start_queue(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: StartQueueData = serde_json::from_value(v)?;
    sender.send(RoomEventData::StartQueue(StartQueueData{id: data.id.clone(), action: data.action.clone()}));
    Ok(())
}

pub fn cancel_queue(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: CancelQueueData = serde_json::from_value(v)?;
    sender.send(RoomEventData::CancelQueue(data));
    Ok(())
}

pub fn prestart(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: PreStartData = serde_json::from_value(v)?;
    sender.send(RoomEventData::PreStart(data));
    Ok(())
}

pub fn join(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: JoinRoomData = serde_json::from_value(v)?;
    sender.send(RoomEventData::Join(data));
    Ok(())
}

pub fn choose_ng_hero(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: UserNGHeroData = serde_json::from_value(v)?;
    sender.send(RoomEventData::ChooseNGHero(data));
    Ok(())
}

pub fn invite(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: InviteRoomData = serde_json::from_value(v)?;
    sender.send(RoomEventData::Invite(data));
    Ok(())
}
