use serde_json::{self, Value};
use std::env;
use std::io::{self, Write};
use serde_derive::{Serialize, Deserialize};
use serde_json::json;
use std::io::{ErrorKind};
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
use failure::Error;

use crate::room::*;
use crate::msg::*;
use crate::elo::*;
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
    pub from: String,
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

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct StartGameData {
    pub game: u32,
    pub action: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct StartGameSendData {
    pub game: u32,  
    pub member: Vec<HeroCell>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct HeroCell {
    pub id: String,
    pub team: u16,
    pub name: String,
    pub hero: String,
    pub buff: BTreeMap<String, f32>,
    pub tags: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GameOverData {
    pub game: u32,  
    pub win: Vec<String>,
    pub lose: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GameCloseData {
    pub game: u32,
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
    StartGame(StartGameData),
    GameOver(GameOverData),
    GameClose(GameCloseData),
}

// Prints the elapsed time.
fn show(dur: Duration) {
    println!(
        "Elapsed: {}.{:03} sec",
        dur.as_secs(),
        dur.subsec_nanos() / 1_000_000
    );
}

fn SendGameList(game: &Rc<RefCell<FightGame>>, msgtx: &Sender<MqttMsg>, conn: &mut mysql::PooledConn)
    -> Result<(), Error> {
    let mut res: StartGameSendData = Default::default();
    res.game = game.borrow().game_id;
    for (i, t) in game.borrow().teams.iter().enumerate() {
        let ids = t.borrow().get_users_id_hero();
        for (id, hero) in &ids {
            let sql = format!("select userid,name from user where userid='{}';", id);
            println!("sql: {}", sql);
            let qres = conn.query(sql.clone())?;
            let mut userid: String = "".to_owned();
            let mut name: String = "".to_owned();

            let mut count = 0;
            for row in qres {
                count += 1;
                let a = row?.clone();
                userid = mysql::from_value(a.get("userid").unwrap());
                name = mysql::from_value(a.get("name").unwrap());
                let h: HeroCell = HeroCell {id:id.clone(), team: i as u16, name:name, hero:hero.clone(), ..Default::default() };
                res.member.push(h);
                break;
            }
        }
    }
    info!("{:?}", res);
    msgtx.try_send(MqttMsg{topic:format!("game/{}/res/start_game", res.game), 
                                        msg: json!(res).to_string()})?;
    Ok(())
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

fn get_user(id: &String, users: &BTreeMap<String, Rc<RefCell<User>>>) -> Option<Rc<RefCell<User>>> {
    let u = users.get(id);
    if let Some(u) = u {
        return Some(Rc::clone(u))
    }
    None
}

fn get_users(ids: &Vec<String>, users: &BTreeMap<String, Rc<RefCell<User>>>) -> Result<Vec<Rc<RefCell<User>>>, Error> {
    let mut res: Vec<Rc<RefCell<User>>> = vec![];
    for id in ids {
        let u = get_user(id, users);
        if let Some(u) = u {
            res.push(u);
        }
    }
    if ids.len() == res.len() {
        Ok(res)
    }
    else {
        Err(failure::err_msg("some user not found"))
    }
}

fn user_score(u: &Rc<RefCell<User>>, value: i16, msgtx: &Sender<MqttMsg>, conn: &mut mysql::PooledConn) -> Result<(), Error> {
    u.borrow_mut().ng += value;
    msgtx.try_send(MqttMsg{topic:format!("member/{}/res/login", u.borrow().id), 
        msg: format!(r#"{{"msg":"ok", "ng":{}, "rk":{} }}"#, u.borrow().ng, u.borrow().rk)})?;
    let sql = format!("UPDATE user_ng as a JOIN user as b ON a.id=b.id SET score={} WHERE b.userid='{}';", u.borrow().ng, u.borrow().id);
    println!("sql: {}", sql);
    let qres = conn.query(sql.clone())?;
    Ok(())
}

fn get_ng(team : &Vec<Rc<RefCell<User>>>) -> Vec<i32> {
    let mut res: Vec<i32> = vec![];
    for u in team {
        res.push(u.borrow().ng.into());
    }
    res
}

fn get_rk(team : &Vec<Rc<RefCell<User>>>) -> Vec<i32> {
    let mut res: Vec<i32> = vec![];
    for u in team {
        res.push(u.borrow().rk.into());
    }
    res
}

fn settlement_ng_score(win: &Vec<Rc<RefCell<User>>>, lose: &Vec<Rc<RefCell<User>>>, msgtx: &Sender<MqttMsg>, conn: &mut mysql::PooledConn) {
    if win.len() == 0 || lose.len() == 0 {
        return;
    }
    let win_ng = get_ng(win);
    let lose_ng = get_ng(lose);
    let elo = EloRank {k:20.0};
    let (rw, rl) = elo.compute_elo_team(&win_ng, &lose_ng);

    for (i, u) in win.iter().enumerate() {
        user_score(u, (rw[i]-win_ng[i]) as i16, msgtx, conn);
    }
    for (i, u) in lose.iter().enumerate() {
        user_score(u, (rl[i]-lose_ng[i]) as i16, msgtx, conn);
    }
}

pub fn init(msgtx: Sender<MqttMsg>, pool: mysql::Pool) 
    -> Result<Sender<RoomEventData>, Error> {
    let (tx, rx):(Sender<RoomEventData>, Receiver<RoomEventData>) = bounded(1000);
    let start = Instant::now();
    let update200ms = tick(Duration::from_millis(200));
    let update100ms = tick(Duration::from_millis(100));
    
    thread::spawn(move || -> Result<(), Error> {
        let mut conn = pool.get_conn()?;
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
                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/prestart", r), msg: r#"{"msg":"prestart"}"#.to_string()})?;
                                }
                                game_id = 1;
                                fg.set_game_id(game_id);
                                info!("game id {}", game_id);
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
                                /*
                                for r in &group.borrow().room_names {
                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/start", r), 
                                        msg: format!(r#"{{"room":"{}","msg":"start","server":"59.126.81.58:{}","game":{}}}"#, 
                                            r, game_port, id.clone())})?;
                                }*/
                                GameingGroups.remove(&group.borrow().game_id);
                                GameingGroups.insert(group.borrow().game_id.clone(), group.clone());
                                info!("GameingGroups {:#?}", GameingGroups);
                                info!("game_port: {}", game_port);
                                info!("game id {}", group.borrow().game_id);
                                let cmd = Command::new("/home/damody/LinuxNoEditor/CF1/Binaries/Linux/CF1Server")
                                        .arg(format!("-Port={}", game_port))
                                        .arg(format!("-gameid {}", group.borrow().game_id))
                                        .spawn();
                                        match cmd {
                                            Ok(_) => {},
                                            Err(_) => {},
                                        }
                            },
                            PrestartStatus::Cancel => {
                                group.borrow_mut().update_names();
                                group.borrow_mut().clear_queue();
                                for r in &group.borrow().room_names {
                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/prestart", r), 
                                        msg: format!(r#"{{"msg":"stop queue"}}"#)})?;
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
                    let handle = || -> Result<(), Error> {
                        if let Ok(d) = d {
                            match d {
                                RoomEventData::GameClose(x) => {
                                    let g = GameingGroups.remove(&x.game);
                                    if let Some(g) = g {
                                        g.borrow_mut().leave_room();
                                    }
                                    info!("GameingGroups {:#?}", GameingGroups);
                                },
                                RoomEventData::GameOver(x) => {
                                    let win = get_users(&x.win, &TotalUsers)?;
                                    let lose = get_users(&x.lose, &TotalUsers)?;
                                    settlement_ng_score(&win, &lose, &msgtx, &mut conn);
                                    let g = GameingGroups.remove(&x.game);
                                    if let Some(g) = g {
                                        g.borrow_mut().leave_room();
                                    }
                                    info!("GameingGroups {:#?}", GameingGroups);
                                },
                                RoomEventData::StartGame(x) => {
                                    info!("GameingGroups {:#?}", GameingGroups);
                                    let g = GameingGroups.get(&x.game);
                                    if let Some(g) = g {
                                        SendGameList(&g, &msgtx, &mut conn);
                                        for r in &g.borrow().room_names {
                                            game_port = 7778;
                                            msgtx.try_send(MqttMsg{topic:format!("room/{}/res/start", r), 
                                                msg: format!(r#"{{"room":"{}","msg":"start","server":"127.0.0.1:{}","game":{}}}"#, 
                                                    r, game_port, g.borrow().game_id)})?;
                                        }
                                    }
                                    
                                },
                                RoomEventData::Leave(x) => {
                                    let u = TotalUsers.get(&x.room);
                                    if let Some(u) = u {
                                            let r = TotalRoom.get(&u.borrow().rid);
                                            let mut is_null = false;
                                            if let Some(r) = r {
                                                let m = r.borrow().master.clone();
                                                r.borrow_mut().rm_user(&x.id);
                                                if r.borrow().users.len() > 0 {
                                                    r.borrow().publish_update(&msgtx, m);
                                                }
                                                else {
                                                    is_null = true;
                                                }
                                                msgtx.try_send(MqttMsg{topic:format!("room/{}/res/leave", x.id), 
                                                    msg: format!(r#"{{"msg":"ok"}}"#)})?;
                                            }
                                            if is_null {
                                                TotalRoom.remove(&u.borrow().rid);
                                                QueueRoom.remove(&u.borrow().rid);
                                            }
                                    }
                                },
                                RoomEventData::ChooseNGHero(x) => {
                                    let u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        u.borrow_mut().hero = x.hero;
                                        msgtx.try_send(MqttMsg{topic:format!("member/{}/res/choose_hero", u.borrow().id), 
                                            msg: format!(r#"{{"id":"{}", "hero":"{}"}}"#, u.borrow().id, u.borrow().hero)})?;
                                    }
                                },
                                RoomEventData::Invite(x) => {
                                    if TotalUsers.contains_key(&x.from) {
                                        msgtx.try_send(MqttMsg{topic:format!("room/{}/res/invite", x.invite.clone()), 
                                            msg: format!(r#"{{"room":"{}","from":"{}"}}"#, x.room.clone(), x.from.clone())})?;
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
                                                let m = r.borrow().master.clone();
                                                r.borrow().publish_update(&msgtx, m);
                                                r.borrow().publish_update(&msgtx, x.join.clone());
                                                msgtx.try_send(MqttMsg{topic:format!("room/{}/res/join", x.join.clone()), 
                                                    msg: format!(r#"{{"room":"{}","msg":"ok"}}"#, x.room.clone())})?;
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
                                                            msg: format!(r#"{{"msg":"ok"}}"#)})?;
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
                                                    msg: format!(r#"{{"msg":"ok"}}"#)})?;
                                            } else {
                                                msgtx.try_send(MqttMsg{topic:format!("room/{}/res/start_queue", y.borrow().master.clone()), 
                                                    msg: format!(r#"{{"msg":"fail"}}"#)})?;
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
                                                    msg: format!(r#"{{"msg":"ok"}}"#)})?;
                                            } else {
                                                msgtx.try_send(MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master.clone()), 
                                                    msg: format!(r#"{{"msg":"fail"}}"#)})?;
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
                                            msg: format!(r#"{{"msg":"ok", "ng":{}, "rk":{} }}"#, x.u.ng, x.u.rk)})?;
                                    } else {
                                        //msgtx.try_send(MqttMsg{topic:format!("member/{}/res/login", x.u.id.clone()), 
                                        //    msg: format!(r#"{{"msg":"fail"}}"#)})?;
                                        msgtx.try_send(MqttMsg{topic:format!("member/{}/res/login", x.u.id.clone()), 
                                            msg: format!(r#"{{"msg":"ok", "ng":{}, "rk":{} }}"#, x.u.ng, x.u.rk)})?;
                                    }
                                },
                                RoomEventData::Logout(x) => {
                                    let mut success = false;
                                    let u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        let gid = u.borrow().gid;
                                        let rid = u.borrow().rid;
                                        let r = TotalRoom.get(&u.borrow().rid);
                                        let mut is_null = false;
                                        if let Some(r) = r {
                                            let m = r.borrow().master.clone();
                                            r.borrow_mut().rm_user(&x.id);
                                            if r.borrow().users.len() > 0 {
                                                r.borrow().publish_update(&msgtx, m);
                                            }
                                            msgtx.try_send(MqttMsg{topic:format!("room/{}/res/leave", x.id), 
                                                msg: format!(r#"{{"msg":"ok"}}"#)})?;
                                        }
                                        if is_null {
                                            TotalRoom.remove(&u.borrow().rid);
                                            QueueRoom.remove(&u.borrow().rid);
                                        }
                                        if gid != 0 {
                                            let g = ReadyGroups.get(&gid);
                                            if let Some(gr) = g {
                                                gr.borrow_mut().user_cancel(&x.id);
                                                ReadyGroups.remove(&gid);
                                                let r = QueueRoom.remove(&rid);
                                                if let Some(r) = r {
                                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master), 
                                                        msg: format!(r#"{{"msg":"ok"}}"#)})?;
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
                                            msg: format!(r#"{{"msg":"ok"}}"#)})?;
                                    } else {
                                        msgtx.try_send(MqttMsg{topic:format!("member/{}/res/logout", x.id.clone()), 
                                            msg: format!(r#"{{"msg":"fail"}}"#)})?;
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
                                            last_master: "".to_owned(),
                                            avg_ng: 0,
                                            avg_rk: 0,
                                            ready: 0,
                                        };
                                        let mut u = TotalUsers.get(&x.id);
                                        if let Some(u) = u {
                                            new_room.add_user(Rc::clone(&u));
                                            let rid = new_room.rid;
                                            let r = Rc::new(RefCell::new(new_room));
                                            r.borrow().publish_update(&msgtx, x.id.clone());
                                            TotalRoom.insert(
                                                rid,
                                                Rc::clone(&r),
                                            );
                                            success = true;
                                        }
                                    }
                                    if success {
                                        msgtx.try_send(MqttMsg{topic:format!("room/{}/res/create", x.id.clone()), 
                                            msg: format!(r#"{{"msg":"ok"}}"#)})?;
                                    } else {
                                        msgtx.try_send(MqttMsg{topic:format!("room/{}/res/create", x.id.clone()), 
                                            msg: format!(r#"{{"msg":"fail"}}"#)})?;
                                    }
                                },
                                RoomEventData::Close(x) => {
                                    let mut success = false;
                                    if let Some(y) =  TotalRoom.remove(&get_rid_by_id(&x.id, &TotalUsers)) {
                                        let data = TotalRoom.remove(&get_rid_by_id(&x.id, &TotalUsers));
                                        y.borrow_mut().leave_room();
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
                                            msg: format!(r#"{{"msg":"ok"}}"#)})?;
                                    } else {
                                        msgtx.try_send(MqttMsg{topic:format!("room/{}/res/cancel_queue", x.id.clone()), 
                                            msg: format!(r#"{{"msg":"fail"}}"#)})?;
                                    }
                                },
                            }
                        }
                        Ok(())
                    };
                    if let Err(msg) = handle() {
                        println!("{:?}", msg);
                    }
                }
            }
        }
    });
    Ok(tx)
}

pub fn create(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), Error>
{
    let data: CreateRoomData = serde_json::from_value(v)?;
    sender.send(RoomEventData::Create(CreateRoomData{id: data.id.clone()}));
    Ok(())
}

pub fn close(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), Error>
{
    let data: CloseRoomData = serde_json::from_value(v)?;
    sender.send(RoomEventData::Close(CloseRoomData{id: data.id.clone()}));
    Ok(())
}

pub fn start_queue(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), Error>
{
    let data: StartQueueData = serde_json::from_value(v)?;
    sender.send(RoomEventData::StartQueue(StartQueueData{id: data.id.clone(), action: data.action.clone()}));
    Ok(())
}

pub fn cancel_queue(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), Error>
{
    let data: CancelQueueData = serde_json::from_value(v)?;
    sender.send(RoomEventData::CancelQueue(data));
    Ok(())
}

pub fn prestart(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), Error>
{
    let data: PreStartData = serde_json::from_value(v)?;
    sender.send(RoomEventData::PreStart(data));
    Ok(())
}

pub fn join(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), Error>
{
    let data: JoinRoomData = serde_json::from_value(v)?;
    sender.send(RoomEventData::Join(data));
    Ok(())
}

pub fn choose_ng_hero(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), Error>
{
    let data: UserNGHeroData = serde_json::from_value(v)?;
    sender.send(RoomEventData::ChooseNGHero(data));
    Ok(())
}

pub fn invite(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), Error>
{
    let data: InviteRoomData = serde_json::from_value(v)?;
    sender.send(RoomEventData::Invite(data));
    Ok(())
}

pub fn leave(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), Error>
{
    let data: LeaveData = serde_json::from_value(v)?;
    sender.send(RoomEventData::Leave(data));
    Ok(())
}


pub fn start_game(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), Error>
{
    let data: StartGameData = serde_json::from_value(v)?;
    sender.send(RoomEventData::StartGame(data));
    Ok(())
}

pub fn game_over(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), Error>
{
    let data: GameOverData = serde_json::from_value(v)?;
    sender.send(RoomEventData::GameOver(data));
    Ok(())
}

pub fn game_close(id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), Error>
{
    let data: GameCloseData = serde_json::from_value(v)?;
    sender.send(RoomEventData::GameClose(data));
    Ok(())
}
