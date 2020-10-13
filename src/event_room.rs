extern crate redis;
use redis::Commands;

use log::{error, info, trace, warn};
use serde_derive::{Deserialize, Serialize};
use serde_json::json;
use serde_json::{self, Value};
use std::env;
use std::io::ErrorKind;
use std::io::{self, Write};
use std::panic;
use std::thread;
use std::time::{Duration, Instant};

use ::futures::Future;
use crossbeam_channel::{bounded, select, tick, Receiver, Sender};
use failure::Error;
use mysql;
use rayon::prelude::*;
use rayon::slice::*;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex, RwLock};

use crate::elo::*;
use crate::msg::*;
use crate::room::*;
use std::process::Command;

const TEAM_SIZE: i16 = 2;
const MATCH_SIZE: usize = 2;
const SCORE_INTERVAL: i16 = 100;
const CHOOSE_HERO_TIME: u16 = 300;
const READY_TIME: u16 = 15;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateRoomData {
    pub id: String,
    pub mode: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CloseRoomData {
    pub id: String,
    pub dataid: String,
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RejectRoomData {
    pub room: String,
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JumpData {
    pub id: String,
    pub msg: String,
    pub game: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RestrictedData {
    pub id: String,
    pub time: u16,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CheckRestrctionData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CheckInGameData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LeaveGameData {
    pub id: String,
}

#[derive(Clone, Debug)]
pub struct UserLoginData {
    pub u: User,
    pub dataid: String,
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
    pub room: String,
    pub action: String,
    pub mode: String,
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReadyData {
    pub room: String,
    pub id: String,
    pub accept: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CancelQueueData {
    pub room: String,
    pub action: String,
    pub mode: String,
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PreGameData {
    pub rid: Vec<Vec<u64>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PreStartData {
    pub room: String,
    pub id: String,
    pub accept: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StartGetData {
    pub room: String,
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LeaveData {
    pub room: String,
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct StartGameData {
    pub game: u64,
    pub id: String,
    pub mode: String,
    pub players: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct StartGameSendData {
    pub game: u64,
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
    pub game: u64,
    pub mode: String,
    pub win: Vec<String>,
    pub lose: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GameCloseData {
    pub game: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct StatusData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ReconnectData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct DeadData {
    pub ServerDead: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GameInfoData {
    pub game: u64,
    pub users: Vec<UserInfoData>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct UserInfoData {
    pub id: String,
    pub hero: String,
    pub level: u16,
    pub equ: Vec<String>,
    pub damage: u16,
    pub take_damage: u16,
    pub heal: u16,
    pub kill: u16,
    pub death: u16,
    pub assist: u16,
    pub gift: UserGift,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct UserGift {
    pub a: u16,
    pub b: u16,
    pub c: u16,
    pub d: u16,
    pub e: u16,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ToGameGroup {
    pub rids: Vec<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ControlData {
    pub mode: String,
    pub msg: String,
    pub password: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct CheckStateData {
    pub msg: String,
}

#[derive(Debug)]
pub enum RoomEventData {
    Reset(),
    Login(UserLoginData),
    Logout(UserLogoutData),
    Create(CreateRoomData),
    Close(CloseRoomData),
    Invite(InviteRoomData),
    ChooseNGHero(UserNGHeroData),
    BanHero(UserNGHeroData),
    LockedNGHero(UserNGHeroData),
    NGGameChooseHero(BTreeMap<u64, Vec<u64>>),
    Join(JoinRoomData),
    Reject(RejectRoomData),
    Jump(JumpData),
    CheckRestriction(CheckRestrctionData),
    CheckInGame(CheckInGameData),
    LeaveGame(LeaveGameData),
    StartQueue(StartQueueData),
    Ready(ReadyData),
    CancelQueue(CancelQueueData),
    UpdateGame(PreGameData),
    PreStart(PreStartData),
    StartGet(StartGetData),
    Leave(LeaveData),
    StartGame(StartGameData),
    GameOver(GameOverData),
    GameInfo(GameInfoData),
    GameClose(GameCloseData),
    Status(StatusData),
    Reconnect(ReconnectData),
    MainServerDead(DeadData),
    Control(ControlData),
    CheckState(CheckStateData),
}

#[derive(Clone, Debug)]
pub struct SqlLoginData {
    pub id: String,
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct SqlScoreData {
    pub id: String,
    pub ng: i16,
    pub rk: i16,
    pub at: i16,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct SqlGameInfoData {
    pub game: u64,
    pub id: String,
    pub hero: String,
    pub level: u16,
    pub equ: String,
    pub damage: u16,
    pub take_damage: u16,
    pub heal: u16,
    pub kill: u16,
    pub death: u16,
    pub assist: u16,
    pub gift: UserGift,
}

pub enum SqlData {
    Login(SqlLoginData),
    UpdateScore(SqlScoreData),
    UpdateGameInfo(SqlGameInfoData),
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct QueueRoomData {
    pub rid: u64,
    pub gid: u64,
    pub user_len: i16,
    pub user_ids: Vec<String>,
    pub avg_ng: i16,
    pub avg_rk: i16,
    pub avg_at: i16,
    pub ready: i8,
    pub notify: bool,
    pub queue_cnt: i16,
    pub mode: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ReadyGroupData {
    pub gid: u64,
    pub rid: Vec<u64>,
    pub user_len: i16,
    pub user_ids: Vec<String>,
    pub avg_ng: i16,
    pub avg_rk: i16,
    pub avg_at: i16,
    pub game_status: u16,
    pub queue_cnt: i16,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ReadyGameData {
    pub gid: Vec<u64>,
    pub group: Vec<Vec<u64>>,
    pub team_len: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RemoveRoomData {
    pub rid: u64,
}

pub enum QueueData {
    UpdateRoom(QueueRoomData),
    RemoveRoom(RemoveRoomData),
    Control(ControlData),
}

// Prints the elapsed time.
fn show(dur: Duration) {
    println!(
        "Elapsed: {}.{:03} sec",
        dur.as_secs(),
        dur.subsec_nanos() / 1_000_000
    );
}

fn SendGameList(
    game: &Rc<RefCell<FightGame>>,
    msgtx: &Sender<MqttMsg>,
    conn: &mut mysql::PooledConn,
) -> Result<(), Error> {
    let mut res: StartGameSendData = Default::default();
    res.game = game.borrow().game_id;
    for (i, t) in game.borrow().teams.iter().enumerate() {
        let ids = t.borrow().get_users_id_hero();
        for (id, name, hero) in &ids {
            let h: HeroCell = HeroCell {
                id: id.clone(),
                team: (i + 1) as u16,
                name: name.clone(),
                hero: hero.clone(),
                ..Default::default()
            };
            res.member.push(h);
        }
    }
    //info!("{:?}", res);
    msgtx.try_send(MqttMsg {
        topic: format!("game/{}/res/start_game", res.game),
        msg: json!(res).to_string(),
    })?;
    Ok(())
}

fn get_rid_by_id(id: &String, users: &BTreeMap<String, Rc<RefCell<User>>>) -> u64 {
    let u = users.get(id);
    if let Some(u) = u {
        return u.borrow().rid;
    }
    return 0;
}

fn get_gid_by_id(id: &String, users: &BTreeMap<String, Rc<RefCell<User>>>) -> u64 {
    let u = users.get(id);
    if let Some(u) = u {
        return u.borrow().gid;
    }
    return 0;
}

fn get_game_id_by_id(id: &String, users: &BTreeMap<String, Rc<RefCell<User>>>) -> u64 {
    let u = users.get(id);
    if let Some(u) = u {
        return u.borrow().game_id;
    }
    return 0;
}

fn get_user(id: &String, users: &BTreeMap<String, Rc<RefCell<User>>>) -> Option<Rc<RefCell<User>>> {
    let u = users.get(id);
    if let Some(u) = u {
        return Some(Rc::clone(u));
    }
    None
}

fn get_users(
    ids: &Vec<String>,
    users: &BTreeMap<String, Rc<RefCell<User>>>,
) -> Result<Vec<Rc<RefCell<User>>>, Error> {
    let mut res: Vec<Rc<RefCell<User>>> = vec![];
    for id in ids {
        let u = get_user(id, users);
        if let Some(u) = u {
            res.push(u);
        }
    }
    if ids.len() == res.len() {
        Ok(res)
    } else {
        Err(failure::err_msg("some user not found"))
    }
}

fn user_score(
    u: &Rc<RefCell<User>>,
    value: i16,
    msgtx: &Sender<MqttMsg>,
    sender: &Sender<SqlData>,
    conn: &mut mysql::PooledConn,
    mode: String,
) -> Result<(), Error> {
    if mode == "ng" {
        u.borrow_mut().ng += value;
    } else if mode == "rk" {
        u.borrow_mut().rk += value;
    } else if mode == "at" {
        u.borrow_mut().at += value;
    }
    msgtx.try_send(MqttMsg {
        topic: format!("member/{}/res/login", u.borrow().id),
        msg: format!(
            r#"{{"msg":"ok", "ng":{}, "rk":{}, "at":{}, "hero":"{}"}}"#,
            u.borrow().ng,
            u.borrow().rk,
            u.borrow().at,
            u.borrow().hero,
        ),
    })?;
    println!("Update!");
    println!("in");
    let sql = format!(
        "UPDATE user SET ng={}, rk={}, at={}, hero=(SELECT hero FROM Hero_usage WHERE steam_id='{}' ORDER BY choose_count DESC LIMIT 1) WHERE id='{}';",
        u.borrow().ng.clone(),
        u.borrow().rk.clone(),
        u.borrow().at.clone(),
        u.borrow().id.clone(),
        u.borrow().id.clone()
    );
    let qres = conn.query(sql.clone())?;
    Ok(())
}

fn get_ng(team: &Vec<Rc<RefCell<User>>>) -> Vec<i32> {
    let mut res: Vec<i32> = vec![];
    for u in team {
        res.push(u.borrow().ng.into());
    }
    res
}

fn get_rk(team: &Vec<Rc<RefCell<User>>>) -> Vec<i32> {
    let mut res: Vec<i32> = vec![];
    for u in team {
        res.push(u.borrow().rk.into());
    }
    res
}

fn get_at(team: &Vec<Rc<RefCell<User>>>) -> Vec<i32> {
    let mut res: Vec<i32> = vec![];
    for u in team {
        res.push(u.borrow().at.into());
    }
    res
}

fn check_is_black(user_ids: Vec<String>, g_user_ids: Vec<String>, conn: &mut mysql::PooledConn) -> Result<bool, Error>  {
    let mut isBlack = false;
    for user_id in user_ids {
        for g_user_id in &g_user_ids {
            let mut sql = format!(r#"select count(*) from black_list where user={} and black={};"#, user_id, g_user_id);
            let qres = conn.query(sql)?;
            let mut count = 0;
            for row in qres {
                let a = row?.clone();
                count = mysql::from_value(a.get("count(*)").unwrap());
                break;
            }
            if count > 0 {
                isBlack = true;
                break;
            }
            sql = format!(r#"select count(*) from black_list where user={} and black={};"#, g_user_id, user_id);
            let qres2 = conn.query(sql)?;
            for row in qres2 {
                let a = row?.clone();
                count = mysql::from_value(a.get("count(*)").unwrap());
                break;
            }
            if count > 0 {
                isBlack = true;
                break;
            }
        }
        if isBlack {
            break;
        }
    }
    Ok(isBlack)
}

fn settlement_score(
    win: &Vec<Rc<RefCell<User>>>,
    lose: &Vec<Rc<RefCell<User>>>,
    msgtx: &Sender<MqttMsg>,
    sender: &Sender<SqlData>,
    conn: &mut mysql::PooledConn,
    mode: String,
) {
    if win.len() == 0 || lose.len() == 0 {
        return;
    }
    let mut win_score: Vec<i32> = get_ng(win);
    let mut lose_score: Vec<i32> = get_ng(lose);
    if mode == "rk" {
        win_score = get_rk(win);
        lose_score = get_rk(lose);
    } else if mode == "at" {
        win_score = get_at(win);
        lose_score = get_at(lose);
    }
    // println!("win : {:?}, lose : {:?}", win_score, lose_score);
    let elo = EloRank { k: 20.0 };
    let (rw, rl) = elo.compute_elo_team(&win_score, &lose_score);
    println!("Game Over");
    for (i, u) in win.iter().enumerate() {
        user_score(
            u,
            (rw[i] - win_score[i]) as i16,
            msgtx,
            sender,
            conn,
            mode.clone(),
        );
    }
    for (i, u) in lose.iter().enumerate() {
        user_score(
            u,
            (rl[i] - lose_score[i]) as i16,
            msgtx,
            sender,
            conn,
            mode.clone(),
        );
    }
}

pub fn HandleSqlRequest(pool: mysql::Pool) -> Result<Sender<SqlData>, Error> {
    let (tx1, rx1): (Sender<SqlData>, Receiver<SqlData>) = bounded(10000);
    let start = Instant::now();
    let update1000ms = tick(Duration::from_millis(2000));
    let mut NewUsers: Vec<String> = Vec::new();
    let mut len = 0;
    let mut UpdateInfo: Vec<SqlGameInfoData> = Vec::new();
    let mut info_len = 0;

    thread::spawn(move || -> Result<(), Error> {
        let mut conn = pool.get_conn()?;
        loop {
            select! {

                recv(update1000ms) -> _ => {
                    if len > 0 {
                        // insert new user in NewUsers
                        let mut insert_str: String = "insert into user (userid, name, status, hero) values".to_string();
                        for (i, u) in NewUsers.iter().enumerate() {
                            let mut new_user = format!(" ('{}', 'default name', 'online', '')", u);
                            insert_str += &new_user;
                            if i < len-1 {
                                insert_str += ",";
                            }
                        }
                        insert_str += ";";
                        // println!("{}", insert_str);
                        {
                            conn.query(insert_str.clone())?;
                        }

                        len = 0;
                        NewUsers.clear();
                    }

                    if info_len > 0 {

                        let mut insert_info: String = "insert into game_info (userid, game_id, hero, level, damage, take_damage, heal, kill_cnt, death, assist) values".to_string();
                        let mut user_info: String = "insert into user_info (userid, game_id, equ, gift_A, gift_B, gift_C, gift_D, gift_E) values".to_string();
                        for (i, info) in UpdateInfo.iter().enumerate() {
                            let mut new_user = format!(" ({}, {}, '{}', {}, {}, {}, {}, {}, {}, {})", info.id, info.game, info.hero, info.level, info.damage, info.take_damage, info.heal, info.kill, info.death, info.assist);
                            insert_info += &new_user;
                            let mut new_user1 = format!(" ({}, {}, '{}', {}, {}, {}, {}, {})", info.id, info.game, info.equ, info.gift.a, info.gift.b, info.gift.c, info.gift.d, info.gift.e);
                            user_info += &new_user1;
                            if i < info_len-1 {
                                insert_info += ",";
                                user_info += ",";
                            }
                        }
                        insert_info += ";";
                        user_info += ";";
                        {
                            conn.query(insert_info.clone())?;
                        }
                        {
                            conn.query(user_info.clone())?;
                        }


                        info_len = 0;
                        UpdateInfo.clear();
                    }

                }

                recv(rx1) -> d => {
                    let handle = || -> Result<(), Error> {
                        if let Ok(d) = d {
                            match d {

                                SqlData::Login(x) => {
                                    NewUsers.push(x.id.clone());
                                    len+=1;
                                }
                                SqlData::UpdateScore(x) => {
                                    println!("in");
                                    let sql = format!("UPDATE user SET ng={}, rk={}, at={} WHERE id='{}';", x.ng, x.rk, x.at, x.id);
                                    println!("sql: {}", sql);
                                    let qres = conn.query(sql.clone())?;
                                }
                                SqlData::UpdateGameInfo(x) => {
                                    UpdateInfo.push(x.clone());
                                    info_len += 1;
                                }
                            }
                        }
                        Ok(())
                    };
                    if let Err(msg) = handle() {
                        println!("{:?}", msg);
                        continue;
                    }
                }
            }
        }
    });
    Ok(tx1)
}

pub fn HandleQueueRequest(
    msgtx: Sender<MqttMsg>,
    sender: Sender<RoomEventData>,
    pool: mysql::Pool,
) -> Result<Sender<QueueData>, Error> {
    let (tx, rx): (Sender<QueueData>, Receiver<QueueData>) = bounded(10000);
    let start = Instant::now();
    let update = tick(Duration::from_millis(1000));

    thread::spawn(move || -> Result<(), Error> {
        let mut NGQueueRoom: BTreeMap<u64, Rc<RefCell<QueueRoomData>>> = BTreeMap::new();
        let mut RKQueueRoom: BTreeMap<u64, Rc<RefCell<QueueRoomData>>> = BTreeMap::new();
        let mut ATQueueRoom: BTreeMap<u64, Rc<RefCell<QueueRoomData>>> = BTreeMap::new();
        let mut NGReadyGroups: BTreeMap<u64, Rc<RefCell<ReadyGroupData>>> = BTreeMap::new();
        let mut RKReadyGroups: BTreeMap<u64, Rc<RefCell<ReadyGroupData>>> = BTreeMap::new();
        let mut ATReadyGroups: BTreeMap<u64, Rc<RefCell<ReadyGroupData>>> = BTreeMap::new();
        let mut conn = pool.get_conn()?;
        let mut group_id: u64 = 0;
        let mut ngState = "open";
        let mut rkState = "open";
        let mut atState = "open";
        loop {
            select! {
                recv(update) -> _ => {
                    let mut new_now = Instant::now();
                    // ng
                    if ngState == "open" {
                        if NGQueueRoom.len() >= MATCH_SIZE {
                            let mut g: ReadyGroupData = Default::default();
                            let mut tq: Vec<Rc<RefCell<QueueRoomData>>> = vec![];
                            let mut id: Vec<u64> = vec![];
                            let mut new_now = Instant::now();
                            tq = NGQueueRoom.iter().map(|x|Rc::clone(x.1)).collect();
                            //println!("Collect Time: {:?}",Instant::now().duration_since(new_now));
                            //tq.sort_by_key(|x| x.borrow().avg_rk);
                            let mut new_now = Instant::now();
                            tq.sort_by_key(|x| x.borrow().avg_ng);
                            //println!("Sort Time: {:?}",Instant::now().duration_since(new_now));
                            let mut new_now1 = Instant::now();
                            for (k, v) in &mut NGQueueRoom {
                                if g.user_len > 0 && g.user_len < TEAM_SIZE && (g.avg_ng + v.borrow().queue_cnt*SCORE_INTERVAL) < v.borrow().avg_ng {
                                    for r in g.rid {
                                        id.push(r);
                                    }
                                    g = Default::default();
                                    for user_id in &v.borrow().user_ids {
                                        g.user_ids.push(user_id.clone());
                                    }
                                    g.rid.push(v.borrow().rid);
                                    let mut ng = (g.avg_ng * g.user_len + v.borrow().avg_ng * v.borrow().user_len) as i16 / (g.user_len + v.borrow().user_len) as i16;
                                    g.avg_ng = ng;
                                    g.user_len += v.borrow().user_len;
                                    v.borrow_mut().ready = 1;
                                    v.borrow_mut().gid = group_id + 1;
                                    v.borrow_mut().queue_cnt += 1;
                                }
                                if v.borrow().ready == 0 &&
                                    v.borrow().user_len as i16 + g.user_len <= TEAM_SIZE {
                                    let Difference: i16 = i16::abs(v.borrow().avg_ng - g.avg_ng);
                                    if g.avg_ng == 0 || Difference <= SCORE_INTERVAL * v.borrow().queue_cnt {
                                        let mut ng ;
                                        let isBlack = check_is_black(v.borrow().user_ids.clone(), g.user_ids.clone(), &mut conn)?;
                                        if (isBlack) {
                                            continue;
                                        }
                                        if (g.user_len + v.borrow().user_len > 0){
                                            ng = (g.avg_ng * g.user_len + v.borrow().avg_ng * v.borrow().user_len) as i16 / (g.user_len + v.borrow().user_len) as i16;
                                        } else {
                                            g = Default::default();
                                            continue;
                                        }
                                        for user_id in &v.borrow().user_ids {
                                            g.user_ids.push(user_id.clone());
                                        }
                                        g.rid.push(v.borrow().rid);
                                        g.avg_ng = ng;
                                        g.user_len += v.borrow().user_len;
                                        v.borrow_mut().ready = 1;
                                        v.borrow_mut().gid = group_id + 1;
                                        println!("g2 {:?}", g);
                                    }
                                    else {
                                        v.borrow_mut().queue_cnt += 1;
                                    }
                                }
                                if g.user_len == TEAM_SIZE {
                                    println!("match team_size!");
                                    group_id += 1;
                                    info!("new group_id: {}", group_id);
                                    g.gid = group_id;
                                    g.queue_cnt = 1;
                                    NGReadyGroups.insert(group_id, Rc::new(RefCell::new(g.clone())));
                                    g = Default::default();
                                }

                            }
                            //println!("Time 2: {:?}",Instant::now().duration_since(new_now1));
                            if g.user_len < TEAM_SIZE {
                                for r in g.rid {
                                    let mut room = NGQueueRoom.get(&r);
                                    if let Some(room) = room {
                                        room.borrow_mut().ready = 0;
                                        room.borrow_mut().gid = 0;
                                    }
                                }
                                for r in id {
                                    let mut room = NGQueueRoom.get(&r);
                                    if let Some(room) = room {
                                        room.borrow_mut().ready = 0;
                                        room.borrow_mut().gid = 0;
                                    }
                                }
                            }
                        }
                        if NGReadyGroups.len() >= MATCH_SIZE {
                            let mut fg: ReadyGameData = Default::default();
                            let mut prestart = false;
                            let mut total_ng: i16 = 0;
                            let mut rm_ids: Vec<u64> = vec![];
                            println!("NGReadyGroup!! {}", NGReadyGroups.len());
                            let mut new_now2 = Instant::now();
                            for (id, rg) in &mut NGReadyGroups {
                                if rg.borrow().game_status == 0 && fg.team_len < MATCH_SIZE {
                                    if total_ng == 0 {
                                        total_ng += rg.borrow().avg_ng as i16;
                                        fg.group.push(rg.borrow().rid.clone());
                                        fg.gid.push(*id);
                                        fg.team_len += 1;
                                        continue;
                                    }

                                    let mut difference = 0;
                                    if fg.team_len > 0 {
                                        difference = i16::abs(rg.borrow().avg_ng as i16 - total_ng/fg.team_len as i16);
                                    }
                                    if difference <= SCORE_INTERVAL * rg.borrow().queue_cnt {
                                        total_ng += rg.borrow().avg_ng as i16;
                                        fg.group.push(rg.borrow().rid.clone());
                                        fg.team_len += 1;
                                        fg.gid.push(*id);
                                    }
                                    else {
                                        rg.borrow_mut().queue_cnt += 1;
                                    }
                                }
                                if fg.team_len == MATCH_SIZE {
                                    sender.send(RoomEventData::UpdateGame(PreGameData{rid: fg.group.clone()}));
                                    for id in fg.gid {
                                        rm_ids.push(id);
                                    }
                                    fg = Default::default();
                                }
                            }
                            for id in rm_ids {
                                let rg = NGReadyGroups.remove(&id);
                                if let Some(rg) = rg {
                                    for rid in &rg.borrow().rid {
                                        NGQueueRoom.remove(&rid);
                                    }
                                }
                            }
                        }
                    }
                    // ng
                    // rank
                    if rkState == "open" {
                        new_now = Instant::now();
                        if RKQueueRoom.len() >= MATCH_SIZE {
                            let mut g: ReadyGroupData = Default::default();
                            let mut tq: Vec<Rc<RefCell<QueueRoomData>>> = vec![];
                            let mut id: Vec<u64> = vec![];
                            let mut new_now = Instant::now();
                            tq = RKQueueRoom.iter().map(|x|Rc::clone(x.1)).collect();
                            //println!("Collect Time: {:?}",Instant::now().duration_since(new_now));
                            //tq.sort_by_key(|x| x.borrow().avg_rk);
                            let mut new_now = Instant::now();
                            tq.sort_by_key(|x| x.borrow().avg_rk);
                            //println!("Sort Time: {:?}",Instant::now().duration_since(new_now));
                            let mut new_now1 = Instant::now();
                            for (k, v) in &mut RKQueueRoom {
                                if g.user_len > 0 && g.user_len < TEAM_SIZE && (g.avg_rk + v.borrow().queue_cnt*SCORE_INTERVAL) < v.borrow().avg_rk {
                                    for r in g.rid {
                                        id.push(r);
                                    }
                                    g = Default::default();
                                    for user_id in &v.borrow().user_ids {
                                        g.user_ids.push(user_id.clone());
                                    }
                                    g.rid.push(v.borrow().rid);
                                    let mut rk = (g.avg_rk * g.user_len + v.borrow().avg_rk * v.borrow().user_len) as i16 / (g.user_len + v.borrow().user_len) as i16;
                                    g.avg_rk = rk;
                                    g.user_len += v.borrow().user_len;
                                    v.borrow_mut().ready = 1;
                                    v.borrow_mut().gid = group_id + 1;
                                    v.borrow_mut().queue_cnt += 1;
                                }
                                if v.borrow().ready == 0 &&
                                    v.borrow().user_len as i16 + g.user_len <= TEAM_SIZE {

                                    let Difference: i16 = i16::abs(v.borrow().avg_rk - g.avg_rk);
                                    if g.avg_rk == 0 || Difference <= SCORE_INTERVAL * v.borrow().queue_cnt {
                                        let mut rk ;
                                        let isBlack = check_is_black(v.borrow().user_ids.clone(), g.user_ids.clone(), &mut conn)?;
                                        if (isBlack) {
                                            continue;
                                        }
                                        if (g.user_len + v.borrow().user_len > 0){
                                            rk = (g.avg_rk * g.user_len + v.borrow().avg_rk * v.borrow().user_len) as i16 / (g.user_len + v.borrow().user_len) as i16;
                                        } else {
                                            g = Default::default();
                                            continue;
                                        }
                                        for user_id in &v.borrow().user_ids {
                                            g.user_ids.push(user_id.clone());
                                        }
                                        g.rid.push(v.borrow().rid);
                                        g.avg_rk = rk;
                                        g.user_len += v.borrow().user_len;
                                        v.borrow_mut().ready = 1;
                                        v.borrow_mut().gid = group_id + 1;
                                    }
                                    else {
                                        v.borrow_mut().queue_cnt += 1;
                                    }
                                }
                                println!("{}", g.user_len);
                                if g.user_len == TEAM_SIZE {
                                    println!("match team_size!");
                                    group_id += 1;
                                    info!("new group_id: {}", group_id);
                                    g.gid = group_id;
                                    g.queue_cnt = 1;
                                    RKReadyGroups.insert(group_id, Rc::new(RefCell::new(g.clone())));
                                    g = Default::default();
                                }

                            }
                            //println!("Time 2: {:?}",Instant::now().duration_since(new_now1));
                            if g.user_len < TEAM_SIZE {
                                for r in g.rid {
                                    let mut room = RKQueueRoom.get(&r);
                                    if let Some(room) = room {
                                        room.borrow_mut().ready = 0;
                                        room.borrow_mut().gid = 0;
                                    }
                                }
                                for r in id {
                                    let mut room = RKQueueRoom.get(&r);
                                    if let Some(room) = room {
                                        room.borrow_mut().ready = 0;
                                        room.borrow_mut().gid = 0;
                                    }
                                }
                            }
                        }
                        if RKReadyGroups.len() >= MATCH_SIZE {
                            let mut fg: ReadyGameData = Default::default();
                            let mut prestart = false;
                            let mut total_rk: i16 = 0;
                            let mut rm_ids: Vec<u64> = vec![];
                            println!("RKReadyGroup!! {}", RKReadyGroups.len());
                            let mut new_now2 = Instant::now();
                            for (id, rg) in &mut RKReadyGroups {
                                if rg.borrow().game_status == 0 && fg.team_len < MATCH_SIZE {
                                    if total_rk == 0 {
                                        total_rk += rg.borrow().avg_rk as i16;
                                        fg.group.push(rg.borrow().rid.clone());
                                        fg.gid.push(*id);
                                        fg.team_len += 1;
                                        continue;
                                    }

                                    let mut difference = 0;
                                    if fg.team_len > 0 {
                                        difference = i16::abs(rg.borrow().avg_rk as i16 - total_rk/fg.team_len as i16);
                                    }
                                    if difference <= SCORE_INTERVAL * rg.borrow().queue_cnt {
                                        total_rk += rg.borrow().avg_rk as i16;
                                        fg.group.push(rg.borrow().rid.clone());
                                        fg.team_len += 1;
                                        fg.gid.push(*id);
                                    }
                                    else {
                                        rg.borrow_mut().queue_cnt += 1;
                                    }
                                }
                                if fg.team_len == MATCH_SIZE {
                                    sender.send(RoomEventData::UpdateGame(PreGameData{rid: fg.group.clone()}));
                                    for id in fg.gid {
                                        rm_ids.push(id);
                                    }
                                    fg = Default::default();
                                }
                            }
                            for id in rm_ids {
                                let rg = RKReadyGroups.remove(&id);
                                if let Some(rg) = rg {
                                    for rid in &rg.borrow().rid {
                                        RKQueueRoom.remove(&rid);
                                    }
                                }
                            }
                        }
                    }
                    // rank
                    // AT
                    if atState == "open" {
                        new_now = Instant::now();
                        if ATQueueRoom.len() >= MATCH_SIZE {
                            let mut g: ReadyGroupData = Default::default();
                            let mut tq: Vec<Rc<RefCell<QueueRoomData>>> = vec![];
                            let mut id: Vec<u64> = vec![];
                            let mut new_now = Instant::now();
                            tq = ATQueueRoom.iter().map(|x|Rc::clone(x.1)).collect();
                            //println!("Collect Time: {:?}",Instant::now().duration_since(new_now));
                            //tq.sort_by_key(|x| x.borrow().avg_rk);
                            let mut new_now = Instant::now();
                            tq.sort_by_key(|x| x.borrow().avg_at);
                            //println!("Sort Time: {:?}",Instant::now().duration_since(new_now));
                            let mut new_now1 = Instant::now();
                            for (k, v) in &mut ATQueueRoom {
                                if g.user_len > 0 && g.user_len < TEAM_SIZE && (g.avg_at + v.borrow().queue_cnt*SCORE_INTERVAL) < v.borrow().avg_at {
                                    for r in g.rid {
                                        id.push(r);
                                    }
                                    g = Default::default();
                                    g.rid.push(v.borrow().rid);
                                    let mut at = (g.avg_at * g.user_len + v.borrow().avg_at * v.borrow().user_len) as i16 / (g.user_len + v.borrow().user_len) as i16;
                                    g.avg_at = at;
                                    g.user_len += v.borrow().user_len;
                                    v.borrow_mut().ready = 1;
                                    v.borrow_mut().gid = group_id + 1;
                                    v.borrow_mut().queue_cnt += 1;
                                }
                                if v.borrow().ready == 0 &&
                                    v.borrow().user_len as i16 + g.user_len <= TEAM_SIZE {

                                    let Difference: i16 = i16::abs(v.borrow().avg_at - g.avg_at);
                                    if g.avg_at == 0 || Difference <= SCORE_INTERVAL * v.borrow().queue_cnt {
                                        g.rid.push(v.borrow().rid);
                                        let mut at ;
                                        if (g.user_len + v.borrow().user_len > 0){
                                            at = (g.avg_at * g.user_len + v.borrow().avg_at * v.borrow().user_len) as i16 / (g.user_len + v.borrow().user_len) as i16;
                                        } else {
                                            g = Default::default();
                                            continue;
                                        }
                                        g.avg_at = at;
                                        g.user_len += v.borrow().user_len;
                                        v.borrow_mut().ready = 1;
                                        v.borrow_mut().gid = group_id + 1;
                                    }
                                    else {
                                        v.borrow_mut().queue_cnt += 1;
                                    }
                                }
                                if g.user_len == TEAM_SIZE {
                                    println!("match team_size!");
                                    group_id += 1;
                                    info!("new group_id: {}", group_id);
                                    g.gid = group_id;
                                    g.queue_cnt = 1;
                                    ATReadyGroups.insert(group_id, Rc::new(RefCell::new(g.clone())));
                                    g = Default::default();
                                }

                            }
                            //println!("Time 2: {:?}",Instant::now().duration_since(new_now1));
                            if g.user_len < TEAM_SIZE {
                                for r in g.rid {
                                    let mut room = ATQueueRoom.get(&r);
                                    if let Some(room) = room {
                                        room.borrow_mut().ready = 0;
                                        room.borrow_mut().gid = 0;
                                    }
                                }
                                for r in id {
                                    let mut room = ATQueueRoom.get(&r);
                                    if let Some(room) = room {
                                        room.borrow_mut().ready = 0;
                                        room.borrow_mut().gid = 0;
                                    }
                                }
                            }
                        }
                        if ATReadyGroups.len() >= MATCH_SIZE {
                            let mut fg: ReadyGameData = Default::default();
                            let mut prestart = false;
                            let mut total_at: i16 = 0;
                            let mut rm_ids: Vec<u64> = vec![];
                            println!("ATReadyGroup!! {}", ATReadyGroups.len());
                            let mut new_now2 = Instant::now();
                            for (id, rg) in &mut ATReadyGroups {
                                if rg.borrow().game_status == 0 && fg.team_len < MATCH_SIZE {
                                    if total_at == 0 {
                                        total_at += rg.borrow().avg_at as i16;
                                        fg.group.push(rg.borrow().rid.clone());
                                        fg.gid.push(*id);
                                        fg.team_len += 1;
                                        continue;
                                    }

                                    let mut difference = 0;
                                    if fg.team_len > 0 {
                                        difference = i16::abs(rg.borrow().avg_at as i16 - total_at/fg.team_len as i16);
                                    }
                                    if difference <= SCORE_INTERVAL * rg.borrow().queue_cnt {
                                        total_at += rg.borrow().avg_at as i16;
                                        fg.group.push(rg.borrow().rid.clone());
                                        fg.team_len += 1;
                                        fg.gid.push(*id);
                                    }
                                    else {
                                        rg.borrow_mut().queue_cnt += 1;
                                    }
                                }
                                if fg.team_len == MATCH_SIZE {
                                    sender.send(RoomEventData::UpdateGame(PreGameData{rid: fg.group.clone()}));
                                    for id in fg.gid {
                                        rm_ids.push(id);
                                    }
                                    fg = Default::default();
                                }
                            }
                            for id in rm_ids {
                                let rg = ATReadyGroups.remove(&id);
                                if let Some(rg) = rg {
                                    for rid in &rg.borrow().rid {
                                        ATQueueRoom.remove(&rid);
                                    }
                                }
                            }
                        }
                    }
                    // AT
                }

                recv(rx) -> d => {
                    let handle = || -> Result<(), Error> {
                        if let Ok(d) = d {
                            match d {
                                QueueData::UpdateRoom(x) => {
                                    if x.mode == "ng" {
                                        NGQueueRoom.insert(x.rid.clone(), Rc::new(RefCell::new(x.clone())));
                                    }else if x.mode == "rk" {
                                        RKQueueRoom.insert(x.rid.clone(), Rc::new(RefCell::new(x.clone())));
                                    }else if x.mode == "at" {
                                        ATQueueRoom.insert(x.rid.clone(), Rc::new(RefCell::new(x.clone())));
                                    }
                                }
                                QueueData::RemoveRoom(x) => {
                                    // ng
                                    let mut r = NGQueueRoom.get(&x.rid);
                                    if let Some(r) = r {
                                        let mut rg = NGReadyGroups.get(&r.borrow().gid);
                                        if let Some(rg) = rg {
                                            for rid in &rg.borrow().rid {
                                                if rid == &x.rid {
                                                    continue;
                                                }
                                                let mut room = NGQueueRoom.get(rid);
                                                if let Some(room) = room {
                                                    room.borrow_mut().gid = 0;
                                                    room.borrow_mut().ready = 0;
                                                }
                                            }
                                        }
                                        NGReadyGroups.remove(&r.borrow().gid);
                                    }
                                    NGQueueRoom.remove(&x.rid);
                                    // ng
                                    // rank
                                    r = RKQueueRoom.get(&x.rid);
                                    if let Some(r) = r {
                                        let mut rg = RKReadyGroups.get(&r.borrow().gid);
                                        if let Some(rg) = rg {
                                            for rid in &rg.borrow().rid {
                                                if rid == &x.rid {
                                                    continue;
                                                }
                                                let mut room = RKQueueRoom.get(rid);
                                                if let Some(room) = room {
                                                    room.borrow_mut().gid = 0;
                                                    room.borrow_mut().ready = 0;
                                                }
                                            }
                                        }
                                        RKReadyGroups.remove(&r.borrow().gid);
                                    }
                                    RKQueueRoom.remove(&x.rid);
                                    // rank
                                    // AT
                                    r = ATQueueRoom.get(&x.rid);
                                    if let Some(r) = r {
                                        let mut rg = ATReadyGroups.get(&r.borrow().gid);
                                        if let Some(rg) = rg {
                                            for rid in &rg.borrow().rid {
                                                if rid == &x.rid {
                                                    continue;
                                                }
                                                let mut room = ATQueueRoom.get(rid);
                                                if let Some(room) = room {
                                                    room.borrow_mut().gid = 0;
                                                    room.borrow_mut().ready = 0;
                                                }
                                            }
                                        }
                                        ATReadyGroups.remove(&r.borrow().gid);
                                    }
                                    ATQueueRoom.remove(&x.rid);
                                    // AT
                                },
                                QueueData::Control(x) => {
                                    if (x.mode == "rk") {
                                        if x.msg == "close" {
                                            rkState = "close";
                                        } else if x.msg == "open" {
                                            rkState = "open";
                                        }
                                    } else if (x.mode == "ng") {
                                        if x.msg == "close" {
                                            ngState = "close";
                                        } else if x.msg == "open" {
                                            ngState = "open";
                                        }
                                    } else if (x.mode == "at") {
                                        if x.msg == "close" {
                                            atState = "close";
                                        } else if x.msg == "open" {
                                            atState = "open";
                                        }
                                    }
                                }
                            }
                        }
                        Ok(())
                    };
                    if let Err(msg) = handle() {
                        println!("{:?}", msg);
                        continue;
                    }
                }
            }
        }
    });
    Ok(tx)
}

pub fn init(
    msgtx: Sender<MqttMsg>,
    sender: Sender<SqlData>,
    pool: mysql::Pool,
    redis_client: redis::Client,
    QueueSender1: Option<Sender<QueueData>>,
    isBackup: bool,
) -> Result<(Sender<RoomEventData>, Sender<QueueData>), Error> {
    let (tx, rx): (Sender<RoomEventData>, Receiver<RoomEventData>) = bounded(10000);
    let mut tx1: Sender<QueueData>;
    match QueueSender1.clone() {
        Some(s) => {
            tx1 = s;
            println!("in");
        }
        None => {
            tx1 = HandleQueueRequest(msgtx.clone(), tx.clone(), pool.clone())?;
            println!("2 in");
        }
    }
    let start = Instant::now();
    let update5000ms = tick(Duration::from_millis(5000));
    let update200ms = tick(Duration::from_millis(200));
    let update1000ms = tick(Duration::from_millis(1000));
    let QueueSender = tx1.clone();

    let tx2 = tx.clone();
    thread::spawn(move || -> Result<(), Error> {
        let mut conn = pool.get_conn()?;
        let redis_conn: &mut redis::Connection = &mut redis_client.get_connection()?;
        let _ : () = redis::cmd("FLUSHALL").query(redis_conn)?;
        let mut isServerLive = true;
        let mut isBackup = isBackup.clone();
        let mut TotalRoom: BTreeMap<u64, Rc<RefCell<RoomData>>> = BTreeMap::new();
        //let mut QueueRoom: BTreeMap<u64, Rc<RefCell<RoomData>>> = BTreeMap::new();
        let mut ReadyGroups: BTreeMap<u64, Rc<RefCell<FightGroup>>> = BTreeMap::new();
        let mut PreStartGroups: BTreeMap<u64, Rc<RefCell<FightGame>>> = BTreeMap::new();
        let mut GameingGroups: BTreeMap<u64, Rc<RefCell<FightGame>>> = BTreeMap::new();
        let mut TotalUsers: BTreeMap<String, Rc<RefCell<User>>> = BTreeMap::new();
        let mut RestrictedUsers: BTreeMap<String, Rc<RefCell<RestrictedData>>> = BTreeMap::new();
        let mut InGameUsers: BTreeMap<String, Rc<RefCell<User>>> = BTreeMap::new();
        let mut LossSend: Vec<MqttMsg> = vec![];
        let mut AbandonGames: BTreeMap<u64, bool> = BTreeMap::new();
        let mut room_id: u64 = 1;
        let mut group_id: u64 = 0;
        let mut game_id: u64 = 0;
        let mut game_port: u16 = 7777;
        let mut ngState = "open";
        let mut rkState = "open";
        let mut atState = "open";

        let sql = format!(r#"select id, ng, rk, at, name, hero from user;"#);
        let qres2: mysql::QueryResult = conn.query(sql.clone())?;
        let mut userid: String = "".to_owned();
        let mut ng: i16 = 0;
        let mut rk: i16 = 0;
        let mut name: String = "".to_owned();
        let id = 0;
        for row in qres2 {
            let a = row?.clone();
            let user = User {
                id: mysql::from_value(a.get("id").unwrap()),
                name: mysql::from_value(a.get("name").unwrap()),
                hero: mysql::from_value(a.get("hero").unwrap()),
                ng: mysql::from_value(a.get("ng").unwrap()),
                rk: mysql::from_value(a.get("rk").unwrap()),
                at: mysql::from_value(a.get("at").unwrap()),
                ..Default::default()
            };
            println!("{:?}", user);
            userid = mysql::from_value(a.get("id").unwrap());
            //println!("userid: {}", userid);
            //ng = mysql::from_value(a.get("ng").unwrap());
            //rk = mysql::from_value(a.get("rk").unwrap());
            //name = mysql::from_value(a.get("name").unwrap());
            TotalUsers.insert(userid, Rc::new(RefCell::new(user.clone())));
        }
        /*
        let get_game_id = format!("select MAX(game_id) from game_info;");
        let qres3: mysql::QueryResult = conn.query(get_game_id.clone())?;
        for row in qres3 {
            let a = row?.clone();
            game_id = mysql::from_value(a.get("MAX(game_id)").unwrap());
            //println!("game id: {}", game_id);
        }
        */
        println!("game id: {}", game_id);
        loop {
            select! {
                recv(update200ms) -> _ => {
                    //show(start.elapsed());
                    // update prestart groups
                    let mut rm_ids: Vec<u64> = vec![];
                    let mut start_cnt: u16 = 0;
                    for (id, group) in &mut PreStartGroups {
                        //if start_cnt >= 10 {
                        //    thread::sleep(Duration::from_millis(1000));
                        //    break;
                        //}
                        let res = group.borrow().check_prestart();
                        match res {
                            PrestartStatus::Ready => {
                                // //here to select hero
                                // if group.borrow().choose_time == 0 {
                                //    for user in group.borrow().user_names.clone() {
                                //         msgtx.try_send(MqttMsg{topic:format!("member/{}/res/ng_choose_hero", user),
                                //                 msg: format!(r#"{{"id":"{}","hero":""}}"#, user)})?;
                                //     }
                                // }
                                // let mut choose_cnt = 0;
                                // for user in group.borrow().user_names.clone() {
                                //     let u = get_user(&user, &TotalUsers);
                                //     if let Some(u) = u {
                                //         if u.borrow().hero.chars().count() > 2 {
                                //             choose_cnt += 1;
                                //         }
                                //     }
                                // }
                                // if choose_cnt >= 10 {
                                //     start_cnt += 1;
                                //     rm_ids.push(*id);
                                //     game_port += 1;
                                //     if game_port > 65500 {
                                //         game_port = 7777;
                                //     }
                                //     group.borrow_mut().ready();
                                //     group.borrow_mut().update_names();
                                //     group.borrow_mut().game_port = game_port;

                                //     GameingGroups.remove(&group.borrow().game_id);
                                //     GameingGroups.insert(group.borrow().game_id.clone(), group.clone());
                                //     // start game
                                //     msgtx.try_send(MqttMsg{topic:format!("game/{}/res/game_signal", group.borrow().game_id),
                                //                 msg: format!(r#"{{"id":"{}","hero":""}}"#, group.borrow().game_id)})?;
                                //     for user in group.borrow().user_names.clone() {
                                //         let u = get_user(&user, &TotalUsers);
                                //         if let Some(u) = u {
                                //             let _ : () = redis_conn.set(format!("g{}", u.borrow().id), game_id)?;
                                //         }
                                //     }
                                // }else if group.borrow().choose_time >= CHOOSE_HERO_TIME {
                                //     rm_ids.push(*id);
                                //     for user in group.borrow().user_names.clone() {
                                //         msgtx.try_send(MqttMsg{topic:format!("member/{}/res/ng_choose_hero", user),
                                //             msg: format!(r#"{{"msg":"timeout"}}"#)})?;
                                //     }
                                // }
                                // group.borrow_mut().choose_time += 2;
                            },
                            PrestartStatus::Cancel => {
                                group.borrow_mut().update_names();
                                group.borrow_mut().clear_queue();
                                group.borrow_mut().game_status = 0;
                                let mut rm_rid: Vec<u64> = vec![];
                                for t in &group.borrow().teams {
                                    for c in &t.borrow().checks {
                                        if c.check < 0 {
                                            let u = TotalUsers.get(&c.id);
                                            if let Some(u) = u {
                                                rm_rid.push(u.borrow().rid);
                                            }
                                        }
                                    }
                                }
                                for r in &group.borrow().room_names {
                                    if !isBackup || (isBackup && isServerLive == false) {
                                        msgtx.try_send(MqttMsg{topic:format!("room/{}/res/prestart", r),
                                            msg: format!(r#"{{"msg":"stop queue"}}"#)})?;
                                        LossSend.push(MqttMsg{topic:format!("room/{}/res/prestart", r),
                                            msg: format!(r#"{{"msg":"stop queue"}}"#)});
                                    }

                                }
                                // for t in &group.borrow().teams {
                                //     for r in &t.borrow().rooms {
                                //         if !rm_rid.contains(&r.borrow().rid) {
                                //             let mut user_ids: Vec<String> = Vec::new();
                                //             for user in &r.borrow().users {
                                //                 user_ids.push(user.borrow().id.clone());
                                //             }
                                //             let mut data = QueueRoomData {
                                //                 rid: r.borrow().rid.clone(),
                                //                 gid: 0,
                                //                 user_len: r.borrow().users.len().clone() as i16,
                                //                 user_ids: user_ids,
                                //                 avg_ng: r.borrow().avg_ng.clone(),
                                //                 avg_rk: r.borrow().avg_rk.clone(),
                                //                 avg_at: r.borrow().avg_at.clone(),
                                //                 ready: 0,
                                //                 notify: false,
                                //                 queue_cnt: 1,
                                //                 mode: r.borrow().mode.clone(),
                                //             };
                                //             QueueSender.send(QueueData::UpdateRoom(data));
                                //         }
                                //     }
                                // }
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
                recv(update1000ms) -> _ => {
                    if !isBackup || (isBackup && isServerLive == false) {
                        //msgtx.try_send(MqttMsg{topic:format!("server/0/res/heartbeat"),
                        //                    msg: format!(r#"{{"msg":"live"}}"#)})?;
                    }
                    let mut rm_list: Vec<String> = Vec::new();
                    for (id, restriction) in &mut RestrictedUsers {
                        restriction.borrow_mut().time -= 1;
                        if restriction.borrow().time == 0 {
                            rm_list.push(id.clone());
                        }
                    }
                    for rm in rm_list {
                        RestrictedUsers.remove(&rm);
                    }
                }

                recv(update5000ms) -> _ => {
                    // check isInGame
                    let mut inGameRm_list: Vec<String> = Vec::new();
                    for (id, u) in &mut InGameUsers {
                        // println!("in game id : {}", id);
                        let inGame: std::result::Result<String, redis::RedisError> = redis_conn.get(format!("g{}",id.clone()));
                        match inGame {
                           Ok(game_id) => {
                               // pinrlnt!("game_id : {}", game_id);
                                let gameInfo: std::result::Result<String, redis::RedisError> = redis_conn.get(format!("gid{}",game_id));
                                match gameInfo {
                                    Ok(game_info) => {
                                        // println!("gid{} : {}", game_id, game_info);
                                        // println!("r : {}", res);
                                        let data: StartGameData = serde_json::from_str(&String::from(game_info))?;
                                        let mut gameOverData = GameOverData{
                                            game: data.game,
                                            mode: data.mode,
                                            win: Vec::new(),
                                            lose: Vec::new()
                                        };
                                        let mut find_res = false;
                                        for player_id in data.players.clone() {
                                            if player_id.len() > 0 {
                                                let isGameOver: std::result::Result<String, redis::RedisError> = redis_conn.get(format!("r{}",player_id.clone()));
                                                match isGameOver {
                                                    Ok(res) => {
                                                        // println!("res : {}", res);
                                                        if res == "W" {
                                                            gameOverData.win.push(player_id.clone());
                                                        } else {
                                                            gameOverData.lose.push(player_id.clone());
                                                        }
                                                        let _: () = redis_conn.del(format!("r{}", player_id.clone()))?;
                                                        let _: () = redis_conn.del(format!("g{}", player_id.clone()))?;
                                                        let mqttmsg = MqttMsg{topic:format!("member/{}/res/check_in_game", player_id.clone()),
                                                            msg: format!(r#"{{"msg":"game over"}}"#)};
                                                        find_res = true;
                                                    },
                                                    Err(e) => {
                
                                                    }
                                                }
                                            }
                                        }
                                        if (find_res) {
                                            let _: () = redis_conn.del(format!("gid{}", game_id))?;
                                            tx2.try_send(RoomEventData::GameOver(gameOverData));
                                        }
                                    },
                                    Err(e) => {

                                    }
                                }
                           },
                           Err(e) => {
                                inGameRm_list.push(id.clone());
                                let mqttmsg = MqttMsg{topic:format!("member/{}/res/check_in_game", id.clone()),
                                    msg: format!(r#"{{"msg":"game over"}}"#)};
                                msgtx.try_send(mqttmsg);
                           }
                        }
                    }
                    for rm in inGameRm_list {
                        InGameUsers.remove(&rm);
                    }
                    //println!("rx len: {}, tx len: {}", rx.len(), tx2.len());
                    LossSend.clear();
                    let mut rm_list: Vec<u64> = Vec::new();
                    for (id, group) in &mut PreStartGroups {
                        group.borrow_mut().ready_cnt += 5;
                        let res1 = group.borrow().check_start_get();
                        if res1 == true {
                            for r in &group.borrow().room_names {
                                msgtx.try_send(MqttMsg{topic:format!("room/{}/res/start_get", r), msg: format!(r#"{{"msg":"start", "room":"{}",
                                "game":"{}", "players":{:?}}}"#, r, game_id, &group.borrow().user_names)});
                            }
                            rm_list.push(id.clone());
                        }else if group.borrow().ready_cnt >= READY_TIME {
                            for team in &group.borrow().teams {
                                for r in &team.borrow().rooms {
                                    // let res = r.borrow_mut().check_start_get();
                                    // if res == false {
                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/start_get", r.borrow().master), msg: r#"{"msg":"timeout"}"#.to_string()})?;
                                    // } else {
                                    //     let mqttmsg = MqttMsg{topic:format!("room/{}/send/start_queue", r.borrow().master.clone()),
                                    //         msg: format!(r#"{{"room":"{}", "action": "start queue", "mode":"{}", "id":"{}"}}"#,r.borrow().master.clone(), r.borrow().mode.clone(), r.borrow().master.clone())};
                                    //     msgtx.try_send(mqttmsg)?;
                                    // }
                                }
                            }
                            rm_list.push(id.clone());
                        }
                    }
                    for rm in rm_list{
                        PreStartGroups.remove(&rm);
                    }
                }
                recv(rx) -> d => {
                    let handle = || -> Result<(), Error> {
                        let mut mqttmsg: MqttMsg = MqttMsg{topic: format!(""), msg: format!("")};
                        if let Ok(d) = d {
                            match d {
                                RoomEventData::Status(x) => {
                                    let u = get_user(&x.id, &TotalUsers);
                                    if let Some(u) = u {
                                        if u.borrow().game_id != 0 {
                                            mqttmsg = MqttMsg{topic:format!("member/{}/res/status", x.id),
                                                msg: format!(r#"{{"msg":"gaming"}}"#)};
                                            //msgtx.try_send(MqttMsg{topic:format!("member/{}/res/status", x.id),
                                            //    msg: format!(r#"{{"msg":"gaming"}}"#)})?;
                                        } else {
                                            mqttmsg = MqttMsg{topic:format!("member/{}/res/status", x.id),
                                                msg: format!(r#"{{"msg":"normal"}}"#)};
                                            //msgtx.try_send(MqttMsg{topic:format!("member/{}/res/status", x.id),
                                            //    msg: format!(r#"{{"msg":"normal"}}"#)})?;
                                        }
                                    } else {
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/status", x.id),
                                            msg: format!(r#"{{"msg":"id not found"}}"#)};
                                        //msgtx.try_send(MqttMsg{topic:format!("member/{}/res/status", x.id),
                                        //        msg: format!(r#"{{"msg":"id not found"}}"#)})?;
                                    }
                                    //info!("Status TotalUsers {:#?}", TotalUsers);
                                },
                                RoomEventData::Reconnect(x) => {
                                    let u = get_user(&x.id, &TotalUsers);
                                    if let Some(u) = u {
                                        let g = GameingGroups.get(&u.borrow().game_id);
                                        if let Some(g) = g {
                                            mqttmsg = MqttMsg{topic:format!("member/{}/res/reconnect", x.id),
                                                msg: format!(r#"{{"server":"172.104.78.55:{}"}}"#, g.borrow().game_port)};
                                            //msgtx.try_send(MqttMsg{topic:format!("member/{}/res/reconnect", x.id),
                                            //    msg: format!(r#"{{"server":"172.104.78.55:{}"}}"#, g.borrow().game_port)})?;
                                        }
                                    }
                                },
                                RoomEventData::GameClose(x) => {
                                    //let p = PreStartGroups.remove(&x.game);
                                    let g = GameingGroups.remove(&x.game);
                                    if let Some(g) = g {
                                        for u in &g.borrow().user_names {
                                            let u = get_user(&u, &TotalUsers);
                                            match u {
                                                Some(u) => {
                                                    // remove room
                                                    let r = TotalRoom.get(&u.borrow().rid);
                                                    let mut is_null = false;
                                                    if let Some(r) = r {
                                                        r.borrow_mut().rm_user(&u.borrow().id);
                                                        r.borrow_mut().ready = 0;
                                                        if r.borrow().users.len() == 0 {
                                                            is_null = true;
                                                            //info!("remove success {}", u.borrow().id);
                                                        }
                                                    }
                                                    else {
                                                        //info!("remove fail {}", u.borrow().id);
                                                    }
                                                    if is_null {
                                                        TotalRoom.remove(&u.borrow().rid);
                                                        //QueueRoom.remove(&u.borrow().rid);
                                                    }
                                                    u.borrow_mut().rid = 0;
                                                    u.borrow_mut().gid = 0;
                                                    u.borrow_mut().game_id = 0;
                                                },
                                                None => {
                                                    //info!("remove fail ");
                                                }
                                            }
                                        }
                                        //info!("GameClose {}", x.game);
                                        //info!("TotalUsers {:#?}", TotalUsers);
                                    }
                                    else {
                                        //info!("GameingGroups {:#?}", GameingGroups);
                                    }
                                },
                                RoomEventData::GameOver(x) => {
                                    let win = get_users(&x.win, &TotalUsers)?;
                                    let lose = get_users(&x.lose, &TotalUsers)?;
                                    settlement_score(&win, &lose, &msgtx, &sender, &mut conn, x.mode);
                                },
                                RoomEventData::GameInfo(x) => {
                                    //println!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                                    for u in &x.users {
                                        let mut update_info: SqlGameInfoData = Default::default();
                                        update_info.game = x.game.clone();
                                        update_info.id = u.id.clone();
                                        update_info.hero = u.hero.clone();
                                        update_info.level = u.level.clone();
                                        for (i, e) in u.equ.iter().enumerate() {
                                            update_info.equ += e;
                                            if i < u.equ.len()-1 {
                                                update_info.equ += ", ";
                                            }
                                        }
                                        update_info.damage = u.damage.clone();
                                        update_info.take_damage = u.take_damage.clone();
                                        update_info.heal = u.heal.clone();
                                        update_info.kill = u.kill.clone();
                                        update_info.death = u.death.clone();
                                        update_info.assist = u.assist.clone();
                                        update_info.gift = u.gift.clone();
                                        sender.send(SqlData::UpdateGameInfo(update_info));
                                    }
                                },
                                RoomEventData::StartGame(x) => {
                                    let u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        if !isBackup || (isBackup && isServerLive == false) {
                                            AbandonGames.insert(x.game.clone(), true);
                                            let _ : () = redis_conn.set(format!("gid{}", x.game.clone()), serde_json::to_string(&x)?)?;
                                            for player in &x.players {
                                                let u2 = get_user(player, &TotalUsers);
                                                if let Some(u2) = u2 {
                                                    InGameUsers.insert(u2.borrow().id.clone(), u2.clone());
                                                    let _ : () = redis_conn.set(format!("g{}", u2.borrow().id.clone()), x.game.clone())?;
                                                    msgtx.try_send(MqttMsg{topic:format!("member/{}/res/check_in_game", u2.borrow().id.clone()),
                                                        msg: format!(r#"{{"msg":"in game"}}"#, )})?;
                                                }
                                            }
                                        }
                                    }
                                },
                                RoomEventData::Leave(x) => {
                                    let u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                            let r = TotalRoom.get(&u.borrow().rid);
                                            //println!("id: {}, rid: {}", u.borrow().id, &get_rid_by_id(&u.borrow().id, &TotalUsers));
                                            let mut is_null = false;
                                            if let Some(r) = r {
                                                let m = r.borrow().master.clone();
                                                r.borrow_mut().rm_user(&x.id);
                                                if r.borrow().users.len() > 0 {
                                                    r.borrow().publish_update(&msgtx, m)?;
                                                }
                                                else {
                                                    is_null = true;
                                                }
                                                mqttmsg = MqttMsg{topic:format!("room/{}/res/leave", x.id),
                                                    msg: format!(r#"{{"id": "{}","msg":"room leave"}}"#, x.id)};
                                                //msgtx.try_send(MqttMsg{topic:format!("room/{}/res/leave", x.id),
                                                //    msg: format!(r#"{{"msg":"ok"}}"#)})?;
                                            }
                                            if is_null {
                                                TotalRoom.remove(&u.borrow().rid);
                                                //println!("Totalroom rid: {}", &u.borrow().rid);
                                                QueueSender.send(QueueData::RemoveRoom(RemoveRoomData{rid: u.borrow().rid}));
                                                //QueueRoom.remove(&u.borrow().rid);
                                            }
                                            u.borrow_mut().rid = 0;
                                            //println!("id: {}, rid: {}", u.borrow().id, &get_rid_by_id(&u.borrow().id, &TotalUsers));
                                    }
                                },
                                RoomEventData::ChooseNGHero(x) => {
                                    let u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        u.borrow_mut().hero = x.hero.clone();
                                        // conn.query(format!("update user set hero='{}' where id='{}';",u.borrow().hero, u.borrow().id))?;
                                        // key : steamid, value : hero
                                    }
                                    let _ : () = redis_conn.set(x.id.clone(), x.hero.clone())?;
                                    mqttmsg = MqttMsg{topic:format!("member/{}/res/ng_choose_hero", x.id.clone()),
                                        msg: format!(r#"{{"id":"{}", "hero":"{}"}}"#, x.id.clone(), x.hero.clone())};
                                },
                                RoomEventData::LockedNGHero(x) => {
                                    let u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        u.borrow_mut().hero = x.hero;
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/ng_locked_hero", u.borrow().id),
                                            msg: format!(r#"{{"id":"{}", "hero":"{}"}}"#, u.borrow().id, u.borrow().hero)};
                                    }
                                },
                                RoomEventData::BanHero(x) => {
                                    let u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        u.borrow_mut().hero = x.hero;
                                        let _ : () = redis_conn.set(format!("b{}", u.borrow().id.clone()), u.borrow().hero.clone())?;
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/ban_hero", u.borrow().id),
                                            msg: format!(r#"{{"id":"{}", "hero":"{}"}}"#, u.borrow().id, u.borrow().hero)};
                                    }
                                },
                                RoomEventData::NGGameChooseHero(x) => {
                                    for (id, rg) in &x {
                                        for rid in rg {
                                            let r = TotalRoom.get(rid);
                                            if let Some(r) = r {
                                                for user in r.borrow().users.clone() {
                                                    let mut mqttmsg1: MqttMsg = MqttMsg{topic: format!(""), msg: format!("")};
                                                    let msgtx2 = msgtx.clone();
                                                    mqttmsg1 = MqttMsg{topic:format!("member/{}/res/ng_choose_hero", user.borrow().id),
                                                        msg: format!(r#"{{"id":"{}", "action":"choose hero"}}"#, user.borrow().id)};
                                                    msgtx2.try_send(mqttmsg1);
                                                }
                                            }
                                        }
                                    }
                                },
                                RoomEventData::Invite(x) => {
                                    if TotalUsers.contains_key(&x.from) {
                                        mqttmsg = MqttMsg{topic:format!("room/{}/res/invite", x.invite.clone()),
                                            msg: format!(r#"{{"room":"{}","from":"{}"}}"#, x.room.clone(), x.from.clone())};
                                        //msgtx.try_send(MqttMsg{topic:format!("room/{}/res/invite", x.invite.clone()),
                                        //    msg: format!(r#"{{"room":"{}","from":"{}"}}"#, x.room.clone(), x.from.clone())})?;
                                    }
                                },
                                RoomEventData::Join(x) => {
                                    let u = TotalUsers.get(&x.room);
                                    let j = TotalUsers.get(&x.join);
                                    let mut sendok = false;
                                    if let Some(u) = u {
                                        if let Some(j) = j {
                                            let r = TotalRoom.get(&u.borrow().rid);
                                            if let Some(r) = r {
                                                if r.borrow().mode == "rk"{
                                                    if r.borrow().ready == 0 && r.borrow().users.len() < 2 as usize {
                                                        r.borrow_mut().add_user(Rc::clone(j));
                                                        let m = r.borrow().master.clone();
                                                        r.borrow().publish_update(&msgtx, m)?;
                                                        mqttmsg = MqttMsg{topic:format!("room/{}/res/join", x.join.clone()),
                                                            msg: format!(r#"{{"room":"{}","mode":"{}","msg":"ok"}}"#, r.borrow().master, r.borrow().mode)};
                                                        sendok = true;
                                                        // let rid = x.room.parse::<u64>().unwrap();
                                                        let rid = u.borrow().rid;
                                                        u.borrow_mut().rid = rid;
                                                    }
                                                }else if r.borrow().ready == 0 && r.borrow().users.len() < TEAM_SIZE as usize {
                                                    r.borrow_mut().add_user(Rc::clone(j));
                                                    let m = r.borrow().master.clone();
                                                    r.borrow().publish_update(&msgtx, m)?;
                                                    mqttmsg = MqttMsg{topic:format!("room/{}/res/join", x.join.clone()),
                                                        msg: format!(r#"{{"room":"{}","mode":"{}","msg":"ok"}}"#, r.borrow().master, r.borrow().mode)};
                                                    sendok = true;
                                                    // let rid = x.room.parse::<u64>().unwrap();
                                                    let rid = u.borrow().rid;
                                                    u.borrow_mut().rid = rid;
                                                }
                                            }
                                        }
                                    }
                                    if x.join == "-1" {
                                        mqttmsg = MqttMsg{topic:format!("room/{}/res/join", x.join.clone()),
                                            msg: format!(r#"{{"room":"{}","msg":"rejected"}}"#, x.room.clone())};
                                    }else if sendok == false {
                                        mqttmsg = MqttMsg{topic:format!("room/{}/res/join", x.join.clone()),
                                            msg: format!(r#"{{"room":"{}","msg":"full"}}"#, x.room.clone())};
                                    }
                                },
                                RoomEventData::Reject(x) => {
                                    if TotalUsers.contains_key(&x.id) {
                                        mqttmsg = MqttMsg{topic:format!("room/{}/res/reject", x.room.clone()),
                                            msg: format!(r#"{{"room":"{}","id":"{}","msg":"reject"}}"#, x.room.clone(), x.id.clone())};
                                    }
                                },
                                RoomEventData::Jump(x) => {
                                    if TotalUsers.contains_key(&x.id) {
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/jump", x.game.clone()),
                                            msg: format!(r#"{{"id":"{}","mgs":"jump"}}"#, x.id.clone())};
                                    }
                                },
                                RoomEventData::CheckRestriction(x) => {
                                    let r = RestrictedUsers.get(&x.id);
                                    if let Some(r) = r {
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/check_restriction", x.id.clone()),
                                            msg: format!(r#"{{"time":"{}"}}"#, r.borrow().time)};
                                    }
                                },
                                RoomEventData::CheckInGame(x) => {
                                    let inGame: std::result::Result<u64, redis::RedisError> = redis_conn.get(format!("g{}",x.id.clone()));
                                    match inGame {
                                       Ok(v) => {
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/check_in_game", x.id.clone()),
                                            msg: format!(r#"{{"msg":"in game"}}"#)};
                                       },
                                       Err(e) => {
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/check_in_game", x.id.clone()),
                                            msg: format!(r#"{{"msg":"out of game"}}"#)};
                                       }
                                    }
                                },
                                RoomEventData::LeaveGame(x) => {
                                    let _: () = redis_conn.del(format!("g{}", x.id))?;
                                    tx2.try_send(RoomEventData::CheckInGame(CheckInGameData{id: x.id}));
                                },
                                RoomEventData::Reset() => {
                                    TotalRoom.clear();
                                    //QueueRoom.clear();
                                    ReadyGroups.clear();
                                    PreStartGroups.clear();
                                    GameingGroups.clear();
                                    TotalUsers.clear();
                                    room_id = 0;
                                },
                                RoomEventData::PreStart(x) => {
                                    let u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        let gid = u.borrow().gid;
                                        if u.borrow().start_get == false {
                                            if gid != 0 {
                                                let g = ReadyGroups.get(&gid);
                                                if let Some(gr) = g {
                                                    if x.accept == true {
                                                        gr.borrow_mut().user_ready(&x.id);
                                                        u.borrow_mut().start_get = true;
                                                        // mqttmsg = MqttMsg{topic:format!("room/{}/res/start_get", u.borrow().id),
                                                        //     msg: format!(r#"{{"msg":"start", "room":"{}"}}"#, &x.room)};
                                                    } else {
                                                        println!("accept false!");
                                                        gr.borrow_mut().user_cancel(&x.id);
                                                        for r in &gr.borrow().rooms {
                                                            println!("r_rid: {}, u_rid: {}", r.borrow().rid, u.borrow().rid);
                                                            if r.borrow().rid != u.borrow().rid {
                                                                let mut user_ids: Vec<String> = Vec::new();
                                                                for user in &r.borrow().users {
                                                                    user_ids.push(user.borrow().id.clone());
                                                                }
                                                                let mut data = QueueRoomData {
                                                                    rid: r.borrow().rid.clone(),
                                                                    gid: 0,
                                                                    user_len: r.borrow().users.len().clone() as i16,
                                                                    user_ids: user_ids,
                                                                    avg_ng: r.borrow().avg_ng.clone(),
                                                                    avg_rk: r.borrow().avg_rk.clone(),
                                                                    avg_at: r.borrow().avg_at.clone(),
                                                                    ready: 0,
                                                                    notify: false,
                                                                    queue_cnt: 1,
                                                                    mode: r.borrow().mode.clone(),
                                                                };
                                                                QueueSender.send(QueueData::UpdateRoom(data));
                                                            }
                                                        }
                                                        ReadyGroups.remove(&gid);
                                                        let r = TotalRoom.get(&u.borrow().rid);
                                                        //QueueSender.send(QueueData::RemoveRoom(RemoveRoomData{rid: u.borrow().rid}));
                                                        if let Some(r) = r {
                                                            mqttmsg = MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master),
                                                                msg: format!(r#"{{"msg":"ok"}}"#)};
                                                            //msgtx.try_send(MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master),
                                                            //    msg: format!(r#"{{"msg":"ok"}}"#)})?;
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    error!("gid not found {}", gid);
                                                }
                                            }
                                        }
                                    }
                                },
                                RoomEventData::UpdateGame(x) => {
                                    //println!("Update Game!");
                                    let mut fg: FightGame = Default::default();
                                    for r in &x.rid {
                                        let mut g: FightGroup = Default::default();
                                        for rid in r {
                                            let room = TotalRoom.get(&rid);
                                            if let Some(room) = room {
                                                g.add_room(Rc::clone(&room));
                                            }
                                        }
                                        g.prestart();
                                        group_id += 1;
                                        g.set_group_id(group_id);
                                        g.game_status = 1;
                                        ReadyGroups.insert(group_id, Rc::new(RefCell::new(g.clone())));
                                        let mut rg = ReadyGroups.get(&group_id);
                                        if let Some(rg) = rg {
                                            fg.teams.push(Rc::clone(rg));
                                        }
                                    }

                                    fg.update_names();
                                    for r in &fg.room_names {
                                        //thread::sleep_ms(100);
                                        if !isBackup || (isBackup && isServerLive == false) {
                                            msgtx.try_send(MqttMsg{topic:format!("room/{}/res/ready", r), msg: r#"{"msg":"ready"}"#.to_string()})?;
                                        }
                                    }

                                    game_id += 1;
                                    fg.set_game_id(game_id);
                                    PreStartGroups.insert(game_id, Rc::new(RefCell::new(fg)));

                                },
                                RoomEventData::StartGet(x) => {
                                    let mut u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        u.borrow_mut().start_get = true;
                                        println!("start get");
                                    }
                                },
                                RoomEventData::StartQueue(x) => {
                                    let mut success = false;
                                    let mut hasRoom = false;
                                    let u = TotalUsers.get(&x.id);
                                    let mut rid = 0;
                                    let mut ng = 0;
                                    let mut rk = 0;
                                    let mut at = 0;
                                    if let Some(u) = u {
                                        if u.borrow().rid != 0 {
                                            hasRoom = true;
                                            rid = u.borrow().rid;
                                        }else{
                                            rid = u.borrow().id.parse().unwrap();
                                            ng = u.borrow().ng;
                                            rk = u.borrow().rk;
                                            at = u.borrow().at;
                                        }
                                        if hasRoom {
                                            let r = TotalRoom.get(&rid);
                                            if let Some(y) = r {
                                                y.borrow_mut().update_avg();
                                                let mut user_ids: Vec<String> = Vec::new();
                                                for user in &y.borrow().users {
                                                    user_ids.push(user.borrow().id.clone());
                                                }
                                                let mut data = QueueRoomData {
                                                    rid: y.borrow().rid.clone(),
                                                    gid: 0,
                                                    user_len: y.borrow().users.len().clone() as i16,
                                                    user_ids: user_ids,
                                                    avg_ng: y.borrow().avg_ng.clone(),
                                                    avg_rk: y.borrow().avg_rk.clone(),
                                                    avg_at: y.borrow().avg_at.clone(),
                                                    ready: 0,
                                                    notify: false,
                                                    queue_cnt: 1,
                                                    mode: y.borrow().mode.clone(),
                                                };
                                                QueueSender.send(QueueData::UpdateRoom(data));
                                                success = true;
                                                if success {
                                                    mqttmsg = MqttMsg{topic:format!("room/{}/res/start_queue", y.borrow().master.clone()),
                                                        msg: format!(r#"{{"msg":"ok", "mode": "{}"}}"#, y.borrow().mode.clone())};
                                                } else {
                                                    mqttmsg = MqttMsg{topic:format!("room/{}/res/start_queue", y.borrow().master.clone()),
                                                        msg: format!(r#"{{"msg":"fail"}}"#)}
                                                }
                                            }
                                        }else{
                                            tx2.try_send(RoomEventData::Create(CreateRoomData{id: x.id.clone(), mode: x.mode.clone()}));
                                            let mut user_ids: Vec<String> = Vec::new();
                                            user_ids.push(x.id.clone());
                                            let mut data = QueueRoomData {
                                                rid: rid,
                                                gid: 0,
                                                user_len: 1,
                                                user_ids: user_ids,
                                                avg_ng: ng,
                                                avg_rk: rk,
                                                avg_at: at,
                                                ready: 0,
                                                notify: false,
                                                queue_cnt: 1,
                                                mode: x.mode.clone(),
                                            };
                                            QueueSender.send(QueueData::UpdateRoom(data));
                                            success = true;
                                            if success {
                                                mqttmsg = MqttMsg{topic:format!("room/{}/res/start_queue", rid),
                                                    msg: format!(r#"{{"msg":"ok"}}"#)};
                                            } else {
                                                mqttmsg = MqttMsg{topic:format!("room/{}/res/start_queue", rid),
                                                    msg: format!(r#"{{"msg":"fail"}}"#)}
                                            }
                                        }
                                    }
                                },
                                RoomEventData::Ready(x) => {
                                    let u = TotalUsers.get(&x.id);
                                    let mut rid = 0;
                                    let mut ng = 0;
                                    let mut rk = 0;
                                    if let Some(u) = u {
                                        if u.borrow().rid != 0 {
                                            rid = u.borrow().rid;
                                        }
                                        let r = TotalRoom.get(&rid);
                                        if let Some(y) = r {
                                            let mut user_ids: Vec<String> = Vec::new();
                                            for user in &y.borrow().users {
                                                user_ids.push(user.borrow().id.clone());
                                            }
                                            let mut ready = y.borrow().ready;
                                            let mut data = QueueRoomData {
                                                rid: y.borrow().rid.clone(),
                                                gid: 0,
                                                user_len: y.borrow().users.len().clone() as i16,
                                                user_ids: user_ids,
                                                avg_ng: y.borrow().avg_ng.clone(),
                                                avg_rk: y.borrow().avg_rk.clone(),
                                                avg_at: y.borrow().avg_at.clone(),
                                                ready: ready + 1,
                                                notify: true,
                                                queue_cnt: 1,
                                                mode: y.borrow().mode.clone(),
                                            };
                                                QueueSender.send(QueueData::UpdateRoom(data));
                                        }
                                    }
                                },
                                RoomEventData::CancelQueue(x) => {
                                    let mut success = false;
                                    let u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        //let r = QueueRoom.remove(&u.borrow().rid);
                                        let g = ReadyGroups.get(&u.borrow().gid);
                                        if let Some(g) = g {
                                            for r in &g.borrow().rooms {
                                                r.borrow_mut().ready = 0;
                                            }
                                        }
                                        let gr = ReadyGroups.remove(&u.borrow().gid);
                                        let r = TotalRoom.get(&u.borrow().rid);
                                        //println!("Totalroom rid: {}", &u.borrow().rid);
                                        QueueSender.send(QueueData::RemoveRoom(RemoveRoomData{rid: u.borrow().rid}));
                                        if let Some(r) = r {
                                            success = true;
                                            if success {
                                                mqttmsg = MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master.clone()),
                                                    msg: format!(r#"{{"msg":"cancelled"}}"#)};
                                            } else {
                                                mqttmsg = MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master.clone()),
                                                    msg: format!(r#"{{"msg":"fail"}}"#)};
                                            }
                                        }
                                    }
                                },
                                RoomEventData::Login(x) => {
                                    let mut success = true;
                                    if TotalUsers.contains_key(&x.u.id) {
                                        let u2 = TotalUsers.get(&x.u.id);
                                        if let Some(u2) = u2 {
                                            u2.borrow_mut().online = true;
                                            mqttmsg = MqttMsg{topic:format!("member/{}/res/login", u2.borrow().id.clone()),
                                                msg: format!(r#"{{"msg":"ok", "ng":{}, "rk":{}, "at":{}, "hero":"{}"}}"#, u2.borrow().ng, u2.borrow().rk, u2.borrow().at, u2.borrow().hero)};
                                        }
                                    } 
                                    else {
                                        TotalUsers.insert(x.u.id.clone(), Rc::new(RefCell::new(x.u.clone())));
                                        sender.send(SqlData::Login(SqlLoginData {id: x.dataid.clone(), name: name.clone()}));
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/login", x.u.id.clone()),
                                            msg: format!(r#"{{"msg":"ok", "ng":{}, "rk":{}, "at":{}, "hero":"A01"}}"#, 1200, 1200, 1200)};
                                    }
                                },
                                RoomEventData::Logout(x) => {
                                    let mut success = false;
                                    let u = TotalUsers.get(&x.id);
                                    let u2 = get_user(&x.id, &TotalUsers);
                                    if let Some(u2) = u2 {
                                        u2.borrow_mut().online = false;
                                    }
                                    if let Some(u) = u {
                                        let mut is_null = false;
                                        if u.borrow().game_id == 0 {
                                            let gid = u.borrow().gid;
                                            let rid = u.borrow().rid;
                                            let r = TotalRoom.get(&u.borrow().rid);
                                            if let Some(r) = r {
                                                let m = r.borrow().master.clone();
                                                r.borrow_mut().rm_user(&x.id);
                                                if r.borrow().users.len() > 0 {
                                                    r.borrow().publish_update(&msgtx, m)?;
                                                }
                                                else {
                                                    is_null = true;
                                                }
                                                //mqttmsg = MqttMsg{topic:format!("room/{}/res/leave", x.id),
                                                //    msg: format!(r#"{{"msg":"ok"}}"#)};
                                                if !isBackup || (isBackup && isServerLive == false) {
                                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/leave", x.id),
                                                        msg: format!(r#"{{"msg":"ok"}}"#)})?;
                                                    LossSend.push(MqttMsg{topic:format!("room/{}/res/leave", x.id),
                                                        msg: format!(r#"{{"msg":"ok"}}"#)});
                                                }
                                            }
                                            if gid != 0 {
                                                let g = ReadyGroups.get(&gid);
                                                if let Some(gr) = g {
                                                    gr.borrow_mut().user_cancel(&x.id);
                                                    ReadyGroups.remove(&gid);
                                                    let r = TotalRoom.get(&rid);
                                                    //println!("Totalroom rid: {}", &u.borrow().rid);
                                                    QueueSender.send(QueueData::RemoveRoom(RemoveRoomData{rid: rid}));
                                                    if let Some(r) = r {
                                                        //mqttmsg = MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master),
                                                        //    msg: format!(r#"{{"msg":"ok"}}"#)};
                                                        if !isBackup || (isBackup && isServerLive == false) {
                                                            msgtx.try_send(MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master),
                                                                msg: format!(r#"{{"msg":"ok"}}"#)})?;
                                                            LossSend.push(MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master),
                                                                msg: format!(r#"{{"msg":"ok"}}"#)});
                                                        }
                                                    }
                                                    //TotalRoom.remove(&rid);
                                                }
                                            }
                                            //println!("{:?}", TotalRoom);
                                            //TotalRoom.remove(&u.borrow().rid);
                                            success = true;
                                        } else {
                                            success = true;
                                        }
                                        if is_null {
                                            TotalRoom.remove(&u.borrow().rid);
                                            //println!("Totalroom rid: {}", &u.borrow().rid);
                                            QueueSender.send(QueueData::RemoveRoom(RemoveRoomData{rid: u.borrow().rid}));
                                            //QueueRoom.remove(&u.borrow().rid);
                                        }
                                    }
                                    /*
                                    if success {
                                        success = false;
                                        let mut u = TotalUsers.get(&x.id);
                                        if let Some(u) = u {
                                            if u.borrow().game_id == 0 {
                                                success = true;
                                            }
                                        }
                                        if success {
                                            TotalUsers.remove(&x.id);
                                        }
                                    }
                                    */
                                    if success {
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/logout", x.id.clone()),
                                            msg: format!(r#"{{"msg":"ok"}}"#)};
                                        //msgtx.try_send(MqttMsg{topic:format!("member/{}/res/logout", x.id.clone()),
                                        //    msg: format!(r#"{{"msg":"ok"}}"#)})?;
                                    } else {
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/logout", x.id.clone()),
                                            msg: format!(r#"{{"msg":"fail"}}"#)};
                                        //msgtx.try_send(MqttMsg{topic:format!("member/{}/res/logout", x.id.clone()),
                                        //    msg: format!(r#"{{"msg":"fail"}}"#)})?;
                                    }
                                },
                                RoomEventData::Create(x) => {
                                    let mut success = false;
                                    // println!("rid: {}", &get_rid_by_id(&x.id, &TotalUsers));
                                    while TotalRoom.contains_key(&room_id) {
                                        room_id += 1
                                    }
                                    if !TotalRoom.contains_key(&room_id) {
                                        println!("rid: {}", &room_id);
                                        // room_id = x.id.parse::<u64>().unwrap();
                                        let mut new_room = RoomData {
                                            rid: room_id,
                                            users: vec![],
                                            master: x.id.clone(),
                                            last_master: "".to_owned(),
                                            avg_ng: 0,
                                            avg_rk: 0,
                                            avg_at: 0,
                                            ready: 0,
                                            queue_cnt: 1,
                                            mode: x.mode.clone(),
                                        };
                                        let mut u = TotalUsers.get(&x.id);
                                        if let Some(u) = u {
                                            new_room.add_user(Rc::clone(&u));
                                            let rid = new_room.rid;
                                            let r = Rc::new(RefCell::new(new_room));
                                            r.borrow().publish_update(&msgtx, x.id.clone())?;
                                            TotalRoom.insert(
                                                rid,
                                                Rc::clone(&r),
                                            );
                                            success = true;
                                            u.borrow_mut().rid = rid;
                                        }
                                    }
                                    if success {
                                        mqttmsg = MqttMsg{topic:format!("room/{}/res/create", x.id.clone()),
                                            msg: format!(r#"{{"msg":"ok"}}"#)};
                                    } else {
                                        mqttmsg = MqttMsg{topic:format!("room/{}/res/create", x.id.clone()),
                                            msg: format!(r#"{{"msg":"fail"}}"#)};
                                    }
                                },
                                RoomEventData::Close(x) => {
                                    let mut success = false;
                                    if let Some(y) = TotalRoom.remove(&get_rid_by_id(&x.id, &TotalUsers)) {
                                        success = true;
                                    }
                                    if success {
                                        mqttmsg = MqttMsg{topic:format!("room/{}/res/close", x.id.clone()),
                                            msg: format!(r#"{{"msg":"ok"}}"#)};
                                    } else {
                                        mqttmsg = MqttMsg{topic:format!("room/{}/res/close", x.id.clone()),
                                            msg: format!(r#"{{"msg":"fail"}}"#)};
                                    }
                                },
                                RoomEventData::MainServerDead(x) => {
                                    isServerLive = false;
                                    isBackup = false;
                                    for msg in LossSend.clone() {
                                        msgtx.try_send(msg.clone())?;
                                    }
                                    for (k, u) in &TotalUsers {
                                        tx2.try_send(RoomEventData::Logout(UserLogoutData{id: u.borrow().id.clone()}));
                                    }
                                },
                                RoomEventData::Control(x) => {
                                    if (x.password == "HibikiHibiki") {
                                        QueueSender.send(QueueData::Control(x.clone()));
                                        if (x.mode == "rk") {
                                            if x.msg == "close" {
                                                rkState = "close";
                                            } else if x.msg == "open" {
                                                rkState = "open";
                                            }
                                        } else if (x.mode == "ng") {
                                            if x.msg == "close" {
                                                ngState = "close";
                                            } else if x.msg == "open" {
                                                ngState = "open";
                                            }
                                        } else if (x.mode == "at") {
                                            if x.msg == "close" {
                                                atState = "close";
                                            } else if x.msg == "open" {
                                                atState = "open";
                                            }
                                        }
                                        mqttmsg = MqttMsg{topic:format!("server/res/check_state"),
                                            msg: format!(r#"{{"ng":"{}", "rk":"{}", "at":"{}"}}"#, ngState, rkState, atState)};
                                    }
                                },
                                RoomEventData::CheckState(x) => {
                                    mqttmsg = MqttMsg{topic:format!("server/res/check_state"),
                                            msg: format!(r#"{{"ng":"{}", "rk":"{}", "at":"{}"}}"#, ngState, rkState, atState)};
                                },
                            }
                        }
                        //println!("isBackup: {}, isServerLive: {}", isBackup, isServerLive);
                        if mqttmsg.topic != "" {
                            LossSend.push(mqttmsg.clone());
                            if !isBackup || (isBackup && isServerLive == false) {
                                // println!("send");
                                msgtx.try_send(mqttmsg.clone())?;
                            }
                        }
                        if msgtx.is_full() {
                            println!("FULL!!");
                            //thread::sleep(Duration::from_millis(5000));
                        }
                        Ok(())
                    };
                    if let Err(msg) = handle() {
                        println!("Error msgtx len: {}", msgtx.len());
                        println!("Error rx len: {}", rx.len());
                        println!("init {:?}", msg);
                        continue;
                        panic!("Error found");
                    }
                }
            }
        }
    });
    Ok((tx, tx1))
}

pub fn create(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: CreateRoomData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::Create(data));
    Ok(())
}

pub fn close(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    #[derive(Serialize, Deserialize, Clone, Debug)]
    struct Create_json {
        id: String,
    }
    let data: Create_json = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::Close(CloseRoomData {
        id: id.clone(),
        dataid: data.id.clone(),
    }));
    Ok(())
}

pub fn start_queue(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: StartQueueData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::StartQueue(data));
    Ok(())
}

pub fn ready_queue(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: ReadyData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::Ready(data));
    Ok(())
}

pub fn cancel_queue(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: CancelQueueData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::CancelQueue(data));
    Ok(())
}

pub fn ready(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: PreStartData = serde_json::from_value(v)?;
    // println!("get ready!!");
    sender.try_send(RoomEventData::PreStart(data));
    Ok(())
}

pub fn start_get(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: StartGetData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::StartGet(data));
    Ok(())
}

pub fn join(id: String, v: Value, sender: Sender<RoomEventData>) -> std::result::Result<(), Error> {
    let data: JoinRoomData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::Join(data));
    Ok(())
}

pub fn reject(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: RejectRoomData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::Reject(data));
    Ok(())
}

pub fn jump(id: String, v: Value, sender: Sender<RoomEventData>) -> std::result::Result<(), Error> {
    let data: JumpData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::Jump(data));
    Ok(())
}

pub fn checkRestriction(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: CheckRestrctionData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::CheckRestriction(data));
    Ok(())
}

pub fn checkInGame(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: CheckInGameData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::CheckInGame(data));
    Ok(())
}

pub fn leaveGame(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: LeaveGameData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::LeaveGame(data));
    Ok(())
}

pub fn choose_ng_hero(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: UserNGHeroData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::ChooseNGHero(data));
    Ok(())
}

pub fn ban_hero(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: UserNGHeroData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::BanHero(data));
    Ok(())
}

pub fn lock_ng_hero(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: UserNGHeroData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::LockedNGHero(data));
    Ok(())
}

pub fn invite(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: InviteRoomData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::Invite(data));
    Ok(())
}

pub fn leave(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: LeaveData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::Leave(data));
    Ok(())
}

pub fn start_game(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: StartGameData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::StartGame(data));
    Ok(())
}

pub fn game_over(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: GameOverData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::GameOver(data));
    Ok(())
}

pub fn game_info(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: GameInfoData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::GameInfo(data));
    Ok(())
}

pub fn game_close(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: GameCloseData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::GameClose(data));
    Ok(())
}

pub fn status(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: StatusData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::Status(data));
    Ok(())
}

pub fn reconnect(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: ReconnectData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::Reconnect(data));
    Ok(())
}

pub fn server_dead(id: String, sender: Sender<RoomEventData>) -> std::result::Result<(), Error> {
    sender.try_send(RoomEventData::MainServerDead(DeadData { ServerDead: id }));
    Ok(())
}

pub fn control(v: Value, sender: Sender<RoomEventData>) -> std::result::Result<(), Error> {
    let data: ControlData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::Control(data));
    Ok(())
}

pub fn checkState(v: Value, sender: Sender<RoomEventData>) -> std::result::Result<(), Error> {
    let data: CheckStateData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::CheckState(data));
    Ok(())
}
