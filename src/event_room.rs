extern crate chrono;
extern crate redis;
use redis::Commands;

use chrono::prelude::*;
use chrono::Duration as Cduration;
use chrono::DateTime;
use log::{error, info, trace, warn};
use serde_derive::{Deserialize, Serialize};
use serde_json::json;
use serde_json::{self, Value};
use std::env;
use std::io::ErrorKind;
use std::io::{self, Write};
use std::panic;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

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
use crate::game::*;
use crate::game_flow::*;
use crate::msg::*;
use crate::room::*;
use std::process::Command;

pub const TEAM_SIZE: i16 = 5;
pub const MATCH_SIZE: usize = 2;
pub const SCORE_INTERVAL: i64 = 2;
pub const READY_TIME: f32 = 30.0;
pub const RANK_RANGE: i64 = 50;
pub const NG_RANGE: i64 = 50;
pub const ARAM_RANGE: i64 = 50;
pub const SWAP_TIME: i32 = 15;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HeroData {
    pub name: String,
    pub enable: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GetHerosData {
    pub game_id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SwapHeroData {
    pub id: String,
    pub from: String,
    pub is_accept: bool,
    pub game_id: u64,
    pub action: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HeroSwappingData {
    pub id: String,
    pub from: String,
    pub game_id: u64,
    pub time: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UpdateHerosData {
    pub password: String,
    pub name: String,
    pub enable: bool,
}

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
pub struct CheckRoomData {
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SerialNumberData {
    pub steamID: String,
    pub serialNumber: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AddGoodData {
    pub name: String,
    pub kind: String,
    pub price: u64,
    pub password: String,
    pub quantity: u64,
    pub description: String,
    pub imageURL: String,
    pub snList: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GoodData {
    pub id: u64,
    pub name: String,
    pub kind: String,
    pub price: i64,
    pub quantity: u64,
    pub description: String,
    pub imageURL: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct BuyGoodResData {
    pub steamID: String,
    pub goodData: GoodData,
    pub sn: String,
    pub balance: i64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct BuyGoodData {
    pub steamID: String,
    pub id: u64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GetGoodData {
    pub steamID: String,
}

#[derive(Serialize, Deserialize)]
pub struct JoinRoomCell {
    pub room: String,
    pub mode: String,
    pub team: Vec<String>,
    pub msg: String,
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
    pub time: i32,
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
    pub game: u64,
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
    pub mode: String,
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
pub struct GameStartData {
    pub id: String,
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
    pub time: u64,
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
pub struct ContinueData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct CheckStateData {
    pub id: String,
    pub msg: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GameRoomData {
    pub master: String,
    pub isOpen: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SetPasswordData {
    pub game: u64,
    pub password: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BanUserData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LoadingData {
    pub game: u64,
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UpdateQueueData {
    pub ng_solo: i32,
    pub ng: i32,
    pub rk: i32,
    pub at: i32,
    pub aram: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SystemBanData {
    pub password: String,
    pub id: String,
    pub time: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UpdateRoomQueueCntData {
    pub rid: u64,
}

#[derive(Debug)]
pub enum RoomEventData {
    Reset(),
    Login(UserLoginData),
    Logout(UserLogoutData),
    Create(CreateRoomData),
    Close(CloseRoomData),
    Invite(InviteRoomData),
    ChooseHero(UserNGHeroData),
    BanHero(UserNGHeroData),
    GetHeros(GetHerosData),
    SwapHero(SwapHeroData),
    LockedHero(UserNGHeroData),
    NGGameChooseHero(BTreeMap<u64, Vec<u64>>),
    Join(JoinRoomData),
    CheckRoom(CheckRoomData),
    BuyGood(BuyGoodData),
    GetGood(GetGoodData),
    Reject(RejectRoomData),
    Jump(JumpData),
    CheckRestriction(CheckRestrctionData),
    CheckInGame(CheckInGameData),
    SetPassword(SetPasswordData),
    LeaveGame(LeaveGameData),
    StartQueue(StartQueueData),
    Ready(ReadyData),
    CancelQueue(CancelQueueData),
    UpdateGame(PreGameData),
    UpdateRoomQueueCnt(UpdateRoomQueueCntData),
    PreStart(PreStartData),
    StartGet(StartGetData),
    Leave(LeaveData),
    StartGame(StartGameData),
    GameStart(GameStartData),
    GameOver(GameOverData),
    GameInfo(GameInfoData),
    GameClose(GameCloseData),
    Status(StatusData),
    Reconnect(ReconnectData),
    MainServerDead(DeadData),
    Control(ControlData),
    CheckState(CheckStateData),
    BanUser(BanUserData),
    Loading(LoadingData),
    UpdateQueue(UpdateQueueData),
    Free(),
    SystemBan(SystemBanData),
    UpdateHeros(UpdateHerosData),
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
    pub aram: i16,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct UserChooseData {
    pub steam_id: String,
    pub hero: String,
    pub ban_hero: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct SqlGameInfoData {
    pub game: u64,
    pub mode: String,
    pub chooseData: Vec<UserChooseData>,
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
    pub avg_aram: i16,
    pub ready: i8,
    pub notify: bool,
    pub queue_cnt: i64,
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
    pub avg_aram: i16,
    pub game_status: u16,
    pub queue_cnt: i64,
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

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct JumpCountData {
    pub count: i32,
    pub time: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct PreReadyData {
    pub isReady: bool,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GamingData {
    pub game: u64,
    pub mode: String,
    pub steam_id1: String,
    pub steam_id2: String,
    pub steam_id3: String,
    pub steam_id4: String,
    pub steam_id5: String,
    pub steam_id6: String,
    pub steam_id7: String,
    pub steam_id8: String,
    pub steam_id9: String,
    pub steam_id10: String,
    pub hero1: String,
    pub hero2: String,
    pub hero3: String,
    pub hero4: String,
    pub hero5: String,
    pub hero6: String,
    pub hero7: String,
    pub hero8: String,
    pub hero9: String,
    pub hero10: String,
    pub status: String,
    pub win_team: i16,
}

pub enum QueueData {
    UpdateRoom(QueueRoomData),
    RemoveRoom(RemoveRoomData),
    Control(ControlData),
    Continue(ContinueData),
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

fn get_ng_game_id_by_id(
    id: &String,
    games: &BTreeMap<u64, Rc<RefCell<NGGame>>>,
    users: &BTreeMap<String, Rc<RefCell<User>>>,
) -> u64 {
    for (game_id, fg) in games {
        for user_id in &fg.borrow().user_names {
            if let Some(u) = users.get(user_id) {
                if u.borrow().id == *id {
                    return fg.borrow().game_id;
                }
            }
        }
    }
    return 0;
}

fn get_aram_game_id_by_id(
    id: &String,
    games: &BTreeMap<u64, Rc<RefCell<ARAMGame>>>,
    users: &BTreeMap<String, Rc<RefCell<User>>>,
) -> u64 {
    for (game_id, fg) in games {
        for user_id in &fg.borrow().user_names {
            if let Some(u) = users.get(user_id) {
                if u.borrow().id == *id {
                    return fg.borrow().game_id;
                }
            }
        }
    }
    return 0;
}

fn get_rk_game_id_by_id(
    id: &String,
    games: &BTreeMap<u64, Rc<RefCell<RKGame>>>,
    users: &BTreeMap<String, Rc<RefCell<User>>>,
) -> u64 {
    for (game_id, fg) in games {
        for user_id in &fg.borrow().user_names {
            if let Some(u) = users.get(user_id) {
                if u.borrow().id == *id {
                    return fg.borrow().game_id;
                }
            }
        }
    }
    return 0;
}

fn get_at_game_id_by_id(
    id: &String,
    games: &BTreeMap<u64, Rc<RefCell<ATGame>>>,
    users: &BTreeMap<String, Rc<RefCell<User>>>,
) -> u64 {
    for (game_id, fg) in games {
        for user_id in &fg.borrow().user_names {
            if let Some(u) = users.get(user_id) {
                if u.borrow().id == *id {
                    return fg.borrow().game_id;
                }
            }
        }
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
    isWin: bool,
    raindrop: i64,
) -> Result<(), Error> {
    if mode == "ng" {
        info!("user: {}, ng: {} + {}, raindrop: {} + {}, line: {}",u.borrow().id, u.borrow().ng, value, u.borrow().raindrop, raindrop, line!());
        u.borrow_mut().ng += value;
    } else if mode == "rk" {
        info!("user: {}, rk: {} + {}, raindrop: {} + {}, line: {}",u.borrow().id, u.borrow().rk, value, u.borrow().raindrop, raindrop, line!());
        u.borrow_mut().rk += value;
    } else if mode == "at" {
        info!("user: {}, at: {} + {}, raindrop: {} + {}, line: {}",u.borrow().id, u.borrow().at, value, u.borrow().raindrop, raindrop, line!());
        u.borrow_mut().at += value;
    }
    u.borrow_mut().raindrop += raindrop;
    msgtx.try_send(MqttMsg {
        topic: format!("member/{}/res/login", u.borrow().id),
        msg: format!(
            r#"{{"msg":"ok", "ng":{}, "rk":{}, "at":{}, "aram":{}, "raindrop":{} ,"hero":"{}", "phone":"{}", "email":"{}"}}"#,
            u.borrow().ng,
            u.borrow().rk,
            u.borrow().at,
            u.borrow().aram,
            u.borrow().raindrop,
            u.borrow().hero,
            u.borrow().phone,
            u.borrow().email,
        ),
    })?;
    let mut sql = format!(
        "UPDATE user SET ng={}, rk={}, at={}, raindrop=raindrop+{} WHERE id='{}';",
        u.borrow().ng.clone(),
        u.borrow().rk.clone(),
        u.borrow().at.clone(),
        raindrop,
        u.borrow().id.clone()
    );
    if isWin {
        sql = format!(
            "UPDATE user SET ng={}, rk={}, at={}, raindrop=raindrop+{}, first_win=true WHERE id='{}';",
            u.borrow().ng.clone(),
            u.borrow().rk.clone(),
            u.borrow().at.clone(),
            raindrop,
            u.borrow().id.clone()
        );
    }
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

fn check_is_black(
    user_ids: Vec<String>,
    g_user_ids: Vec<String>,
    conn: &mut mysql::PooledConn,
) -> Result<bool, Error> {
    let mut isBlack = false;
    for user_id in user_ids {
        for g_user_id in &g_user_ids {
            let mut sql = format!(
                r#"select count(*) from black_list where user={} and black={};"#,
                user_id, g_user_id
            );
            let qres = conn.query(sql.clone())?;
            let mut count = 0;
            for row in qres {
                let a = row?.clone();
                if let Some(n) = a.get("count(*)") {
                    count = mysql::from_value_opt(n)?;
                } else {
                    warn!("mysql error: {}, can not get count(*), line : {}", sql.clone(), line!());
                }
                break;
            }
            if count > 0 {
                isBlack = true;
                break;
            }
            sql = format!(
                r#"select count(*) from black_list where user={} and black={};"#,
                g_user_id, user_id
            );
            let qres2 = conn.query(sql.clone())?;
            for row in qres2 {
                let a = row?.clone();
                if let Some(n) = a.get("count(*)") {
                    count = mysql::from_value_opt(n)?;
                } else {
                    warn!("mysql error: {}, can not get count(*), line : {}", sql.clone(), line!());
                }
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
    time: u64,
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
    let elo = EloRank { k: 40.0 };
    let (rw, rl) = elo.compute_elo_team(&win_score, &lose_score);
    let mut raindrop: i64 = (20 + 2 * time / 60) as i64;
    if time > 3000 {
        raindrop = 20 + 2 * 3000 / 60;
    }
    if time < 900 {
        raindrop = 20 + 2 * 900 / 60;
    }
    for (i, u) in win.iter().enumerate() {
        if !u.borrow().first_win {
            user_score(
                u,
                (rw[i] - win_score[i]) as i16,
                msgtx,
                sender,
                conn,
                mode.clone(),
                true,
                raindrop + 400,
            );
        } else {
            user_score(
                u,
                (rw[i] - win_score[i]) as i16,
                msgtx,
                sender,
                conn,
                mode.clone(),
                true,
                raindrop,
            );
        }
        u.borrow_mut().first_win = true;
    }
    for (i, u) in lose.iter().enumerate() {
        user_score(
            u,
            (rl[i] - lose_score[i]) as i16,
            msgtx,
            sender,
            conn,
            mode.clone(),
            false,
            raindrop/2,
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
                        let mut insert_str: String = "REPLACE into user (id, name, status, hero) values".to_string();
                        for (i, u) in NewUsers.iter().enumerate() {
                            let mut new_user = format!(" ('{}', 'default name', 'online', '')", u);
                            insert_str += &new_user;
                            if i < len-1 {
                                insert_str += ",";
                            }
                        }
                        insert_str += ";";
                        // println!("{}", insert_str);
                        conn.query(insert_str.clone())?;
                        len = 0;
                        NewUsers.clear();
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
                                    let sql = format!("UPDATE user SET ng={}, rk={}, at={}, aram={} WHERE id='{}';", x.ng, x.rk, x.at, x.id, x.aram);
                                    println!("sql: {}", sql);
                                    let qres = conn.query(sql.clone())?;
                                }
                                SqlData::UpdateGameInfo(x) => {
                                    info!("update Game Info : {:?}, line : {}", x, line!());
                                    let mut values = format!("values ({}, '{}'", x.game, x.mode);
                                    for data in x.chooseData.clone() {
                                        values = format!("{} ,'{}'", values, data.steam_id);
                                    }
                                    for data in x.chooseData.clone() {
                                        values = format!("{} ,'{}'", values, data.hero);
                                    }
                                    for data in x.chooseData.clone() {
                                        values = format!("{} ,'{}'", values, data.ban_hero);
                                    }
                                    let sql = format!(
                                        "REPLACE INTO Gaming(game, mode, steam_id1, steam_id2, steam_id3, steam_id4, steam_id5, steam_id6, steam_id7, steam_id8, steam_id9, steam_id10, hero1, hero2, hero3, hero4, hero5, hero6, hero7, hero8, hero9, hero10, ban1, ban2, ban3, ban4, ban5, ban6, ban7, ban8, ban9, ban10) {});",
                                        values
                                    );
                                    info!("sql : {}, line: {}", sql, line!());
                                    let qres = conn.query(sql.clone())?;
                                }
                            }
                        }
                        Ok(())
                    };
                    if let Err(msg) = handle() {
                        println!("Mysql Error : {:?}", msg);
                        continue;
                    }
                }
            }
        }
    });
    Ok(tx1)
}

fn canGroupNG(
    readyGroup: &mut ReadyGroupData,
    queueRoom: &Rc<RefCell<QueueRoomData>>,
    conn: &mut mysql::PooledConn,
    group_id: u64,
) -> Result<bool, Error> {
    let mut res = false;
    // info!(
    //     "room : {:?} try_join group {:?}, line : {}",
    //     queueRoom,
    //     readyGroup,
    //     line!()
    // );
    if queueRoom.borrow().ready == 0
        && queueRoom.borrow().user_len as i16 + readyGroup.user_len <= TEAM_SIZE
    {
        let Difference: i64 = i64::abs((queueRoom.borrow().avg_ng - readyGroup.avg_ng).into());
        let mut ng = 0;
        if readyGroup.avg_ng == 0
            || Difference <= NG_RANGE + SCORE_INTERVAL * queueRoom.borrow().queue_cnt
        {
            // let isBlack = check_is_black(
            //     queueRoom.borrow().user_ids.clone(),
            //     readyGroup.user_ids.clone(),
            //     conn,
            // )?;
            // if (isBlack) {
            //     res = false;
            //     return Ok(res);
            // }
            // info!(
            //     "group user_len : {}, room user_len : {}",
            //     readyGroup.user_len,
            //     queueRoom.borrow().user_len
            // );
            if (readyGroup.user_len + queueRoom.borrow().user_len <= 0) {
                res = false;
                return Ok(res);
            }
            ng = (readyGroup.avg_ng * readyGroup.user_len
                + queueRoom.borrow().avg_ng * queueRoom.borrow().user_len) as i16
                / (readyGroup.user_len + queueRoom.borrow().user_len) as i16;
            for user_id in &queueRoom.borrow().user_ids {
                readyGroup.user_ids.push(user_id.clone());
            }
            if queueRoom.borrow().queue_cnt > readyGroup.queue_cnt {
                readyGroup.queue_cnt = queueRoom.borrow().queue_cnt;
            }
            readyGroup.rid.push(queueRoom.borrow().rid);
            readyGroup.avg_ng = ng;
            readyGroup.user_len += queueRoom.borrow().user_len;
            queueRoom.borrow_mut().ready = 1;
            queueRoom.borrow_mut().gid = group_id + 1;
            info!("group {:?}, line: {}", readyGroup, line!());
        }
    }
    Ok(res)
}

fn canGroupRK(
    readyGroup: &mut ReadyGroupData,
    queueRoom: &Rc<RefCell<QueueRoomData>>,
    conn: &mut mysql::PooledConn,
    group_id: u64,
) -> Result<bool, Error> {
    let mut res = false;
    // info!(
    //     "room : {:?} try_join group {:?}, line : {}",
    //     queueRoom,
    //     readyGroup,
    //     line!()
    // );
    if queueRoom.borrow().ready == 0
        && queueRoom.borrow().user_len as i16 + readyGroup.user_len <= TEAM_SIZE
    {
        let Difference: i64 = i64::abs((queueRoom.borrow().avg_rk - readyGroup.avg_rk).into());
        let mut rk = 0;
        if readyGroup.avg_rk == 0
            || Difference <= RANK_RANGE + SCORE_INTERVAL * queueRoom.borrow().queue_cnt
        {
            // let isBlack = check_is_black(
            //     queueRoom.borrow().user_ids.clone(),
            //     readyGroup.user_ids.clone(),
            //     conn,
            // )?;
            // if (isBlack) {
            //     res = false;
            //     return Ok(res);
            // }
            // info!(
            //     "group user_len : {}, room user_len : {}",
            //     readyGroup.user_len,
            //     queueRoom.borrow().user_len
            // );
            if (readyGroup.user_len + queueRoom.borrow().user_len <= 0) {
                res = false;
                return Ok(res);
            }
            rk = (readyGroup.avg_rk * readyGroup.user_len
                + queueRoom.borrow().avg_rk * queueRoom.borrow().user_len) as i16
                / (readyGroup.user_len + queueRoom.borrow().user_len) as i16;
            for user_id in &queueRoom.borrow().user_ids {
                readyGroup.user_ids.push(user_id.clone());
            }
            if queueRoom.borrow().queue_cnt > readyGroup.queue_cnt {
                readyGroup.queue_cnt = queueRoom.borrow().queue_cnt;
            }
            readyGroup.rid.push(queueRoom.borrow().rid);
            readyGroup.avg_rk = rk;
            readyGroup.user_len += queueRoom.borrow().user_len;
            queueRoom.borrow_mut().ready = 1;
            queueRoom.borrow_mut().gid = group_id + 1;
            info!("group {:?}, line: {}", readyGroup, line!());
        }
    }
    Ok(res)
}

fn canGroupAT(
    readyGroup: &mut ReadyGroupData,
    queueRoom: &Rc<RefCell<QueueRoomData>>,
    conn: &mut mysql::PooledConn,
    group_id: u64,
) -> Result<bool, Error> {
    let mut res = false;
    // info!("room : {:?} try_join group {:?}, line : {}", queueRoom, readyGroup, line!());
    if queueRoom.borrow().ready == 0
        && queueRoom.borrow().user_len as i16 + readyGroup.user_len <= TEAM_SIZE
    {
        let Difference: i64 = i64::abs((queueRoom.borrow().avg_at - readyGroup.avg_at).into());
        let mut at = 0;
        if readyGroup.avg_at == 0
            || Difference <= RANK_RANGE + SCORE_INTERVAL * queueRoom.borrow().queue_cnt
        {
            // let isBlack = check_is_black(
            //     queueRoom.borrow().user_ids.clone(),
            //     readyGroup.user_ids.clone(),
            //     conn,
            // )?;
            // if (isBlack) {
            //     res = false;
            //     return Ok(res);
            // }
            // info!(
            //     "group user_len : {}, room user_len : {}",
            //     readyGroup.user_len,
            //     queueRoom.borrow().user_len
            // );
            if (readyGroup.user_len + queueRoom.borrow().user_len <= 0) {
                res = false;
                return Ok(res);
            }
            at = (readyGroup.avg_at * readyGroup.user_len
                + queueRoom.borrow().avg_at * queueRoom.borrow().user_len) as i16
                / (readyGroup.user_len + queueRoom.borrow().user_len) as i16;
            for user_id in &queueRoom.borrow().user_ids {
                readyGroup.user_ids.push(user_id.clone());
            }
            if queueRoom.borrow().queue_cnt > readyGroup.queue_cnt {
                readyGroup.queue_cnt = queueRoom.borrow().queue_cnt;
            }
            readyGroup.rid.push(queueRoom.borrow().rid);
            readyGroup.avg_at = at;
            readyGroup.user_len += queueRoom.borrow().user_len;
            queueRoom.borrow_mut().ready = 1;
            queueRoom.borrow_mut().gid = group_id + 1;
            info!("group {:?}, line: {}", readyGroup, line!());
        }
    }
    Ok(res)
}

fn canGroupARAM(
    readyGroup: &mut ReadyGroupData,
    queueRoom: &Rc<RefCell<QueueRoomData>>,
    conn: &mut mysql::PooledConn,
    group_id: u64,
) -> Result<bool, Error> {
    let mut res = false;
    // info!("room : {:?} try_join group {:?}, line : {}", queueRoom, readyGroup, line!());
    if queueRoom.borrow().ready == 0
        && queueRoom.borrow().user_len as i16 + readyGroup.user_len <= TEAM_SIZE
    {
        let Difference: i64 = i64::abs((queueRoom.borrow().avg_aram - readyGroup.avg_aram).into());
        let mut aram = 0;
        if readyGroup.avg_aram == 0
            || Difference <= RANK_RANGE + SCORE_INTERVAL * queueRoom.borrow().queue_cnt
        {
            // let isBlack = check_is_black(
            //     queueRoom.borrow().user_ids.clone(),
            //     readyGroup.user_ids.clone(),
            //     conn,
            // )?;
            // if (isBlack) {
            //     res = false;
            //     return Ok(res);
            // }
            // info!(
            //     "group user_len : {}, room user_len : {}",
            //     readyGroup.user_len,
            //     queueRoom.borrow().user_len
            // );
            if (readyGroup.user_len + queueRoom.borrow().user_len <= 0) {
                res = false;
                return Ok(res);
            }
            aram = (readyGroup.avg_aram * readyGroup.user_len
                + queueRoom.borrow().avg_aram * queueRoom.borrow().user_len) as i16
                / (readyGroup.user_len + queueRoom.borrow().user_len) as i16;
            for user_id in &queueRoom.borrow().user_ids {
                readyGroup.user_ids.push(user_id.clone());
            }
            if queueRoom.borrow().queue_cnt > readyGroup.queue_cnt {
                readyGroup.queue_cnt = queueRoom.borrow().queue_cnt;
            }
            readyGroup.rid.push(queueRoom.borrow().rid);
            readyGroup.avg_aram = aram;
            readyGroup.user_len += queueRoom.borrow().user_len;
            queueRoom.borrow_mut().ready = 1;
            queueRoom.borrow_mut().gid = group_id + 1;
            info!("group {:?}, line: {}", readyGroup, line!());
        }
    }
    Ok(res)
}

pub fn HandleQueueRequest(
    msgtx: Sender<MqttMsg>,
    sender: Sender<RoomEventData>,
    pool: mysql::Pool,
) -> Result<Sender<QueueData>, Error> {
    let (tx, rx): (Sender<QueueData>, Receiver<QueueData>) = bounded(10000);
    let start = Instant::now();
    let update = tick(Duration::from_millis(1000));
    let update5000ms = tick(Duration::from_millis(5000));

    thread::spawn(move || -> Result<(), Error> {
        let mut SoloNGQueueRoom: BTreeMap<u64, Rc<RefCell<QueueRoomData>>> = BTreeMap::new();
        let mut NGQueueRoom: BTreeMap<u64, Rc<RefCell<QueueRoomData>>> = BTreeMap::new();
        let mut ARAMQueueRoom: BTreeMap<u64, Rc<RefCell<QueueRoomData>>> = BTreeMap::new();
        // let mut NGGroupedQueueRoom: BTreeMap<u64, Rc<RefCell<QueueRoomData>>> = BTreeMap::new();
        let mut RKQueueRoom: BTreeMap<u64, Rc<RefCell<QueueRoomData>>> = BTreeMap::new();
        let mut ATQueueRoom: BTreeMap<u64, Rc<RefCell<QueueRoomData>>> = BTreeMap::new();
        let mut SoloNGReadyGroups: BTreeMap<u64, Rc<RefCell<ReadyGroupData>>> = BTreeMap::new();
        let mut NGReadyGroups: BTreeMap<u64, Rc<RefCell<ReadyGroupData>>> = BTreeMap::new();
        let mut ARAMReadyGroups: BTreeMap<u64, Rc<RefCell<ReadyGroupData>>> = BTreeMap::new();
        let mut RKReadyGroups: BTreeMap<u64, Rc<RefCell<ReadyGroupData>>> = BTreeMap::new();
        let mut ATReadyGroups: BTreeMap<u64, Rc<RefCell<ReadyGroupData>>> = BTreeMap::new();
        let mut matchGroup: BTreeMap<u64, Rc<RefCell<u64>>> = BTreeMap::new();
        let mut conn = pool.get_conn()?;
        let mut group_id: u64 = 0;
        let mut ngState = "open";
        let mut rkState = "open";
        let mut atState = "open";
        let mut aramState = "open";
        loop {
            select! {
                recv(update) -> _ => {
                    let mut new_now = Instant::now();
                    // sng
                    if ngState == "open" {
                        if SoloNGQueueRoom.len() >= MATCH_SIZE {
                            let mut g: ReadyGroupData = Default::default();
                            let mut tq: Vec<Rc<RefCell<QueueRoomData>>> = vec![];
                            let mut id: Vec<u64> = vec![];
                            let mut new_now = Instant::now();
                            tq = SoloNGQueueRoom.iter().map(|x|Rc::clone(x.1)).collect();
                            let mut new_now = Instant::now();
                            tq.sort_by_key(|x| x.borrow().avg_ng);
                            let mut new_now1 = Instant::now();
                            for (k, v) in &SoloNGQueueRoom {
                                v.borrow_mut().queue_cnt += 1;
                                let updateRoomQueueCntData = UpdateRoomQueueCntData {
                                    rid: v.borrow().rid,
                                };
                                sender.try_send(RoomEventData::UpdateRoomQueueCnt(updateRoomQueueCntData));
                            }
                            for (k, v) in &SoloNGQueueRoom {
                                for (k2 , v2) in &SoloNGQueueRoom {
                                    let mut isMatch = false;
                                    if let Some(gid) = matchGroup.get(&v2.borrow().gid.clone()) {
                                        isMatch = true;
                                    }
                                    if !isMatch {
                                        v2.borrow_mut().ready = 0;
                                        v2.borrow_mut().gid = 0;
                                    }
                                }
                                g = Default::default();
                                canGroupNG(&mut g, v, &mut conn, group_id);
                                for (k2, mut v2) in &SoloNGQueueRoom {
                                    canGroupNG(&mut g, v2, &mut conn, group_id);
                                }
                                if g.user_len == TEAM_SIZE {
                                    println!("match team_size!, line: {}", line!());
                                    group_id += 1;
                                    info!("new group_id: {}, line: {}", group_id, line!());
                                    g.gid = group_id;
                                    SoloNGReadyGroups.insert(group_id, Rc::new(RefCell::new(g.clone())));
                                    matchGroup.insert(group_id, Rc::new(RefCell::new(group_id.clone())));
                                    g = Default::default();
                                }
                                // info!("{:?}", matchGroup);
                            }
                            //println!("Time 2: {:?}",Instant::now().duration_since(new_now1));
                            if g.user_len < TEAM_SIZE {
                                for r in g.rid {
                                    let mut room = SoloNGQueueRoom.get(&r);
                                    if let Some(room) = room {
                                        room.borrow_mut().ready = 0;
                                        room.borrow_mut().gid = 0;
                                    }
                                }
                                for r in id {
                                    let mut room = SoloNGQueueRoom.get(&r);
                                    if let Some(room) = room {
                                        room.borrow_mut().ready = 0;
                                        room.borrow_mut().gid = 0;
                                    }
                                }
                            }
                        }
                        if SoloNGReadyGroups.len() > 0 {
                            println!("SoloNGReadyGroup!! {}, line: {}", SoloNGReadyGroups.len(), line!());
                            println!("SoloNGReadyGroup!! {:?}, line: {}", SoloNGReadyGroups, line!());
                        }
                        if SoloNGReadyGroups.len() >= MATCH_SIZE {
                            let mut prestart = false;
                            let mut total_ng: i16 = 0;
                            let mut rm_ids: Vec<u64> = vec![];
                            let mut new_now2 = Instant::now();
                            let mut isMatch = false;
                            for (id, rg) in &SoloNGReadyGroups {
                                let mut fg: ReadyGameData = Default::default();
                                total_ng = 0;
                                if rg.borrow().game_status == 0 && fg.team_len < MATCH_SIZE {
                                    if total_ng == 0 {
                                        total_ng += rg.borrow().avg_ng as i16;
                                        fg.group.push(rg.borrow().rid.clone());
                                        fg.gid.push(*id);
                                        fg.team_len += 1;
                                        for (id2, rg2) in &SoloNGReadyGroups {
                                            let mut isInFG = false;
                                            for gid in &fg.gid {
                                                if gid == id2 {
                                                    isInFG = true;
                                                }
                                            }
                                            if fg.team_len < MATCH_SIZE && !isInFG {
                                                let mut difference = 0;
                                                if fg.team_len > 0 {
                                                    difference = i64::abs((rg2.borrow().avg_ng as i16 - total_ng/fg.team_len as i16).into());
                                                }
                                                if difference <= NG_RANGE + SCORE_INTERVAL * rg2.borrow().queue_cnt {
                                                    total_ng += rg2.borrow().avg_ng as i16;
                                                    fg.group.push(rg2.borrow().rid.clone());
                                                    fg.team_len += 1;
                                                    fg.gid.push(*id2);
                                                }
                                                else {
                                                    rg2.borrow_mut().queue_cnt += 1;
                                                }
                                            }
                                            if fg.team_len == MATCH_SIZE {
                                                sender.send(RoomEventData::UpdateGame(PreGameData{rid: fg.group.clone(), mode: "ng".to_string()}));
                                                for id in fg.gid {
                                                    rm_ids.push(id);
                                                }
                                                fg = Default::default();
                                                isMatch = true;
                                            }
                                            if isMatch {
                                                break;
                                            }
                                        }
                                        if isMatch {
                                            break;
                                        }
                                    }
                                }
                            }
                            for id in rm_ids {
                                let rg = SoloNGReadyGroups.remove(&id);
                                if let Some(rg) = rg {
                                    for rid in &rg.borrow().rid {
                                        SoloNGQueueRoom.remove(&rid);
                                    }
                                }
                            }
                            // println!("GroupedRoom : {:?}", NGGroupedQueueRoom);
                            // println!("MatchedGroup : {:?}", NGMatchedGroups);
                        }
                    } else {
                        for (k, v) in &mut SoloNGQueueRoom {
                            for uid in &v.borrow().user_ids {
                                sender.try_send(RoomEventData::CancelQueue(CancelQueueData{action: "cancel_queue".to_string(), id: uid.to_string(), room: "".to_string(), mode: "ng".to_string()}));
                                // sender.try_send(RoomEventData::Leave(LeaveData{id: uid.to_string(), room: "".to_string()}));
                            }
                        }
                    }
                    // sng
                    // ng
                    if ngState == "open" {
                        if NGQueueRoom.len() >= MATCH_SIZE {
                            let mut g: ReadyGroupData = Default::default();
                            let mut tq: Vec<Rc<RefCell<QueueRoomData>>> = vec![];
                            let mut id: Vec<u64> = vec![];
                            let mut new_now = Instant::now();
                            tq = NGQueueRoom.iter().map(|x|Rc::clone(x.1)).collect();
                            let mut new_now = Instant::now();
                            tq.sort_by_key(|x| x.borrow().avg_ng);
                            let mut new_now1 = Instant::now();
                            for (k, v) in &NGQueueRoom {
                                v.borrow_mut().queue_cnt += 1;
                                let updateRoomQueueCntData = UpdateRoomQueueCntData {
                                    rid: v.borrow().rid,
                                };
                                sender.try_send(RoomEventData::UpdateRoomQueueCnt(updateRoomQueueCntData));
                            }
                            for (k, v) in &NGQueueRoom {
                                for (k2 , v2) in &NGQueueRoom {
                                    let mut isMatch = false;
                                    if let Some(gid) = matchGroup.get(&v2.borrow().gid.clone()) {
                                        isMatch = true;
                                    }
                                    if !isMatch {
                                        v2.borrow_mut().ready = 0;
                                        v2.borrow_mut().gid = 0;
                                    }
                                }
                                g = Default::default();
                                canGroupNG(&mut g, v, &mut conn, group_id);
                                for (k2, mut v2) in &NGQueueRoom {
                                    canGroupNG(&mut g, v2, &mut conn, group_id);
                                }
                                if g.user_len == TEAM_SIZE {
                                    println!("match team_size!, line: {}", line!());
                                    group_id += 1;
                                    info!("new group_id: {}, line: {}", group_id, line!());
                                    g.gid = group_id;
                                    NGReadyGroups.insert(group_id, Rc::new(RefCell::new(g.clone())));
                                    matchGroup.insert(group_id, Rc::new(RefCell::new(group_id.clone())));
                                    g = Default::default();
                                }
                                // info!("{:?}", matchGroup);
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
                        if NGReadyGroups.len() > 0 {
                            println!("NGReadyGroup!! {}, line: {}", NGReadyGroups.len(), line!());
                            println!("NGReadyGroup!! {:?}, line: {}", NGReadyGroups, line!());
                        }
                        if NGReadyGroups.len() >= MATCH_SIZE {
                            let mut prestart = false;
                            let mut total_ng: i16 = 0;
                            let mut rm_ids: Vec<u64> = vec![];
                            let mut new_now2 = Instant::now();
                            let mut isMatch = false;
                            for (id, rg) in &NGReadyGroups {
                                let mut fg: ReadyGameData = Default::default();
                                total_ng = 0;
                                if rg.borrow().game_status == 0 && fg.team_len < MATCH_SIZE {
                                    if total_ng == 0 {
                                        total_ng += rg.borrow().avg_ng as i16;
                                        fg.group.push(rg.borrow().rid.clone());
                                        fg.gid.push(*id);
                                        fg.team_len += 1;
                                        for (id2, rg2) in &NGReadyGroups {
                                            let mut isInFG = false;
                                            for gid in &fg.gid {
                                                if gid == id2 {
                                                    isInFG = true;
                                                }
                                            }
                                            if fg.team_len < MATCH_SIZE && !isInFG {
                                                let mut difference = 0;
                                                if fg.team_len > 0 {
                                                    difference = i64::abs((rg2.borrow().avg_ng as i16 - total_ng/fg.team_len as i16).into());
                                                }
                                                if difference <= NG_RANGE + SCORE_INTERVAL * rg2.borrow().queue_cnt {
                                                    total_ng += rg2.borrow().avg_ng as i16;
                                                    fg.group.push(rg2.borrow().rid.clone());
                                                    fg.team_len += 1;
                                                    fg.gid.push(*id2);
                                                }
                                                else {
                                                    rg2.borrow_mut().queue_cnt += 1;
                                                }
                                            }
                                            if fg.team_len == MATCH_SIZE {
                                                sender.send(RoomEventData::UpdateGame(PreGameData{rid: fg.group.clone(), mode: "ng".to_string()}));
                                                for id in fg.gid {
                                                    rm_ids.push(id);
                                                }
                                                fg = Default::default();
                                                isMatch = true;
                                            }
                                            if isMatch {
                                                break;
                                            }
                                        }
                                        if isMatch {
                                            break;
                                        }
                                    }
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
                            // println!("GroupedRoom : {:?}", NGGroupedQueueRoom);
                            // println!("MatchedGroup : {:?}", NGMatchedGroups);
                        }
                    } else {
                        for (k, v) in &mut NGQueueRoom {
                            for uid in &v.borrow().user_ids {
                                sender.try_send(RoomEventData::CancelQueue(CancelQueueData{action: "cancel_queue".to_string(), id: uid.to_string(), room: "".to_string(), mode: "ng".to_string()}));
                                // sender.try_send(RoomEventData::Leave(LeaveData{id: uid.to_string(), room: "".to_string()}));
                            }
                        }
                    }
                    // ng
                    // aram
                    if aramState == "open" {
                        if ARAMQueueRoom.len() >= MATCH_SIZE {
                            let mut g: ReadyGroupData = Default::default();
                            let mut tq: Vec<Rc<RefCell<QueueRoomData>>> = vec![];
                            let mut id: Vec<u64> = vec![];
                            let mut new_now = Instant::now();
                            tq = ARAMQueueRoom.iter().map(|x|Rc::clone(x.1)).collect();
                            let mut new_now = Instant::now();
                            tq.sort_by_key(|x| x.borrow().avg_aram);
                            let mut new_now1 = Instant::now();
                            for (k, v) in &ARAMQueueRoom {
                                v.borrow_mut().queue_cnt += 1;
                                let updateRoomQueueCntData = UpdateRoomQueueCntData {
                                    rid: v.borrow().rid,
                                };
                                sender.try_send(RoomEventData::UpdateRoomQueueCnt(updateRoomQueueCntData));
                            }
                            for (k, v) in &ARAMQueueRoom {
                                for (k2 , v2) in &ARAMQueueRoom {
                                    let mut isMatch = false;
                                    if let Some(gid) = matchGroup.get(&v2.borrow().gid.clone()) {
                                        isMatch = true;
                                    }
                                    if !isMatch {
                                        v2.borrow_mut().ready = 0;
                                        v2.borrow_mut().gid = 0;
                                    }
                                }
                                g = Default::default();
                                canGroupARAM(&mut g, v, &mut conn, group_id);
                                for (k2, mut v2) in &ARAMQueueRoom {
                                    canGroupARAM(&mut g, v2, &mut conn, group_id);
                                }
                                if g.user_len == TEAM_SIZE {
                                    println!("match team_size!, line: {}", line!());
                                    group_id += 1;
                                    info!("new group_id: {}, line: {}", group_id, line!());
                                    g.gid = group_id;
                                    ARAMReadyGroups.insert(group_id, Rc::new(RefCell::new(g.clone())));
                                    matchGroup.insert(group_id, Rc::new(RefCell::new(group_id.clone())));
                                    g = Default::default();
                                }
                                // info!("{:?}", matchGroup);
                            }
                            //println!("Time 2: {:?}",Instant::now().duration_since(new_now1));
                            if g.user_len < TEAM_SIZE {
                                for r in g.rid {
                                    let mut room = ARAMQueueRoom.get(&r);
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
                        if ARAMReadyGroups.len() > 0 {
                            println!("ARAMReadyGroup!! {}, line: {}", ARAMReadyGroups.len(), line!());
                            println!("ARAMReadyGroup!! {:?}, line: {}", ARAMReadyGroups, line!());
                        }
                        if ARAMReadyGroups.len() >= MATCH_SIZE {
                            let mut prestart = false;
                            let mut total_aram: i16 = 0;
                            let mut rm_ids: Vec<u64> = vec![];
                            let mut new_now2 = Instant::now();
                            let mut isMatch = false;
                            for (id, rg) in &ARAMReadyGroups {
                                let mut fg: ReadyGameData = Default::default();
                                total_aram = 0;
                                if rg.borrow().game_status == 0 && fg.team_len < MATCH_SIZE {
                                    if total_aram == 0 {
                                        total_aram += rg.borrow().avg_aram as i16;
                                        fg.group.push(rg.borrow().rid.clone());
                                        fg.gid.push(*id);
                                        fg.team_len += 1;
                                        for (id2, rg2) in &ARAMReadyGroups {
                                            let mut isInFG = false;
                                            for gid in &fg.gid {
                                                if gid == id2 {
                                                    isInFG = true;
                                                }
                                            }
                                            if fg.team_len < MATCH_SIZE && !isInFG {
                                                let mut difference = 0;
                                                if fg.team_len > 0 {
                                                    difference = i64::abs((rg2.borrow().avg_aram as i16 - total_aram/fg.team_len as i16).into());
                                                }
                                                if difference <= ARAM_RANGE + SCORE_INTERVAL * rg2.borrow().queue_cnt {
                                                    total_aram += rg2.borrow().avg_aram as i16;
                                                    fg.group.push(rg2.borrow().rid.clone());
                                                    fg.team_len += 1;
                                                    fg.gid.push(*id2);
                                                }
                                                else {
                                                    rg2.borrow_mut().queue_cnt += 1;
                                                }
                                            }
                                            if fg.team_len == MATCH_SIZE {
                                                sender.send(RoomEventData::UpdateGame(PreGameData{rid: fg.group.clone(), mode: "aram".to_string()}));
                                                for id in fg.gid {
                                                    rm_ids.push(id);
                                                }
                                                fg = Default::default();
                                                isMatch = true;
                                            }
                                            if isMatch {
                                                break;
                                            }
                                        }
                                        if isMatch {
                                            break;
                                        }
                                    }
                                }
                            }
                            for id in rm_ids {
                                let rg = ARAMReadyGroups.remove(&id);
                                if let Some(rg) = rg {
                                    for rid in &rg.borrow().rid {
                                        ARAMQueueRoom.remove(&rid);
                                    }
                                }
                            }
                            // println!("GroupedRoom : {:?}", ARAMGroupedQueueRoom);
                            // println!("MatchedGroup : {:?}", ARAMMatchedGroups);
                        }
                    } else {
                        for (k, v) in &mut ARAMQueueRoom {
                            for uid in &v.borrow().user_ids {
                                sender.try_send(RoomEventData::CancelQueue(CancelQueueData{action: "cancel_queue".to_string(), id: uid.to_string(), room: "".to_string(), mode: "aram".to_string()}));
                                // sender.try_send(RoomEventData::Leave(LeaveData{id: uid.to_string(), room: "".to_string()}));
                            }
                        }
                    }
                    // aram
                    // rank
                    if rkState == "open" {
                        if RKQueueRoom.len() >= MATCH_SIZE {
                            let mut g: ReadyGroupData = Default::default();
                            let mut tq: Vec<Rc<RefCell<QueueRoomData>>> = vec![];
                            let mut id: Vec<u64> = vec![];
                            let mut new_now = Instant::now();
                            tq = RKQueueRoom.iter().map(|x|Rc::clone(x.1)).collect();
                            let mut new_now = Instant::now();
                            tq.sort_by_key(|x| x.borrow().avg_rk);
                            let mut new_now1 = Instant::now();
                            for (k, v) in &RKQueueRoom {
                                v.borrow_mut().queue_cnt += 1;
                                let updateRoomQueueCntData = UpdateRoomQueueCntData {
                                    rid: v.borrow().rid,
                                };
                                sender.try_send(RoomEventData::UpdateRoomQueueCnt(updateRoomQueueCntData));
                            }
                            for (k, v) in &RKQueueRoom {
                                for (k2 , v2) in &RKQueueRoom {
                                    let mut isMatch = false;
                                    if let Some(gid) = matchGroup.get(&v2.borrow().gid.clone()) {
                                        isMatch = true;
                                    }
                                    if !isMatch {
                                        v2.borrow_mut().ready = 0;
                                        v2.borrow_mut().gid = 0;
                                    }
                                }
                                g = Default::default();
                                canGroupRK(&mut g, v, &mut conn, group_id);
                                for (k2, mut v2) in &RKQueueRoom {
                                    canGroupRK(&mut g, v2, &mut conn, group_id);
                                }
                                if g.user_len == TEAM_SIZE {
                                    println!("match team_size!, line: {}", line!());
                                    group_id += 1;
                                    info!("new group_id: {}, line: {}", group_id, line!());
                                    g.gid = group_id;
                                    RKReadyGroups.insert(group_id, Rc::new(RefCell::new(g.clone())));
                                    matchGroup.insert(group_id, Rc::new(RefCell::new(group_id.clone())));
                                    g = Default::default();
                                }
                                info!("{:?}", matchGroup);
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
                        if RKReadyGroups.len() > 0 {
                            println!("RKReadyGroup!! {}, line: {}", RKReadyGroups.len(), line!());
                            println!("RKReadyGroup!! {:?}, line: {}", RKReadyGroups, line!());
                        }
                        if RKReadyGroups.len() >= MATCH_SIZE {
                            let mut prestart = false;
                            let mut total_rk: i16 = 0;
                            let mut rm_ids: Vec<u64> = vec![];
                            let mut new_now2 = Instant::now();
                            let mut isMatch = false;
                            for (id, rg) in &RKReadyGroups {
                                let mut fg: ReadyGameData = Default::default();
                                total_rk = 0;
                                if rg.borrow().game_status == 0 && fg.team_len < MATCH_SIZE {
                                    if total_rk == 0 {
                                        total_rk += rg.borrow().avg_rk as i16;
                                        fg.group.push(rg.borrow().rid.clone());
                                        fg.gid.push(*id);
                                        fg.team_len += 1;
                                        for (id2, rg2) in &RKReadyGroups {
                                            let mut isInFG = false;
                                            for gid in &fg.gid {
                                                if gid == id2 {
                                                    isInFG = true;
                                                }
                                            }
                                            if fg.team_len < MATCH_SIZE && !isInFG {
                                                let mut difference = 0;
                                                if fg.team_len > 0 {
                                                    difference = i64::abs((rg2.borrow().avg_rk as i16 - total_rk/fg.team_len as i16).into());
                                                }
                                                if difference <= RANK_RANGE + SCORE_INTERVAL * rg2.borrow().queue_cnt {
                                                    total_rk += rg2.borrow().avg_rk as i16;
                                                    fg.group.push(rg2.borrow().rid.clone());
                                                    fg.team_len += 1;
                                                    fg.gid.push(*id2);
                                                }
                                                else {
                                                    rg2.borrow_mut().queue_cnt += 1;
                                                }
                                            }
                                            if fg.team_len == MATCH_SIZE {
                                                sender.send(RoomEventData::UpdateGame(PreGameData{rid: fg.group.clone(), mode: "rk".to_string()}));
                                                for id in fg.gid {
                                                    rm_ids.push(id);
                                                }
                                                fg = Default::default();
                                                isMatch = true;
                                            }
                                            if isMatch {
                                                break;
                                            }
                                        }
                                        if isMatch {
                                            break;
                                        }
                                    }
                                }
                            }
                            println!("del : {:?}", rm_ids);
                            for id in rm_ids {
                                let rg = RKReadyGroups.remove(&id);
                                if let Some(rg) = rg {
                                    for rid in &rg.borrow().rid {
                                        RKQueueRoom.remove(&rid);
                                    }
                                }
                            }
                            // println!("GroupedRoom : {:?}", RKGroupedQueueRoom);
                            // println!("MatchedGroup : {:?}", RKMatchedGroups);
                        }
                    } else {
                        for (k, v) in &mut RKQueueRoom {
                            for uid in &v.borrow().user_ids {
                                sender.try_send(RoomEventData::CancelQueue(CancelQueueData{action: "cancel_queue".to_string(), id: uid.to_string(), room: "".to_string(), mode: "rk".to_string()}));
                                sender.try_send(RoomEventData::Leave(LeaveData{id: uid.to_string(), room: "".to_string()}));
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
                            for (k, v) in &ATQueueRoom {
                                v.borrow_mut().queue_cnt += 1;
                                let updateRoomQueueCntData = UpdateRoomQueueCntData {
                                    rid: v.borrow().rid,
                                };
                                sender.try_send(RoomEventData::UpdateRoomQueueCnt(updateRoomQueueCntData));
                            }
                            for (k, v) in &ATQueueRoom {
                                for (k2 ,v2) in &ATQueueRoom {
                                    let mut isMatch = false;
                                    if let Some(gid) = matchGroup.get(&v2.borrow().gid.clone()) {
                                        isMatch = true;
                                    }
                                    if !isMatch {
                                        v2.borrow_mut().ready = 0;
                                        v2.borrow_mut().gid = 0;
                                    }
                                }
                                g = Default::default();
                                canGroupAT(&mut g, v, &mut conn, group_id);
                                for (k2, mut v2) in &ATQueueRoom {
                                    canGroupAT(&mut g, v2, &mut conn, group_id);
                                }
                                if g.user_len == TEAM_SIZE {
                                    println!("match team_size!, line: {}", line!());
                                    group_id += 1;
                                    info!("new group_id: {}, line: {}", group_id, line!());
                                    g.gid = group_id;
                                    g.queue_cnt = 1;
                                    ATReadyGroups.insert(group_id, Rc::new(RefCell::new(g.clone())));
                                    matchGroup.insert(group_id, Rc::new(RefCell::new(group_id.clone())));
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
                        if ATReadyGroups.len() > 0 {
                            println!("ATReadyGroup!! {}, line: {}", ATReadyGroups.len(), line!());
                            println!("ATReadyGroup!! {:?}, line: {}", ATReadyGroups, line!());
                        }
                        if ATReadyGroups.len() >= MATCH_SIZE {
                            let mut prestart = false;
                            let mut total_at: i16 = 0;
                            let mut rm_ids: Vec<u64> = vec![];
                            let mut new_now2 = Instant::now();
                            let mut isMatch = false;
                            for (id, rg) in &ATReadyGroups {
                                let mut fg: ReadyGameData = Default::default();
                                total_at = 0;
                                if rg.borrow().game_status == 0 && fg.team_len < MATCH_SIZE {
                                    if total_at == 0 {
                                        total_at += rg.borrow().avg_at as i16;
                                        fg.group.push(rg.borrow().rid.clone());
                                        fg.gid.push(*id);
                                        fg.team_len += 1;
                                        for (id2, rg2) in &ATReadyGroups {
                                            let mut isInFG = false;
                                            for gid in &fg.gid {
                                                if gid == id2 {
                                                    isInFG = true;
                                                }
                                            }
                                            if fg.team_len < MATCH_SIZE && !isInFG {
                                                let mut difference = 0;
                                                if fg.team_len > 0 {
                                                    difference = i64::abs((rg2.borrow().avg_at as i16 - total_at/fg.team_len as i16).into());
                                                }
                                                if difference <= RANK_RANGE + SCORE_INTERVAL * rg2.borrow().queue_cnt {
                                                    total_at += rg2.borrow().avg_at as i16;
                                                    fg.group.push(rg2.borrow().rid.clone());
                                                    fg.team_len += 1;
                                                    fg.gid.push(*id2);
                                                }
                                                else {
                                                    rg2.borrow_mut().queue_cnt += 1;
                                                }
                                            }
                                            if fg.team_len == MATCH_SIZE {
                                                sender.send(RoomEventData::UpdateGame(PreGameData{rid: fg.group.clone(), mode: "at".to_string()}));
                                                for id in fg.gid {
                                                    rm_ids.push(id);
                                                }
                                                fg = Default::default();
                                                isMatch = true;
                                            }
                                            if isMatch {
                                                break;
                                            }
                                        }
                                        if isMatch {
                                            break;
                                        }
                                    }
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
                    } else {
                        for (k, v) in &mut ATQueueRoom {
                            for uid in &v.borrow().user_ids {
                                sender.try_send(RoomEventData::CancelQueue(CancelQueueData{action: "cancel_queue".to_string(), id: uid.to_string(), room: "".to_string(), mode: "at".to_string()}));
                                sender.try_send(RoomEventData::Leave(LeaveData{id: uid.to_string(), room: "".to_string()}));
                            }
                        }
                    }
                    // AT
                }
                recv(update5000ms) -> _ => {
                    let mut ng_solo_cnt: i32 = 0;
                    let mut ng_cnt: i32 = 0;
                    let mut rk_cnt: i32 = 0;
                    let mut at_cnt: i32 = 0;
                    let mut aram_cnt: i32 = 0;
                    for (k, v) in &mut SoloNGQueueRoom {
                        ng_solo_cnt += v.borrow().user_len as i32;
                    }
                    for (k, v) in &mut NGQueueRoom {
                        ng_cnt += v.borrow().user_len as i32;
                    }
                    for (k, v) in &RKQueueRoom {
                        rk_cnt += v.borrow().user_len as i32;
                    }
                    for (k, v) in &ATQueueRoom {
                        at_cnt += v.borrow().user_len as i32;
                    }
                    for (k, v) in &ARAMQueueRoom {
                        aram_cnt += v.borrow().user_len as i32;
                    }
                    sender.try_send(RoomEventData::UpdateQueue(UpdateQueueData{ng_solo: ng_solo_cnt, ng: ng_cnt, rk: rk_cnt, at: at_cnt, aram: aram_cnt}));
                }
                recv(rx) -> d => {
                    let handle = || -> Result<(), Error> {
                        if let Ok(d) = d {
                            match d {
                                QueueData::UpdateRoom(x) => {
                                    if x.mode == "ng" {
                                        NGQueueRoom.insert(x.rid.clone(), Rc::new(RefCell::new(x.clone())));
                                    }else if x.mode == "sng" {
                                        SoloNGQueueRoom.insert(x.rid.clone(), Rc::new(RefCell::new(x.clone())));
                                    }else if x.mode == "rk" {
                                        RKQueueRoom.insert(x.rid.clone(), Rc::new(RefCell::new(x.clone())));
                                    }else if x.mode == "at" {
                                        ATQueueRoom.insert(x.rid.clone(), Rc::new(RefCell::new(x.clone())));
                                    }else if x.mode == "aram" {
                                        ARAMQueueRoom.insert(x.rid.clone(), Rc::new(RefCell::new(x.clone())));
                                    }
                                }
                                QueueData::RemoveRoom(x) => {
                                    //sng
                                    let mut r = SoloNGQueueRoom.get(&x.rid);
                                    if let Some(r) = r {
                                        let mut rg = SoloNGReadyGroups.get(&r.borrow().gid);
                                        if let Some(rg) = rg {
                                            for rid in &rg.borrow().rid {
                                                if rid == &x.rid {
                                                    continue;
                                                }
                                                let mut room = SoloNGQueueRoom.get(rid);
                                                if let Some(room) = room {
                                                    room.borrow_mut().gid = 0;
                                                    room.borrow_mut().ready = 0;
                                                }
                                            }
                                        }
                                        SoloNGReadyGroups.remove(&r.borrow().gid);
                                    }
                                    SoloNGQueueRoom.remove(&x.rid);
                                    //sng
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
                                    // aram
                                    let mut r = ARAMQueueRoom.get(&x.rid);
                                    if let Some(r) = r {
                                        let mut rg = ARAMReadyGroups.get(&r.borrow().gid);
                                        if let Some(rg) = rg {
                                            for rid in &rg.borrow().rid {
                                                if rid == &x.rid {
                                                    continue;
                                                }
                                                let mut room = ARAMQueueRoom.get(rid);
                                                if let Some(room) = room {
                                                    room.borrow_mut().gid = 0;
                                                    room.borrow_mut().ready = 0;
                                                }
                                            }
                                        }
                                        ARAMReadyGroups.remove(&r.borrow().gid);
                                    }
                                    ARAMQueueRoom.remove(&x.rid);
                                    // aram
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
                                QueueData::Continue(x) => {

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
        let _: () = redis::cmd("FLUSHALL").query(redis_conn)?;
        let mut isServerLive = true;
        let mut isBackup = isBackup.clone();
        let mut TotalRoom: BTreeMap<u64, Rc<RefCell<RoomData>>> = BTreeMap::new();
        let mut TotalHeros: BTreeMap<String, Rc<RefCell<HeroData>>> = BTreeMap::new();
        //let mut QueueRoom: BTreeMap<u64, Rc<RefCell<RoomData>>> = BTreeMap::new();
        let mut ReadyGroups: BTreeMap<u64, Rc<RefCell<FightGroup>>> = BTreeMap::new();
        let mut PreStartGroups: BTreeMap<u64, Rc<RefCell<FightGame>>> = BTreeMap::new();
        let mut GameingGroups: BTreeMap<u64, Rc<RefCell<FightGame>>> = BTreeMap::new();
        let mut NGGameingGroups: BTreeMap<u64, Rc<RefCell<NGGame>>> = BTreeMap::new();
        let mut ARAMGameingGroups: BTreeMap<u64, Rc<RefCell<ARAMGame>>> = BTreeMap::new();
        let mut RKGameingGroups: BTreeMap<u64, Rc<RefCell<RKGame>>> = BTreeMap::new();
        let mut ATGameingGroups: BTreeMap<u64, Rc<RefCell<ATGame>>> = BTreeMap::new();
        let mut TotalUsers: BTreeMap<String, Rc<RefCell<User>>> = BTreeMap::new();
        let mut RestrictedUsers: BTreeMap<String, Rc<RefCell<RestrictedData>>> = BTreeMap::new();
        let mut JumpUsers: BTreeMap<String, Rc<RefCell<JumpCountData>>> = BTreeMap::new();
        let mut InGameUsers: BTreeMap<String, Rc<RefCell<User>>> = BTreeMap::new();
        let mut GameingRoom: BTreeMap<u64, Rc<RefCell<GameRoomData>>> = BTreeMap::new();
        let mut LossSend: Vec<MqttMsg> = vec![];
        let mut AbandonGames: BTreeMap<u64, bool> = BTreeMap::new();
        let mut HeroSwapping: BTreeMap<String, Rc<RefCell<HeroSwappingData>>> = BTreeMap::new();
        let mut room_id: u64 = 1;
        let mut group_id: u64 = 0;
        let mut game_id: u64 = 0;
        let mut game_port: u16 = 7777;
        let mut ngState = "open";
        let mut bForceCloseNgState = false;
        let mut rkState = "close";
        let mut bForceCloseRkState = false;
        let mut atState = "close";
        let mut bForceCloseAtState = false;
        let mut aramState = "open";
        let mut bForceCloseAramState = false;
        let mut currentState = "ng";
        let sql = format!(r#"select * from user;"#);
        let qres2: mysql::QueryResult = conn.query(sql.clone())?;
        let mut userid: String = "".to_owned();
        let mut ng: i16 = 0;
        let mut rk: i16 = 0;
        let mut name: String = "".to_owned();
        let mut current_ng_solo_queue_cnt: i32 = 0;
        let mut current_ng_queue_cnt: i32 = 0;
        let mut current_rk_queue_cnt: i32 = 0;
        let mut current_at_queue_cnt: i32 = 0;
        let mut current_aram_queue_cnt: i32 = 0;
        let mut ng_solo_queue_cnt: i32 = 0;
        let mut ng_queue_cnt: i32 = 0;
        let mut rk_queue_cnt: i32 = 0;
        let mut at_queue_cnt: i32 = 0;
        let mut aram_queue_cnt: i32 = 0;
        let mut current_ng_game_cnt: i32 = 0;
        let mut current_rk_game_cnt: i32 = 0;
        let mut current_at_game_cnt: i32 = 0;
        let mut current_aram_game_cnt: i32 = 0;
        let mut current_online_cnt: i32 = 0;
        let mut isUpdateCount = false;
        let mut isNewDay = false;
        let id = 0;
        for row in qres2 {
            let a = row?.clone();
            let user = User {
                id: mysql::from_value_opt(a.get("id").ok_or(Error::from(core::fmt::Error))?)?,
                name: mysql::from_value_opt(a.get("name").ok_or(Error::from(core::fmt::Error))?)?,
                hero: mysql::from_value_opt(a.get("hero").ok_or(Error::from(core::fmt::Error))?)?,
                ng: mysql::from_value_opt(a.get("ng").ok_or(Error::from(core::fmt::Error))?)?,
                rk: mysql::from_value_opt(a.get("rk").ok_or(Error::from(core::fmt::Error))?)?,
                at: mysql::from_value_opt(a.get("at").ok_or(Error::from(core::fmt::Error))?)?,
                aram: mysql::from_value_opt(a.get("aram").ok_or(Error::from(core::fmt::Error))?)?,
                raindrop: mysql::from_value_opt(a.get("raindrop").ok_or(Error::from(core::fmt::Error))?)?,
                first_win: mysql::from_value_opt(a.get("first_win").ok_or(Error::from(core::fmt::Error))?)?,
                email: mysql::from_value_opt(a.get("email").ok_or(Error::from(core::fmt::Error))?)?,
                phone: mysql::from_value_opt(a.get("phone").ok_or(Error::from(core::fmt::Error))?)?,
                ..Default::default()
            };
            println!("{:?}, line: {}", user, line!());
            TotalUsers.insert(mysql::from_value_opt(a.get("id").ok_or(Error::from(core::fmt::Error))?)?, Rc::new(RefCell::new(user.clone())));
        }
        let sql2 = format!("DELETE FROM Gaming where status='wait';");
        conn.query(sql2.clone())?;
        let sql3 = format!("update user set status='offline';");
        conn.query(sql3.clone())?;
        println!("game id: {}, line: {}", game_id, line!());
        let sql4 = format!("select * from hero_list;");
        let qres3: mysql::QueryResult = conn.query(sql4.clone())?;
        for row in qres3 {
            let a = row?.clone();
            let hero = HeroData {
                name: mysql::from_value_opt(a.get("name").ok_or(Error::from(core::fmt::Error))?)?,
                enable: mysql::from_value_opt(a.get("enable").ok_or(Error::from(core::fmt::Error))?)?,
            };
            println!("{:?}, line: {}", hero, line!());
            TotalHeros.insert(hero.name.clone(), Rc::new(RefCell::new(hero.clone())));
        }
        loop {
            select! {
                recv(update200ms) -> _ => {
                    let mut rm_ids: Vec<u64> = vec![];
                    let mut start_cnt: u16 = 0;
                    for (id, group) in &mut PreStartGroups {
                        let res = group.borrow().check_prestart();
                        match res {
                            PrestartStatus::Ready => {
                                for r in &group.borrow().room_names {
                                    msgtx.try_send(MqttMsg{topic:format!("room/{}/res/start_get", r), msg: format!(r#"{{"msg":"start", "room":"{}",
                                        "game":"{}", "players":{:?}}}"#, r, group.borrow().game_id, &group.borrow().user_names)});
                                }
                                group.borrow_mut().next_status();
                                if group.borrow().mode == "ng" {
                                    let ngGame = NGGame {
                                        teams: group.borrow().teams.clone(),
                                        room_names: group.borrow().room_names.clone(),
                                        user_names: group.borrow().user_names.clone(),
                                        game_id: group.borrow().game_id,
                                        user_count: group.borrow().user_count,
                                        ..Default::default()
                                    };
                                    NGGameingGroups.insert(group.borrow().game_id,  Rc::new(RefCell::new(ngGame.clone())));
                                    isUpdateCount = true;
                                }
                                if group.borrow().mode == "aram" {
                                    let aramGame = ARAMGame {
                                        teams: group.borrow().teams.clone(),
                                        room_names: group.borrow().room_names.clone(),
                                        user_names: group.borrow().user_names.clone(),
                                        game_id: group.borrow().game_id,
                                        user_count: group.borrow().user_count,
                                        TotalHeros: TotalHeros.clone(),
                                        ..Default::default()
                                    };
                                    ARAMGameingGroups.insert(group.borrow().game_id,  Rc::new(RefCell::new(aramGame.clone())));
                                    isUpdateCount = true;
                                }
                                if group.borrow().mode == "rk" {
                                    let rkGame = RKGame {
                                        teams: group.borrow().teams.clone(),
                                        room_names: group.borrow().room_names.clone(),
                                        user_names: group.borrow().user_names.clone(),
                                        game_id: group.borrow().game_id,
                                        user_count: group.borrow().user_count,
                                        ..Default::default()
                                    };
                                    RKGameingGroups.insert(group.borrow().game_id,  Rc::new(RefCell::new(rkGame.clone())));
                                    isUpdateCount = true;
                                }
                                if group.borrow().mode == "at" {
                                    let atGame = ATGame {
                                        teams: group.borrow().teams.clone(),
                                        room_names: group.borrow().room_names.clone(),
                                        user_names: group.borrow().user_names.clone(),
                                        game_id: group.borrow().game_id,
                                        user_count: group.borrow().user_count,
                                        ..Default::default()
                                    };
                                    ATGameingGroups.insert(group.borrow().game_id,  Rc::new(RefCell::new(atGame.clone())));
                                    isUpdateCount = true;
                                }
                                // group.borrow_mut().ban_time = BAN_HERO_TIME;
                                // GameingGroups.insert(game_id, group.clone());
                                rm_ids.push(id.clone());
                            },
                            PrestartStatus::Cancel => {
                                isUpdateCount = true;
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
                                rm_ids.push(*id);
                            },
                            PrestartStatus::Wait => {
                                let res1 = group.borrow().check_start_get();
                                if !res1 {
                                    if group.borrow().ready_cnt >= READY_TIME {
                                        for team in &group.borrow().teams {
                                            for r in &team.borrow().rooms {
                                                for user in &r.borrow().users {
                                                    if user.borrow().start_get == false {
                                                        isUpdateCount = true;
                                                        info!("group out of time: {:?}", group);
                                                        tx2.try_send(RoomEventData::PreStart(PreStartData{room: user.borrow().rid.to_string(), id: user.borrow().id.clone(), accept: false}));
                                                        rm_ids.push(*id);
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
                                    }
                                    group.borrow_mut().ready_cnt += 0.2;
                                }
                            }
                        }
                    }
                    for k in rm_ids {
                        PreStartGroups.remove(&k);
                    }
                }
                recv(update1000ms) -> _ => {
                    let now = Local::now();
                    let rank_open_time = now.date().and_hms(10, 0, 0);
                    let rank_close_time = now.date().and_hms(16, 0, 0);
                    let midnight = now.date().and_hms(16,0,0);
                    let mut isRankOpen = false;
                    let mut time_result = rank_open_time.signed_duration_since(now).to_std();
                    let mut duration = Duration::new(0, 0);
                    match time_result {
                        Ok(v) => {
                            duration = v;
                        },
                        Err(e) => {
                        }
                    }
                    if duration == Duration::new(0, 0) {
                        isRankOpen = true;
                    }
                    time_result = midnight.signed_duration_since(now).to_std();
                    duration = Duration::new(0, 0);
                    match time_result {
                        Ok(v) => {
                            isNewDay = true;
                        },
                        Err(e) => {
                            if isNewDay {
                                isNewDay = false;
                                let sql = format!("update user set first_win=false;");
                                let qres = conn.query(sql.clone())?;
                                for (_, u) in &TotalUsers {
                                    u.borrow_mut().first_win = false;
                                }
                            }
                        }
                    }
                    time_result = rank_close_time.signed_duration_since(now).to_std();
                    duration = Duration::new(0, 0);
                    match time_result {
                        Ok(v) => {
                            duration = v;
                        },
                        Err(e) => {
                        }
                    }
                    if duration == Duration::new(0, 0) {
                        isRankOpen = false;
                    }
                    if isRankOpen {
                        rkState = "open";
                        atState = "open";
                        if currentState == "ng" {
                            currentState = "rk";
                            msgtx.try_send(MqttMsg{topic:format!("server/res/check_state"),
                                    msg: format!(r#"{{"ng":"{}", "rk":"{}", "at":"{}", "aram":"{}"}}"#, ngState, rkState, atState, aramState)})?;
                        }
                    } else {
                        rkState = "close";
                        atState = "close";
                        if currentState == "rk" {
                            currentState = "ng";
                            msgtx.try_send(MqttMsg{topic:format!("server/res/check_state"),
                                    msg: format!(r#"{{"ng":"{}", "rk":"{}", "at":"{}", "aram":"{}"}}"#, ngState, rkState, atState, aramState)})?;
                        }
                    }
                    if bForceCloseAtState {
                        atState = "close";
                    }
                    if bForceCloseNgState {
                        ngState = "close";
                    }
                    if bForceCloseRkState {
                        rkState = "close";
                    }
                    // // test
                    // rkState = "open";
                    // atState = "open";
                    // // test
                    // msgtx.try_send(MqttMsg{topic:format!("server/res/check_state"),
                    //     msg: format!(r#"{{"ng":"{}", "rk":"{}", "at":"{}"}}"#, ngState, rkState, atState)})?;
                    // println!("Duration between {:?} and {:?}: {:?}", now, rank_close_time, duration);
                    if !isBackup || (isBackup && isServerLive == false) {
                        //msgtx.try_send(MqttMsg{topic:format!("server/0/res/heartbeat"),
                        //                    msg: format!(r#"{{"msg":"live"}}"#)})?;
                    }
                    let mut rm_list: Vec<String> = Vec::new();
                    for (id, restriction) in &mut RestrictedUsers {
                        restriction.borrow_mut().time -= 1;
                        if restriction.borrow().time <= 0 {
                            rm_list.push(id.clone());
                        }
                    }
                    for rm in rm_list {
                        RestrictedUsers.remove(&rm);
                    }
                    let mut rest_list: Vec<String> = Vec::new();
                    for (id, jumpCountData) in &mut JumpUsers {
                        jumpCountData.borrow_mut().time -= 1;
                        if jumpCountData.borrow().time <= 0 {
                            rest_list.push(id.clone());
                        }
                    }
                    for rest in rest_list {
                        JumpUsers.remove(&rest);
                    }
                    for (game_id, fg) in &mut NGGameingGroups {
                        process_ng(msgtx.clone(), tx2.clone(), sender.clone(), TotalUsers.clone(), game_id, fg);
                    }
                    for (game_id, fg) in &mut ARAMGameingGroups {
                        process_aram(msgtx.clone(), tx2.clone(), sender.clone(), TotalUsers.clone(), game_id, fg);
                    }
                    for (game_id, fg) in &mut RKGameingGroups {
                        process_rk(msgtx.clone(), tx2.clone(), sender.clone(), TotalUsers.clone(), game_id, fg);
                    }
                    for (game_id, fg) in &mut ATGameingGroups {
                        process_at(msgtx.clone(), tx2.clone(), sender.clone(), TotalUsers.clone(), game_id, fg);
                    }
                    let mut rm_swapping_list: Vec<String> = Vec::new();
                    for (user_id, heroSwappingData) in &mut HeroSwapping {
                        let time = heroSwappingData.borrow().time.clone();
                        if time == 0 {
                            msgtx.try_send(MqttMsg{topic:format!("game/{}/res/swap_hero", heroSwappingData.borrow().game_id),
                                msg: format!(r#"{{"id":"{}", "from":"{}", "action":"timeout"}}"#, heroSwappingData.borrow().id, heroSwappingData.borrow().from)})?;
                            rm_swapping_list.push(user_id.clone());
                        }
                        heroSwappingData.borrow_mut().time = time - 1;
                    }
                    for rm in rm_swapping_list {
                        HeroSwapping.remove(&rm);
                    }
                }

                recv(update5000ms) -> _ => {
                    // check isInGame
                    let mut inGameRm_list: Vec<String> = Vec::new();
                    let mut del_list: Vec<u64> = Vec::new();
                    let sql = format!("select * from Gaming where status='finished';",);
                    let qres = conn.query(sql.clone())?;
                    for row in qres {
                        let ea = row?.clone();
                        let gamingData = GamingData{
                            game: mysql::from_value_opt(ea.get("game").ok_or(Error::from(core::fmt::Error))?)?,
                            mode: mysql::from_value_opt(ea.get("mode").ok_or(Error::from(core::fmt::Error))?)?,
                            steam_id1: mysql::from_value_opt(ea.get("steam_id1").ok_or(Error::from(core::fmt::Error))?)?,
                            steam_id2: mysql::from_value_opt(ea.get("steam_id2").ok_or(Error::from(core::fmt::Error))?)?,
                            steam_id3: mysql::from_value_opt(ea.get("steam_id3").ok_or(Error::from(core::fmt::Error))?)?,
                            steam_id4: mysql::from_value_opt(ea.get("steam_id4").ok_or(Error::from(core::fmt::Error))?)?,
                            steam_id5: mysql::from_value_opt(ea.get("steam_id5").ok_or(Error::from(core::fmt::Error))?)?,
                            steam_id6: mysql::from_value_opt(ea.get("steam_id6").ok_or(Error::from(core::fmt::Error))?)?,
                            steam_id7: mysql::from_value_opt(ea.get("steam_id7").ok_or(Error::from(core::fmt::Error))?)?,
                            steam_id8: mysql::from_value_opt(ea.get("steam_id8").ok_or(Error::from(core::fmt::Error))?)?,
                            steam_id9: mysql::from_value_opt(ea.get("steam_id9").ok_or(Error::from(core::fmt::Error))?)?,
                            steam_id10: mysql::from_value_opt(ea.get("steam_id10").ok_or(Error::from(core::fmt::Error))?)?,
                            hero1: mysql::from_value_opt(ea.get("hero1").ok_or(Error::from(core::fmt::Error))?)?,
                            hero2: mysql::from_value_opt(ea.get("hero2").ok_or(Error::from(core::fmt::Error))?)?,
                            hero3: mysql::from_value_opt(ea.get("hero3").ok_or(Error::from(core::fmt::Error))?)?,
                            hero4: mysql::from_value_opt(ea.get("hero4").ok_or(Error::from(core::fmt::Error))?)?,
                            hero5: mysql::from_value_opt(ea.get("hero5").ok_or(Error::from(core::fmt::Error))?)?,
                            hero6: mysql::from_value_opt(ea.get("hero6").ok_or(Error::from(core::fmt::Error))?)?,
                            hero7: mysql::from_value_opt(ea.get("hero7").ok_or(Error::from(core::fmt::Error))?)?,
                            hero8: mysql::from_value_opt(ea.get("hero8").ok_or(Error::from(core::fmt::Error))?)?,
                            hero9: mysql::from_value_opt(ea.get("hero9").ok_or(Error::from(core::fmt::Error))?)?,
                            hero10: mysql::from_value_opt(ea.get("hero10").ok_or(Error::from(core::fmt::Error))?)?,
                            status: mysql::from_value_opt(ea.get("status").ok_or(Error::from(core::fmt::Error))?)?,
                            win_team: mysql::from_value_opt(ea.get("win_team").ok_or(Error::from(core::fmt::Error))?)?,
                        };
                        AbandonGames.insert(gamingData.game, true);
                        let mut gameOverData = GameOverData{
                            game: gamingData.game,
                            mode: gamingData.mode,
                            win: Vec::new(),
                            lose: Vec::new(),
                            time: 0,
                        };
                        if gamingData.win_team == 1 {
                            gameOverData.win.push(gamingData.steam_id1.clone());
                            gameOverData.win.push(gamingData.steam_id2.clone());
                            gameOverData.win.push(gamingData.steam_id3.clone());
                            gameOverData.win.push(gamingData.steam_id4.clone());
                            gameOverData.win.push(gamingData.steam_id5.clone());
                            gameOverData.lose.push(gamingData.steam_id6.clone());
                            gameOverData.lose.push(gamingData.steam_id7.clone());
                            gameOverData.lose.push(gamingData.steam_id8.clone());
                            gameOverData.lose.push(gamingData.steam_id9.clone());
                            gameOverData.lose.push(gamingData.steam_id10.clone());
                        }
                        if gamingData.win_team == 2 {
                            gameOverData.lose.push(gamingData.steam_id1.clone());
                            gameOverData.lose.push(gamingData.steam_id2.clone());
                            gameOverData.lose.push(gamingData.steam_id3.clone());
                            gameOverData.lose.push(gamingData.steam_id4.clone());
                            gameOverData.lose.push(gamingData.steam_id5.clone());
                            gameOverData.win.push(gamingData.steam_id6.clone());
                            gameOverData.win.push(gamingData.steam_id7.clone());
                            gameOverData.win.push(gamingData.steam_id8.clone());
                            gameOverData.win.push(gamingData.steam_id9.clone());
                            gameOverData.win.push(gamingData.steam_id10.clone());
                        }
                        del_list.push(gameOverData.game);
                        if let Some(fg) = NGGameingGroups.get(&gameOverData.game) {
                            gameOverData.time = fg.borrow().time;
                        }
                        if let Some(fg) = ARAMGameingGroups.get(&gameOverData.game) {
                            gameOverData.time = fg.borrow().time;
                        }
                        if let Some(fg) = RKGameingGroups.get(&gameOverData.game) {
                            gameOverData.time = fg.borrow().time;
                        }
                        if let Some(fg) = ATGameingGroups.get(&gameOverData.game) {
                            gameOverData.time = fg.borrow().time;
                        }
                        tx2.try_send(RoomEventData::GameOver(gameOverData));
                    }
                    for game in del_list {
                        let sql2 = format!(
                            "DELETE FROM Gaming where game={};",
                            game
                        );
                        let qres2 = conn.query(sql2.clone())?;
                        if let Some(fg) = NGGameingGroups.get(&game) {
                            for uid in &fg.borrow().user_names {
                                if let Some(u) = TotalUsers.get(uid) {
                                    u.borrow_mut().isLocked = false;
                                    u.borrow_mut().hero = "".to_string();
                                    u.borrow_mut().gid = 0;
                                }
                            }
                        }
                        NGGameingGroups.remove(&game);

                        if let Some(fg) = ARAMGameingGroups.get(&game) {
                            for uid in &fg.borrow().user_names {
                                if let Some(u) = TotalUsers.get(uid) {
                                    u.borrow_mut().isLocked = false;
                                    u.borrow_mut().hero = "".to_string();
                                    u.borrow_mut().gid = 0;
                                }
                            }
                        }
                        ARAMGameingGroups.remove(&game);

                        if let Some(fg) = RKGameingGroups.get(&game) {
                            for uid in &fg.borrow().user_names {
                                if let Some(u) = TotalUsers.get(uid) {
                                    u.borrow_mut().isLocked = false;
                                    u.borrow_mut().hero = "".to_string();
                                    u.borrow_mut().gid = 0;
                                }
                            }
                        }
                        RKGameingGroups.remove(&game);

                        if let Some(fg) = ATGameingGroups.get(&game) {
                            for uid in &fg.borrow().user_names {
                                if let Some(u) = TotalUsers.get(uid) {
                                    u.borrow_mut().isLocked = false;
                                    u.borrow_mut().hero = "".to_string();
                                    u.borrow_mut().gid = 0;
                                }
                            }
                        }
                        ATGameingGroups.remove(&game);
                    }
                    for rm in inGameRm_list {
                        InGameUsers.remove(&rm);
                    }
                    //println!("rx len: {}, tx len: {}", rx.len(), tx2.len());
                    LossSend.clear();
                    //get online and game count
                    let sql = format!(r#"select count(*) from user where status = 'online';"#);
                    let qres2: mysql::QueryResult = conn.query(sql.clone())?;
                    let mut online_cnt = 0;
                    let mut ng_cnt = 0;
                    let mut rk_cnt = 0;
                    let mut at_cnt = 0;
                    let mut aram_cnt = 0;
                    for row in qres2 {
                        let a = row?.clone();
                        online_cnt = mysql::from_value_opt(a.get("count(*)").ok_or(Error::from(core::fmt::Error))?)?;
                        break;
                    }
                    let mut timeout_list: Vec<u64> = vec![];
                    for (game_id, fg) in &mut NGGameingGroups {
                        fg.borrow_mut().time += 5;
                        if fg.borrow_mut().time >= 7200 {
                            let delSql = format!(r#"delete from Gaming where game='{}';"#, game_id);
                            conn.query(delSql.clone())?;
                            timeout_list.push(*game_id);
                        }
                        ng_cnt += 1;
                    }
                    for (game_id, fg) in &mut RKGameingGroups {
                        fg.borrow_mut().time += 5;
                        if fg.borrow_mut().time >= 7200 {
                            let delSql = format!(r#"delete from Gaming where game='{}';"#, game_id);
                            conn.query(delSql.clone())?;
                            timeout_list.push(*game_id);
                        }
                        rk_cnt += 1;
                    }
                    for (game_id, fg) in &mut ATGameingGroups {
                        fg.borrow_mut().time += 5;
                        if fg.borrow_mut().time >= 7200 {
                            let delSql = format!(r#"delete from Gaming where game='{}';"#, game_id);
                            conn.query(delSql.clone())?;
                            timeout_list.push(*game_id);
                        }
                        at_cnt += 1;
                    }
                    for (game_id, fg) in &mut ARAMGameingGroups {
                        fg.borrow_mut().time += 5;
                        if fg.borrow_mut().time >= 7200 {
                            let delSql = format!(r#"delete from Gaming where game='{}';"#, game_id);
                            conn.query(delSql.clone())?;
                            timeout_list.push(*game_id);
                        }
                        aram_cnt += 1;
                    }
                    for rm in timeout_list {
                        NGGameingGroups.remove(&rm);
                        RKGameingGroups.remove(&rm);
                        ATGameingGroups.remove(&rm);
                        ARAMGameingGroups.remove(&rm);
                    }
                    if isUpdateCount || online_cnt != current_online_cnt || current_ng_game_cnt != ng_cnt ||
                    current_rk_game_cnt != rk_cnt ||
                    current_at_game_cnt != at_cnt ||
                    current_aram_game_cnt != aram_cnt ||
                    current_ng_solo_queue_cnt != ng_solo_queue_cnt ||
                    current_ng_queue_cnt != ng_queue_cnt ||
                    current_rk_queue_cnt != rk_queue_cnt ||
                    current_at_queue_cnt != at_queue_cnt ||
                    current_aram_queue_cnt != aram_queue_cnt{
                        isUpdateCount = false;
                        current_online_cnt = online_cnt;
                        current_ng_game_cnt = ng_cnt;
                        current_rk_game_cnt = rk_cnt;
                        current_at_game_cnt = at_cnt;
                        current_aram_game_cnt = aram_cnt;
                        current_ng_solo_queue_cnt = ng_solo_queue_cnt;
                        current_ng_queue_cnt = ng_queue_cnt;
                        current_rk_queue_cnt = rk_queue_cnt;
                        current_at_queue_cnt = at_queue_cnt;
                        current_aram_game_cnt = aram_queue_cnt;
                        msgtx.try_send(MqttMsg{topic:format!("server/res/online_count"),
                            msg: format!(r#"{{"count":{}, "ngGameCount":{}, "rkGameCount":{}, "atGameCount":{}, "aramGameCount":{},"ngSoloQueueCount":{} ,"ngQueueCount":{}, "rkQueueCount":{}, "atQueueCount":{}, "aramQueueCount":{}}}"#, online_cnt, ng_cnt, rk_cnt, at_cnt, aram_cnt,ng_solo_queue_cnt, ng_queue_cnt, rk_queue_cnt, at_queue_cnt, aram_queue_cnt)})?;   
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
                                    // let u = get_user(&x.id, &TotalUsers);
                                    // if let Some(u) = u {
                                    //     let g = GameingGroups.get(&u.borrow().game_id);
                                    //     if let Some(g) = g {
                                    //         mqttmsg = MqttMsg{topic:format!("member/{}/res/reconnect", x.id),
                                    //             msg: format!(r#"{{"server":"172.104.78.55:{}"}}"#, g.borrow().game_port)};
                                    //         //msgtx.try_send(MqttMsg{topic:format!("member/{}/res/reconnect", x.id),
                                    //         //    msg: format!(r#"{{"server":"172.104.78.55:{}"}}"#, g.borrow().game_port)})?;
                                    //     }
                                    // }
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
                                    settlement_score(&win, &lose, &msgtx, &sender, &mut conn, x.mode, x.time);
                                    if let Some(fg) = GameingGroups.get(&x.game) {
                                        fg.borrow_mut().next_status();
                                    }
                                    msgtx.try_send(MqttMsg{topic:format!("game/{}/res/game_status", x.game),
                                        msg: format!(r#"{{"status":"finished", "game": {}}}"#,x.game)})?;
                                    isUpdateCount = true;
                                },
                                RoomEventData::GameInfo(x) => {
                                    // println!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                                    // for u in &x.users {
                                    //     let mut update_info: SqlGameInfoData = Default::default();
                                    //     update_info.game = x.game.clone();
                                    //     update_info.id = u.id.clone();
                                    //     update_info.hero = u.hero.clone();
                                    //     update_info.level = u.level.clone();
                                    //     for (i, e) in u.equ.iter().enumerate() {
                                    //         update_info.equ += e;
                                    //         if i < u.equ.len()-1 {
                                    //             update_info.equ += ", ";
                                    //         }
                                    //     }
                                    //     update_info.damage = u.damage.clone();
                                    //     update_info.take_damage = u.take_damage.clone();
                                    //     update_info.heal = u.heal.clone();
                                    //     update_info.kill = u.kill.clone();
                                    //     update_info.death = u.death.clone();
                                    //     update_info.assist = u.assist.clone();
                                    //     update_info.gift = u.gift.clone();
                                    //     sender.send(SqlData::UpdateGameInfo(update_info));
                                    // }
                                },
                                RoomEventData::StartGame(x) => {
                                    let u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        if !isBackup || (isBackup && isServerLive == false) {
                                            // AbandonGames.insert(x.game.clone(), true);
                                            let _ : () = redis_conn.set(format!("gid{}", x.game.clone()), serde_json::to_string(&x)?)?;
                                            // let _ : () = redis_conn.expire(format!("gid{}", x.game.clone()), 420)?;
                                            let gameRoomData = GameRoomData{
                                                master: x.id,
                                                isOpen: false,
                                            };
                                            GameingRoom.insert(x.game.clone(), Rc::new(RefCell::new(gameRoomData)));
                                            for player in &x.players {
                                                let u2 = get_user(player, &TotalUsers);
                                                if let Some(u2) = u2 {
                                                    ReadyGroups.remove(&u2.borrow().gid);
                                                    InGameUsers.insert(u2.borrow().id.clone(), u2.clone());
                                                    let _ : () = redis_conn.set(format!("g{}", u2.borrow().id.clone()), x.game.clone())?;
                                                    // let _ : () = redis_conn.expire(format!("g{}", u2.borrow().id.clone()), 420)?;
                                                    msgtx.try_send(MqttMsg{topic:format!("member/{}/res/check_in_game", u2.borrow().id.clone()),
                                                        msg: format!(r#"{{"msg":"in game"}}"#, )})?;
                                                }
                                            }
                                        }
                                    }
                                }
                                RoomEventData::GameStart(x) => {
                                    let u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        if !isBackup || (isBackup && isServerLive == false) {
                                            ReadyGroups.remove(&u.borrow().gid);
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
                                                tx2.try_send(RoomEventData::CancelQueue(CancelQueueData{action: "cancel_queue".to_string(), id: r.borrow().master.clone(), room: r.borrow().master.clone(), mode: r.borrow().mode.clone()}));
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
                                RoomEventData::ChooseHero(x) => {
                                    let mut isDup = false;
                                    let u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        if let Some(fg) = NGGameingGroups.get(&u.borrow().game_id) {
                                            if fg.borrow().user_names[0..5].contains(&x.id) {
                                                for uid in &fg.borrow().user_names[0..5] {
                                                    if *uid != x.id {
                                                        if let Some(u2) = TotalUsers.get(uid) {
                                                            if u2.borrow().hero.clone() == x.hero.clone() {
                                                                isDup = true;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            if fg.borrow().user_names[5..10].contains(&x.id) {
                                                for uid in &fg.borrow().user_names[5..10] {
                                                    if *uid != x.id {
                                                        if let Some(u2) = TotalUsers.get(uid) {
                                                            if u2.borrow().hero.clone() == x.hero.clone() {
                                                                isDup = true;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if !isDup {
                                            println!("hero : {}, line: {}", x.hero.clone(), line!());
                                            u.borrow_mut().hero = x.hero.clone();
                                        }
                                    }
                                    if !isDup {
                                        let _ : () = redis_conn.set(x.id.clone(), x.hero.clone())?;
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/ng_choose_hero", x.id.clone()),
                                            msg: format!(r#"{{"id":"{}", "hero":"{}"}}"#, x.id.clone(), x.hero.clone())};
                                    }
                                },
                                RoomEventData::LockedHero(x) => {
                                    let mut isDup = false;
                                    let u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        if let Some(fg) = NGGameingGroups.get(&u.borrow().game_id) {
                                            if fg.borrow().user_names[0..5].contains(&x.id) {
                                                for uid in &fg.borrow().user_names[0..5] {
                                                    if *uid != x.id {
                                                        if let Some(u2) = TotalUsers.get(uid) {
                                                            if u2.borrow().hero.clone() == x.hero.clone() {
                                                                isDup = true;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            if fg.borrow().user_names[5..10].contains(&x.id) {
                                                for uid in &fg.borrow().user_names[5..10] {
                                                    if *uid != x.id {
                                                        if let Some(u2) = TotalUsers.get(uid) {
                                                            if u2.borrow().hero.clone() == x.hero.clone() {
                                                                isDup = true;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if !isDup {
                                            u.borrow_mut().isLocked = true;
                                            u.borrow_mut().hero = x.hero.clone();
                                            mqttmsg = MqttMsg{topic:format!("game/{}/res/locked_hero", u.borrow().game_id),
                                                msg: format!(r#"{{"id":"{}", "hero":"{}"}}"#, u.borrow().id, x.hero.clone())};
                                        }
                                    }
                                },
                                RoomEventData::BanHero(x) => {
                                    let u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        u.borrow_mut().isLocked = true;
                                        u.borrow_mut().ban_hero = x.hero.clone();
                                        // if let Some(fg) = GameingGroups.get(&u.borrow().game_id) {
                                        //     fg.borrow_mut().lock_cnt += 1;
                                        // }
                                        println!("gid : {}, line: {}", u.borrow().game_id, line!());
                                        mqttmsg = MqttMsg{topic:format!("game/{}/res/ban_hero", u.borrow().game_id),
                                            msg: format!(r#"{{"id":"{}", "hero":"{}"}}"#, u.borrow().id, x.hero)};
                                        println!("send ban hero : {}, {}, line: {}", u.borrow().id, x.hero, line!());
                                    }
                                },
                                RoomEventData::GetHeros(x) => {
                                    let mut heros: Vec<HeroData> = Vec::new();
                                    for (name, hero)in &TotalHeros {
                                        let hero_tmp = HeroData {
                                            name: hero.borrow().name.clone(),
                                            enable: hero.borrow().enable,
                                        };
                                        heros.push(hero_tmp);
                                    }
                                    mqttmsg = MqttMsg{topic:format!("game/{}/res/get_heros", x.game_id),
                                        msg: format!(r#"{{"heros":"{}"}}"#, serde_json::to_string(&heros)?)};
                                },
                                RoomEventData::SwapHero(x) => {
                                    if let Some(u) = TotalUsers.get(&x.id) {
                                        if let Some(u2) = TotalUsers.get(&x.from) {
                                            if x.action == "response" {
                                                HeroSwapping.remove(&x.id);
                                                HeroSwapping.remove(&x.from);
                                                if x.is_accept {
                                                    if u.borrow().hero != "" && u2.borrow().hero != "" {
                                                        let hero_tmp = u.borrow().hero.clone();
                                                        u.borrow_mut().hero = u2.borrow().hero.clone();
                                                        u2.borrow_mut().hero = hero_tmp;
                                                        let msgtx2 = msgtx.clone();
                                                        let mqttmsg1 = MqttMsg{topic:format!("member/{}/res/ng_choose_hero", u.borrow().id.clone()),
                                                            msg: format!(r#"{{"id":"{}", "hero":"{}"}}"#, u.borrow().id.clone(), u.borrow().hero.clone())};
                                                        msgtx2.try_send(mqttmsg1);
                                                        let mqttmsg1 = MqttMsg{topic:format!("member/{}/res/ng_choose_hero", u2.borrow().id.clone()),
                                                            msg: format!(r#"{{"id":"{}", "hero":"{}"}}"#, u2.borrow().id.clone(), u2.borrow().hero.clone())};
                                                        msgtx2.try_send(mqttmsg1);
                                                        mqttmsg = MqttMsg{topic:format!("game/{}/res/swap_hero", x.game_id),
                                                            msg: format!(r#"{{"id":"{}", "from":"{}", "action":"success"}}"#, x.id, x.from)};
                                                    }
                                                } else {
                                                    mqttmsg = MqttMsg{topic:format!("game/{}/res/swap_hero", x.game_id),
                                                        msg: format!(r#"{{"id":"{}", "from":"{}", "action":"reject"}}"#, x.id, x.from)};
                                                }
                                            } else if x.action == "request" {
                                                if let Some(u) = HeroSwapping.get(&x.id) {
                                                    mqttmsg = MqttMsg{topic:format!("game/{}/res/swap_hero", x.game_id),
                                                        msg: format!(r#"{{"id":"{}", "from":"{}", "action":"busy"}}"#, x.id, x.from)};
                                                } else {
                                                    HeroSwapping.insert(x.id.clone(), Rc::new(RefCell::new(HeroSwappingData{
                                                        id: x.id.clone(),
                                                        from: x.from.clone(),
                                                        game_id: x.game_id.clone(),
                                                        time: SWAP_TIME,
                                                    })));
                                                    HeroSwapping.insert(x.from.clone(), Rc::new(RefCell::new(HeroSwappingData{
                                                        id: x.id.clone(),
                                                        from: x.from.clone(),
                                                        game_id: x.game_id.clone(),
                                                        time: SWAP_TIME,
                                                    })));
                                                    mqttmsg = MqttMsg{topic:format!("game/{}/res/swap_hero", x.game_id),
                                                        msg: format!(r#"{{"id":"{}", "from":"{}", "action":"request"}}"#, x.id, x.from)};
                                                }
                                            } else if x.action == "cancel" {
                                                HeroSwapping.remove(&x.id);
                                                HeroSwapping.remove(&x.from);
                                                mqttmsg = MqttMsg{topic:format!("game/{}/res/swap_hero", x.game_id),
                                                        msg: format!(r#"{{"id":"{}", "from":"{}", "action":"cancel"}}"#, x.id, x.from)};
                                            }
                                        }
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
                                            let old_room = TotalRoom.get(&j.borrow().rid);
                                            if let Some(old_room) = old_room {
                                                println!("rid : {}", j.borrow().rid.clone());
                                                tx2.try_send(RoomEventData::Leave(LeaveData{room: old_room.borrow().master.clone(), id: j.borrow().id.clone()}));
                                                println!("rid : {}", j.borrow().rid.clone());
                                            }
                                            let r = TotalRoom.get(&u.borrow().rid);
                                            if let Some(r) = r {
                                                println!("len : {}, line: {}", r.borrow().users.len(), line!());
                                                println!("ready : {}, line: {}", r.borrow().ready, line!());
                                                if r.borrow().mode == "rk"{
                                                    if r.borrow().ready == 0 && r.borrow().users.len() < TEAM_SIZE as usize {
                                                        r.borrow_mut().add_user(Rc::clone(j));
                                                        let m = r.borrow().master.clone();
                                                        r.borrow().publish_update(&msgtx, m)?;
                                                        let mut room = JoinRoomCell {
                                                            room: r.borrow().master.clone(),
                                                            mode: r.borrow().mode.clone(),
                                                            team: vec![],
                                                            msg: String::from("ok"),
                                                        };
                                                        for user in &r.borrow().users {
                                                            room.team.push(user.borrow().id.clone());
                                                        }
                                                        mqttmsg = MqttMsg{topic:format!("room/{}/res/join", x.join.clone()),
                                                            msg: serde_json::to_string(&room)?};
                                                        sendok = true;
                                                        let rid = u.borrow().rid;
                                                        u.borrow_mut().rid = rid;
                                                    }
                                                }else if r.borrow().ready == 0 && r.borrow().users.len() < TEAM_SIZE as usize {
                                                    r.borrow_mut().add_user(Rc::clone(j));
                                                    let m = r.borrow().master.clone();
                                                    r.borrow().publish_update(&msgtx, m)?;
                                                    let mut room = JoinRoomCell {
                                                        room: r.borrow().master.clone(),
                                                        mode: r.borrow().mode.clone(),
                                                        team: vec![],
                                                        msg: String::from("ok"),
                                                    };
                                                    for user in &r.borrow().users {
                                                        room.team.push(user.borrow().id.clone());
                                                    }
                                                    mqttmsg = MqttMsg{topic:format!("room/{}/res/join", x.join.clone()),
                                                        msg: serde_json::to_string(&room)?};
                                                    sendok = true;
                                                    let rid = u.borrow().rid;
                                                    u.borrow_mut().rid = rid;
                                                }
                                                if sendok {
                                                    QueueSender.send(QueueData::RemoveRoom(RemoveRoomData{rid: u.borrow().rid}));
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
                                RoomEventData::CheckRoom(x) => {
                                    if let Some(u) = TotalUsers.get(&x.id) {
                                        if let Some(r) = TotalRoom.get(&u.borrow().rid) {
                                            let m = r.borrow().master.clone();
                                            if r.borrow().users.len() > 0 {
                                                r.borrow().publish_update(&msgtx, m)?;
                                            }
                                        }
                                    }
                                },
                                RoomEventData::BuyGood(x) => {
                                    if let Some(u) = TotalUsers.get(&x.steamID) {
                                        let mut buyGoodResData: BuyGoodResData = BuyGoodResData::default();
                                        let mut sql = format!("select * from Goods where id = {};", x.id);
                                        let qres: mysql::QueryResult = conn.query(sql.clone())?;
                                        buyGoodResData.goodData = Default::default();
                                        buyGoodResData.steamID = x.steamID.clone();
                                        for row in qres {
                                            let ea = row?.clone();
                                            buyGoodResData.goodData.id = mysql::from_value(ea.get("id").unwrap());
                                            buyGoodResData.goodData.name = mysql::from_value(ea.get("name").unwrap());
                                            buyGoodResData.goodData.kind = mysql::from_value(ea.get("kind").unwrap());
                                            buyGoodResData.goodData.price = mysql::from_value(ea.get("price").unwrap());
                                            buyGoodResData.goodData.quantity = mysql::from_value(ea.get("quantity").unwrap());
                                            buyGoodResData.goodData.description = mysql::from_value(ea.get("description").unwrap());
                                            buyGoodResData.goodData.imageURL = mysql::from_value(ea.get("imageURL").unwrap());
                                        }
                                        buyGoodResData.balance = u.borrow().raindrop - buyGoodResData.goodData.price;
                                        if buyGoodResData.balance >= 0 {
                                            u.borrow_mut().raindrop = buyGoodResData.balance;
                                            let mut sql2 = format!("insert into Items (steam_id, name, kind, imageURL, description, sn, date) values ('{}', '{}', '{}', '{}', '{}', '', now())",
                                                    buyGoodResData.steamID, buyGoodResData.goodData.name, buyGoodResData.goodData.kind, buyGoodResData.goodData.imageURL, buyGoodResData.goodData.description);
                                            if buyGoodResData.goodData.kind == "序號" {
                                                sql = format!(
                                                    "select sn from Serial_numbers where good_id = {} and sold = false limit 1;",
                                                    x.id
                                                );
                                                let qres2: mysql::QueryResult = conn.query(sql.clone())?;
                                                for row in qres2 {
                                                    let ea = row?.clone();
                                                    buyGoodResData.sn = mysql::from_value(ea.get("sn").unwrap());
                                                }
                                                sql = format!(
                                                    "UPDATE Serial_numbers SET sold=true WHERE sn='{}'",
                                                    buyGoodResData.sn
                                                );
                                                conn.query(sql.clone())?;
                                                sql2 = format!("insert into Items (steam_id, name, kind, imageURL, description, sn, date) values ('{}', '{}', '{}', '{}', '{}', '{}', now())",
                                                    buyGoodResData.steamID, buyGoodResData.goodData.name, buyGoodResData.goodData.kind, buyGoodResData.goodData.imageURL, buyGoodResData.goodData.description, buyGoodResData.sn);
                                            }
                                            sql = format!("update user set raindrop = {} where id='{}'", buyGoodResData.balance, x.steamID);
                                            conn.query(sql.clone())?;
                                            conn.query(sql2.clone())?;
                                        }
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/buy_good", x.steamID.clone()),
                                            msg: format!(r#"{{"balance":{},"msg":"Ok"}}"#, buyGoodResData.balance)};
                                    }
                                },
                                RoomEventData::GetGood(x) => {
                                    if let Some(u) = TotalUsers.get(&x.steamID) {
                                        let sql = format!("select *, (select count(*) from Serial_numbers as b where a.id=b.good_id and b.sold = false) as count from Goods as a;");
                                        println!("{}", sql);
                                        let qres: mysql::QueryResult = conn.query(sql.clone())?;
                                        let mut goods: Vec<GoodData> = Vec::new();
                                        for row in qres {
                                            let ea = row?.clone();
                                            let mut goodData: GoodData = Default::default();
                                            goodData.id = mysql::from_value(ea.get("id").unwrap());
                                            goodData.name = mysql::from_value(ea.get("name").unwrap());
                                            goodData.kind = mysql::from_value(ea.get("kind").unwrap());
                                            goodData.price = mysql::from_value(ea.get("price").unwrap());
                                            goodData.quantity = mysql::from_value(ea.get("count").unwrap());
                                            goodData.description = mysql::from_value(ea.get("description").unwrap());
                                            goodData.imageURL = mysql::from_value(ea.get("imageURL").unwrap());
                                            goods.push(goodData);
                                        }
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/get_good", x.steamID.clone()),
                                            msg: format!("{}", serde_json::to_string(&goods)?)};
                                    }
                                },
                                RoomEventData::Reject(x) => {
                                    if TotalUsers.contains_key(&x.id) {
                                        mqttmsg = MqttMsg{topic:format!("room/{}/res/reject", x.room.clone()),
                                            msg: format!(r#"{{"room":"{}","id":"{}","msg":"reject"}}"#, x.room.clone(), x.id.clone())};
                                    }
                                },
                                RoomEventData::Jump(x) => {
                                    println!("jump : {:?}", x);
                                    if let Some(u) = TotalUsers.get(&x.id) {
                                        if let Some(gr) = ReadyGroups.get(&u.borrow().gid) {
                                            gr.borrow_mut().user_cancel(&x.id);
                                            for r in &gr.borrow().rooms {
                                                info!("r_rid: {}, u_rid: {}, u_uid: {}, queue_cnt: {}, line: {}", r.borrow().rid, u.borrow().rid, u.borrow().id.clone(), r.borrow().queue_cnt.clone(), line!());
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
                                                        avg_aram: r.borrow().avg_aram.clone(),
                                                        ready: 0,
                                                        notify: false,
                                                        queue_cnt: r.borrow().queue_cnt.clone(),
                                                        mode: r.borrow().mode.clone(),
                                                    };
                                                    QueueSender.send(QueueData::UpdateRoom(data));
                                                }
                                            }
                                            ReadyGroups.remove(&u.borrow().gid);
                                        }
                                    }
                                    if TotalUsers.contains_key(&x.id) {
                                        if !AbandonGames.contains_key(&x.game) {
                                            tx2.try_send(RoomEventData::BanUser(BanUserData{id: x.id.clone()}));
                                            AbandonGames.insert(x.game, true);
                                        }
                                        let mut rm_list: Vec<u64> = Vec::new();
                                        if let Some(fg) = NGGameingGroups.get(&x.game) {
                                            for uid in &fg.borrow().user_names {
                                                if let Some(u) = TotalUsers.get(uid) {
                                                    u.borrow_mut().isLocked = false;
                                                    u.borrow_mut().hero = "".to_string();
                                                    u.borrow_mut().ban_hero = "".to_string();
                                                    u.borrow_mut().gid = 0;
                                                }
                                            }
                                            rm_list.push(fg.borrow().game_id);
                                            mqttmsg = MqttMsg{topic:format!("game/{}/res/jump", x.game.clone()),
                                                msg: format!(r#"{{"id":"{}","mgs":"jump"}}"#, x.id.clone())};
                                        }
                                        if let Some(fg) = ARAMGameingGroups.get(&x.game) {
                                            for uid in &fg.borrow().user_names {
                                                if let Some(u) = TotalUsers.get(uid) {
                                                    u.borrow_mut().isLocked = false;
                                                    u.borrow_mut().hero = "".to_string();
                                                    u.borrow_mut().ban_hero = "".to_string();
                                                    u.borrow_mut().gid = 0;
                                                }
                                            }
                                            rm_list.push(fg.borrow().game_id);
                                            mqttmsg = MqttMsg{topic:format!("game/{}/res/jump", x.game.clone()),
                                                msg: format!(r#"{{"id":"{}","mgs":"jump"}}"#, x.id.clone())};
                                        }
                                        if let Some(fg) = RKGameingGroups.get(&x.game) {
                                            for uid in &fg.borrow().user_names {
                                                if let Some(u) = TotalUsers.get(uid) {
                                                    u.borrow_mut().isLocked = false;
                                                    u.borrow_mut().hero = "".to_string();
                                                    u.borrow_mut().ban_hero = "".to_string();
                                                    u.borrow_mut().gid = 0;
                                                }
                                            }
                                            rm_list.push(fg.borrow().game_id);
                                            mqttmsg = MqttMsg{topic:format!("game/{}/res/jump", x.game.clone()),
                                                msg: format!(r#"{{"id":"{}","mgs":"jump"}}"#, x.id.clone())};
                                        }
                                        if let Some(fg) = ATGameingGroups.get(&x.game) {
                                            for uid in &fg.borrow().user_names {
                                                if let Some(u) = TotalUsers.get(uid) {
                                                    u.borrow_mut().isLocked = false;
                                                    u.borrow_mut().hero = "".to_string();
                                                    u.borrow_mut().ban_hero = "".to_string();
                                                    u.borrow_mut().gid = 0;
                                                }
                                            }
                                            rm_list.push(fg.borrow().game_id);
                                            mqttmsg = MqttMsg{topic:format!("game/{}/res/jump", x.game.clone()),
                                                msg: format!(r#"{{"id":"{}","mgs":"jump"}}"#, x.id.clone())};
                                        }
                                        for rm in rm_list {
                                            let sql = format!(
                                                "DELETE FROM Gaming where game={} and status='wait';",
                                                rm
                                            );
                                            let qres = conn.query(sql.clone())?;
                                            NGGameingGroups.remove(&rm);
                                            ARAMGameingGroups.remove(&rm);
                                            RKGameingGroups.remove(&rm);
                                            ATGameingGroups.remove(&rm);
                                        }
                                    }
                                    isUpdateCount = true;
                                },
                                RoomEventData::BanUser(x) => {
                                    info!("ban user : {:?}, line: {}", x, line!());
                                    let reset_time = 7200;
                                    if !JumpUsers.contains_key(&x.id){
                                        let jumpCountData = JumpCountData{
                                            count: 1,
                                            time: reset_time,
                                        };
                                        JumpUsers.insert(x.id.clone(), Rc::new(RefCell::new(jumpCountData)));
                                        let mut new_restriced = RestrictedData {
                                            id: x.id.clone(),
                                            time: 60,
                                        };
                                        let r = Rc::new(RefCell::new(new_restriced));
                                        RestrictedUsers.insert(
                                            x.id.clone(),
                                            Rc::clone(&r),
                                        );
                                    } else {
                                        if let Some(j) = JumpUsers.get_mut(&x.id) {
                                            j.borrow_mut().count += 1;
                                            j.borrow_mut().time = reset_time;
                                            let mut new_restriced = RestrictedData {
                                                id: x.id.clone(),
                                                time: 60 * (j.borrow().count+1),
                                            };
                                            let r = Rc::new(RefCell::new(new_restriced));
                                            RestrictedUsers.insert(
                                                x.id.clone(),
                                                Rc::clone(&r),
                                            );
                                        }
                                    }
                                    tx2.try_send(RoomEventData::CheckRestriction(CheckRestrctionData{id: x.id.clone()}));
                                },
                                RoomEventData::Loading(x) => {
                                    if let Some(fg) = NGGameingGroups.get(&x.game) {
                                        if let Some(u) = TotalUsers.get(&x.id) {
                                            u.borrow_mut().isLoading = true;
                                            info!("id : {}, isLoading : {}, line : {}", x.id, u.borrow_mut().isLoading, line!());
                                        }
                                    }
                                    if let Some(fg) = ARAMGameingGroups.get(&x.game) {
                                        if let Some(u) = TotalUsers.get(&x.id) {
                                            u.borrow_mut().isLoading = true;
                                            info!("id : {}, isLoading : {}, line : {}", x.id, u.borrow_mut().isLoading, line!());
                                        }
                                    }
                                    if let Some(fg) = RKGameingGroups.get(&x.game) {
                                        if let Some(u) = TotalUsers.get(&x.id) {
                                            u.borrow_mut().isLoading = true;
                                            info!("id : {}, isLoading : {}, line : {}", x.id, u.borrow_mut().isLoading, line!());
                                        }
                                    }
                                    if let Some(fg) = ATGameingGroups.get(&x.game) {
                                        if let Some(u) = TotalUsers.get(&x.id) {
                                            u.borrow_mut().isLoading = true;
                                            info!("id : {}, isLoading : {}, line : {}", x.id, u.borrow_mut().isLoading, line!());
                                        }
                                    }
                                },
                                RoomEventData::CheckRestriction(x) => {
                                    let sql = format!(r#"select UNIX_TIMESTAMP(end) from BAN where id="{}";"#, x.id.clone());
                                    let qres: mysql::QueryResult = conn.query(sql.clone())?;
                                    let mut isBan = false;
                                    let mut rm_list: Vec<String> = Vec::new();
                                    for row in qres {
                                        isBan = true;
                                        let a = row?.clone();
                                        let dateTime: String = mysql::from_value_opt(a.get("UNIX_TIMESTAMP(end)").ok_or(Error::from(core::fmt::Error))?)?;
                                        let ndt = NaiveDateTime::parse_from_str(&dateTime, "%s")?;
                                        let dt = DateTime::<Utc>::from_utc(ndt, Utc);
                                        let now = Local::now();
                                        let mut time_result = dt.signed_duration_since(now).to_std();
                                        let mut duration = Duration::new(0, 0);
                                        match time_result {
                                            Ok(v) => {
                                                duration = v;
                                            },
                                            Err(e) => {
                                                rm_list.push(x.id.clone());
                                            }
                                        }
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/check_restriction", x.id.clone()),
                                            msg: format!(r#"{{"time":"{}"}}"#, duration.as_secs())};
                                    }
                                    for rm in rm_list {
                                        let sql2 = format!(r#"delete from BAN where id="{}";"#, rm);
                                        conn.query(sql2.clone())?;    
                                    }
                                    if !isBan {
                                        info!("{:?}, line: {}", RestrictedUsers, line!());
                                        let r = RestrictedUsers.get(&x.id);
                                        if let Some(r) = r {
                                            if let Some(u) = TotalUsers.get(&x.id) {
                                                tx2.try_send(RoomEventData::Leave(LeaveData{room: u.borrow().rid.to_string(), id: x.id.clone()}));
                                            }
                                            mqttmsg = MqttMsg{topic:format!("member/{}/res/check_restriction", x.id.clone()),
                                                msg: format!(r#"{{"time":"{}"}}"#, r.borrow().time)};
                                        }
                                    }
                                },
                                RoomEventData::CheckInGame(x) => {
                                    let inGame: std::result::Result<u64, redis::RedisError> = redis_conn.get(format!("g{}",x.id.clone()));
                                    match inGame {
                                       Ok(v) => {
                                        if let Some(gameRoom) = GameingRoom.get(&v) {
                                            mqttmsg = MqttMsg{topic:format!("member/{}/res/check_in_game", x.id.clone()),
                                                msg: format!(r#"{{"game": {}, "master":"{}", "isOpen": {}, "msg":"in game"}}"#, v, gameRoom.borrow().master, gameRoom.borrow().isOpen)};
                                        }
                                       },
                                       Err(e) => {
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/check_in_game", x.id.clone()),
                                            msg: format!(r#"{{"msg":"out of game"}}"#)};
                                       }
                                    }
                                },
                                RoomEventData::SetPassword(x) => {
                                    println!("{:?}, line: {}", x, line!());
                                    mqttmsg = MqttMsg{topic:format!("game/{}/res/password", x.game.clone()),
                                        msg: format!(r#"{{"password":"{}"}}"#, x.password.clone())};
                                },
                                RoomEventData::LeaveGame(x) => {
                                    let _: () = redis_conn.del(format!("g{}", x.id))?;
                                    if let Some(u) = TotalUsers.get(&x.id) {
                                        let jumpData = JumpData{
                                            id: u.borrow().id.clone(),
                                            game: x.game,
                                            msg: "jump".to_string(),
                                        };
                                        tx2.try_send(RoomEventData::Jump(jumpData));
                                    }
                                    // tx2.try_send(RoomEventData::CheckInGame(CheckInGameData{id: x.id}));
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
                                                        u.borrow_mut().hero = "".to_string();
                                                        u.borrow_mut().ban_hero = "".to_string();
                                                        u.borrow_mut().isLoading = false;
                                                        mqttmsg = MqttMsg{topic:format!("room/{}/res/pre_start", &x.room),
                                                            msg: format!(r#"{{"msg":"pre_start", "room":"{}", "id":"{}"}}"#, &x.room, u.borrow().id)};
                                                    } else {
                                                        println!("accept false!");
                                                        tx2.try_send(RoomEventData::BanUser(BanUserData{id: x.id.clone()}));
                                                        gr.borrow_mut().user_cancel(&x.id);
                                                        for r in &gr.borrow().rooms {
                                                            info!("r_rid: {}, u_rid: {}, u_uid: {}, queue_cnt: {}, line: {}", r.borrow().rid, u.borrow().rid, u.borrow().id.clone(), r.borrow().queue_cnt.clone(), line!());
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
                                                                    avg_aram: r.borrow().avg_aram.clone(),
                                                                    ready: 0,
                                                                    notify: false,
                                                                    queue_cnt: r.borrow().queue_cnt.clone(),
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
                                                    // error!("gid not found {}", gid);
                                                }
                                            }
                                        }
                                    }
                                },
                                RoomEventData::UpdateGame(x) => {
                                    println!("Update Game!");
                                    println!("{:?}", x);
                                    let mut fg: FightGame = Default::default();
                                    let mut g: FightGroup;
                                    for r in &x.rid {
                                        group_id += 1;
                                        g = Default::default();
                                        for rid in r {
                                            let room = TotalRoom.get(&rid);
                                            if let Some(room) = room {
                                                g.add_room(Rc::clone(&room));
                                            }
                                        }
                                        g.prestart();
                                        g.set_group_id(group_id);
                                        g.game_status = 1;
                                        ReadyGroups.insert(group_id, Rc::new(RefCell::new(g.clone())));
                                        let mut rg = ReadyGroups.get(&group_id);
                                        if let Some(rg) = rg {
                                            fg.teams.push(Rc::clone(rg));
                                        }
                                    }
                                    fg.update_names();
                                    for user_name in &fg.user_names {
                                        let preReadyData = PreReadyData {
                                            isReady: false
                                        };
                                    }
                                    for r in &fg.room_names {
                                        //thread::sleep_ms(100);
                                        if !isBackup || (isBackup && isServerLive == false) {
                                            msgtx.try_send(MqttMsg{topic:format!("room/{}/res/ready", r), msg: r#"{"msg":"ready"}"#.to_string()})?;
                                        }
                                    }

                                    game_id += 1;
                                    fg.set_game_id(game_id);
                                    fg.set_mode(x.mode);
                                    fg.ready_cnt = 0.0;
                                    info!("PreStartGroups : {:?}, line: {}", fg, line!());
                                    // info!("game_id: {}, game_mode: {}, game_player: {:?} line: {}", fg.game_id, fg.mode, fg.user_names, line!());
                                    PreStartGroups.insert(game_id, Rc::new(RefCell::new(fg)));

                                },
                                RoomEventData::UpdateRoomQueueCnt(x) => {
                                    if let Some(r) = TotalRoom.get(&x.rid) {
                                        r.borrow_mut().queue_cnt += 1;
                                    }
                                },
                                RoomEventData::StartGet(x) => {
                                    let mut u = TotalUsers.get(&x.id);
                                    if let Some(u) = u {
                                        u.borrow_mut().start_get = true;
                                        // println!("start get");
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
                                        }
                                        if hasRoom {
                                            let r = TotalRoom.get(&rid);
                                            if let Some(y) = r {
                                                y.borrow_mut().update_avg();
                                                println!("xmode : {}", x.mode.clone());
                                                y.borrow_mut().mode = x.mode.clone();
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
                                                    avg_aram: y.borrow().avg_aram.clone(),
                                                    ready: 0,
                                                    notify: false,
                                                    queue_cnt: 1,
                                                    mode: x.mode.clone(),
                                                };
                                                QueueSender.send(QueueData::UpdateRoom(data));
                                                success = true;
                                                if success {
                                                    mqttmsg = MqttMsg{topic:format!("room/{}/res/start_queue", y.borrow().master.clone()),
                                                        msg: format!(r#"{{"msg":"ok", "mode": "{}"}}"#, x.mode.clone())};
                                                } else {
                                                    mqttmsg = MqttMsg{topic:format!("room/{}/res/start_queue", y.borrow().master.clone()),
                                                        msg: format!(r#"{{"msg":"fail"}}"#)}
                                                }
                                            }
                                        }else{
                                            tx2.try_send(RoomEventData::Create(CreateRoomData{id: x.id.clone(), mode: x.mode.clone()}));
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
                                                avg_aram: y.borrow().avg_aram.clone(),
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
                                    isUpdateCount = true;
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
                                    tx2.try_send(RoomEventData::Logout(UserLogoutData{id: x.u.id.clone()}));
                                    let mut success = true;
                                    isUpdateCount = true;
                                    if TotalUsers.contains_key(&x.u.id) {
                                        let u2 = TotalUsers.get(&x.u.id);
                                        if let Some(u2) = u2 {
                                            let sql = format!(r#"SELECT hero FROM Hero_usage WHERE steam_id='{}' ORDER BY choose_count DESC LIMIT 1;"#, u2.borrow().id.clone());
                                            let qres2: mysql::QueryResult = conn.query(sql.clone())?;
                                            let mut hero = "".to_string();
                                            for row in qres2 {
                                                let a = row?.clone();
                                                hero = mysql::from_value_opt(a.get("hero").ok_or(Error::from(core::fmt::Error))?)?;
                                                break;
                                            }
                                            let sql = format!(r#"SELECT * FROM user where id='{}';"#, u2.borrow().id.clone());
                                            let qres2: mysql::QueryResult = conn.query(sql.clone())?;
                                            for row in qres2 {
                                                let a = row?.clone();
                                                u2.borrow_mut().ng = mysql::from_value_opt(a.get("ng").ok_or(Error::from(core::fmt::Error))?)?;
                                                u2.borrow_mut().rk = mysql::from_value_opt(a.get("rk").ok_or(Error::from(core::fmt::Error))?)?;
                                                u2.borrow_mut().at = mysql::from_value_opt(a.get("at").ok_or(Error::from(core::fmt::Error))?)?;
                                                u2.borrow_mut().aram = mysql::from_value_opt(a.get("aram").ok_or(Error::from(core::fmt::Error))?)?;
                                                u2.borrow_mut().raindrop = mysql::from_value_opt(a.get("raindrop").ok_or(Error::from(core::fmt::Error))?)?;
                                                u2.borrow_mut().phone = mysql::from_value_opt(a.get("phone").ok_or(Error::from(core::fmt::Error))?)?;
                                                u2.borrow_mut().email = mysql::from_value_opt(a.get("email").ok_or(Error::from(core::fmt::Error))?)?;
                                            }
                                            u2.borrow_mut().online = true;
                                            mqttmsg = MqttMsg{topic:format!("member/{}/res/login", u2.borrow().id.clone()),
                                                msg: format!(r#"{{"msg":"ok", "ng":{}, "rk":{}, "at":{},"aram":{}, "raindrop": {}, "hero":"{}", "phone":"{}", "email":"{}"}}"#, u2.borrow().ng, u2.borrow().rk, u2.borrow().at, u2.borrow().aram, u2.borrow().raindrop, hero, u2.borrow().phone, u2.borrow().email)};
                                        }
                                    }
                                    else {
                                        TotalUsers.insert(x.u.id.clone(), Rc::new(RefCell::new(x.u.clone())));
                                        sender.send(SqlData::Login(SqlLoginData {id: x.dataid.clone(), name: name.clone()}));
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/login", x.u.id.clone()),
                                            msg: format!(r#"{{"msg":"ok", "ng":{}, "rk":{}, "at":{}, "aram":{}, "raindrop": {}, "hero":"", "phone":"", "email":""}}"#, 1200, 1200, 1200, 0, 1200)};
                                    }
                                },
                                RoomEventData::Logout(x) => {
                                    let mut success = false;
                                    isUpdateCount = true;
                                    let u = TotalUsers.get(&x.id);
                                    let u2 = get_user(&x.id, &TotalUsers);
                                    if let Some(u2) = u2 {
                                        u2.borrow_mut().online = false;
                                    }
                                    if let Some(u) = u {
                                        let ng_game_id = get_ng_game_id_by_id(&u.borrow().id, &NGGameingGroups, &TotalUsers);
                                        let aram_game_id = get_aram_game_id_by_id(&u.borrow().id, &ARAMGameingGroups, &TotalUsers);
                                        let rk_game_id = get_rk_game_id_by_id(&u.borrow().id, &RKGameingGroups, &TotalUsers);
                                        let at_game_id = get_at_game_id_by_id(&u.borrow().id, &ATGameingGroups, &TotalUsers);
                                        if ng_game_id > 0 {
                                            println!("ng game id : {}", ng_game_id);
                                            let jumpData = JumpData{
                                                id: u.borrow().id.clone(),
                                                game: ng_game_id,
                                                msg: "jump".to_string(),
                                            };
                                            tx2.try_send(RoomEventData::Jump(jumpData));
                                        }
                                        if aram_game_id > 0 {
                                            println!("aram game id : {}", aram_game_id);
                                            let jumpData = JumpData{
                                                id: u.borrow().id.clone(),
                                                game: aram_game_id,
                                                msg: "jump".to_string(),
                                            };
                                            tx2.try_send(RoomEventData::Jump(jumpData));
                                        }
                                        if rk_game_id > 0 {
                                            println!("rk game id : {}", rk_game_id);
                                            let jumpData = JumpData{
                                                id: u.borrow().id.clone(),
                                                game: rk_game_id,
                                                msg: "jump".to_string(),
                                            };
                                            tx2.try_send(RoomEventData::Jump(jumpData));
                                        }
                                        if at_game_id > 0 {
                                            println!("at game id : {}", at_game_id);
                                            let jumpData = JumpData{
                                                id: u.borrow().id.clone(),
                                                game: at_game_id,
                                                msg: "jump".to_string(),
                                            };
                                            tx2.try_send(RoomEventData::Jump(jumpData));
                                        }
                                        let mut is_null = false;
                                        let gid = u.borrow().gid;
                                        let rid = u.borrow().rid;
                                        let r = TotalRoom.get(&u.borrow().rid);
                                        if let Some(r) = r {
                                            tx2.try_send(RoomEventData::PreStart(PreStartData{room: u.borrow().rid.to_string(), id: u.borrow().id.clone(), accept: false}));
                                            let m = r.borrow().master.clone();
                                            r.borrow_mut().rm_user(&x.id);
                                            if r.borrow().users.len() > 0 {
                                                r.borrow().publish_update(&msgtx, m)?;
                                            }
                                            else {
                                                is_null = true;
                                            }
                                            QueueSender.send(QueueData::RemoveRoom(RemoveRoomData{rid: rid}));
                                            //mqttmsg = MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master),
                                            //    msg: format!(r#"{{"msg":"ok"}}"#)};
                                            if !isBackup || (isBackup && isServerLive == false) {
                                                msgtx.try_send(MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master),
                                                    msg: format!(r#"{{"msg":"ok"}}"#)})?;
                                                LossSend.push(MqttMsg{topic:format!("room/{}/res/cancel_queue", r.borrow().master),
                                                    msg: format!(r#"{{"msg":"ok"}}"#)});
                                            }
                                            //mqttmsg = MqttMsg{topic:format!("room/{}/res/leave", x.id),
                                            //    msg: format!(r#"{{"msg":"ok"}}"#)};
                                            if !isBackup || (isBackup && isServerLive == false) {
                                                msgtx.try_send(MqttMsg{topic:format!("room/{}/res/leave", x.id),
                                                    msg: format!(r#"{{"id":"{}","msg":"ok"}}"#, x.id.clone())})?;
                                                LossSend.push(MqttMsg{topic:format!("room/{}/res/leave", x.id),
                                                    msg: format!(r#"{{"id":"{}","msg":"ok"}}"#, x.id.clone())});
                                            }
                                        }
                                        if gid != 0 {
                                            // let g = ReadyGroups.get(&gid);
                                            // if let Some(gr) = g {
                                            //     gr.borrow_mut().user_cancel(&x.id);
                                            //     ReadyGroups.remove(&gid);
                                            //     let r = TotalRoom.get(&rid);
                                            //     //println!("Totalroom rid: {}", &u.borrow().rid);
                                            //     //TotalRoom.remove(&rid);
                                            // }
                                            let mut rm_list: Vec<u64> = Vec::new();
                                            if let Some(fg) = GameingGroups.get(&gid) {
                                                for t in &fg.borrow().teams {
                                                    for r in &t.borrow().rooms {
                                                        for user in &r.borrow().users {
                                                            user.borrow_mut().isLocked = false;
                                                            user.borrow_mut().gid = 0;
                                                        }
                                                    }
                                                }
                                                rm_list.push(fg.borrow().game_id);
                                            }
                                            for rm in rm_list {
                                                GameingGroups.remove(&rm);
                                            }
                                        }
                                        //println!("{:?}", TotalRoom);
                                        //TotalRoom.remove(&u.borrow().rid);
                                        success = true;
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
                                            msg: format!(r#"{{"id":"{}","msg":"ok"}}"#, x.id.clone())};
                                        //msgtx.try_send(MqttMsg{topic:format!("member/{}/res/logout", x.id.clone()),
                                        //    msg: format!(r#"{{"msg":"ok"}}"#)})?;
                                    } else {
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/logout", x.id.clone()),
                                            msg: format!(r#"{{"id":"{}","msg":"fail"}}"#, x.id.clone())};
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
                                        println!("rid: {}, line: {}", &room_id, line!());
                                        let mut new_room = RoomData {
                                            rid: room_id,
                                            users: vec![],
                                            master: x.id.clone(),
                                            last_master: "".to_owned(),
                                            avg_ng: 0,
                                            avg_rk: 0,
                                            avg_at: 0,
                                            avg_aram: 0,
                                            ready: 0,
                                            queue_cnt: 1,
                                            mode: x.mode.clone(),
                                        };
                                        let mut u = TotalUsers.get(&x.id);
                                        if let Some(u) = u {
                                            if u.borrow().rid != 0 {
                                                let r = TotalRoom.get(&u.borrow().rid);
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
                                                }
                                                if is_null {
                                                    TotalRoom.remove(&u.borrow().rid);
                                                    QueueSender.send(QueueData::RemoveRoom(RemoveRoomData{rid: u.borrow().rid}));
                                                }
                                                u.borrow_mut().rid = 0;
                                            }
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
                                            msg: format!(r#"{{"msg":"ok", "room":"{}"}}"#, room_id)};
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
                                                bForceCloseRkState = true;
                                                rkState = "close";
                                            } else if x.msg == "open" {
                                                bForceCloseRkState = false;
                                                rkState = "open";
                                            }
                                        } else if (x.mode == "ng") {
                                            if x.msg == "close" {
                                                bForceCloseNgState = true;
                                                ngState = "close";
                                            } else if x.msg == "open" {
                                                bForceCloseNgState = false;
                                                ngState = "open";
                                            }
                                        } else if (x.mode == "at") {
                                            if x.msg == "close" {
                                                bForceCloseAtState = true;
                                                atState = "close";
                                            } else if x.msg == "open" {
                                                bForceCloseAtState = false;
                                                atState = "open";
                                            }
                                        }
                                        mqttmsg = MqttMsg{topic:format!("server/res/check_state"),
                                            msg: format!(r#"{{"ng":"{}", "rk":"{}", "at":"{}", "aram":"{}"}}"#, ngState, rkState, atState, aramState)};
                                    }
                                },
                                RoomEventData::CheckState(x) => {
                                    if let Some(u) = TotalUsers.get(&x.id) {
                                        mqttmsg = MqttMsg{topic:format!("member/{}/res/check_state", x.id),
                                                msg: format!(r#"{{"ng":"{}", "rk":"{}", "at":"{}", "aram":"{}"}}"#, ngState, rkState, atState, aramState)};
                                    }
                                },
                                RoomEventData::Free() => {
                                    let sql = format!(
                                        "select * from Free order by week DESC limit 1;",
                                    );
                                    let qres = conn.query(sql.clone())?;
                                    let mut hero1 = "".to_string();
                                    let mut hero2 = "".to_string();
                                    let mut hero3 = "".to_string();
                                    let mut hero4 = "".to_string();
                                    let mut hero5 = "".to_string();
                                    let mut hero6 = "".to_string();
                                    let mut hero7 = "".to_string();
                                    let mut hero8 = "".to_string();
                                    let mut hero9 = "".to_string();
                                    let mut hero10 = "".to_string();
                                    let mut hero11 = "".to_string();
                                    let mut hero12 = "".to_string();
                                    let mut hero13 = "".to_string();
                                    let mut hero14 = "".to_string();
                                    let mut hero15 = "".to_string();
                                    let mut hero16 = "".to_string();
                                    let mut hero17 = "".to_string();
                                    let mut hero18 = "".to_string();
                                    let mut hero19 = "".to_string();
                                    let mut hero20 = "".to_string();
                                    for row in qres {
                                        let a = row?.clone();
                                        hero1 = mysql::from_value_opt(a.get("hero1").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero2 = mysql::from_value_opt(a.get("hero2").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero3 = mysql::from_value_opt(a.get("hero3").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero4 = mysql::from_value_opt(a.get("hero4").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero5 = mysql::from_value_opt(a.get("hero5").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero6 = mysql::from_value_opt(a.get("hero6").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero7 = mysql::from_value_opt(a.get("hero7").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero8 = mysql::from_value_opt(a.get("hero8").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero9 = mysql::from_value_opt(a.get("hero9").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero10 = mysql::from_value_opt(a.get("hero10").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero11 = mysql::from_value_opt(a.get("hero11").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero12 = mysql::from_value_opt(a.get("hero12").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero13 = mysql::from_value_opt(a.get("hero13").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero14 = mysql::from_value_opt(a.get("hero14").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero15 = mysql::from_value_opt(a.get("hero15").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero16 = mysql::from_value_opt(a.get("hero16").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero17 = mysql::from_value_opt(a.get("hero17").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero18 = mysql::from_value_opt(a.get("hero18").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero19 = mysql::from_value_opt(a.get("hero19").ok_or(Error::from(core::fmt::Error))?)?;
                                        hero20 = mysql::from_value_opt(a.get("hero20").ok_or(Error::from(core::fmt::Error))?)?;
                                        break;
                                    }
                                    mqttmsg = MqttMsg{topic:format!("server/res/free"),
                                            msg: format!(r#"{{"hero1":"{}", "hero2":"{}", "hero3":"{}"
                                            , "hero4":"{}", "hero5":"{}", "hero6":"{}", "hero7":"{}", "hero8":"{}", "hero9":"{}"
                                            , "hero10":"{}", "hero11":"{}", "hero12":"{}", "hero13":"{}", "hero14":"{}", "hero15":"{}"
                                            , "hero16":"{}", "hero17":"{}", "hero18":"{}", "hero19":"{}", "hero20":"{}"}}"#
                                            , hero1, hero2, hero3, hero4, hero5, hero6, hero7, hero8, hero9, hero10
                                            , hero11, hero12, hero13, hero14, hero15, hero16, hero17, hero18, hero19, hero20)};
                                },
                                RoomEventData::UpdateQueue(x) => {
                                    ng_solo_queue_cnt = x.ng_solo;
                                    ng_queue_cnt = x.ng;
                                    rk_queue_cnt = x.rk;
                                    at_queue_cnt = x.at;
                                    aram_queue_cnt = x.aram;
                                },
                                RoomEventData::SystemBan(x) => {
                                    if (x.password == "HibikiHibiki") {
                                        let sql = format!(
                                            "replace BAN values ('{}', date_add(now(), interval {} second));",
                                            x.id.clone(), x.time.clone()
                                        );
                                        conn.query(sql.clone())?;
                                    }
                                },
                                RoomEventData::UpdateHeros(x) => {
                                    if (x.password == "HibikiHibiki") {
                                        let sql = format!(
                                            "replace hero_list values ('{}', {});",
                                            x.name.clone(), x.enable.clone()
                                        );
                                        if let Some(hero) = TotalHeros.get(&x.name) {
                                            hero.borrow_mut().enable = x.enable;
                                        }
                                        conn.query(sql.clone())?;
                                    }
                                }
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

pub fn check_room(id: String, v: Value, sender: Sender<RoomEventData>) -> std::result::Result<(), Error> {
    let data: CheckRoomData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::CheckRoom(data));
    Ok(())
}

pub fn buy_good(id: String, v: Value, sender: Sender<RoomEventData>) -> std::result::Result<(), Error> {
    let data: BuyGoodData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::BuyGood(data));
    Ok(())
}

pub fn get_good(id: String, v: Value, sender: Sender<RoomEventData>) -> std::result::Result<(), Error> {
    let data: GetGoodData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::GetGood(data));
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
    sender.try_send(RoomEventData::ChooseHero(data));
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

pub fn get_heros(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: GetHerosData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::GetHeros(data));
    Ok(())
}

pub fn swap_hero(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: SwapHeroData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::SwapHero(data));
    Ok(())
}

pub fn lock_hero(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: UserNGHeroData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::LockedHero(data));
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

pub fn set_password(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: SetPasswordData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::SetPassword(data));
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

pub fn free(v: Value, sender: Sender<RoomEventData>) -> std::result::Result<(), Error> {
    sender.try_send(RoomEventData::Free());
    Ok(())
}

pub fn systemBan(v: Value, sender: Sender<RoomEventData>) -> std::result::Result<(), Error> {
    let data: SystemBanData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::SystemBan(data));
    Ok(())
}

pub fn updateHeros(v: Value, sender: Sender<RoomEventData>) -> std::result::Result<(), Error> {
    let data: UpdateHerosData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::UpdateHeros(data));
    Ok(())
}

pub fn loading(
    id: String,
    v: Value,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let data: LoadingData = serde_json::from_value(v)?;
    sender.try_send(RoomEventData::Loading(data));
    Ok(())
}
